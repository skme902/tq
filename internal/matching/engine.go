// Adapted from temporalio/temporal v1.32.1 af08a5e5, service/matching/matching_engine.go
// (getTaskQueuePartitionManager, unloadTaskQueuePartition*, pollTask and the
// pollActivityTaskQueue start/finish loop). Namespace registry, history client, versioning,
// queries/nexus, worker shutdown tracking and describe APIs are removed.
// Copyright (c) 2025 Temporal Technologies Inc. MIT License; see UPSTREAM_LICENSE and NOTICE.

package matching

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/skme902/tq/clock"
	"github.com/skme902/tq/internal/backoff"
	"github.com/skme902/tq/metrics"
	"github.com/skme902/tq/store"
)

const (
	// For non-forwarded polls, subtract up to this fraction of the long poll interval as jitter.
	forwardedPollJitterRatio = 1.0 / 6
	// common.CriticalLongPollTimeout
	forwardedPollMinInterval = 10 * time.Second
	// how many times AddTask retries on a queue that unloaded underneath it (library addition)
	addTaskMaxAttempts = 3
)

// Options configures an Engine.
type Options struct {
	Config            Config
	Store             store.Store
	TimeSource        clock.TimeSource
	Logger            *slog.Logger
	Metrics           metrics.Handler
	Starter           Starter
	Validator         Validator
	QueueConfigSource QueueConfigSource
	Router            PartitionRouter
}

// Engine owns the loaded queues (upstream matchingEngineImpl).
type Engine struct {
	config         Config
	store          store.Store
	timeSource     clock.TimeSource
	logger         *slog.Logger
	metricsHandler metrics.Handler
	starter        Starter
	validator      Validator
	configSource   QueueConfigSource
	router         PartitionRouter
	// deadLetters is store as a DeadLetterStore, or nil if it does not implement one (then
	// MaxAttempts is ignored and the dead-letter APIs fail).
	deadLetters store.DeadLetterStore

	ctx    context.Context
	cancel context.CancelFunc

	partitionsLock sync.RWMutex
	partitions     map[store.QueueKey]*taskQueuePartitionManagerImpl
	stopped        bool

	// leases holds every outstanding Delivery (backlog and sync-matched). leasesClosed is set
	// under leasesLock when Shutdown decides the drain is over and when Stop snapshots leases;
	// newDelivery refuses a lease after that.
	leasesLock   sync.Mutex
	leases       map[*Delivery]struct{}
	leasesClosed bool
	// leasesDrained is allocated by Shutdown and closed (under leasesLock) when no leases
	// remain and no PollTask is in flight. newDelivery does not touch it.
	leasesDrained chan struct{}
	// activePolls counts PollTask calls that passed the draining fast-path and have not
	// returned. Shutdown waits for it so a match cannot commit after the drain decision.
	activePolls int

	// draining is set by Shutdown; drainCtx is cancelled at the same time, which ends every
	// waiting poll (library addition, see Shutdown).
	draining    atomic.Bool
	drainCtx    context.Context
	drainCancel context.CancelFunc

	// shutdownOnce single-flights the Stop that ends a drain. The caller that stops records
	// shutdownErr (nil when the drain finished) and closes shutdownDone. shutdownClaimed is
	// set before that Stop so a later Shutdown can tell a direct Stop (return nil) from an
	// in-progress drain.
	shutdownOnce    sync.Once
	shutdownClaimed atomic.Bool
	shutdownErr     error
	shutdownDone    chan struct{}
	// drainBegin publishes the drain start (draining, drainStart, the engine_draining gauge)
	// once; later callers block in it until the first has published, so all of it happens
	// before any caller reaches completeShutdown.
	drainBegin sync.Once
	// drainStart is when the first Shutdown began (UnixNano), for engine_drain_duration.
	drainStart atomic.Int64
	// gaveUp is the first give-up decision (a caller's ctx ended with work in flight), under
	// leasesLock. Every other waiter adopts it, so all callers share one result.
	gaveUp          bool
	gaveUpErr       error
	gaveUpRemaining int
	// stopDone is closed when Stop has finished respooling and unloading; a concurrent or
	// later Stop, and a Shutdown that finds the engine stopped, wait on it.
	stopDone chan struct{}

	// queueTags holds the queues (namespace, name) granted a real taskqueue metrics tag,
	// at most config.MetricsQueueTagLimit of them.
	queueTagsLock sync.Mutex
	queueTags     map[[2]string]struct{}

	// testHookStopAfterSyncLeases, if set, runs in Stop after sync leases are re-spooled and
	// before queues unload (tests only).
	testHookStopAfterSyncLeases func()
	// testHookBeforeDrainSelect, if set, runs in Shutdown after a wait is armed and before
	// selecting on it (tests only).
	testHookBeforeDrainSelect func()
	// testHookBeforeRespool, if set, runs at the start of respoolSyncTaskLocked.
	testHookBeforeRespool func(shuttingDown bool)
}

// NewEngine validates options and returns an engine. Queues load lazily on first use.
func NewEngine(opts Options) (*Engine, error) {
	if opts.Store == nil {
		return nil, errors.New("matching: Options.Store is required")
	}
	if opts.TimeSource == nil {
		opts.TimeSource = clock.NewRealTimeSource()
	}
	if opts.Logger == nil {
		opts.Logger = slog.New(slog.DiscardHandler)
	}
	if opts.Metrics == nil {
		opts.Metrics = metrics.Noop
	}
	if opts.Router == nil {
		opts.Router = NoopRouter
	}
	config := opts.Config.WithDefaults()
	if err := config.validate(); err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(context.Background())
	drainCtx, drainCancel := context.WithCancel(context.Background())
	dls, _ := opts.Store.(store.DeadLetterStore)
	if dls == nil && config.MaxAttempts > 0 {
		opts.Logger.Warn("Config.MaxAttempts is ignored: the store does not implement store.DeadLetterStore")
	}
	return &Engine{
		deadLetters:    dls,
		drainCtx:       drainCtx,
		drainCancel:    drainCancel,
		config:         config,
		store:          opts.Store,
		timeSource:     opts.TimeSource,
		logger:         opts.Logger,
		metricsHandler: opts.Metrics,
		starter:        opts.Starter,
		validator:      opts.Validator,
		configSource:   opts.QueueConfigSource,
		router:         opts.Router,
		ctx:            ctx,
		cancel:         cancel,
		partitions:     make(map[store.QueueKey]*taskQueuePartitionManagerImpl),
		leases:         make(map[*Delivery]struct{}),
		shutdownDone:   make(chan struct{}),
		stopDone:       make(chan struct{}),
		queueTags:      make(map[[2]string]struct{}),
	}, nil
}

// queueMetricsHandler returns the handler for one queue's metrics, tagged namespace, taskqueue
// and partition (upstream GetPerTaskQueuePartitionIDScope), and whether the taskqueue tag is the
// real name. Past MetricsQueueTagLimit distinct queues, or with a negative limit, the taskqueue
// tag is "__omitted__" (upstream: metrics.breakdownByTaskQueue=false).
func (e *Engine) queueMetricsHandler(q store.QueueKey) (metrics.Handler, bool) {
	breakdown := e.grantQueueTag(q)
	name := metrics.OmittedValue
	if breakdown {
		name = q.Name
	}
	ns := q.Namespace
	if ns == "" {
		ns = "_unknown_" // upstream NamespaceTag
	}
	return metrics.WithTags(e.metricsHandler,
		metrics.Tag{Key: metrics.TagNamespace, Value: ns},
		metrics.Tag{Key: metrics.TagTaskQueue, Value: name},
		metrics.Tag{Key: metrics.TagPartition, Value: strconv.Itoa(q.Partition)},
	), breakdown
}

// newTaskTracker is upstream's: 5 s buckets over a 30 s window.
func (e *Engine) newTaskTracker() *taskTracker {
	return newTaskTracker(e.timeSource, 5*time.Second, 30*time.Second)
}

func (e *Engine) grantQueueTag(q store.QueueKey) bool {
	limit := e.config.MetricsQueueTagLimit
	if limit < 0 {
		return false
	}
	k := [2]string{q.Namespace, q.Name}
	e.queueTagsLock.Lock()
	defer e.queueTagsLock.Unlock()
	if _, ok := e.queueTags[k]; ok {
		return true
	}
	if len(e.queueTags) >= limit {
		return false
	}
	e.queueTags[k] = struct{}{}
	return true
}

// Config returns the effective configuration.
func (e *Engine) Config() Config { return e.config }

// Shutdown drains the engine, then stops it (library addition; upstream relies on fx
// lifecycle and history retries). Draining means:
//   - PollTask returns ErrEngineDraining, and polls already waiting return it at once, so no
//     new leases are handed out and AddTask can no longer sync-match: it writes to the backlog.
//   - AddTask, Ack, Nack and ExtendLease keep working. A nacked or expired backlog lease goes
//     back to the backlog; a nacked or expired sync-matched lease is spooled.
//
// Shutdown waits until every outstanding lease is settled and every PollTask that passed the
// draining check has returned, or until ctx is done, then calls Stop. It returns nil after a
// finished drain, including when ctx is already done but nothing is left in flight, or ctx's
// error if it gave up with work still in flight. Stop then ends any leases still outstanding
// (backlog tasks are redelivered after the next load, sync-matched ones are spooled).
// Concurrent callers share that result: the one that calls Stop records it, and the others
// return the same error once the engine has stopped. A caller whose own ctx ends first is the
// one that stops. Shutdown after a direct Stop returns nil.
func (e *Engine) Shutdown(ctx context.Context) error {
	if done, err := e.finishedShutdown(); done {
		return err
	}
	e.drainBegin.Do(func() {
		e.drainStart.Store(e.timeSource.Now().UnixNano())
		e.draining.Store(true)
		e.metricsHandler.Gauge(metrics.EngineDraining, 1)
		e.logger.Info("draining", "leases", e.outstandingLeaseCount())
		e.drainCancel()
	})

	remaining, err := e.waitDrain(ctx)
	return e.completeShutdown(err, remaining)
}

// finishedShutdown reports the result of a Shutdown that has already stopped the engine.
// A direct Stop, with no Shutdown in progress, reports nil. done is false when the caller
// should run the drain.
func (e *Engine) finishedShutdown() (bool, error) {
	select {
	case <-e.shutdownDone:
		return true, e.shutdownErr
	default:
	}
	e.partitionsLock.RLock()
	stopped := e.stopped
	e.partitionsLock.RUnlock()
	if !stopped {
		return false, nil
	}
	<-e.stopDone // a Stop in progress may still be respooling
	if e.shutdownClaimed.Load() || e.draining.Load() {
		// A Shutdown is in progress (it may be between its drain decision and Stop when a
		// direct Stop ran): share its result.
		<-e.shutdownDone
		return true, e.shutdownErr
	}
	select {
	case <-e.shutdownDone:
		return true, e.shutdownErr
	default:
		return true, nil // stopped by Stop, not by Shutdown
	}
}

// waitDrain blocks until no lease and no in-flight poll remains, or ctx is done.
// The decision is made under leasesLock; remaining counts leases plus in-flight polls at
// that point. A finished drain wins over a done ctx. Leasing is closed before returning so
// newDelivery cannot admit a lease in the window before Stop. The first give-up is recorded
// and wakes every other waiter, which returns that same decision.
func (e *Engine) waitDrain(ctx context.Context) (int, error) {
	for {
		e.leasesLock.Lock()
		if e.gaveUp {
			n, err := e.gaveUpRemaining, e.gaveUpErr
			e.leasesLock.Unlock()
			return n, err
		}
		remaining := len(e.leases) + e.activePolls
		if remaining == 0 {
			e.leasesClosed = true
			e.maybeWakeDrainLocked()
			e.leasesLock.Unlock()
			return 0, nil
		}
		if cerr := ctx.Err(); cerr != nil {
			e.leasesClosed = true
			e.gaveUp, e.gaveUpErr, e.gaveUpRemaining = true, cerr, remaining
			if e.leasesDrained != nil && !isClosed(e.leasesDrained) {
				close(e.leasesDrained)
			}
			e.leasesLock.Unlock()
			return remaining, cerr
		}
		// Only this loop allocates the wake channel. A previous wake can be stale
		// (a lease dropped while a poll was still in flight), so replace a closed one.
		if e.leasesDrained == nil || isClosed(e.leasesDrained) {
			e.leasesDrained = make(chan struct{})
		}
		drained := e.leasesDrained
		e.leasesLock.Unlock()
		if e.testHookBeforeDrainSelect != nil {
			e.testHookBeforeDrainSelect()
		}
		select {
		case <-drained:
		case <-ctx.Done():
		}
	}
}

// completeShutdown runs Stop once. The caller that stops records err and emits the drain
// metrics; every other caller returns that same recorded result.
func (e *Engine) completeShutdown(err error, remaining int) error {
	ran := false
	e.shutdownOnce.Do(func() {
		ran = true
		e.shutdownClaimed.Store(true)
		e.shutdownErr = err
		e.metricsHandler.Gauge(metrics.EngineDrainLeases, float64(remaining))
		e.metricsHandler.Timer(metrics.EngineDrainDuration, e.timeSource.Since(time.Unix(0, e.drainStart.Load())))
		if err != nil {
			e.logger.Warn("drain gave up (context done); stopping with leases outstanding", "leases", remaining, "error", err)
		} else {
			e.logger.Info("drained")
		}
		e.Stop() // waits for a concurrent Stop to finish
		e.metricsHandler.Gauge(metrics.EngineDraining, 0)
		close(e.shutdownDone)
	})
	if ran {
		return err
	}
	<-e.shutdownDone
	return e.shutdownErr
}

// maybeWakeDrainLocked closes leasesDrained when a drain is waiting and nothing is in
// flight. Caller holds e.leasesLock.
func (e *Engine) maybeWakeDrainLocked() {
	if e.leasesDrained != nil && len(e.leases) == 0 && e.activePolls == 0 && !isClosed(e.leasesDrained) {
		close(e.leasesDrained)
	}
}

// beginPoll counts a PollTask that passed the draining fast-path. Once Shutdown has closed
// leasing, the poll is refused and not counted.
func (e *Engine) beginPoll() error {
	e.leasesLock.Lock()
	closed := e.leasesClosed
	if !closed {
		e.activePolls++
	}
	e.leasesLock.Unlock()
	if !closed {
		return nil
	}
	if err := e.drainingErr(); err != nil {
		return err
	}
	return ErrEngineStopped
}

func (e *Engine) endPoll() {
	e.leasesLock.Lock()
	e.activePolls--
	e.maybeWakeDrainLocked()
	e.leasesLock.Unlock()
}

// drainingErr is ErrEngineDraining during a drain, ErrEngineStopped once it has stopped.
func (e *Engine) drainingErr() error {
	if !e.draining.Load() {
		return nil
	}
	e.partitionsLock.RLock()
	defer e.partitionsLock.RUnlock()
	if e.stopped {
		return ErrEngineStopped
	}
	return ErrEngineDraining
}

func isClosed(ch chan struct{}) bool {
	select {
	case <-ch:
		return true
	default:
		return false
	}
}

// Draining reports whether Shutdown has started. It stays true after Shutdown has stopped the
// engine (the engine_draining gauge goes back to 0 then); PollTask then reports ErrEngineStopped.
func (e *Engine) Draining() bool { return e.draining.Load() }

func (e *Engine) outstandingLeaseCount() int {
	e.leasesLock.Lock()
	defer e.leasesLock.Unlock()
	return len(e.leases)
}

// Stop re-spools outstanding sync-matched leases, then unloads every queue. It does not wait
// for consumers; see Shutdown for a graceful drain. It does wait for an Ack, Nack or expiry
// already in progress to finish its side effect (an ack, a redelivery or a respool), so that
// side effect is not cut off by the unload. A concurrent or later Stop returns once the first
// has finished.
func (e *Engine) Stop() {
	e.partitionsLock.Lock()
	if e.stopped {
		e.partitionsLock.Unlock()
		<-e.stopDone
		return
	}
	e.stopped = true
	e.partitionsLock.Unlock()
	defer close(e.stopDone)

	// End every outstanding lease (library addition). Sync-matched tasks only exist in memory
	// while leased; write them back before unloading so a graceful shutdown does not lose them.
	// Backlog tasks are already durable and are redelivered after the next load; their later
	// Ack/Nack/ExtendLease report ErrEngineStopped (REVIEW L6).
	e.leasesLock.Lock()
	e.leasesClosed = true
	leases := make([]*Delivery, 0, len(e.leases))
	for d := range e.leases {
		leases = append(leases, d)
	}
	e.leasesLock.Unlock()
	var settling []*Delivery
	for _, d := range leases {
		// A lease Ack, Nack or expiry already settled may still be in the map: its side
		// effect is running and release has not removed it yet. settleWith fails, so Stop
		// must not finish or re-spool it a second time; it waits for release instead.
		if d.settleWith(ErrEngineStopped) {
			d.stopTimer()
			if d.SyncMatched {
				e.respoolSyncTaskLocked(d, true)
			}
			d.release()
		} else {
			settling = append(settling, d)
		}
	}
	for _, d := range settling {
		<-d.released // bounded: a respool gives up after 30s
	}

	if e.testHookStopAfterSyncLeases != nil {
		e.testHookStopAfterSyncLeases()
	}

	e.partitionsLock.Lock()
	pms := make([]*taskQueuePartitionManagerImpl, 0, len(e.partitions))
	for k, pm := range e.partitions {
		pms = append(pms, pm)
		delete(e.partitions, k)
	}
	e.partitionsLock.Unlock()
	for _, pm := range pms {
		pm.Stop(unloadCauseShuttingDown)
	}
	e.cancel()
}

func (e *Engine) getTaskQueuePartitionManager(
	ctx context.Context,
	partition store.QueueKey,
	create bool,
) (retPM *taskQueuePartitionManagerImpl, retErr error) {
	defer func() {
		if retErr != nil || retPM == nil {
			return
		}
		if retErr = retPM.WaitUntilInitialized(ctx); retErr != nil {
			e.unloadTaskQueuePartition(retPM, unloadCauseInitError)
		}
	}()

	e.partitionsLock.RLock()
	pm, ok := e.partitions[partition]
	stopped := e.stopped
	e.partitionsLock.RUnlock()
	if stopped {
		return nil, ErrEngineStopped
	}
	if ok {
		return pm, nil
	}
	if !create {
		return nil, nil
	}

	newPM := newTaskQueuePartitionManager(e, partition, newTaskQueueConfig(e.config))

	// If it gets here, write lock and check again in case a task queue is created between the two locks
	e.partitionsLock.Lock()
	if e.stopped {
		e.partitionsLock.Unlock()
		return nil, ErrEngineStopped
	}
	pm, ok = e.partitions[partition]
	if ok {
		e.partitionsLock.Unlock()
		// Lost the race with a concurrent load of the same partition. The unstarted newPM can
		// simply be dropped.
		return pm, nil
	}
	e.partitions[partition] = newPM
	e.partitionsLock.Unlock()

	newPM.Start()
	return newPM, nil
}

// Unloads the given task queue partition. If it has already been unloaded (i.e. it's not
// present in the loaded partitions map), then does nothing.
func (e *Engine) unloadTaskQueuePartition(unloadPM *taskQueuePartitionManagerImpl, unloadCause unloadCause) {
	e.unloadTaskQueuePartitionByKey(unloadPM.partition, unloadPM, unloadCause)
}

// Unloads a task queue partition by key. If unloadPM is given and the loaded partition does not
// match unloadPM, then nothing is unloaded.
func (e *Engine) unloadTaskQueuePartitionByKey(
	partition store.QueueKey,
	unloadPM *taskQueuePartitionManagerImpl,
	unloadCause unloadCause,
) bool {
	e.partitionsLock.Lock()
	foundTQM, ok := e.partitions[partition]
	if !ok || (unloadPM != nil && foundTQM != unloadPM) {
		e.partitionsLock.Unlock()
		return false
	}
	delete(e.partitions, partition)
	e.partitionsLock.Unlock()
	foundTQM.Stop(unloadCause)
	return true
}

func (e *Engine) validatePriority(p store.Priority) error {
	// upstream common/priorities.Validate (applied by frontend on AddTask). Keys above
	// PriorityLevels are accepted and clamped by clipPriority, as upstream (REVIEW L3).
	if p.PriorityKey < 0 {
		return &store.InvalidArgumentError{Msg: fmt.Sprintf("priority key %d must not be negative", p.PriorityKey)}
	}
	if len(p.FairnessKey) > fairnessKeyMaxLength {
		return &store.InvalidArgumentError{Msg: "fairness key length exceeds limit"}
	}
	if p.FairnessWeight < 0 {
		return &store.InvalidArgumentError{Msg: "must be greater than zero"}
	}
	return nil
}

// AddTask adds a task: sync-matched to a waiting poller if possible, else persisted.
// It returns whether the task was sync-matched.
func (e *Engine) AddTask(ctx context.Context, queue store.QueueKey, task store.Task) (bool, error) {
	if err := e.validatePriority(task.Priority); err != nil {
		return false, err
	}
	start := e.timeSource.Now()
	task.Attempts = 0 // owned by the queue
	if task.CreateTime.IsZero() {
		task.CreateTime = start
	}
	for attempt := 1; ; attempt++ {
		pm, err := e.getTaskQueuePartitionManager(ctx, queue, true)
		if err != nil {
			return false, err
		}
		t := task
		if age := time.Duration(pm.maxTaskAge.Load()); age > 0 {
			// Library addition (Config.MaxTaskAge): cap the expiry at max age after this
			// AddTask (not CreateTime, so a redriven task gets a fresh window).
			if limit := start.Add(age); t.ExpiryTime.IsZero() || limit.Before(t.ExpiryTime) {
				t.ExpiryTime = limit
			}
		}
		matched, err := pm.AddTask(ctx, &t, nil)
		if matched && err == nil {
			// upstream handler.AddActivityTask: the whole call, for sync matches only
			pm.metricsHandler.Timer(metrics.SyncMatchLatency, e.timeSource.Since(start))
		}
		// Deviation: upstream returns these to history, which retries. We retry on a reloaded
		// queue when the queue unloaded (idle/conflict) underneath the call.
		if err != nil && attempt < addTaskMaxAttempts && isShutdownOrConflict(err) && ctx.Err() == nil {
			continue
		}
		return matched, err
	}
}

// PollOptions are per-poll options.
type PollOptions struct {
	Identity string
	// MinPriority, if > 0, only matches tasks with priority key <= MinPriority.
	MinPriority int32
	// NoWait returns ErrNoTasks immediately if nothing is ready.
	NoWait bool
	// LeaseDuration overrides Config.DeliveryLeaseDuration.
	LeaseDuration time.Duration
	// MaxTasksPerSecond is the poller's requested queue rate limit (upstream
	// TaskQueueMetadata.MaxTasksPerSecond). Like upstream the last poll wins: a poll with nil
	// clears it. An API rate (QueueConfig.RateLimit) takes precedence.
	MaxTasksPerSecond *float64
}

// PollTask long-polls a queue. It returns ErrNoTasks when the poll expires.
func (e *Engine) PollTask(ctx context.Context, queue store.QueueKey, opts PollOptions) (*Delivery, error) {
	if err := e.drainingErr(); err != nil {
		return nil, err
	}
	if err := e.beginPoll(); err != nil {
		return nil, err
	}
	defer e.endPoll()

	// Link the drain cancel before partition load so a poll blocked there ends too.
	// The caller's ctx is kept aside: a drain must not hide the error it would return.
	callerCtx := ctx
	ctx, cancelPoll := context.WithCancel(ctx)
	defer cancelPoll()
	defer context.AfterFunc(e.drainCtx, cancelPoll)()

	pm, err := e.getTaskQueuePartitionManager(ctx, queue, true)
	if err != nil {
		// A load cut short by the drain (not by the caller) reports the drain.
		if derr := e.drainingErr(); derr != nil && callerCtx.Err() == nil && ctx.Err() != nil {
			return nil, derr
		}
		return nil, err
	}
	pollMetadata := &pollMetadata{identity: opts.Identity, maxTasksPerSecond: opts.MaxTasksPerSecond}
	if opts.MinPriority > 0 || opts.NoWait {
		pollMetadata.conditions = &pollConditions{MinPriority: opts.MinPriority, NoWait: opts.NoWait}
	}

	// For non-forwarded polls, subtract a proportional random jitter to spread expiration
	// times across pollers and prevent thundering herd reconnects. Jitter is capped so the
	// interval never falls below forwardedPollMinInterval.
	longPollInterval := e.config.LongPollExpirationInterval
	if pollMetadata.forwardedFrom == "" {
		jitterMax := time.Duration(float64(longPollInterval) * forwardedPollJitterRatio)
		if longPollInterval-jitterMax < forwardedPollMinInterval {
			jitterMax = longPollInterval - forwardedPollMinInterval
		}
		if jitterMax > 0 {
			longPollInterval -= time.Duration(rand.Float64() * float64(jitterMax)) // backoff.FullJitter
		}
	}
	// We set a shorter timeout than the caller's ctx so that an empty result is returned before
	// the caller's own deadline fires.
	ctx, cancel := withDeadlineBuffer(ctx, longPollInterval, returnEmptyTaskTimeBudget, e.timeSource)
	defer cancel()

	for {
		task, dbq, err := pm.PollTask(ctx, pollMetadata)
		if err != nil {
			// Waiting polls end as ErrNoTasks. During a drain that is ErrEngineDraining,
			// unless the caller's own ctx is done: then return ErrNoTasks, as with no drain.
			if errors.Is(err, ErrNoTasks) {
				if derr := e.drainingErr(); derr != nil && callerCtx.Err() == nil {
					return nil, derr
				}
			}
			return nil, err
		}
		if exhausted(task, dbq.maxAttempts()) {
			// Delivered max times already without an outcome (crash, Stop) or re-spooled
			// after its last attempt: dead-letter it instead of delivering it.
			if e.deadLetter(dbq, task, DeadReasonAttemptsExhausted, "", false) {
				continue
			}
		}
		if e.starter != nil {
			req := &StartRequest{Queue: queue, Task: task.event.Data, Identity: opts.Identity}
			if !task.isSyncMatchTask() {
				req.TaskID = task.event.ID
			}
			if serr := e.starter.Start(ctx, req); serr != nil {
				if errors.Is(serr, ErrDropTask) {
					// drop the task as otherwise task would be stuck in a retry-loop
					task.finish(taskFinishResult{dropReason: dropReasonStarter})
				} else {
					// Retryable → re-added to the matcher; other errors → re-spooled.
					task.finish(taskFinishResult{err: serr})
				}
				continue
			}
		}
		d, err := e.newDelivery(task, dbq, queue, opts.LeaseDuration)
		if err == nil && !d.SyncMatched && dbq.maxAttempts() > 0 {
			e.recordAttempt(dbq, task)
		}
		return d, err
	}
}

// respoolSyncTaskLocked writes a sync-matched task (that was never persisted) to the backlog.
func (e *Engine) isStopped() bool {
	e.partitionsLock.RLock()
	defer e.partitionsLock.RUnlock()
	return e.stopped
}

func (e *Engine) respoolSyncTaskLocked(d *Delivery, shuttingDown bool) {
	e.respoolSyncTask(d, shuttingDown, time.Time{})
}

// respoolSyncTask spools a sync-matched task whose lease ended; a non-zero visibleAt delays
// it (library addition, see delay.go).
func (e *Engine) respoolSyncTask(d *Delivery, shuttingDown bool, visibleAt time.Time) {
	if e.testHookBeforeRespool != nil {
		e.testHookBeforeRespool(shuttingDown)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	op := func(ctx context.Context) error {
		pm := d.pqm
		if pm.tqCtx.Err() != nil && !shuttingDown {
			// queue unloaded: reload it (only possible while the engine runs)
			p, err := e.getTaskQueuePartitionManager(ctx, d.Queue, true)
			if err != nil {
				return err
			}
			pm = p.defaultQueue
		}
		t := d.Task
		if !visibleAt.IsZero() {
			t.VisibleAt = visibleAt
		}
		return pm.SpoolTask(&t)
	}
	err := backoff.ThrottleRetryContext(ctx, op, persistenceOperationRetryPolicy, func(err error) bool {
		return !shuttingDown && (isPersistenceTransient(err) || isShutdownOrConflict(err))
	})
	if err != nil {
		e.logger.Error("failed to re-spool sync-matched task after lease loss; task lost",
			"queue", d.Queue.String(), "error", err)
	}
}

// LoadedQueues returns the keys of currently loaded queues.
func (e *Engine) LoadedQueues() []store.QueueKey {
	e.partitionsLock.RLock()
	defer e.partitionsLock.RUnlock()
	keys := make([]store.QueueKey, 0, len(e.partitions))
	for k := range e.partitions {
		keys = append(keys, k)
	}
	return keys
}

// QueueStatus returns per-subqueue status of a loaded queue (false if not loaded).
func (e *Engine) QueueStatus(queue store.QueueKey) ([]SubqueueStatus, bool) {
	e.partitionsLock.RLock()
	pm, ok := e.partitions[queue]
	e.partitionsLock.RUnlock()
	if !ok {
		return nil, false
	}
	return pm.defaultQueue.backlogMgr.InternalStatus(), true
}

// WaitingPollers returns the number of polls currently parked in a loaded queue's matcher
// (0 if not loaded). Library addition for observability and tests.
func (e *Engine) WaitingPollers(queue store.QueueKey) int {
	e.partitionsLock.RLock()
	pm, ok := e.partitions[queue]
	e.partitionsLock.RUnlock()
	if !ok {
		return 0
	}
	return pm.defaultQueue.priMatcher.waitingPollers()
}

// MatcherTasks returns the number of tasks currently queued in a loaded queue's matcher, ready
// to be handed to a poller (0 if not loaded). Unlike QueueStatus.LoadedTasks, which the reader
// counts before it hands tasks to the matcher, this counts only tasks already in the matcher.
// Library addition for observability and tests.
func (e *Engine) MatcherTasks(queue store.QueueKey) int {
	e.partitionsLock.RLock()
	pm, ok := e.partitions[queue]
	e.partitionsLock.RUnlock()
	if !ok {
		return 0
	}
	return pm.defaultQueue.priMatcher.matcherTasks()
}

// RateLimit returns a loaded queue's effective rate limit in tasks/s (summed over read
// partitions) and its source, like upstream GetEffectiveRPSAndSource (DescribeTaskQueue).
// ok is false if the queue is not loaded. Note the reported rate may not be enforced yet:
// see Config.ApplySystemRateLimitOnLoad.
func (e *Engine) RateLimit(queue store.QueueKey) (rps float64, source RateLimitSource, ok bool) {
	e.partitionsLock.RLock()
	pm, ok := e.partitions[queue]
	e.partitionsLock.RUnlock()
	if !ok {
		return 0, RateLimitSourceUnspecified, false
	}
	rps, source = pm.rateLimitManager.GetEffectiveRPSAndSource()
	return rps, source, true
}

// withDeadlineBuffer is ported from common/contextutil/deadline.go WithDeadlineBuffer.
// Deviation: the child timeout runs on the injected TimeSource (via timeoutOn) so long-poll
// expiry is testable with a fake clock; the parent's deadline is a real-clock deadline, so the
// buffer computation stays on time.Until. With the real clock this is identical to upstream.
func withDeadlineBuffer(parent context.Context, timeout, buffer time.Duration, ts clock.TimeSource) (context.Context, context.CancelFunc) {
	if parent.Err() != nil {
		return parent, func() {}
	}
	parentDeadline, parentHasDeadline := parent.Deadline()
	if !parentHasDeadline {
		// No parent deadline, so buffer is available to parent after child deadline expiry.
		return timeoutOn(parent, timeout, ts)
	}
	// If parent deadline itself does not allow buffer then set child timeout to zero. Otherwise
	// compute child deadline such that at least buffer remains after it and before parent deadline.
	remaining := time.Until(parentDeadline) - buffer
	if remaining < timeout {
		timeout = max(0, remaining)
	}
	return timeoutOn(parent, timeout, ts)
}

// timeoutOn is context.WithTimeout on ts. For the real clock it is exactly
// context.WithTimeout. Otherwise the context is cancelled (cause DeadlineExceeded) when ts
// fires; unlike clock.ContextWithTimeout it also propagates parent cancellation.
func timeoutOn(parent context.Context, timeout time.Duration, ts clock.TimeSource) (context.Context, context.CancelFunc) {
	if _, real := ts.(clock.RealTimeSource); real || ts == nil {
		return context.WithTimeout(parent, timeout)
	}
	ctx, cancel := context.WithCancelCause(parent)
	timer := ts.AfterFunc(timeout, func() { cancel(context.DeadlineExceeded) })
	return ctx, func() {
		timer.Stop()
		cancel(context.Canceled)
	}
}
