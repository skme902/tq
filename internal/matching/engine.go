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
	"sync"
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

	ctx    context.Context
	cancel context.CancelFunc

	partitionsLock sync.RWMutex
	partitions     map[store.QueueKey]*taskQueuePartitionManagerImpl
	stopped        bool

	syncLeasesLock sync.Mutex
	syncLeases     map[*Delivery]struct{}
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
	ctx, cancel := context.WithCancel(context.Background())
	return &Engine{
		config:         opts.Config.WithDefaults(),
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
		syncLeases:     make(map[*Delivery]struct{}),
	}, nil
}

// Config returns the effective configuration.
func (e *Engine) Config() Config { return e.config }

// Stop re-spools outstanding sync-matched leases, then unloads every queue.
func (e *Engine) Stop() {
	e.partitionsLock.Lock()
	if e.stopped {
		e.partitionsLock.Unlock()
		return
	}
	e.stopped = true
	e.partitionsLock.Unlock()

	// Sync-matched tasks only exist in memory while leased; write them back before unloading so a
	// graceful shutdown does not lose them (library addition; backlog tasks are already durable).
	e.syncLeasesLock.Lock()
	leases := make([]*Delivery, 0, len(e.syncLeases))
	for d := range e.syncLeases {
		leases = append(leases, d)
	}
	e.syncLeasesLock.Unlock()
	for _, d := range leases {
		if d.settle() {
			d.stopTimer()
			d.release()
			e.respoolSyncTaskLocked(d, true)
		}
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
	if p.PriorityKey < 0 || int(p.PriorityKey) > e.config.PriorityLevels {
		return &store.InvalidArgumentError{
			Msg: fmt.Sprintf("priority key %d out of range [0, %d]", p.PriorityKey, e.config.PriorityLevels),
		}
	}
	// upstream common/priorities.Validate (applied by frontend on AddTask).
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
	if task.CreateTime.IsZero() {
		task.CreateTime = e.timeSource.Now()
	}
	for attempt := 1; ; attempt++ {
		pm, err := e.getTaskQueuePartitionManager(ctx, queue, true)
		if err != nil {
			return false, err
		}
		t := task
		matched, err := pm.AddTask(ctx, &t, nil)
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
	pm, err := e.getTaskQueuePartitionManager(ctx, queue, true)
	if err != nil {
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
			return nil, err
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
		return e.newDelivery(task, dbq, queue, opts.LeaseDuration), nil
	}
}

// respoolSyncTaskLocked writes a sync-matched task (that was never persisted) to the backlog.
func (e *Engine) respoolSyncTaskLocked(d *Delivery, shuttingDown bool) {
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
