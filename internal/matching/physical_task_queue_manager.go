// Ported from temporalio/temporal v1.32.1 af08a5e5, service/matching/physical_task_queue_manager.go
// Changes: fair backlog + priMatcher only (classic/pri backlogs, old matcher, draining, versioning,
// deployments, poller history, scaling decisions, task trackers removed); injected clock.
// Copyright (c) 2025 Temporal Technologies Inc. MIT License; see UPSTREAM_LICENSE and NOTICE.

package matching

import (
	"context"
	"log/slog"
	"math/rand/v2"
	"sync/atomic"
	"time"

	"github.com/skme902/tq/clock"
	"github.com/skme902/tq/internal/counter"
	"github.com/skme902/tq/metrics"
	"github.com/skme902/tq/store"
)

const (
	// Time budget for empty task to propagate through the function stack and be returned to
	// the poll handler.
	returnEmptyTaskTimeBudget = time.Second

	daemonStatusInitialized int32 = 0
	daemonStatusStarted     int32 = 1
	daemonStatusStopped     int32 = 2
)

type unloadCause int

const (
	unloadCauseUnspecified unloadCause = iota
	unloadCauseInitError
	unloadCauseIdle
	unloadCauseConflict // reactive unload due to other node stealing ownership
	unloadCauseShuttingDown
	unloadCauseOtherError
)

func (c unloadCause) String() string {
	switch c {
	case unloadCauseInitError:
		return "InitError"
	case unloadCauseIdle:
		return "Idle"
	case unloadCauseConflict:
		return "Conflict"
	case unloadCauseShuttingDown:
		return "ShuttingDown"
	case unloadCauseOtherError:
		return "OtherError"
	default:
		return "Unspecified"
	}
}

// physicalTaskQueueManagerImpl manages the default (only) physical queue of a partition.
type physicalTaskQueueManagerImpl struct {
	status       int32
	partitionMgr *taskQueuePartitionManagerImpl
	queue        store.QueueKey
	config       *taskQueueConfig

	// This context is valid for lifetime of this physicalTaskQueueManagerImpl.
	// It can be used to notify when the task queue is closing.
	tqCtx       context.Context
	tqCtxCancel context.CancelFunc

	backlogMgr     *fairBacklogManagerImpl
	liveness       *liveness
	priMatcher     *priTaskMatcher
	logger         *slog.Logger
	metricsHandler metrics.Handler
	timeSource     clock.TimeSource

	// outstandingLeases counts deliveries not yet acked/nacked/expired (library addition).
	outstandingLeases atomic.Int64
}

var _ fairBacklogPQM = (*physicalTaskQueueManagerImpl)(nil)

func newPhysicalTaskQueueManager(
	partitionMgr *taskQueuePartitionManagerImpl,
	queue store.QueueKey,
) *physicalTaskQueueManagerImpl {
	e := partitionMgr.engine
	config := partitionMgr.config

	tqCtx, tqCancel := context.WithCancel(context.Background())

	pqMgr := &physicalTaskQueueManagerImpl{
		status:         daemonStatusInitialized,
		partitionMgr:   partitionMgr,
		queue:          queue,
		config:         config,
		tqCtx:          tqCtx,
		tqCtxCancel:    tqCancel,
		logger:         partitionMgr.logger,
		metricsHandler: partitionMgr.metricsHandler,
		timeSource:     e.timeSource,
	}

	pqMgr.liveness = newLiveness(
		e.timeSource,
		func() time.Duration { return config.MaxTaskQueueIdleTime },
		pqMgr.onIdle,
	)

	pqMgr.backlogMgr = newFairBacklogManager(
		tqCtx,
		pqMgr,
		config,
		e.store,
		pqMgr.logger,
		pqMgr.metricsHandler,
		e.timeSource,
		pqMgr.counterFactory,
	)
	var fwdr PartitionRouter // single partition: never forwards (partitions not implemented)
	pqMgr.priMatcher = newPriTaskMatcher(
		tqCtx,
		config,
		queue,
		fwdr,
		e.validator,
		pqMgr.logger,
		pqMgr.metricsHandler,
		e.timeSource,
		partitionMgr.rateLimitManager,
		pqMgr.onRateLimited,
		pqMgr.MarkAlive,
	)
	return pqMgr
}

// onIdle unloads the queue. Deviation: upstream unloads unconditionally; we keep the queue
// loaded while deliveries are leased so their acks are not lost to an unload/reload.
func (c *physicalTaskQueueManagerImpl) onIdle() {
	if c.outstandingLeases.Load() > 0 {
		go c.liveness.markAlive() // re-arm outside the timer callback
		return
	}
	c.UnloadFromPartitionManager(unloadCauseIdle)
}

func (c *physicalTaskQueueManagerImpl) Start() {
	if !atomic.CompareAndSwapInt32(&c.status, daemonStatusInitialized, daemonStatusStarted) {
		return
	}
	c.liveness.Start()
	c.backlogMgr.Start()
	c.priMatcher.Start()
	c.logger.Debug("Started physicalTaskQueueManager")
	c.metricsHandler.Counter(metrics.TaskQueueStarted, 1)
}

// Stop does not unload the queue from its partition. It is intended to be called by the partition manager when
// unloading a queues. For stopping and unloading a queue call UnloadFromPartitionManager instead.
func (c *physicalTaskQueueManagerImpl) Stop(unloadCause unloadCause) {
	if atomic.CompareAndSwapInt32(&c.status, daemonStatusInitialized, daemonStatusStopped) {
		// Deviation (REVIEW M3): stopped before Start ran (the engine publishes a partition
		// before starting it). Start is now a no-op; cancel tqCtx so waiters fail fast.
		c.tqCtxCancel()
		return
	}
	if !atomic.CompareAndSwapInt32(&c.status, daemonStatusStarted, daemonStatusStopped) {
		return
	}
	// this may attempt to write one final ack update, do this before canceling tqCtx
	c.backlogMgr.Stop()
	c.priMatcher.Stop()
	c.liveness.Stop()
	c.tqCtxCancel()
	c.logger.Debug("Stopped physicalTaskQueueManager", "cause", unloadCause.String())
	c.metricsHandler.Counter(metrics.TaskQueueStopped, 1, metrics.Tag{Key: "cause", Value: unloadCause.String()})
}

func (c *physicalTaskQueueManagerImpl) WaitUntilInitialized(ctx context.Context) error {
	// Deviation (REVIEW M3): also return when the queue stops, which matters for a queue that
	// was stopped before it started and so never initializes.
	waitCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	defer context.AfterFunc(c.tqCtx, cancel)()
	err := c.backlogMgr.WaitUntilInitialized(waitCtx)
	if err != nil && ctx.Err() == nil && c.tqCtx.Err() != nil {
		return errTaskQueueClosed
	}
	return err
}

func (c *physicalTaskQueueManagerImpl) SpoolTask(taskInfo *store.Task) error {
	c.liveness.markAlive()
	return c.backlogMgr.SpoolTask(taskInfo)
}

// PollTask blocks waiting for a task.
// Returns error when context deadline is exceeded
func (c *physicalTaskQueueManagerImpl) PollTask(
	ctx context.Context,
	pollMetadata *pollMetadata,
) (*internalTask, error) {
	c.liveness.markAlive()

	for {
		task, err := c.priMatcher.Poll(ctx, pollMetadata)
		if err != nil {
			return nil, err
		}

		// It's possible to get an expired task here: taskReader checks for expiration when
		// reading from persistence, but the one task blocked in the matcher could expire while
		// it's there. In that case, go back for another task.
		if task.event != nil && isTaskExpired(task.event.TaskInfo, c.timeSource.Now()) {
			task.finish(taskFinishResult{dropReason: dropReasonExpiredMemory})
			continue
		}
		return task, nil
	}
}

func (c *physicalTaskQueueManagerImpl) MarkAlive() {
	c.liveness.markAlive()
}

// AddSpooledTask replaces pqm.AddSpooledTask → partitionMgr.AddSpooledTask →
// AddSpooledTaskToMatcher; without versioning the partition hop is a no-op.
func (c *physicalTaskQueueManagerImpl) AddSpooledTask(task *internalTask) error {
	if c.tqCtx.Err() != nil {
		return errTaskQueueClosed
	}
	return c.priMatcher.AddTask(task)
}

func (c *physicalTaskQueueManagerImpl) TrySyncMatch(ctx context.Context, task *internalTask) (syncMatchOutcome, error) {
	if !task.isForwarded() {
		// request sent by AddTask
		c.liveness.markAlive()
	}
	return c.priMatcher.Offer(ctx, task)
}

func (c *physicalTaskQueueManagerImpl) QueueKey() store.QueueKey {
	return c.queue
}

func (c *physicalTaskQueueManagerImpl) UnloadFromPartitionManager(unloadCause unloadCause) {
	c.partitionMgr.unloadPhysicalQueue(c, unloadCause)
}

func (c *physicalTaskQueueManagerImpl) counterFactory() counter.Counter {
	src := rand.NewPCG(rand.Uint64(), rand.Uint64())
	return counter.NewHybridCounter(c.config.FairnessCounter, src)
}

// onRateLimited replaces upstream's tasksRateLimited task tracker with a counter metric.
func (c *physicalTaskQueueManagerImpl) onRateLimited() {
	c.metricsHandler.Counter(metrics.TasksRateLimited, 1)
}

// UserDataChanged is upstream's: re-match in-matcher tasks after a config change (this is
// also what wakes a matcher whose rate-limit timer was armed under the old rate).
func (c *physicalTaskQueueManagerImpl) UserDataChanged() {
	c.priMatcher.ReprocessAllTasks()
}

// GetFairnessWeightOverrides is upstream's (physical_task_queue_manager.go:876-878): the
// rateLimitManager's snapshot, so writer weights match the per-key limiter (REVIEW L4).
func (c *physicalTaskQueueManagerImpl) GetFairnessWeightOverrides() fairnessWeightOverrides {
	return c.partitionMgr.GetRateLimitManager().GetFairnessWeightOverrides()
}
