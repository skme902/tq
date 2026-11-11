// Ported from temporalio/temporal v1.32.1 af08a5e5, service/matching/fair_backlog_manager.go
// Changes: draining/migration, scale manager, and proto stats APIs removed; protos → store types;
// future.FutureImpl → channel; metrics trimmed; injected clock.
// Copyright (c) 2025 Temporal Technologies Inc. MIT License; see UPSTREAM_LICENSE and NOTICE.

package matching

import (
	"context"
	"errors"
	"log/slog"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/skme902/tq/clock"
	"github.com/skme902/tq/internal/backoff"
	"github.com/skme902/tq/internal/counter"
	"github.com/skme902/tq/metrics"
	"github.com/skme902/tq/store"
)

// ioTimeout bounds individual store calls made from background loops.
const ioTimeout = 5 * time.Second

type (
	// fairBacklogPQM is the subset of physicalTaskQueueManager used by the backlog manager.
	fairBacklogPQM interface {
		QueueKey() store.QueueKey
		UnloadFromPartitionManager(cause unloadCause)
		AddSpooledTask(task *internalTask) error
		GetFairnessWeightOverrides() fairnessWeightOverrides
	}

	fairBacklogManagerImpl struct {
		pqMgr      fairBacklogPQM
		config     *taskQueueConfig
		tqCtx      context.Context
		db         *taskQueueDB
		taskWriter *fairTaskWriter

		subqueueLock        sync.Mutex
		subqueues           []*fairTaskReader // subqueue index -> fairTaskReader
		subqueuesByPriority map[priorityKey]subqueueIndex
		priorityBySubqueue  map[subqueueIndex]priorityKey

		logger         *slog.Logger
		metricsHandler metrics.Handler
		timeSource     clock.TimeSource
		counterFactory func() counter.Counter

		initialized    chan struct{}
		initializedErr error
		// skipFinalUpdate controls behavior on Stop: if it's false, we try to write one final
		// update before unloading
		skipFinalUpdate atomic.Bool

		// lastStoreErr is the most recent store error seen by signalIfFatal (library
		// addition, for Engine.Health); nil if none.
		lastStoreErr atomic.Pointer[storeErrNote]
	}
)

func newFairBacklogManager(
	tqCtx context.Context,
	pqMgr fairBacklogPQM,
	config *taskQueueConfig,
	st store.Store,
	logger *slog.Logger,
	metricsHandler metrics.Handler,
	timeSource clock.TimeSource,
	counterFactory func() counter.Counter,
) *fairBacklogManagerImpl {
	bmg := &fairBacklogManagerImpl{
		pqMgr:               pqMgr,
		config:              config,
		tqCtx:               tqCtx,
		db:                  newTaskQueueDB(config, st, pqMgr.QueueKey(), logger, metricsHandler, timeSource),
		subqueuesByPriority: make(map[priorityKey]subqueueIndex),
		priorityBySubqueue:  make(map[subqueueIndex]priorityKey),
		metricsHandler:      metricsHandler,
		timeSource:          timeSource,
		counterFactory:      counterFactory,
		logger:              logger,
		initialized:         make(chan struct{}),
	}
	bmg.taskWriter = newFairTaskWriter(bmg, bmg.newCounterForSubqueue)
	return bmg
}

// signalIfFatal calls UnloadFromPartitionManager of the physicalTaskQueueManager
// if and only if the supplied error represents a fatal condition, e.g. the existence
// of a newer lease by another backlogManager. Returns true if the unload signal
// is emitted, false otherwise.
func (c *fairBacklogManagerImpl) signalIfFatal(err error) bool {
	if err == nil {
		return false
	}
	// Deviation (Engine.Health): remember persistence errors for the degraded report.
	if isStoreProblem(err) {
		c.lastStoreErr.Store(&storeErrNote{at: c.timeSource.Now(), err: err.Error()})
	}
	if store.IsConditionFailed(err) {
		c.metricsHandler.Counter(metrics.ConditionFailedErrors, 1)
		c.skipFinalUpdate.Store(true)
		c.pqMgr.UnloadFromPartitionManager(unloadCauseConflict)
		return true
	}
	return false
}

func (c *fairBacklogManagerImpl) Start() {
	c.taskWriter.Start()
}

func (c *fairBacklogManagerImpl) isInitialized() bool {
	select {
	case <-c.initialized:
		return true
	default:
		return false
	}
}

// isStoreProblem reports whether err from a backlog operation is a persistence failure worth
// reporting in Engine.Health. In-process conditions are not: a full writer channel
// (ErrResourceExhausted, a throttle), the queue shutting down or closed, a caller cancel, or
// a non-contiguous ID block.
func isStoreProblem(err error) bool {
	switch {
	case errors.Is(err, context.Canceled),
		errors.Is(err, ErrResourceExhausted),
		errors.Is(err, errShutdown),
		errors.Is(err, errTaskQueueClosed),
		errors.Is(err, errMatcherStopped),
		errors.Is(err, errNonContiguousBlocks):
		return false
	}
	return true
}

func (c *fairBacklogManagerImpl) Stop() {
	c.lastStoreErr.Store(nil) // an unloaded queue is not reported degraded
	// Maybe try to write one final update of ack level. Skip the update if we never
	// initialized. Also skip if we're stopping due to lost ownership (the update will
	// fail in that case). Ignore any errors. Don't bother with GC, the next reload will
	// handle that.
	if !c.isInitialized() || c.initializedErr != nil || c.skipFinalUpdate.Load() {
		return
	}

	c.subqueueLock.Lock()
	for i, r := range c.subqueues {
		_, ackLevel := r.getLevels()
		// oldestTime can be time.Time{} here since countDelta is 0
		c.db.updateFairAckLevel(subqueueIndex(i), ackLevel, 0, -1, time.Time{})
	}
	c.subqueueLock.Unlock()

	ctx, cancel := context.WithTimeout(c.tqCtx, ioTimeout)
	_ = c.db.SyncState(ctx)
	cancel()
}

func (c *fairBacklogManagerImpl) initState(state taskQueueState, err error) {
	defer func() {
		c.initializedErr = err
		close(c.initialized)
	}()

	if err != nil {
		// We can't recover from here without starting over, so unload the whole task queue.
		// Skip final update since we never initialized.
		c.skipFinalUpdate.Store(true)
		c.pqMgr.UnloadFromPartitionManager(unloadCauseInitError)
		return
	}

	c.subqueueLock.Lock()
	defer c.subqueueLock.Unlock()

	c.loadSubqueuesLocked(state.subqueues)
	go c.periodicSync()
}

func (c *fairBacklogManagerImpl) WaitUntilInitialized(ctx context.Context) error {
	select {
	case <-c.initialized:
		return c.initializedErr
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (c *fairBacklogManagerImpl) loadSubqueuesLocked(subqueues []store.SubqueueInfo) {
	// TODO(pri): This assumes that subqueues never shrinks, and priority/fairness index of
	// existing subqueues never changes. If we change that, this logic will need to change.
	for i := range subqueues {
		subqueueIdx := subqueueIndex(i)
		if i >= len(c.subqueues) {
			r := newFairTaskReader(c, subqueueIdx, fairLevelFromStore(subqueues[i].FairAckLevel))
			r.Start()
			c.subqueues = append(c.subqueues, r)
		}
		c.subqueuesByPriority[priorityKey(subqueues[i].Key.Priority)] = subqueueIdx
		c.priorityBySubqueue[subqueueIdx] = priorityKey(subqueues[i].Key.Priority)
	}
}

func (c *fairBacklogManagerImpl) getSubqueueForPriority(priority priorityKey) subqueueIndex {
	priority = c.config.clipPriority(priority)

	c.subqueueLock.Lock()
	defer c.subqueueLock.Unlock()

	if i, ok := c.subqueuesByPriority[priority]; ok {
		return i
	}

	// We need to allocate a new subqueue. Note this is doing io under subqueueLock,
	// but we want to serialize these updates.
	subqueues, err := c.db.AllocateSubqueue(c.tqCtx, store.SubqueueKey{Priority: int32(priority)})
	if err != nil {
		c.signalIfFatal(err)
		// If we failed to write the metadata update, just use subqueueZero.
		// If err was a fatal error (most likely case), the subsequent call to SpoolTask will fail.
		return subqueueZero
	}

	c.loadSubqueuesLocked(subqueues)

	if i, ok := c.subqueuesByPriority[priority]; ok {
		return i
	}

	// But if something went wrong, return zero.
	return subqueueZero
}

func (c *fairBacklogManagerImpl) periodicSync() {
	for {
		ch, t := c.timeSource.NewTimer(c.config.UpdateAckInterval)
		select {
		case <-c.tqCtx.Done():
			t.Stop()
			return
		case <-ch:
			ctx, cancel := context.WithTimeout(c.tqCtx, ioTimeout)
			err := c.db.SyncState(ctx)
			cancel()
			if c.signalIfFatal(err) {
				return
			}
			c.subqueueLock.Lock()
			readers := slices.Clone(c.subqueues)
			c.subqueueLock.Unlock()
			for _, r := range readers {
				r.maybeGC()
			}
		}
	}
}

func (c *fairBacklogManagerImpl) SpoolTask(taskInfo *store.Task) error {
	subqueue := c.getSubqueueForPriority(priorityKey(taskInfo.Priority.PriorityKey))
	err := c.taskWriter.appendTask(subqueue, taskInfo)
	c.signalIfFatal(err)
	return err
}

func (c *fairBacklogManagerImpl) getAndPinAckLevels() ([]fairLevel, func(error)) {
	c.subqueueLock.Lock()
	subqueues := slices.Clone(c.subqueues)
	c.subqueueLock.Unlock()

	levels := make([]fairLevel, len(subqueues))
	for i, s := range subqueues {
		levels[i] = s.getAndPinAckLevel()
	}
	unpin := func(writeErr error) {
		for _, s := range subqueues {
			s.unpinAckLevel(writeErr)
		}
	}
	return levels, unpin
}

func (c *fairBacklogManagerImpl) wroteNewTasks(resp createFairTasksResponse) {
	c.subqueueLock.Lock()
	subqueues := slices.Clone(c.subqueues)
	c.subqueueLock.Unlock()

	for subqueue, subqueueResp := range resp {
		subqueues[subqueue].wroteNewTasks(subqueueResp)
	}
}

func (c *fairBacklogManagerImpl) addSpooledTask(task *internalTask) error {
	return c.pqMgr.AddSpooledTask(task)
}

func (c *fairBacklogManagerImpl) BacklogCountHint() (total int64) {
	c.subqueueLock.Lock()
	defer c.subqueueLock.Unlock()
	for _, r := range c.subqueues {
		total += int64(r.getLoadedTasks())
	}
	return
}

// BacklogStatsByPriority is upstream's: per priority key, the summed approximate backlog count
// and the greatest age of a loaded backlog task.
func (c *fairBacklogManagerImpl) BacklogStatsByPriority() map[int32]*QueueStats {
	c.subqueueLock.Lock()
	defer c.subqueueLock.Unlock()

	result := make(map[int32]*QueueStats)
	backlogCounts := c.db.getApproximateBacklogCountsBySubqueue()
	for subqueueIdx, priorityKey := range c.priorityBySubqueue {
		pk := int32(priorityKey)
		if _, ok := result[pk]; !ok {
			result[pk] = &QueueStats{}
		}
		if int(subqueueIdx) < len(backlogCounts) {
			result[pk].ApproximateBacklogCount += backlogCounts[subqueueIdx]
		}
		if int(subqueueIdx) >= len(c.subqueues) {
			continue
		}
		if oldest := c.subqueues[subqueueIdx].getOldestBacklogTime(); !oldest.IsZero() {
			if age := c.timeSource.Since(oldest); age > result[pk].ApproximateBacklogAge {
				result[pk].ApproximateBacklogAge = age
			}
		}
	}
	return result
}

// SubqueueStatus is a debugging snapshot of one subqueue (replaces InternalTaskQueueStatus).
type SubqueueStatus struct {
	Priority                int32
	ReadLevel, AckLevel     store.Level
	LoadedTasks             int
	ApproximateBacklogCount int64
	TaskIDBlockStart        int64
	TaskIDBlockEnd          int64
	RangeID                 int64
}

func (c *fairBacklogManagerImpl) InternalStatus() []SubqueueStatus {
	currentTaskIDBlock := c.taskWriter.getCurrentTaskIDBlock()

	c.subqueueLock.Lock()
	defer c.subqueueLock.Unlock()

	c.db.Lock()
	counts := make([]int64, len(c.db.subqueues))
	for i, s := range c.db.subqueues {
		counts[i] = s.ApproximateBacklogCount
	}
	rangeID := c.db.rangeID
	c.db.Unlock()

	status := make([]SubqueueStatus, len(c.subqueues))
	for i, r := range c.subqueues {
		readLevel, ackLevel := r.getLevels()
		status[i] = SubqueueStatus{
			Priority:         int32(c.priorityBySubqueue[subqueueIndex(i)]),
			ReadLevel:        readLevel.toStore(),
			AckLevel:         ackLevel.toStore(),
			LoadedTasks:      r.getLoadedTasks(),
			TaskIDBlockStart: currentTaskIDBlock.start,
			TaskIDBlockEnd:   currentTaskIDBlock.end,
			RangeID:          rangeID,
		}
		if i < len(counts) {
			status[i].ApproximateBacklogCount = counts[i]
		}
	}
	return status
}

func (c *fairBacklogManagerImpl) respoolTaskAfterError(task *store.Task) error {
	// We cannot just remove it from persistence because then it will be lost.
	// We handle this by writing the task back to persistence with a higher taskID.
	// This will allow subsequent tasks to make progress, and hopefully by the time this task is picked-up
	// again the underlying reason for failing to start will be resolved.
	// Note the task may get written to a different subqueue than it came from.
	c.metricsHandler.Counter(metrics.TaskRewrites, 1)
	err := backoff.ThrottleRetryContext(c.tqCtx, func(context.Context) error {
		return c.SpoolTask(task)
	}, persistenceOperationRetryPolicy, isPersistenceTransient)
	if err == nil {
		return nil
	}

	// OK, we also failed to write to persistence.
	// This should only happen in very extreme cases where persistence is completely down.
	// We still can't lose the old task, so we just unload the entire task queue.
	// We haven't advanced the ack level past this task, so when the task queue reloads,
	// it will see this task again.
	c.logger.Error("Persistent store operation failure", "op", "StopTaskQueue", "error", err)
	// Skip final update since persistence is having problems.
	c.skipFinalUpdate.Store(true)
	c.pqMgr.UnloadFromPartitionManager(unloadCauseOtherError)
	return err
}

func (c *fairBacklogManagerImpl) getDB() *taskQueueDB {
	return c.db
}

func (c *fairBacklogManagerImpl) newCounterForSubqueue(subqueue subqueueIndex) counter.Counter {
	cntr := c.counterFactory()
	// restore persisted keys
	for _, entry := range c.db.getTopKFairnessKeys(subqueue) {
		_ = cntr.GetPass(entry.Key, entry.Count, 0)
	}
	return cntr
}

func (c *fairBacklogManagerImpl) setPriority(task *internalTask) {
	c.config.setDefaultPriority(task)
}

func recordDroppedTask(h metrics.Handler, reason dropReason) {
	if reason != dropReasonNone {
		h.Counter(metrics.TasksDropped, 1, metrics.Tag{Key: "reason", Value: string(reason)})
	}
}
