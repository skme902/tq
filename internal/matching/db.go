// Ported from temporalio/temporal v1.32.1 af08a5e5, service/matching/db.go
// Changes: fair-path only (classic CreateTasks/GetTasks/ack level and draining/migration and
// partition scale state removed); protos replaced by store types; backlog gauges are the
// unversioned (non-attribution) ones; no task lag gauge; injected clock.
// Copyright (c) 2025 Temporal Technologies Inc. MIT License; see UPSTREAM_LICENSE and NOTICE.

package matching

import (
	"cmp"
	"context"
	"fmt"
	"log/slog"
	"slices"
	"sync"
	"time"

	"github.com/skme902/tq/clock"
	"github.com/skme902/tq/internal/counter"
	"github.com/skme902/tq/metrics"
	"github.com/skme902/tq/store"
)

const (
	initialRangeID = 1 // Id of the first range of a new task queue

	// Subqueue zero corresponds to "the queue" before migrating metadata to subqueues.
	subqueueZero = subqueueIndex(0)

	// persistenceTTL replaces Partition().PersistenceTTL(): normal partitions have no TTL.
	persistenceTTL = time.Duration(0)
)

type (
	taskQueueDB struct {
		// constants
		config         *taskQueueConfig
		queue          store.QueueKey
		store          store.Store
		logger         *slog.Logger
		metricsHandler metrics.Handler
		timeSource     clock.TimeSource

		// mutable
		sync.Mutex
		rangeID   int64
		subqueues []*dbSubqueue

		// used to avoid unnecessary metadata writes:
		// Deviation: upstream compares lastChange/lastWrite timestamps. With an injected clock
		// (and on coarse wall clocks) a change and a write can share a timestamp, and the change
		// is then never written. A change counter is exact: every change and write is under db.Lock.
		changeSeq  uint64    // incremented when metadata is changed in memory
		writtenSeq uint64    // changeSeq as of the last successful metadata write
		lastWrite  time.Time // updated when metadata is successfully written to db
	}

	dbSubqueue struct {
		store.SubqueueInfo
		oldestTime time.Time // time of oldest task if backlog, otherwise zero time
	}

	taskQueueState struct {
		rangeID   int64
		subqueues []store.SubqueueInfo
	}

	subqueueIndex int

	createFairTasksResponse map[subqueueIndex][]*store.TaskInfo // subqueue -> tasks
)

// newTaskQueueDB returns an instance of an object that represents
// persistence view of a physical task queue. All mutations / reads to queues
// wrt persistence go through this object.
//
// This class will serialize writes to persistence that do condition updates, to provide the
// guarantee that there is only one writer who updates queue in persistence at any given point
// in time.
func newTaskQueueDB(
	config *taskQueueConfig,
	st store.Store,
	queue store.QueueKey,
	logger *slog.Logger,
	metricsHandler metrics.Handler,
	timeSource clock.TimeSource,
) *taskQueueDB {
	return &taskQueueDB{
		config:         config,
		queue:          queue,
		store:          st,
		logger:         logger,
		metricsHandler: metricsHandler,
		timeSource:     timeSource,
	}
}

// RangeID returns the current persistence view of rangeID
func (db *taskQueueDB) RangeID() int64 {
	db.Lock()
	defer db.Unlock()
	return db.rangeID
}

// GetMaxFairReadLevel returns the current max read level of a subqueue.
func (db *taskQueueDB) GetMaxFairReadLevel(subqueue subqueueIndex) fairLevel {
	db.Lock()
	defer db.Unlock()
	return fairLevelFromStore(db.subqueues[subqueue].FairMaxReadLevel)
}

// RenewLease renews the lease on a taskqueue. If there is no previous lease,
// this method will attempt to steal taskqueue from current owner
func (db *taskQueueDB) RenewLease(
	ctx context.Context,
) (taskQueueState, error) {
	db.Lock()
	defer db.Unlock()

	if db.rangeID == 0 {
		if err := db.takeOverTaskQueueLocked(ctx); err != nil {
			return taskQueueState{}, err
		}
	} else {
		if err := db.updateTaskQueueLocked(ctx, true); err != nil {
			return taskQueueState{}, err
		}
	}
	return taskQueueState{
		rangeID:   db.rangeID,
		subqueues: db.cloneSubqueues(),
	}, nil
}

func (db *taskQueueDB) takeOverTaskQueueLocked(
	ctx context.Context,
) error {
	response, err := db.store.GetQueue(ctx, db.queue)
	switch {
	case err == nil:
		db.rangeID = response.RangeID
		db.subqueues = db.ensureDefaultSubqueuesLocked(response.Info.Subqueues)
		err := db.updateTaskQueueLocked(ctx, true)
		if err != nil {
			db.rangeID = 0
			return err
		}
		db.lastWrite, db.writtenSeq = db.timeSource.Now(), db.changeSeq
		return nil

	case store.IsNotFound(err):
		db.rangeID = initialRangeID
		db.subqueues = db.ensureDefaultSubqueuesLocked(nil)
		if err := db.store.CreateQueue(ctx, &store.CreateQueueRequest{
			RangeID: db.rangeID,
			Info:    db.cachedQueueInfo(),
		}); err != nil {
			db.rangeID = 0
			return err
		}
		db.lastWrite, db.writtenSeq = db.timeSource.Now(), db.changeSeq
		softAssert(db.logger, db.subqueues[0].FairMaxReadLevel == (store.Level{}), "should have maxReadLevel 0 here")
		softAssert(db.logger, db.subqueues[0].FairAckLevel == (store.Level{}), "should have ackLevel 0 here")
		return nil

	default:
		return err
	}
}

func (db *taskQueueDB) updateTaskQueueLocked(ctx context.Context, incrementRangeId bool) error {
	newRangeID := db.rangeID
	if incrementRangeId {
		newRangeID++
	}
	if err := db.store.UpdateQueue(ctx, &store.UpdateQueueRequest{
		RangeID:     newRangeID,
		Info:        db.cachedQueueInfo(),
		PrevRangeID: db.rangeID,
	}); err != nil {
		return err
	}
	db.lastWrite, db.writtenSeq = db.timeSource.Now(), db.changeSeq
	db.rangeID = newRangeID
	return nil
}

// shouldUpdateMetadataOnAppendLocked returns whether a task append should also write the
// metadata blob. Caller must hold db.Mutex.
func (db *taskQueueDB) shouldUpdateMetadataOnAppendLocked() bool {
	interval := db.config.MetadataUpdateOnAppendInterval
	return interval <= 0 || db.timeSource.Since(db.lastWrite) >= interval
}

func (db *taskQueueDB) SyncState(ctx context.Context) error {
	db.Lock()
	defer db.Unlock()
	defer db.emitPhysicalBacklogGaugesLocked()

	// We only need to write if something changed, or if we're past half of the persistence TTL.
	ttl := min(24*time.Hour, cmp.Or(persistenceTTL, 24*time.Hour))
	needWrite := db.changeSeq != db.writtenSeq || db.timeSource.Since(db.lastWrite) > ttl/2
	if !needWrite {
		// If we don't write, though, we wouldn't know if someone else has stolen ownership
		// momentarily. So instead, do a (cheaper) read to just check the range id.
		return db.verifyOwnershipLocked(ctx)
	}

	return db.updateTaskQueueLocked(ctx, false)
}

func (db *taskQueueDB) verifyOwnershipLocked(ctx context.Context) error {
	response, err := db.store.GetQueue(ctx, db.queue)
	if err != nil {
		if store.IsNotFound(err) {
			return &store.ConditionFailedError{Msg: "task queue ownership lost: row deleted"}
		}
		return err
	}
	if response.RangeID != db.rangeID {
		return &store.ConditionFailedError{
			Msg: fmt.Sprintf("task queue ownership lost: stored rangeID %d, in-memory rangeID %d",
				response.RangeID, db.rangeID),
		}
	}
	return nil
}

func (db *taskQueueDB) updateFairAckLevel(subqueue subqueueIndex, newAckLevel fairLevel, countDelta, knownCount int64, oldestTime time.Time) {
	db.Lock()
	defer db.Unlock()

	db.changeSeq++
	dbQueue := db.subqueues[subqueue]
	if prev := fairLevelFromStore(dbQueue.FairAckLevel); newAckLevel.less(prev) {
		softAssert(db.logger, false, fmt.Sprintf(
			"ack level in subqueue %d should not move backwards: cur %v new %v", subqueue, prev, newAckLevel))
	}
	dbQueue.FairAckLevel = newAckLevel.toStore()

	if knownCount >= 0 {
		// Reset approximateBacklogCount to fix the count divergence issue
		dbQueue.ApproximateBacklogCount = knownCount
		dbQueue.oldestTime = oldestTime
	} else if countDelta != 0 {
		db.updateBacklogStatsLocked(subqueue, countDelta, oldestTime)
	}
}

// Use this to reset ApproximateBacklogCount when the backlog count is known, e.g. when you're
// read to the end of the backlog.
func (db *taskQueueDB) setKnownFairBacklogCount(subqueue subqueueIndex, count int64) {
	db.Lock()
	defer db.Unlock()

	if db.subqueues[subqueue].ApproximateBacklogCount != count {
		db.changeSeq++
		db.subqueues[subqueue].ApproximateBacklogCount = count
		if count == 0 {
			db.subqueues[subqueue].oldestTime = time.Time{}
		}
	}
}

func (db *taskQueueDB) updateBacklogStatsLocked(subqueue subqueueIndex, countDelta int64, oldestTime time.Time) {
	// Prevent under-counting
	count := &db.subqueues[subqueue].ApproximateBacklogCount
	if *count+countDelta < 0 {
		db.logger.Info("ApproximateBacklogCount could have under-counted.")
		*count = 0
	} else {
		*count += countDelta
	}
	db.subqueues[subqueue].oldestTime = oldestTime
}

func (db *taskQueueDB) persistTopKFairnessKeys(subqueue subqueueIndex, entries []counter.TopKEntry) {
	db.Lock()
	defer db.Unlock()

	counts := make([]store.FairnessKeyCount, len(entries))
	for i, entry := range entries {
		counts[i] = store.FairnessKeyCount{Key: entry.Key, Count: entry.Count}
	}

	db.subqueues[subqueue].TopKFairnessCounts = counts
	db.changeSeq++
}

func (db *taskQueueDB) getTopKFairnessKeys(subqueue subqueueIndex) []counter.TopKEntry {
	db.Lock()
	defer db.Unlock()

	if subqueue >= subqueueIndex(len(db.subqueues)) {
		return nil
	}
	counts := db.subqueues[subqueue].TopKFairnessCounts
	entries := make([]counter.TopKEntry, len(counts))
	for i, count := range counts {
		entries[i] = counter.TopKEntry{Key: count.Key, Count: count.Count}
	}
	return entries
}

func (db *taskQueueDB) getTotalApproximateBacklogCount() int64 {
	db.Lock()
	defer db.Unlock()

	var total int64
	for _, s := range db.subqueues {
		total += s.ApproximateBacklogCount
	}
	return total
}

// CreateFairTasks creates a batch of given tasks for this task queue
func (db *taskQueueDB) CreateFairTasks(
	ctx context.Context,
	reqs []*writeTaskRequest,
) (createFairTasksResponse, error) {
	db.Lock()
	defer db.Unlock()

	if len(reqs) == 0 {
		return nil, nil
	}

	newTasks := make(createFairTasksResponse)
	newMaxLevel := make(map[subqueueIndex]fairLevel)
	allTasks := make([]store.TaskInfo, len(reqs))
	allSubqueues := make([]int, len(reqs))
	for i, req := range reqs {
		task := &store.TaskInfo{
			ID:   req.id,
			Pass: req.pass,
			Data: *req.taskInfo,
		}
		allTasks[i] = *task
		allSubqueues[i] = int(req.subqueue)
		newTasks[req.subqueue] = append(newTasks[req.subqueue], task)
		newMaxLevel[req.subqueue] = newMaxLevel[req.subqueue].max(req.fairLevel)
	}

	for sq, tasks := range newTasks {
		db.subqueues[sq].ApproximateBacklogCount += int64(len(tasks))
	}

	// Unlike in CreateTasks, we can set the persisted FairMaxReadLevel before persisting.
	// The max read level is not used by fairTaskReader, so there's no correctness issue.
	for sq, level := range newMaxLevel {
		db.subqueues[sq].FairMaxReadLevel = fairLevelFromStore(db.subqueues[sq].FairMaxReadLevel).max(level).toStore()
	}

	updateMetadata := db.shouldUpdateMetadataOnAppendLocked()

	resp, err := db.store.CreateTasks(ctx, &store.CreateTasksRequest{
		RangeID:        db.rangeID,
		Info:           db.cachedQueueInfo(),
		Tasks:          allTasks,
		Subqueues:      allSubqueues,
		UpdateMetadata: updateMetadata,
	})

	if err == nil {
		// Only update lastWrite for persistence implementations that update metadata on CreateTasks,
		// otherwise we have a change to ApproximateBacklogCount we need to write.
		if resp.UpdatedMetadata {
			db.lastWrite, db.writtenSeq = db.timeSource.Now(), db.changeSeq
		} else {
			db.changeSeq++
		}
	} else if writeDefinitelyFailed(err) {
		// Tasks definitely were not created, restore the counter.
		for i, tasks := range newTasks {
			db.subqueues[i].ApproximateBacklogCount -= int64(len(tasks))
		}
	}
	return newTasks, err
}

// writeDefinitelyFailed returns whether an error from a CreateTasks call indicates the tasks
// were definitely not persisted.
func writeDefinitelyFailed(err error) bool {
	if store.IsConditionFailed(err) {
		return true
	}
	if re, ok := err.(*store.ResourceExhaustedError); ok && re.DefinitelyNotWritten {
		return true
	}
	return false
}

// GetFairTasks returns a batch of tasks after the given level
func (db *taskQueueDB) GetFairTasks(
	ctx context.Context,
	subqueue subqueueIndex,
	inclusiveMinLevel fairLevel,
	batchSize int,
) (*store.GetTasksResponse, error) {
	return db.store.GetTasks(ctx, &store.GetTasksRequest{
		Key:          db.queue,
		Subqueue:     int(subqueue),
		InclusiveMin: inclusiveMinLevel.toStore(),
		PageSize:     batchSize,
	})
}

// CompleteFairTasksLessThan deletes of tasks less than the given level. Limit is
// the upper bound of number of tasks that can be deleted by this method. It may
// or may not be honored
func (db *taskQueueDB) CompleteFairTasksLessThan(
	ctx context.Context,
	exclusiveMaxLevel fairLevel,
	limit int,
	subqueue subqueueIndex,
) (int, error) {
	// For fair tasks, we do an LWT to avoid issues with lost ownership, therefore we should
	// serialize this with other LWTs by holding db.lock to avoid timeouts or conflicts.
	db.Lock()
	defer db.Unlock()

	n, err := db.store.CompleteTasksLessThan(ctx, &store.CompleteTasksLessThanRequest{
		Key:          db.queue,
		ExclusiveMax: exclusiveMaxLevel.toStore(),
		Subqueue:     int(subqueue),
		Limit:        limit,
		// We might have lost ownership without knowing it. A new owner assigns passes
		// starting from the ack level it loaded, which may be below our in-memory ack level,
		// so an unconditional delete could delete the new owner's tasks.
		ConditionRangeID: db.rangeID,
	})
	// ConditionFailedError means we lost ownership: not a store failure, caller will unload.
	if err != nil && !store.IsConditionFailed(err) {
		db.logger.Error("Persistent store operation failure",
			"op", "CompleteTasksLessThan", "error", err, "ackLevel", exclusiveMaxLevel.String())
	}
	return n, err
}

func (db *taskQueueDB) AllocateSubqueue(
	ctx context.Context,
	key store.SubqueueKey,
) ([]store.SubqueueInfo, error) {
	db.Lock()
	defer db.Unlock()

	newSubqueue := db.newSubqueueLocked(key)
	db.subqueues = append(db.subqueues, newSubqueue)

	// ensure written to metadata before returning
	err := db.updateTaskQueueLocked(ctx, false)
	if err != nil {
		// If this was a conflict, caller will shut down partition. Otherwise, we don't know
		// for sure if this write made it to persistence or not. We should forget about the new
		// subqueue and let a future call to AllocateSubqueue add it again.
		db.subqueues = db.subqueues[:len(db.subqueues)-1]
		return nil, err
	}

	return db.cloneSubqueues(), nil
}

func (db *taskQueueDB) expiryTime() time.Time {
	if ttl := persistenceTTL; ttl > 0 {
		return db.timeSource.Now().Add(ttl)
	}
	return time.Time{}
}

func (db *taskQueueDB) cachedQueueInfo() *store.QueueInfo {
	infos := make([]store.SubqueueInfo, len(db.subqueues))
	for i := range db.subqueues {
		infos[i] = db.subqueues[i].SubqueueInfo
		infos[i].TopKFairnessCounts = slices.Clone(infos[i].TopKFairnessCounts)
	}
	return &store.QueueInfo{
		Key:            db.queue,
		Subqueues:      infos,
		LastUpdateTime: db.timeSource.Now().UTC(),
		ExpiryTime:     db.expiryTime(),
	}
}

// emitPhysicalBacklogGaugesLocked is upstream's without versioning: the backlog count per
// task_priority and the backlog age, named physical_* when the periodic (logical) emission is
// on and unprefixed when it is off, only when the queue has its real taskqueue tag.
// task_lag_per_tl is not ported (upstream marks it a poor estimate).
func (db *taskQueueDB) emitPhysicalBacklogGaugesLocked() {
	if !db.config.BreakdownMetricsByTaskQueue {
		return
	}
	countGauge, ageGauge := db.backlogGaugeNames()
	var oldestTime time.Time
	counts := make(map[int32]int64)
	for _, s := range db.subqueues {
		counts[s.Key.Priority] += s.ApproximateBacklogCount
		oldestTime = minNonZeroTime(oldestTime, s.oldestTime)
	}
	for priority, count := range counts {
		db.metricsHandler.Gauge(countGauge, float64(count), metrics.TaskPriorityTag(priority))
	}
	if oldestTime.IsZero() {
		db.metricsHandler.Gauge(ageGauge, 0)
	} else {
		db.metricsHandler.Gauge(ageGauge, db.timeSource.Since(oldestTime).Seconds())
	}
}

func (db *taskQueueDB) backlogGaugeNames() (count, age string) {
	if db.config.BacklogMetricsEmitInterval > 0 { // upstream attributionEnabled
		return metrics.PhysicalApproximateBacklogCount, metrics.PhysicalApproximateBacklogAgeSeconds
	}
	return metrics.ApproximateBacklogCount, metrics.ApproximateBacklogAgeSeconds
}

// emitZeroPhysicalBacklogGauges is upstream's: called when the queue unloads so stale values
// do not persist.
func (db *taskQueueDB) emitZeroPhysicalBacklogGauges() {
	if !db.config.BreakdownMetricsByTaskQueue {
		return
	}
	priorities := make(map[int32]struct{})
	db.Lock()
	for _, s := range db.subqueues {
		priorities[s.Key.Priority] = struct{}{}
	}
	db.Unlock()
	countGauge, ageGauge := db.backlogGaugeNames()
	for k := range priorities {
		db.metricsHandler.Gauge(countGauge, 0, metrics.TaskPriorityTag(k))
	}
	db.metricsHandler.Gauge(ageGauge, 0)
}

// getApproximateBacklogCountsBySubqueue is upstream's.
func (db *taskQueueDB) getApproximateBacklogCountsBySubqueue() []int64 {
	db.Lock()
	defer db.Unlock()
	result := make([]int64, len(db.subqueues))
	for id, s := range db.subqueues {
		result[id] = s.ApproximateBacklogCount
	}
	return result
}

func minNonZeroTime(a, b time.Time) time.Time {
	switch {
	case a.IsZero():
		return b
	case b.IsZero():
		return a
	case a.Before(b):
		return a
	default:
		return b
	}
}

func (db *taskQueueDB) ensureDefaultSubqueuesLocked(infos []store.SubqueueInfo) []*dbSubqueue {
	subqueues := make([]*dbSubqueue, len(infos))
	for i, info := range infos {
		info.TopKFairnessCounts = slices.Clone(info.TopKFairnessCounts)
		subqueues[i] = &dbSubqueue{SubqueueInfo: info}
	}

	// check for default priority and add if not present (this may be initializing subqueue 0)
	defKey := store.SubqueueKey{Priority: int32(db.config.DefaultPriorityKey)}
	hasDefault := slices.ContainsFunc(subqueues, func(s *dbSubqueue) bool {
		return s.Key == defKey
	})
	if !hasDefault {
		subqueues = append(subqueues, db.newSubqueueLocked(defKey))
	}
	return subqueues
}

func (db *taskQueueDB) newSubqueueLocked(key store.SubqueueKey) *dbSubqueue {
	// For fair queues: ack level and max read level start at zero.
	s := &dbSubqueue{}
	s.Key = key
	return s
}

// clone db.subqueues so we can return it outside our lock
func (db *taskQueueDB) cloneSubqueues() []store.SubqueueInfo {
	infos := make([]store.SubqueueInfo, len(db.subqueues))
	for i := range db.subqueues {
		infos[i] = db.subqueues[i].SubqueueInfo
		infos[i].TopKFairnessCounts = slices.Clone(infos[i].TopKFairnessCounts)
	}
	return infos
}
