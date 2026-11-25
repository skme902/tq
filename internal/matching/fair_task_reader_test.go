// Ported from temporalio/temporal v1.32.2 6d085822, service/matching/fair_task_reader_test.go
// (added in #12566). gomock's MockphysicalTaskQueueManager is replaced by fakePQM, and the
// backlog manager is built with newFairBacklogManager (never started) over a testStore that the
// tests must not touch (no reads, GC effectively off).
// Copyright (c) 2025 Temporal Technologies Inc. MIT License; see UPSTREAM_LICENSE and NOTICE.

package matching

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/skme902/tq/clock"
	"github.com/skme902/tq/internal/counter"
	"github.com/skme902/tq/metrics"
	"github.com/skme902/tq/store"
)

func TestFairTaskReaderBacklogCountWithPendingWrite(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name         string
		batchSize    int
		initialCount int64
		deferWrites  bool
		writerUnpins bool
	}{
		{name: "write awaiting notification across batch boundary", batchSize: 3, initialCount: 4},
		{name: "buffered write across batch boundary", batchSize: 3, initialCount: 4, deferWrites: true},
		{name: "unpinned buffered write across batch boundary", batchSize: 3, initialCount: 4, deferWrites: true, writerUnpins: true},
		{name: "write awaiting notification with count drift", batchSize: 5, initialCount: 6},
		{name: "buffered write with count drift", batchSize: 5, initialCount: 6, deferWrites: true},
		{name: "unpinned buffered write with count drift", batchSize: 5, initialCount: 6, deferWrites: true, writerUnpins: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			reader, blm := newBacklogCountTestReader(t, tc.batchSize, tc.initialCount)
			tasks := newBacklogCountTestTasks(4)

			// The read fetched two tasks before a concurrent write persisted two more.
			// Hold notifications as the read loop does, without starting background goroutines.
			// Calling readTasksImpl directly below relies on it not starting another read: either
			// we're at the end, or loadedTasks exceeds GetTasksReloadAt (0).
			reader.getAndPinAckLevel()
			reader.readPending = true
			if tc.deferWrites {
				reader.wroteNewTasks(tasks[2:])
			}
			if tc.writerUnpins {
				reader.unpinAckLevel(nil)
			}

			reader.lock.Lock()
			reader.mergeTasksLocked(tasks[:2], mergeReadToEnd)
			reader.lock.Unlock()
			require.Equal(t, tc.initialCount, blm.db.getTotalApproximateBacklogCount(), "a short read must not overwrite the concurrent write's increment")

			// Finish the write notification and merge any buffered tasks as the read loop does.
			if tc.deferWrites {
				reader.readTasksImpl()
			} else {
				reader.readPending = false
				reader.wroteNewTasks(tasks[2:])
			}
			if !tc.writerUnpins {
				reader.unpinAckLevel(nil)
			}
			require.Equal(t, min(4, tc.batchSize), reader.getLoadedTasks())
			require.Equal(t, tc.batchSize >= 4, reader.atEnd)
			require.EqualValues(t, 4, blm.db.getTotalApproximateBacklogCount())
		})
	}
}

func TestFairTaskReaderBacklogCountAfterOutOfOrderCompletion(t *testing.T) {
	t.Parallel()

	reader, blm := newBacklogCountTestReader(t, 3, 3)
	tasks := newBacklogCountTestTasks(5)
	finish := func(id int64) {
		t.Helper()
		reader.lock.Lock()
		entry, ok := reader.outstandingTasks.Get(outstandingTask{level: fairLevel{pass: 1, id: id}})
		reader.lock.Unlock()
		require.True(t, ok)
		require.NotNil(t, entry.task)
		entry.task.finish(taskFinishResult{consumedToken: true})
	}
	write := func(task *store.TaskInfo) {
		reader.getAndPinAckLevel()
		blm.db.Lock()
		blm.db.updateBacklogStatsLocked(subqueueZero, 1, time.Time{})
		blm.db.Unlock()
		reader.wroteNewTasks([]*store.TaskInfo{task})
		reader.unpinAckLevel(nil)
	}

	reader.mergeTasks(tasks[:3], mergeReadToEnd)
	// Task 3 completes before tasks 1 and 2, so the ack level can't advance yet.
	finish(3)
	write(tasks[3])
	require.True(t, reader.atEnd)
	// The reset excludes completed task 3 from worker demand even though it remains in the DB.
	require.EqualValues(t, 3, blm.db.getTotalApproximateBacklogCount())

	// Another write exceeds the batch capacity, so subsequent acknowledgements use deltas.
	write(tasks[4])
	require.False(t, reader.atEnd)
	finish(1)
	// Until the previously excluded ack advances the ack level, the estimate may be conservative.
	require.GreaterOrEqual(t, blm.db.getTotalApproximateBacklogCount(), int64(3))
	finish(2)
	// Tasks 4 and 5 are still pending. Task 3 must not be subtracted a second time.
	require.EqualValues(t, 2, blm.db.getTotalApproximateBacklogCount())
}

// Not upstream (review r1): adminPurge writes a known count, so it must also reset the
// exclusions a previous reset recorded, or acks of tasks written after the purge are skipped.
func TestFairTaskReaderBacklogCountAfterPurge(t *testing.T) {
	t.Parallel()

	reader, blm := newBacklogCountTestReader(t, 3, 3)
	tasks := newBacklogCountTestTasks(8)
	finish := func(id int64) {
		t.Helper()
		reader.lock.Lock()
		entry, ok := reader.outstandingTasks.Get(outstandingTask{level: fairLevel{pass: 1, id: id}})
		reader.lock.Unlock()
		require.True(t, ok)
		require.NotNil(t, entry.task)
		entry.task.finish(taskFinishResult{consumedToken: true})
	}
	write := func(batch []*store.TaskInfo) {
		reader.getAndPinAckLevel()
		blm.db.Lock()
		blm.db.updateBacklogStatsLocked(subqueueZero, int64(len(batch)), time.Time{})
		blm.db.Unlock()
		reader.wroteNewTasks(batch)
		reader.unpinAckLevel(nil)
	}

	reader.mergeTasks(tasks[:3], mergeReadToEnd)
	finish(3)
	write(tasks[3:4]) // the reset after this write excludes completed task 3
	require.True(t, reader.atEnd)
	require.EqualValues(t, 3, blm.db.getTotalApproximateBacklogCount())

	_, _, err := reader.adminPurge(context.Background())
	require.NoError(t, err)
	reader.releasePurgeHold()
	require.EqualValues(t, 0, blm.db.getTotalApproximateBacklogCount())

	// Four new tasks exceed the batch, so the reader leaves the end and acks apply deltas.
	write(tasks[4:8])
	require.False(t, reader.atEnd)
	require.EqualValues(t, 4, blm.db.getTotalApproximateBacklogCount())
	finish(5)
	finish(6)
	require.EqualValues(t, 2, blm.db.getTotalApproximateBacklogCount())
}

// newBacklogCountTestReader returns a reader whose backlog manager is never started, so tests
// must not trigger reads or GC. GC is configured to effectively never run.
func newBacklogCountTestReader(t *testing.T, batchSize int, initialCount int64) (*fairTaskReader, *fairBacklogManagerImpl) {
	t.Helper()

	cfg := DefaultConfig()
	cfg.GetTasksBatchSize = batchSize
	cfg.GetTasksReloadAt = 0
	cfg.MaxTaskDeleteBatchSize = 100
	cfg.TaskDeleteInterval = time.Hour
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	pqm := &fakePQM{key: store.QueueKey{Namespace: "test-namespace", Name: "test-queue"}}
	blm := newFairBacklogManager(ctx, pqm, newTaskQueueConfig(cfg), newTestStore(), discardLogger(),
		newCaptureMetrics(), clock.NewRealTimeSource(), func() counter.Counter { return counter.NewMapCounter(1000) })
	blm.db.subqueues = []*dbSubqueue{{SubqueueInfo: store.SubqueueInfo{ApproximateBacklogCount: initialCount}}}
	return newFairTaskReader(blm, subqueueZero, fairLevel{}), blm
}

// newBacklogCountTestTasks returns tasks with ids 1..n in pass 1.
func newBacklogCountTestTasks(n int64) []*store.TaskInfo {
	tasks := make([]*store.TaskInfo, 0, n)
	for id := int64(1); id <= n; id++ {
		tasks = append(tasks, &store.TaskInfo{Pass: 1, ID: id})
	}
	return tasks
}

// Ported from upstream main 97a4129d4 (#11580, after v1.32.2): a write that fails while the
// queue unloads leaves the reader not at the end with nothing loaded, and maybeReadTasksLocked
// is a no-op once tqCtx is done. A later write on the way down must not report "fair reader
// stuck". Upstream relied on existing tests; this reproduces the sequence from its description.
func TestFairTaskReaderNoStuckReportDuringUnload(t *testing.T) {
	t.Parallel()

	reader, blm := newBacklogCountTestReader(t, 10, 0)
	reader.mergeTasks(nil, mergeReadToEnd)
	require.True(t, reader.atEnd)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	blm.tqCtx = ctx // the queue is unloading (e.g. lost its rangeID lease)

	reader.getAndPinAckLevel()
	reader.unpinAckLevel(errors.New("write outcome unknown"))
	require.False(t, reader.atEnd)

	reader.getAndPinAckLevel()
	reader.wroteNewTasks(newBacklogCountTestTasks(1))
	// The reader is in the state the check looks for: not at the end, nothing loaded (the write
	// was above readLevel and dropped), no read pending.
	require.False(t, reader.atEnd)
	require.Zero(t, reader.getLoadedTasks())
	reader.unpinAckLevel(nil)

	require.Empty(t, blm.metricsHandler.(*captureMetrics).snapshot(metrics.FairReaderStuckDetected))
}
