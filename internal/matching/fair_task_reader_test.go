// Ported from temporalio/temporal v1.32.2 6d085822, service/matching/fair_task_reader_test.go
// (added in #12566). gomock's MockphysicalTaskQueueManager is replaced by fakePQM, and the
// backlog manager is built with newFairBacklogManager (never started) over a testStore that the
// tests must not touch (no reads, GC effectively off).
// Copyright (c) 2025 Temporal Technologies Inc. MIT License; see UPSTREAM_LICENSE and NOTICE.

package matching

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/skme902/tq/clock"
	"github.com/skme902/tq/internal/counter"
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
