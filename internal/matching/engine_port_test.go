// Ported from temporalio/temporal v1.32.1 af08a5e5, service/matching/matching_engine_test.go
// (fair suite): TestAddConsumeWorkflowTasksNoDBErrors, TestAddConsumeWorkflowTasksDBErrors,
// TestConcurrentAddWorkflowTasksNoDBErrors (via addConsumeAllWorkflowTasksNonConcurrently and
// concurrentPublishAndConsumeValidateBacklogCounter). History/RecordTaskStarted mocks are not
// needed (no history service); a poll is Ack'ed immediately, which is upstream's implicit ack.
// Copyright (c) 2025 Temporal Technologies Inc. MIT License; see UPSTREAM_LICENSE and NOTICE.

package matching

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/skme902/tq/clock"
	"github.com/skme902/tq/store"
)

var portQueue = store.QueueKey{Namespace: "nsid", Name: "makeToast"}

func newPortEngine(t *testing.T, ts *testStore, mut func(*Config)) *Engine {
	t.Helper()
	cfg := DefaultConfig()
	cfg.LongPollExpirationInterval = 100 * time.Millisecond // upstream defaultTestConfig
	cfg.MaxTaskDeleteBatchSize = 1
	if mut != nil {
		mut(&cfg)
	}
	e, err := NewEngine(Options{Config: cfg, Store: ts, TimeSource: clock.NewRealTimeSource(), Logger: discardLogger()})
	require.NoError(t, err)
	t.Cleanup(e.Stop)
	return e
}

func portPQM(t *testing.T, e *Engine) *physicalTaskQueueManagerImpl {
	t.Helper()
	pm, err := e.getTaskQueuePartitionManager(context.Background(), portQueue, true)
	require.NoError(t, err)
	return pm.defaultQueue
}

func portAddTask(t *testing.T, e *Engine) {
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		_, err := e.AddTask(context.Background(), portQueue, store.Task{Payload: []byte("x")})
		require.NoError(c, err)
	}, 10*time.Second, time.Millisecond, "failed to add task")
}

func portPollTask(t *testing.T, e *Engine) {
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		d, err := e.PollTask(context.Background(), portQueue, PollOptions{Identity: "nobody"})
		require.NoError(c, err) // DB could have failed while fetching tasks; try again
		require.NoError(c, d.Ack())
	}, 3*time.Second, time.Millisecond, "failed to poll task")
}

func addConsumeAllTasksNonConcurrently(t *testing.T, ts *testStore, taskCount int) {
	e := newPortEngine(t, ts, nil)
	for range taskCount {
		portAddTask(t, e)
	}
	_, creates := ts.counts()
	require.Equal(t, taskCount, creates)
	require.Equal(t, taskCount, ts.TotalTaskCount(portQueue))

	pqm := portPQM(t, e)
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	require.NoError(t, pqm.WaitUntilInitialized(ctx))
	cancel()
	// Relaxed for fairBacklogManager as upstream: it can reset backlog count on read.
	// Not upstream: wait for it. An injected CreateTasks condition failure unloads the queue, and
	// the reloaded one starts from the last persisted (stale) count until its reader's first
	// GetTasks (which may itself be retrying injected faults) resets it; WaitUntilInitialized
	// does not wait for that read, so under load an immediate check races it.
	// No writes happen while waiting, so the queue is not unloaded again. Read the loaded
	// partition directly: getTaskQueuePartitionManager would unload it on an init timeout.
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		e.partitionsLock.RLock()
		pm := e.partitions[portQueue]
		e.partitionsLock.RUnlock()
		if !assert.NotNil(c, pm, "queue not loaded") {
			return
		}
		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()
		if !assert.NoError(c, pm.defaultQueue.WaitUntilInitialized(ctx)) {
			return
		}
		assert.InDelta(c, taskCount, totalApproximateBacklogCount(pm.defaultQueue.backlogMgr), 2)
	}, 10*time.Second, 10*time.Millisecond)

	for range taskCount {
		portPollTask(t, e)
	}
	require.LessOrEqual(t, int64(0), totalApproximateBacklogCount(portPQM(t, e).backlogMgr))
}

func TestPortAddConsumeTasksNoDBErrors(t *testing.T) {
	t.Parallel()
	addConsumeAllTasksNonConcurrently(t, newTestStore(), 200)
}

func TestPortAddConsumeTasksDBErrors(t *testing.T) {
	t.Parallel()
	ts := newTestStore()
	ts.addFault("CreateTasks", &store.ConditionFailedError{Msg: "injected"}, 0.1)
	ts.addFault("GetTasks", &store.TransientError{Err: context.DeadlineExceeded}, 0.1)
	addConsumeAllTasksNonConcurrently(t, ts, 200)
}

func TestPortConcurrentAddTasksNoDBErrors(t *testing.T) {
	t.Parallel()
	const numWorkers, tasksToAdd = 150, 100
	ts := newTestStore()
	e := newPortEngine(t, ts, func(c *Config) {
		c.LongPollExpirationInterval = 10 * time.Millisecond
		c.UpdateAckInterval = time.Millisecond
	})
	var wg sync.WaitGroup
	for range numWorkers {
		wg.Go(func() {
			for range tasksToAdd {
				portAddTask(t, e)
			}
		})
	}
	wg.Wait()

	pqm := portPQM(t, e)
	backlogCount := totalApproximateBacklogCount(pqm.backlogMgr)
	db := pqm.backlogMgr.getDB()
	db.Lock()
	ackLevel := fairLevelFromStore(db.subqueues[subqueueZero].FairAckLevel)
	db.Unlock()
	resp, err := ts.Store.GetTasks(context.Background(), &store.GetTasksRequest{
		Key: portQueue, InclusiveMin: fairLevel{pass: max(ackLevel.pass, 1), id: ackLevel.id + 1}.toStore(),
		PageSize: 1 << 30,
	})
	require.NoError(t, err)
	require.InDelta(t, int64(len(resp.Tasks)), backlogCount, 2)
	require.Equal(t, numWorkers*tasksToAdd, len(resp.Tasks))
}
