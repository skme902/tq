// Ported from temporalio/temporal v1.32.1 af08a5e5, service/matching/physical_task_queue_manager_test.go
// Kept (fair suite): TestCheckIdleTaskQueue (minus the pollerHistory part), TestTQMDoesFinalUpdateOnIdleUnload,
// TestTQMDoesNotDoFinalUpdateOnOwnershipLost, TestTQMInterruptsPollOnClose. Dropped:
// TestReaderSignaling/TestReaderBacklogAge/TestLegacyDescribeTaskQueue (classic backlog manager or
// describe API), TestAddTaskStandby (multi-cluster), TestPollScaling* (poller scaling decisions not
// implemented), TestDrainCompletionNoReloadDraining (classic->fair drain migration not implemented).
// upstream's testTaskManager update counter is replaced by countingStore.
// Copyright (c) 2025 Temporal Technologies Inc. MIT License; see UPSTREAM_LICENSE and NOTICE.

package matching

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/skme902/tq/clock"
	"github.com/skme902/tq/store"
	"github.com/skme902/tq/store/memstore"
)

type countingStore struct {
	*memstore.Store
	updates atomic.Int64
}

func (s *countingStore) UpdateQueue(ctx context.Context, req *store.UpdateQueueRequest) error {
	s.updates.Add(1)
	return s.Store.UpdateQueue(ctx, req)
}

func newPTQMTest(t *testing.T, mut func(*Config)) (*Engine, *countingStore, *physicalTaskQueueManagerImpl) {
	t.Helper()
	cs := &countingStore{Store: memstore.New()}
	cfg := DefaultConfig()
	if mut != nil {
		mut(&cfg)
	}
	e, err := NewEngine(Options{Config: cfg, Store: cs, TimeSource: clock.NewRealTimeSource(), Logger: discardLogger()})
	require.NoError(t, err)
	t.Cleanup(e.Stop)
	pm, err := e.getTaskQueuePartitionManager(context.Background(), store.QueueKey{Namespace: "ns", Name: "tq"}, true)
	require.NoError(t, err)
	return e, cs, pm.defaultQueue
}

func TestCheckIdleTaskQueue(t *testing.T) {
	t.Parallel()
	_, _, tqMgr := newPTQMTest(t, nil)
	time.Sleep(50 * time.Millisecond) //nolint:forbidigo
	require.Equal(t, daemonStatusStarted, atomic.LoadInt32(&tqMgr.status))
	tqMgr.Stop(unloadCauseShuttingDown)
	require.Equal(t, daemonStatusStopped, atomic.LoadInt32(&tqMgr.status))
}

func TestTQMDoesFinalUpdateOnIdleUnload(t *testing.T) {
	t.Parallel()
	e, cs, tqMgr := newPTQMTest(t, func(c *Config) { c.MaxTaskQueueIdleTime = time.Second })
	baseline := cs.updates.Load() // 0 for a fresh queue (created with CreateQueue), as upstream
	require.Eventually(t, func() bool {
		// will unload due to idleness and do one final update
		return cs.updates.Load() == baseline+1
	}, 5*time.Second, 50*time.Millisecond)
	require.Eventually(t, func() bool {
		e.partitionsLock.RLock()
		defer e.partitionsLock.RUnlock()
		_, loaded := e.partitions[tqMgr.queue]
		return !loaded
	}, time.Second, 10*time.Millisecond)
	require.Equal(t, int64(0), baseline)
	require.Equal(t, baseline+1, cs.updates.Load(), "expected exactly one final update on idle unload")
}

func TestTQMDoesNotDoFinalUpdateOnOwnershipLost(t *testing.T) {
	t.Parallel()
	_, cs, tqMgr := newPTQMTest(t, func(c *Config) { c.UpdateAckInterval = 100 * time.Millisecond })
	baseline := cs.updates.Load()

	// simulate stolen lock (directly on the inner store, so it is not counted)
	ctx := context.Background()
	resp, err := cs.Store.GetQueue(ctx, tqMgr.queue)
	require.NoError(t, err)
	require.NoError(t, cs.Store.UpdateQueue(ctx, &store.UpdateQueueRequest{
		PrevRangeID: resp.RangeID, RangeID: resp.RangeID + 1, Info: resp.Info,
	}))

	// change something to ensure it does the periodic write
	tqMgr.backlogMgr.getDB().updateFairAckLevel(0, fairLevel{pass: 0, id: 123456}, 10, -1, time.Time{})

	// on the next periodic write, it'll fail due to conflict and unload the task queue
	require.Eventually(t, func() bool { return tqMgr.tqCtx.Err() != nil }, 5*time.Second, 20*time.Millisecond)

	// no additional updates beyond the failed periodic write; in particular, no final update
	// is done during shutdown after losing ownership.
	require.Equal(t, baseline+1, cs.updates.Load())
}

func TestTQMInterruptsPollOnClose(t *testing.T) {
	t.Parallel()
	_, _, tqMgr := newPTQMTest(t, nil)

	pollStart := time.Now()
	pollCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	done := make(chan struct{})
	go func() {
		defer close(done)
		_, _ = tqMgr.PollTask(pollCtx, &pollMetadata{identity: "p"})
	}()
	time.Sleep(20 * time.Millisecond) //nolint:forbidigo // let the poll block

	tqMgr.Stop(unloadCauseUnspecified) // should interrupt poller

	<-done
	require.Less(t, time.Since(pollStart), 4*time.Second)
}
