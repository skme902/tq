package matching

// Regression tests for REVIEW_2026-11-04 M1-M3 and the Low findings that are internal.

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/skme902/tq/clock"
	"github.com/skme902/tq/store"
)

// M1: a sync match that completes after Stop snapshotted the sync leases must not leave an
// in-memory-only task behind: either the poller is refused and the task is spooled, or the
// lease is re-spooled.
func TestStopRaceSyncMatchNotLost(t *testing.T) {
	ts := newTestStore()
	e := newPortEngine(t, ts, nil)
	pm, err := e.getTaskQueuePartitionManager(context.Background(), portQueue, true)
	require.NoError(t, err)

	type pollResult struct {
		d   *Delivery
		err error
	}
	polled := make(chan pollResult, 1)
	go func() {
		d, err := e.PollTask(context.Background(), portQueue, PollOptions{LeaseDuration: time.Hour})
		polled <- pollResult{d, err}
	}()
	require.Eventually(t, func() bool { return pm.defaultQueue.priMatcher.HasWaitingPoller() }, 5*time.Second, time.Millisecond)

	var addMatched bool
	var addErr error
	var res pollResult
	e.testHookStopAfterSyncLeases = func() {
		addMatched, addErr = pm.AddTask(context.Background(), &store.Task{Payload: []byte("x")}, nil)
		res = <-polled
	}
	e.Stop()

	if addErr != nil {
		return // the producer knows the task was not accepted
	}
	if res.err == nil && addMatched {
		require.Equal(t, 1, ts.TotalTaskCount(portQueue),
			"sync-matched task handed out after Stop's lease snapshot is neither spooled nor re-spooled")
		return
	}
	require.Error(t, res.err)
	require.Equal(t, 1, ts.TotalTaskCount(portQueue), "refused sync match must be spooled")
}

func newFakeClockEngine(t *testing.T, ts *testStore) (*Engine, *clock.EventTimeSource) {
	t.Helper()
	src := clock.NewEventTimeSource().Update(time.Now())
	src.UseAsyncTimers(true) // timer callbacks (idle unload) may block on other goroutines
	e, err := NewEngine(Options{Config: DefaultConfig(), Store: ts, TimeSource: src, Logger: discardLogger()})
	require.NoError(t, err)
	t.Cleanup(e.Stop)
	return e, src
}

func pollBacklogTask(t *testing.T, e *Engine, lease time.Duration) *Delivery {
	t.Helper()
	matched, err := e.AddTask(context.Background(), portQueue, store.Task{Payload: []byte("x")})
	require.NoError(t, err)
	require.False(t, matched)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	d, err := e.PollTask(ctx, portQueue, PollOptions{LeaseDuration: lease})
	require.NoError(t, err)
	return d
}

// M2: an expiry callback that was already dispatched when ExtendLease won the lock must not
// settle the extended lease; a non-positive extension is rejected.
func TestExtendLeaseBeatsStaleExpiry(t *testing.T) {
	e, src := newFakeClockEngine(t, newTestStore())
	d := pollBacklogTask(t, e, time.Minute)

	src.Advance(30 * time.Second)
	require.NoError(t, d.ExtendLease(10*time.Minute))
	d.expire() // the old timer's callback, scheduled before the extension
	require.Equal(t, src.Now().Add(10*time.Minute), d.LeaseDeadline())

	require.Error(t, d.ExtendLease(0))
	require.Error(t, d.ExtendLease(-time.Second))

	src.Advance(9 * time.Minute)
	require.NoError(t, d.ExtendLease(time.Minute), "lease still live")
	src.Advance(2 * time.Minute)
	require.Eventually(t, func() bool {
		d.mu.Lock()
		defer d.mu.Unlock()
		return d.settled
	}, 5*time.Second, time.Millisecond, "extended lease expires on its new deadline")
	require.ErrorIs(t, d.Ack(), ErrLeaseLost)
}

type countingWatcher struct {
	mu     sync.Mutex
	active int
}

func (w *countingWatcher) QueueConfig(store.QueueKey) QueueConfig { return QueueConfig{} }
func (w *countingWatcher) WatchQueueConfig(store.QueueKey, func()) func() {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.active++
	return func() {
		w.mu.Lock()
		defer w.mu.Unlock()
		w.active--
	}
}

// M3: a partition published to e.partitions but stopped (engine Stop or unload) before its
// Start ran must stay stopped: Start is then a no-op, and waiters fail fast.
func TestPartitionStartAfterStopIsNoop(t *testing.T) {
	w := &countingWatcher{}
	e, err := NewEngine(Options{Config: DefaultConfig(), Store: newTestStore(), TimeSource: clock.NewRealTimeSource(),
		Logger: discardLogger(), QueueConfigSource: w})
	require.NoError(t, err)
	t.Cleanup(e.Stop)

	pm := newTaskQueuePartitionManager(e, portQueue, newTaskQueueConfig(e.config))
	pm.Stop(unloadCauseShuttingDown) // the window between publish and Start
	pm.Start()

	require.Equal(t, daemonStatusStopped, atomic.LoadInt32(&pm.defaultQueue.status), "physical queue started after Stop")
	w.mu.Lock()
	require.Zero(t, w.active, "config watch registered after Stop")
	w.mu.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	require.Error(t, pm.WaitUntilInitialized(ctx))
	require.NoError(t, ctx.Err(), "WaitUntilInitialized on a stopped queue must not wait for the caller's deadline")
}
