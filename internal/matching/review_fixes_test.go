package matching

// Regression tests for REVIEW_2026-11-04 M1-M3 and the Low findings that are internal.

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

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
