package taskqueue_test

// Regression tests for Grok Build review round 1 on feat/ops-backoff.

import (
	"context"
	"math"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	taskqueue "github.com/skme902/tq"
	"github.com/skme902/tq/clock"
	"github.com/skme902/tq/store/memstore"
)

// longTimerClock wraps a fake clock and tracks the timers armed for at least an hour (only a
// delay timer is that long here) and whether each was stopped.
type longTimerClock struct {
	*clock.EventTimeSource
	mu   sync.Mutex
	long []*trackedTimer
}

type trackedTimer struct {
	clock.Timer
	stopped atomic.Bool
}

func (t *trackedTimer) Stop() bool { t.stopped.Store(true); return t.Timer.Stop() }

func (c *longTimerClock) AfterFunc(d time.Duration, f func()) clock.Timer {
	t := c.EventTimeSource.AfterFunc(d, f)
	if d < time.Hour {
		return t
	}
	tt := &trackedTimer{Timer: t}
	c.mu.Lock()
	c.long = append(c.long, tt)
	c.mu.Unlock()
	return tt
}

// Stopping the engine (or unloading a queue) stops delay timers, so they do not keep the queue
// and engine reachable until VisibleAt (review backoff r1 P0).
func TestStopReleasesDelayTimers(t *testing.T) {
	ts := &longTimerClock{EventTimeSource: clock.NewEventTimeSource().Update(time.Unix(1_700_000_000, 0))}
	e := newEngine(t, memstore.New(), nil, withClock(ts))
	k := task("later")
	k.VisibleAt = ts.Now().Add(1000 * time.Hour)
	_, err := e.AddTask(context.Background(), q, k)
	require.NoError(t, err)
	addTo(t, e, q, "ready")
	require.NoError(t, pollFrom(t, e, q).Ack()) // the reader has loaded the delayed task
	require.Eventually(t, func() bool {         // the timer is registered asynchronously
		ts.mu.Lock()
		defer ts.mu.Unlock()
		return len(ts.long) == 1
	}, 3*time.Second, time.Millisecond, "the delayed task is on a timer")
	e.Stop()
	ts.mu.Lock()
	defer ts.mu.Unlock()
	require.True(t, ts.long[0].stopped.Load(), "no delay timer survives Stop")
}

// Non-finite RetryBackoff multipliers and a negative MaxDelayedInMemory are rejected
// (review backoff r1 P2).
func TestBackoffConfigValidation(t *testing.T) {
	for _, m := range []float64{math.NaN(), math.Inf(1)} {
		_, err := taskqueue.New(taskqueue.Options{Store: memstore.New(), Config: taskqueue.Config{
			RetryBackoff: taskqueue.RetryBackoff{Initial: time.Second, Multiplier: m}}})
		require.Error(t, err, "multiplier %v", m)
	}
	_, err := taskqueue.New(taskqueue.Options{Store: memstore.New(), Config: taskqueue.Config{MaxDelayedInMemory: -1}})
	require.Error(t, err)
}
