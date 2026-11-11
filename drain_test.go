package taskqueue_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	taskqueue "github.com/skme902/tq"
	"github.com/skme902/tq/metrics"
	"github.com/skme902/tq/metrics/metricstest"
	"github.com/skme902/tq/store/memstore"
)

func shutdownAsync(e *taskqueue.Engine, ctx context.Context) <-chan error {
	done := make(chan error, 1)
	go func() { done <- e.Shutdown(ctx) }()
	return done
}

func requireNotDone(t *testing.T, done <-chan error) {
	t.Helper()
	select {
	case err := <-done:
		t.Fatalf("Shutdown returned early: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
}

func TestShutdownWaitsForLeases(t *testing.T) {
	rec := metricstest.New()
	st := memstore.New()
	e := newEngine(t, st, nil, withMetrics(rec))
	ctx := context.Background()

	add(t, e, task("a"))
	add(t, e, task("b"))
	d1, d2 := poll(t, e), poll(t, e)

	// A poll that is already waiting when the drain starts ends with ErrEngineDraining.
	waiting := make(chan error, 1)
	go func() {
		_, err := e.PollTask(ctx, q, taskqueue.PollOptions{})
		waiting <- err
	}()
	require.Eventually(t, func() bool { return e.WaitingPollers(q) == 1 }, 2*time.Second, time.Millisecond)

	done := shutdownAsync(e, ctx)
	require.ErrorIs(t, <-waiting, taskqueue.ErrEngineDraining)
	require.True(t, e.Draining())
	v, _ := rec.GaugeValue(metrics.EngineDraining)
	require.Equal(t, 1.0, v)

	// New polls are refused; adds still work and cannot sync-match.
	_, err := e.PollTask(ctx, q, taskqueue.PollOptions{})
	require.ErrorIs(t, err, taskqueue.ErrEngineDraining)
	require.False(t, add(t, e, task("c")))

	requireNotDone(t, done)
	require.NoError(t, d1.Ack())
	requireNotDone(t, done)
	// Nack during a drain puts the task back (no poller can take it) and settles the lease.
	require.NoError(t, d2.Nack())
	require.NoError(t, <-done)

	_, err = e.PollTask(ctx, q, taskqueue.PollOptions{})
	require.ErrorIs(t, err, taskqueue.ErrEngineStopped)
	v, _ = rec.GaugeValue(metrics.EngineDraining)
	require.Equal(t, 0.0, v)
	v, _ = rec.GaugeValue(metrics.EngineDrainLeases)
	require.Equal(t, 0.0, v)
	require.Len(t, rec.TimerValues(metrics.EngineDrainDuration), 1)

	// b (nacked) and c (spooled during the drain) are delivered by the next engine.
	e2 := newEngine(t, st, nil)
	got := map[string]bool{}
	for range 2 {
		d := poll(t, e2)
		got[string(d.Task.Payload)] = true
		require.NoError(t, d.Ack())
	}
	require.Equal(t, map[string]bool{"b": true, "c": true}, got)
}

func TestShutdownDeadlineStopsWithLeases(t *testing.T) {
	rec := metricstest.New()
	st := memstore.New()
	e := newEngine(t, st, nil, withMetrics(rec))
	ctx := context.Background()

	add(t, e, task("backlog"))
	backlog := poll(t, e)
	// A sync-matched lease: it exists only in memory, so Stop must spool it.
	got := make(chan *taskqueue.Delivery, 1)
	go func() {
		d, err := e.PollTask(ctx, q, taskqueue.PollOptions{})
		if err == nil {
			got <- d
		}
	}()
	require.Eventually(t, func() bool { return e.WaitingPollers(q) == 1 }, 2*time.Second, time.Millisecond)
	require.True(t, add(t, e, task("sync")))
	syncD := <-got
	require.True(t, syncD.SyncMatched)

	dctx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancel()
	err := e.Shutdown(dctx)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	v, _ := rec.GaugeValue(metrics.EngineDrainLeases)
	require.Equal(t, 2.0, v)
	require.ErrorIs(t, backlog.Ack(), taskqueue.ErrEngineStopped)
	require.ErrorIs(t, syncD.Ack(), taskqueue.ErrEngineStopped)

	e2 := newEngine(t, st, nil)
	seen := map[string]bool{}
	for range 2 {
		d := poll(t, e2)
		seen[string(d.Task.Payload)] = true
		require.NoError(t, d.Ack())
	}
	require.Equal(t, map[string]bool{"backlog": true, "sync": true}, seen)
}

func TestShutdownIdempotent(t *testing.T) {
	e := newEngine(t, memstore.New(), nil)
	add(t, e, task("a"))
	d := poll(t, e)
	ctx := context.Background()
	d1, d2 := shutdownAsync(e, ctx), shutdownAsync(e, ctx)
	requireNotDone(t, d1)
	require.NoError(t, d.Ack())
	require.NoError(t, <-d1)
	require.NoError(t, <-d2)
	require.NoError(t, e.Shutdown(ctx)) // after Stop
	e.Stop()

	e3 := newEngine(t, memstore.New(), nil)
	e3.Stop()
	require.NoError(t, e3.Shutdown(ctx))
	require.False(t, e3.Draining())
}

// A lease that ends by expiry during the drain also completes it.
func TestShutdownLeaseExpiry(t *testing.T) {
	e := newEngine(t, memstore.New(), nil)
	add(t, e, task("a"))
	d, err := e.PollTask(context.Background(), q, taskqueue.PollOptions{LeaseDuration: 100 * time.Millisecond})
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	require.NoError(t, e.Shutdown(ctx))
	require.True(t, errors.Is(d.Ack(), taskqueue.ErrLeaseLost) || errors.Is(d.Ack(), taskqueue.ErrEngineStopped))
}
