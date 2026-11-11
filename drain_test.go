package taskqueue_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	taskqueue "github.com/skme902/tq"
	"github.com/skme902/tq/metrics"
	"github.com/skme902/tq/metrics/metricstest"
	"github.com/skme902/tq/store"
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

// A lease that ends by expiry during the drain also completes it, and the task is
// delivered again by the next engine (the expiry redeliver runs before the lease is dropped).
func TestShutdownLeaseExpiry(t *testing.T) {
	st := memstore.New()
	ts, withClock := fakeClock()
	e := newEngine(t, st, nil, withClock)
	add(t, e, task("a"))
	d, err := e.PollTask(context.Background(), q, taskqueue.PollOptions{LeaseDuration: time.Second})
	require.NoError(t, err)

	done := shutdownAsync(e, context.Background())
	require.Eventually(t, e.Draining, 2*time.Second, time.Millisecond)
	ts.Advance(time.Second)

	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("shutdown did not return after lease expiry")
	}
	require.ErrorIs(t, d.Ack(), taskqueue.ErrLeaseLost)
	requireDelivered(t, st, "a")
}

// A1: acking the last backlog lease during Shutdown must persist the ack before the
// drain wakes, so the next engine does not deliver the task again.
func TestShutdownAckLastBacklogLeaseNotRedelivered(t *testing.T) {
	for range 50 {
		st := memstore.New()
		e := newEngine(t, st, nil)
		add(t, e, task("once"))
		d := poll(t, e)
		done := shutdownAsync(e, context.Background())
		require.Eventually(t, e.Draining, 2*time.Second, time.Millisecond)
		requireNotDone(t, done)
		require.NoError(t, d.Ack())
		require.NoError(t, <-done)
		requireNotDelivered(t, st)
	}
}

// A1: nacking the last sync-matched lease during Shutdown spools it before the drain
// wakes, so the next engine delivers it.
func TestShutdownNackSyncMatchIsDelivered(t *testing.T) {
	for range 50 {
		st := memstore.New()
		e := newEngine(t, st, nil)
		d := syncMatch(t, e, "sync-nack")
		done := shutdownAsync(e, context.Background())
		require.Eventually(t, e.Draining, 2*time.Second, time.Millisecond)
		requireNotDone(t, done)
		require.NoError(t, d.Nack())
		require.NoError(t, <-done)
		requireDelivered(t, st, "sync-nack")
	}
}

// A1: expiry of the last sync-matched lease during Shutdown spools it before the
// lease is dropped, so the next engine delivers it.
func TestShutdownExpireSyncMatchIsDelivered(t *testing.T) {
	for range 20 {
		st := memstore.New()
		ts, withClock := fakeClock()
		e := newEngine(t, st, nil, withClock)
		d := syncMatchLease(t, e, "sync-exp", time.Second)
		done := shutdownAsync(e, context.Background())
		require.Eventually(t, e.Draining, 2*time.Second, time.Millisecond)
		requireNotDone(t, done)
		ts.Advance(time.Second)
		select {
		case err := <-done:
			require.NoError(t, err)
		case <-time.After(5 * time.Second):
			t.Fatal("shutdown did not return after sync-match expiry")
		}
		require.ErrorIs(t, d.Ack(), taskqueue.ErrLeaseLost)
		requireDelivered(t, st, "sync-exp")
	}
}

// A2: Shutdown waits for a PollTask blocked in Starter, then for the lease that poll
// creates. The acked task is not delivered by the next engine.
func TestShutdownWaitsForInFlightPoll(t *testing.T) {
	st := memstore.New()
	started := make(chan struct{})
	release := make(chan struct{})
	starter := starterFunc(func(context.Context, *taskqueue.StartRequest) error {
		close(started)
		<-release
		return nil
	})
	e := newEngine(t, st, nil, func(o *taskqueue.Options) { o.Starter = starter })
	add(t, e, task("held"))

	type pollResult struct {
		d   *taskqueue.Delivery
		err error
	}
	polled := make(chan pollResult, 1)
	go func() {
		d, err := e.PollTask(context.Background(), q, taskqueue.PollOptions{})
		polled <- pollResult{d, err}
	}()
	select {
	case <-started:
	case <-time.After(3 * time.Second):
		t.Fatal("starter was not called")
	}

	done := shutdownAsync(e, context.Background())
	requireNotDone(t, done)
	close(release)

	var res pollResult
	select {
	case res = <-polled:
	case <-time.After(3 * time.Second):
		t.Fatal("poll did not return after starter")
	}
	require.NoError(t, res.err)
	requireNotDone(t, done)
	require.NoError(t, res.d.Ack())
	require.NoError(t, <-done)
	requireNotDelivered(t, st)
}

// A3: concurrent Shutdown callers share the result of the one that stops.
func TestShutdownConcurrentCallersShareDeadline(t *testing.T) {
	rec := metricstest.New()
	st := memstore.New()
	e := newEngine(t, st, nil, withMetrics(rec))
	add(t, e, task("held"))
	d := poll(t, e)

	short, cancelShort := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancelShort()
	long, cancelLong := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelLong()
	shortDone := shutdownAsync(e, short)
	longDone := shutdownAsync(e, long)
	requireNotDone(t, shortDone)
	requireNotDone(t, longDone)

	require.ErrorIs(t, <-shortDone, context.DeadlineExceeded)
	require.ErrorIs(t, <-longDone, context.DeadlineExceeded)
	require.ErrorIs(t, d.Ack(), taskqueue.ErrEngineStopped)
	require.Len(t, rec.TimerValues(metrics.EngineDrainDuration), 1)
	v, _ := rec.GaugeValue(metrics.EngineDrainLeases)
	require.Equal(t, 1.0, v)
}

// A5: a poll blocked in partition load is an in-flight poll, and drain ends it.
func TestShutdownEndsPollBlockedInPartitionLoad(t *testing.T) {
	g := &gateStore{Store: memstore.New(), started: make(chan struct{}), release: make(chan struct{})}
	t.Cleanup(func() { close(g.release) })
	e := newEngine(t, g, nil)

	pollErr := make(chan error, 1)
	go func() {
		_, err := e.PollTask(context.Background(), q, taskqueue.PollOptions{})
		pollErr <- err
	}()
	select {
	case <-g.started:
	case <-time.After(3 * time.Second):
		t.Fatal("poll did not reach partition load")
	}

	sctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	shutErr := shutdownAsync(e, sctx)
	select {
	case err := <-pollErr:
		require.ErrorIs(t, err, taskqueue.ErrEngineDraining)
	case <-time.After(2 * time.Second):
		t.Fatal("poll blocked in partition load was not ended by drain")
	}
	require.NoError(t, <-shutErr)
}

// callerDoneCtx reports a done error without closing Done, so a poll stays parked
// until drain cancels the child context.
type callerDoneCtx struct {
	context.Context
	doneErr error
}

func (c callerDoneCtx) Err() error {
	if c.doneErr != nil {
		return c.doneErr
	}
	return c.Context.Err()
}

// A5: a waiting poll that ends during a drain returns ErrNoTasks when the caller's own
// ctx is already done, which is what it would return with no drain.
func TestDrainPollWithCallerCtxDoneReturnsNoTasks(t *testing.T) {
	e := newEngine(t, memstore.New(), nil)
	ctx := callerDoneCtx{Context: context.Background(), doneErr: context.DeadlineExceeded}
	waiting := make(chan error, 1)
	go func() {
		_, err := e.PollTask(ctx, q, taskqueue.PollOptions{})
		waiting <- err
	}()
	require.Eventually(t, func() bool { return e.WaitingPollers(q) == 1 }, 2*time.Second, time.Millisecond)
	require.NoError(t, e.Shutdown(context.Background()))
	select {
	case err := <-waiting:
		require.ErrorIs(t, err, taskqueue.ErrNoTasks)
	case <-time.After(3 * time.Second):
		t.Fatal("poll did not return")
	}
}

func syncMatch(t *testing.T, e *taskqueue.Engine, payload string) *taskqueue.Delivery {
	t.Helper()
	return syncMatchLease(t, e, payload, 0)
}

func syncMatchLease(t *testing.T, e *taskqueue.Engine, payload string, lease time.Duration) *taskqueue.Delivery {
	t.Helper()
	got := make(chan *taskqueue.Delivery, 1)
	go func() {
		d, err := e.PollTask(context.Background(), q, taskqueue.PollOptions{LeaseDuration: lease})
		if err == nil {
			got <- d
		} else {
			got <- nil
		}
	}()
	require.Eventually(t, func() bool { return e.WaitingPollers(q) == 1 }, 2*time.Second, time.Millisecond)
	require.True(t, add(t, e, task(payload)))
	d := <-got
	require.NotNil(t, d)
	require.True(t, d.SyncMatched)
	return d
}

func requireDelivered(t *testing.T, st *memstore.Store, payload string) {
	t.Helper()
	e := newEngine(t, st, nil)
	d := poll(t, e)
	require.Equal(t, payload, string(d.Task.Payload))
	require.NoError(t, d.Ack())
	e.Stop()
}

func requireNotDelivered(t *testing.T, st *memstore.Store) {
	t.Helper()
	e := newEngine(t, st, nil)
	_, err := e.PollTask(context.Background(), q, taskqueue.PollOptions{NoWait: true})
	require.ErrorIs(t, err, taskqueue.ErrNoTasks)
	e.Stop()
}

// gateStore blocks the first GetQueue until ctx is done or release is closed, so a poll
// can be caught inside partition load.
type gateStore struct {
	store.Store
	started chan struct{}
	release chan struct{}
	once    sync.Once
}

func (g *gateStore) GetQueue(ctx context.Context, key store.QueueKey) (*store.GetQueueResponse, error) {
	g.once.Do(func() { close(g.started) })
	select {
	case <-g.release:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	return g.Store.GetQueue(ctx, key)
}
