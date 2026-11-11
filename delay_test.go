package taskqueue_test

import (
	"context"
	"fmt"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	taskqueue "github.com/skme902/tq"
	"github.com/skme902/tq/clock"
	"github.com/skme902/tq/store"
	"github.com/skme902/tq/store/memstore"
)

// fakeClockEngine runs on ts with idle unload off (EventTimeSource.Advance runs an idle
// unload inline) and a long lease, so tests can advance through delays.
func fakeClockEngine(t *testing.T, st store.Store, ts *clock.EventTimeSource, mod func(*taskqueue.Config)) *taskqueue.Engine {
	t.Helper()
	return newEngine(t, st, func(c *taskqueue.Config) {
		c.MaxTaskQueueIdleTime = 1000 * time.Hour
		c.DeliveryLeaseDuration = 1000 * time.Hour
		if mod != nil {
			mod(c)
		}
	}, withClock(ts))
}

// pollAdvancing polls until a task arrives, nudging the fake clock: the reader may arm a
// delay timer just after an Advance (it reads the clock first), and such a timer only fires
// on a later Advance.
func pollAdvancing(t *testing.T, e *taskqueue.Engine, ts *clock.EventTimeSource) *taskqueue.Delivery {
	t.Helper()
	var d *taskqueue.Delivery
	require.Eventually(t, func() bool {
		ts.Advance(time.Millisecond)
		var err error
		d, err = e.PollTask(context.Background(), q, taskqueue.PollOptions{NoWait: true})
		return err == nil
	}, 5*time.Second, time.Millisecond)
	return d
}

func peekOne(t *testing.T, e *taskqueue.Engine) taskqueue.PeekedTask {
	t.Helper()
	p := peekAll(t, e, taskqueue.PeekOptions{})
	require.Len(t, p, 1)
	return p[0]
}

// A Nack with a delay holds the task back until then, writes VisibleAt to its row, and the
// delay survives a reload.
func TestNackDelay(t *testing.T) {
	st := memstore.New()
	ts := clock.NewEventTimeSource().Update(time.Now())
	e := fakeClockEngine(t, st, ts, nil)
	add(t, e, task("a"))
	d := pollFrom(t, e, q)
	require.NoError(t, d.Nack(taskqueue.NackDelay(time.Minute)))
	requireNoTask(t, e, q)
	require.Equal(t, ts.Now().Add(time.Minute), peekOne(t, e).Task.VisibleAt)

	e.Stop()
	e = fakeClockEngine(t, st, ts, nil)
	requireNoTask(t, e, q)
	ts.Advance(time.Minute + time.Second)
	d = pollAdvancing(t, e, ts)
	require.Equal(t, "a", string(d.Task.Payload))
	require.EqualValues(t, 2, d.Task.Attempts)
	require.NoError(t, d.Ack())
}

// RetryBackoff grows with the attempt count and is capped; NackDelay(0) overrides it.
func TestRetryBackoff(t *testing.T) {
	ts := clock.NewEventTimeSource().Update(time.Now())
	e := fakeClockEngine(t, memstore.New(), ts, func(c *taskqueue.Config) {
		c.RetryBackoff = taskqueue.RetryBackoff{Initial: 10 * time.Second, Max: 25 * time.Second}
	})
	add(t, e, task("a"))
	for _, want := range []time.Duration{10 * time.Second, 20 * time.Second, 25 * time.Second} {
		require.NoError(t, pollFrom(t, e, q).Nack())
		ts.Advance(want - time.Second)
		requireNoTask(t, e, q)
		ts.Advance(time.Second)
	}
	require.NoError(t, pollFrom(t, e, q).Nack(taskqueue.NackDelay(0)))
	require.NoError(t, pollFrom(t, e, q).Ack())

	_, err := taskqueue.New(taskqueue.Options{Store: memstore.New(), Config: taskqueue.Config{
		RetryBackoff: taskqueue.RetryBackoff{Initial: time.Second, Multiplier: 0.5}}})
	require.Error(t, err)
}

// A lease expiry uses the backoff too. Real clock: the lease timer callback reads the clock,
// which an EventTimeSource would deadlock on.
func TestRetryBackoffOnExpiry(t *testing.T) {
	e := newEngine(t, memstore.New(), func(c *taskqueue.Config) {
		c.RetryBackoff = taskqueue.RetryBackoff{Initial: 2 * time.Second}
		c.DeliveryLeaseDuration = 100 * time.Millisecond
	})
	add(t, e, task("a"))
	d := pollFrom(t, e, q)
	expired := time.Now()
	time.Sleep(300 * time.Millisecond)
	require.ErrorIs(t, d.Ack(), taskqueue.ErrLeaseLost)
	requireNoTask(t, e, q)
	d = pollFrom(t, e, q)
	require.Equal(t, "a", string(d.Task.Payload))
	require.GreaterOrEqual(t, time.Since(expired), 1900*time.Millisecond)
}

// An AddTask with a future VisibleAt never sync-matches, even with a poller waiting.
func TestDelayedAddTask(t *testing.T) {
	ts := clock.NewEventTimeSource().Update(time.Now())
	e := fakeClockEngine(t, memstore.New(), ts, nil)
	got := make(chan *taskqueue.Delivery, 1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		// Advance also expires the long poll (fake clock), so poll again until a task.
		for ctx.Err() == nil {
			if d, err := e.PollTask(ctx, q, taskqueue.PollOptions{}); err == nil {
				got <- d
				return
			}
		}
	}()
	require.Eventually(t, func() bool { return loaded(e, q) }, 3*time.Second, time.Millisecond)
	tk := task("later")
	tk.VisibleAt = ts.Now().Add(time.Minute)
	require.False(t, add(t, e, tk), "not sync-matched")
	select {
	case <-got:
		t.Fatal("delivered before VisibleAt")
	case <-time.After(200 * time.Millisecond):
	}
	ts.Advance(time.Minute)
	select {
	case d := <-got:
		require.Equal(t, "later", string(d.Task.Payload))
	case <-time.After(3 * time.Second):
		t.Fatal("not delivered after VisibleAt")
	}
}

// A sync-matched task nacked with a delay is spooled with VisibleAt. Real clock: a sync
// match needs a poller already waiting, which a fake clock makes awkward.
func TestNackDelaySyncMatched(t *testing.T) {
	e := newEngine(t, memstore.New(), nil)
	got := make(chan *taskqueue.Delivery, 1)
	go func() {
		d, err := e.PollTask(context.Background(), q, taskqueue.PollOptions{})
		if err == nil {
			got <- d
		}
	}()
	require.Eventually(t, func() bool { return loaded(e, q) }, 3*time.Second, time.Millisecond)
	time.Sleep(100 * time.Millisecond) // let the poll reach the matcher
	require.True(t, add(t, e, task("s")), "sync-matched")
	d := <-got
	require.True(t, d.SyncMatched)
	nacked := time.Now()
	require.NoError(t, d.Nack(taskqueue.NackDelay(time.Second)))
	requireNoTask(t, e, q)
	d = pollFrom(t, e, q)
	require.Equal(t, "s", string(d.Task.Payload))
	require.False(t, d.SyncMatched)
	require.GreaterOrEqual(t, time.Since(nacked), 900*time.Millisecond)
}

// Delayed tasks at the head of the backlog don't stop the reader from loading ready ones
// behind them (up to GetTasksBatchSize delayed tasks are held without counting).
func TestDelayedDoesNotBlockReady(t *testing.T) {
	ts := clock.NewEventTimeSource().Update(time.Now())
	st := memstore.New()
	e := fakeClockEngine(t, st, ts, func(c *taskqueue.Config) {
		c.GetTasksBatchSize = 4
		c.GetTasksReloadAt = 1
	})
	for range 6 {
		tk := task("delayed")
		tk.VisibleAt = ts.Now().Add(time.Hour)
		add(t, e, tk)
	}
	add(t, e, task("ready"))
	e.Stop()
	e = fakeClockEngine(t, st, ts, func(c *taskqueue.Config) {
		c.GetTasksBatchSize = 4
		c.GetTasksReloadAt = 1
	})
	d := pollFrom(t, e, q)
	require.Equal(t, "ready", string(d.Task.Payload))
	require.NoError(t, d.Ack())
	requireNoTask(t, e, q)
	ts.Advance(time.Hour)
	for range 6 {
		d := pollFrom(t, e, q)
		require.Equal(t, "delayed", string(d.Task.Payload))
		require.NoError(t, d.Ack())
	}
}

// Past MaxDelayedInMemory, delayed tasks count as loaded, so they can hold back a ready
// task behind them (the documented limit). Deleting a delayed task keeps the reader's counts
// sound and it never comes back; trimmed delayed tasks are delayed again on re-read.
func TestDelayedTasksLimitAndDelete(t *testing.T) {
	h := &softAssertHandler{}
	ts := clock.NewEventTimeSource().Update(time.Now())
	e := newEngine(t, memstore.New(), func(c *taskqueue.Config) {
		c.MaxTaskQueueIdleTime = 1000 * time.Hour
		c.GetTasksBatchSize = 3
		c.GetTasksReloadAt = 1
		c.MaxDelayedInMemory = 4
	}, withClock(ts), func(o *taskqueue.Options) { o.Logger = slog.New(h) })
	for i := range 8 {
		tk := task(fmt.Sprint("d", i))
		tk.VisibleAt = ts.Now().Add(time.Hour)
		add(t, e, tk)
	}
	add(t, e, task("ready"))
	requireNoTask(t, e, q) // 4 free + 4 counted (> batch 3): "ready" is not loaded yet

	var ref taskqueue.TaskRef
	for _, p := range peekAll(t, e, taskqueue.PeekOptions{}) {
		if string(p.Task.Payload) == "d0" {
			ref = p.Ref
		}
	}
	ok, err := e.DeleteTask(context.Background(), q, ref)
	require.NoError(t, err)
	require.True(t, ok)
	ts.Advance(time.Hour)
	var got []string
	for range 8 {
		d := pollFrom(t, e, q)
		got = append(got, string(d.Task.Payload))
		require.NoError(t, d.Ack())
	}
	require.ElementsMatch(t, []string{"d1", "d2", "d3", "d4", "d5", "d6", "d7", "ready"}, got)
	requireNoTask(t, e, q)
	h.mu.Lock()
	defer h.mu.Unlock()
	require.Empty(t, h.msgs)
}
