package taskqueue_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	taskqueue "github.com/skme902/tq"
	"github.com/skme902/tq/clock"
	"github.com/skme902/tq/metrics"
	"github.com/skme902/tq/metrics/metricstest"
	"github.com/skme902/tq/store"
	"github.com/skme902/tq/store/memstore"
)

func withClock(ts clock.TimeSource) func(*taskqueue.Options) {
	return func(o *taskqueue.Options) { o.TimeSource = ts }
}

func addTo(t *testing.T, e *taskqueue.Engine, k taskqueue.QueueKey, payload string) {
	t.Helper()
	_, err := e.AddTask(context.Background(), k, task(payload))
	require.NoError(t, err)
}

func pollFrom(t *testing.T, e *taskqueue.Engine, k taskqueue.QueueKey) *taskqueue.Delivery {
	t.Helper()
	var d *taskqueue.Delivery
	require.Eventually(t, func() bool {
		var err error
		d, err = e.PollTask(context.Background(), k, taskqueue.PollOptions{NoWait: true})
		return err == nil
	}, 5*time.Second, time.Millisecond)
	return d
}

// requireNoTask polls briefly. A NoWait poll that finds nothing waits on the engine clock
// (returnEmptyTaskTimeBudget), which a fake clock never advances, so bound it in real time.
func requireNoTask(t *testing.T, e *taskqueue.Engine, k taskqueue.QueueKey) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	d, err := e.PollTask(ctx, k, taskqueue.PollOptions{NoWait: true})
	require.Error(t, err)
	require.Nil(t, d)
}

func queueExists(t *testing.T, st store.Store, k taskqueue.QueueKey) bool {
	t.Helper()
	_, err := st.GetQueue(context.Background(), k)
	if store.IsNotFound(err) {
		return false
	}
	require.NoError(t, err)
	return true
}

// Only idle, empty, unloaded queues without dead tasks are swept; dead tasks age out with
// DeadTaskRetention; a swept queue comes back empty on its next AddTask.
func TestSweepIdleQueues(t *testing.T) {
	st := memstore.New()
	rec := metricstest.New()
	ts := clock.NewEventTimeSource().Update(time.Now())
	empty := taskqueue.QueueKey{Namespace: "ns", Name: "empty"}
	busy := taskqueue.QueueKey{Namespace: "ns", Name: "busy"}
	dead := taskqueue.QueueKey{Namespace: "ns", Name: "dead"}

	e1 := newEngine(t, st, maxAttempts(1), withClock(ts))
	addTo(t, e1, empty, "e")
	require.NoError(t, pollFrom(t, e1, empty).Ack())
	addTo(t, e1, busy, "b")
	addTo(t, e1, dead, "d")
	require.NoError(t, pollFrom(t, e1, dead).Nack())
	e1.Stop()

	e2 := newEngine(t, st, maxAttempts(1), withClock(ts), withMetrics(rec))
	ctx := context.Background()
	_, err := e2.SweepIdleQueues(ctx, taskqueue.SweepOptions{IdleFor: time.Hour})
	var ia *store.InvalidArgumentError
	require.ErrorAs(t, err, &ia, "IdleFor below MinSweepIdle")

	res, err := e2.SweepIdleQueues(ctx, taskqueue.SweepOptions{IdleFor: taskqueue.MinSweepIdle})
	require.NoError(t, err)
	require.Equal(t, 3, res.Scanned)
	require.Empty(t, res.Deleted, "nothing is idle yet")

	ts.Advance(taskqueue.MinSweepIdle + time.Minute)
	res, err = e2.SweepIdleQueues(ctx, taskqueue.SweepOptions{IdleFor: taskqueue.MinSweepIdle})
	require.NoError(t, err)
	require.Equal(t, []taskqueue.QueueKey{empty}, res.Deleted)
	require.False(t, queueExists(t, st, empty))
	require.True(t, queueExists(t, st, busy))
	require.True(t, queueExists(t, st, dead), "dead tasks keep the queue")

	res, err = e2.SweepIdleQueues(ctx, taskqueue.SweepOptions{IdleFor: taskqueue.MinSweepIdle, DeadTaskRetention: time.Hour})
	require.NoError(t, err)
	require.Equal(t, 1, res.DeadTasksPurged)
	require.Equal(t, []taskqueue.QueueKey{dead}, res.Deleted)
	require.Equal(t, int64(2), rec.CounterValue(metrics.QueuesSwept))
	require.Equal(t, int64(1), rec.CounterValue(metrics.DeadTasksExpired))

	// The busy queue still delivers; the swept one is recreated empty.
	require.Equal(t, "b", string(pollFrom(t, e2, busy).Task.Payload))
	requireNoTask(t, e2, empty)
	addTo(t, e2, empty, "again")
	require.Equal(t, "again", string(pollFrom(t, e2, empty).Task.Payload))
}

// A queue loaded on this Engine, or written since the cutoff, is never swept.
func TestSweepSkipsLoadedAndRecent(t *testing.T) {
	st := memstore.New()
	ts := clock.NewEventTimeSource().Update(time.Now())
	e1 := newEngine(t, st, nil, withClock(ts))
	addTo(t, e1, q, "x")
	require.NoError(t, pollFrom(t, e1, q).Ack())
	e1.Stop()

	// Loaded on e2 (idle unload disabled) and then 24h pass: skipped while loaded here.
	ts.Advance(taskqueue.MinSweepIdle + time.Minute)
	e2 := newEngine(t, st, func(c *taskqueue.Config) { c.MaxTaskQueueIdleTime = 1000 * time.Hour }, withClock(ts)) // see TestMaxTaskAge
	addTo(t, e2, q, "y")
	require.NoError(t, pollFrom(t, e2, q).Ack())
	require.True(t, loaded(e2, q))
	ts.Advance(taskqueue.MinSweepIdle + time.Minute)
	res, err := e2.SweepIdleQueues(context.Background(), taskqueue.SweepOptions{IdleFor: taskqueue.MinSweepIdle})
	require.NoError(t, err)
	require.Empty(t, res.Deleted)
	require.True(t, queueExists(t, st, q))

	// MaxDeletes caps a sweep.
	e2.Stop()
	e3 := newEngine(t, st, nil, withClock(ts))
	for _, n := range []string{"a", "b", "c"} {
		k := taskqueue.QueueKey{Namespace: "ns", Name: n}
		addTo(t, e3, k, n)
		require.NoError(t, pollFrom(t, e3, k).Ack())
	}
	e3.Stop()
	ts.Advance(taskqueue.MinSweepIdle + time.Minute)
	e4 := newEngine(t, st, nil, withClock(ts))
	res, err = e4.SweepIdleQueues(context.Background(), taskqueue.SweepOptions{IdleFor: taskqueue.MinSweepIdle, MaxDeletes: 2})
	require.NoError(t, err)
	require.Len(t, res.Deleted, 2)
}

// MaxTaskAge caps a new task's expiry; the per-queue value overrides the engine default.
func TestMaxTaskAge(t *testing.T) {
	ts := clock.NewEventTimeSource().Update(time.Now())
	src := taskqueue.NewMemoryQueueConfigSource(0)
	other := taskqueue.QueueKey{Namespace: "ns", Name: "other"}
	e := newEngine(t, memstore.New(), func(c *taskqueue.Config) {
		c.MaxTaskAge = time.Hour
		c.MaxTaskQueueIdleTime = 1000 * time.Hour // EventTimeSource.Advance runs an idle unload inline
	},
		withClock(ts), func(o *taskqueue.Options) { o.QueueConfigSource = src })
	none := time.Duration(0)
	require.NoError(t, src.SetMaxTaskAge(other, &none))

	addTo(t, e, q, "aged")
	later := task("explicit")
	later.ExpiryTime = ts.Now().Add(10 * time.Minute) // earlier than the cap: kept
	_, err := e.AddTask(context.Background(), q, later)
	require.NoError(t, err)
	addTo(t, e, other, "kept")

	ts.Advance(30 * time.Minute)
	d := pollFrom(t, e, q)
	require.Equal(t, "aged", string(d.Task.Payload))
	require.Equal(t, ts.Now().Add(30*time.Minute), d.Task.ExpiryTime)
	require.NoError(t, d.Nack())
	ts.Advance(31 * time.Minute)
	requireNoTask(t, e, q) // both tasks expired
	require.Equal(t, "kept", string(pollFrom(t, e, other).Task.Payload))

	neg := -time.Second
	require.Error(t, src.SetMaxTaskAge(q, &neg))
	_, err = taskqueue.New(taskqueue.Options{Store: memstore.New(), Config: taskqueue.Config{MaxTaskAge: -1}})
	require.Error(t, err)
}
