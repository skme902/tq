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

func withConfigSource(src taskqueue.QueueConfigSource) func(*taskqueue.Options) {
	return func(o *taskqueue.Options) { o.QueueConfigSource = src }
}

// A paused queue accepts tasks but hands none out (no sync match either); resuming wakes a
// waiting poller.
func TestPauseQueue(t *testing.T) {
	rec := metricstest.New()
	src := taskqueue.NewMemoryQueueConfigSource(0)
	e := newEngine(t, memstore.New(), nil, withConfigSource(src), withMetrics(rec))
	add(t, e, task("a"))
	src.SetPaused(q, true)
	requireNoTask(t, e, q)
	v, ok := rec.GaugeValue(metrics.QueuePaused, qTags()...)
	require.True(t, ok)
	require.Equal(t, 1.0, v)
	desc, err := e.DescribeQueue(context.Background(), q)
	require.NoError(t, err)
	require.True(t, desc.Paused)

	got := make(chan *taskqueue.Delivery, 2)
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	defer wg.Wait() // after cancel (defers run last-in first-out), before the engine cleanup
	defer cancel()
	wg.Add(1)
	go func() {
		defer wg.Done()
		for ctx.Err() == nil {
			if d, err := e.PollTask(ctx, q, taskqueue.PollOptions{}); err == nil {
				got <- d
			}
		}
	}()
	require.Eventually(t, func() bool { // a poller is waiting before the add
		desc, err := e.DescribeQueue(context.Background(), q)
		return err == nil && desc.WaitingPollers > 0
	}, 3*time.Second, time.Millisecond)
	require.False(t, add(t, e, task("b")), "no sync match while paused")
	select {
	case <-got:
		t.Fatal("delivered while paused")
	case <-time.After(200 * time.Millisecond):
	}

	src.SetPaused(q, false)
	var payloads []string
	for range 2 {
		select {
		case d := <-got:
			payloads = append(payloads, string(d.Task.Payload))
			require.NoError(t, d.Ack())
		case <-time.After(3 * time.Second):
			t.Fatal("not delivered after resume")
		}
	}
	require.ElementsMatch(t, []string{"a", "b"}, payloads)
	v, _ = rec.GaugeValue(metrics.QueuePaused, qTags()...)
	require.Equal(t, 0.0, v)
	desc, err = e.DescribeQueue(context.Background(), q)
	require.NoError(t, err)
	require.False(t, desc.Paused)
}

// A paused fairness key is skipped; other keys are dispatched; resuming the key delivers it.
func TestPauseFairnessKey(t *testing.T) {
	src := taskqueue.NewMemoryQueueConfigSource(0)
	e := newEngine(t, memstore.New(), nil, withConfigSource(src))
	require.NoError(t, src.SetFairnessKeyPaused(q, "a", true))
	for _, k := range []string{"a", "b", "a", "b"} {
		tk := task(k)
		tk.Priority = store.Priority{FairnessKey: k}
		add(t, e, tk)
	}
	for range 2 {
		d := pollFrom(t, e, q)
		require.Equal(t, "b", string(d.Task.Payload))
		require.NoError(t, d.Ack())
	}
	requireNoTask(t, e, q)
	desc, err := e.DescribeQueue(context.Background(), q)
	require.NoError(t, err)
	require.Equal(t, []string{"a"}, desc.PausedFairnessKeys)

	require.NoError(t, src.SetFairnessKeyPaused(q, "a", false))
	for range 2 {
		d := pollFrom(t, e, q)
		require.Equal(t, "a", string(d.Task.Payload))
		require.NoError(t, d.Ack())
	}
	desc, err = e.DescribeQueue(context.Background(), q)
	require.NoError(t, err)
	require.Empty(t, desc.PausedFairnessKeys)
}

// A pause set before the queue loads applies from the first poll.
func TestPausedBeforeLoad(t *testing.T) {
	src := taskqueue.NewMemoryQueueConfigSource(0)
	st := memstore.New()
	e := newEngine(t, st, nil, withConfigSource(src))
	add(t, e, task("a"))
	e.Stop()
	src.SetPaused(q, true)
	e = newEngine(t, st, nil, withConfigSource(src))
	requireNoTask(t, e, q)
	src.SetPaused(q, false)
	require.Equal(t, "a", string(pollFrom(t, e, q).Task.Payload))
}
