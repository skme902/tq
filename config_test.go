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

func newStoreSource(t *testing.T, st store.QueueConfigStore, rec metrics.Handler) *taskqueue.StoreQueueConfigSource {
	t.Helper()
	src, err := taskqueue.NewStoreQueueConfigSource(context.Background(), st, taskqueue.StoreQueueConfigSourceOptions{
		PollInterval: 20 * time.Millisecond, Metrics: rec,
	})
	require.NoError(t, err)
	t.Cleanup(src.Close)
	return src
}

func pause(paused bool) func(*taskqueue.QueueConfig) error {
	return func(c *taskqueue.QueueConfig) error { c.Paused = paused; return nil }
}

// A change made through one process's source applies at once there, reaches another
// process's loaded queue through the change feed, and is recorded in the history.
func TestStoreQueueConfigSourceAcrossProcesses(t *testing.T) {
	st := memstore.New()
	rec := metricstest.New()
	srcA, srcB := newStoreSource(t, st, rec), newStoreSource(t, st, nil)
	eB := newEngine(t, st, nil, withConfigSource(srcB))
	add(t, eB, task("a"))

	ctx := context.Background()
	got, err := srcA.Update(ctx, q, taskqueue.ConfigChange{Identity: "alice", Reason: "incident 1"}, pause(true))
	require.NoError(t, err)
	require.True(t, got.Paused)
	require.True(t, srcA.QueueConfig(q).Paused, "applies locally before Update returns")
	require.Eventually(t, func() bool { return srcB.QueueConfig(q).Paused }, 3*time.Second, 10*time.Millisecond)
	requireNoTask(t, eB, q)

	_, err = srcA.Update(ctx, q, taskqueue.ConfigChange{Identity: "bob", Reason: "resolved"}, pause(false))
	require.NoError(t, err)
	d := pollFrom(t, eB, q) // the watcher resumed the loaded queue
	require.Equal(t, "a", string(d.Task.Payload))
	require.NoError(t, d.Ack())

	hist, err := srcA.History(ctx, q, 10)
	require.NoError(t, err)
	require.Len(t, hist, 2)
	require.Equal(t, int64(2), hist[0].Version)
	require.Equal(t, "bob", hist[0].UpdatedBy)
	require.Equal(t, "resolved", hist[0].Reason)
	require.False(t, hist[0].Config.Paused)
	require.Equal(t, "alice", hist[1].UpdatedBy)
	require.True(t, hist[1].Config.Paused)
	require.EqualValues(t, 2, rec.CounterValue(metrics.QueueConfigUpdates))

	// A new process starts from the stored value.
	srcC := newStoreSource(t, st, nil)
	require.False(t, srcC.QueueConfig(q).Paused)
	require.Equal(t, srcA.QueueConfig(q), srcC.QueueConfig(q))
}

// Concurrent read-modify-write updates from two sources are not lost (version CAS + retry).
func TestStoreQueueConfigSourceConcurrentUpdates(t *testing.T) {
	st := memstore.New()
	srcs := []*taskqueue.StoreQueueConfigSource{newStoreSource(t, st, nil), newStoreSource(t, st, nil)}
	const n = 5
	var wg sync.WaitGroup
	errs := make(chan error, 2*n) // require in a worker goroutine does not stop the test cleanly
	for i, src := range srcs {
		for j := range n {
			wg.Add(1)
			go func() {
				defer wg.Done()
				key := string(rune('a'+i)) + string(rune('0'+j))
				_, err := src.Update(context.Background(), q, taskqueue.ConfigChange{Identity: key},
					func(c *taskqueue.QueueConfig) error {
						if c.FairnessWeightOverrides == nil {
							c.FairnessWeightOverrides = map[string]float32{}
						}
						c.FairnessWeightOverrides[key] = 2
						return nil
					})
				errs <- err
			}()
		}
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}
	for _, src := range srcs {
		require.Eventually(t, func() bool { return len(src.QueueConfig(q).FairnessWeightOverrides) == 2*n },
			3*time.Second, 10*time.Millisecond)
	}
	hist, err := srcs[0].History(context.Background(), q, 100)
	require.NoError(t, err)
	require.Len(t, hist, 2*n)
}

// Invalid configs and missing identities are rejected without writing a version; a mutate
// error aborts the update.
func TestStoreQueueConfigSourceValidation(t *testing.T) {
	st := memstore.New()
	src := newStoreSource(t, st, nil)
	ctx := context.Background()
	neg := float32(-1)
	var ia *store.InvalidArgumentError
	_, err := src.Update(ctx, q, taskqueue.ConfigChange{Identity: "x"}, func(c *taskqueue.QueueConfig) error {
		c.RateLimit = &neg
		return nil
	})
	require.ErrorAs(t, err, &ia)
	_, err = src.Update(ctx, q, taskqueue.ConfigChange{}, pause(true))
	require.ErrorAs(t, err, &ia)
	boom := context.Canceled
	_, err = src.Update(ctx, q, taskqueue.ConfigChange{Identity: "x"}, func(*taskqueue.QueueConfig) error { return boom })
	require.ErrorIs(t, err, boom)
	hist, err := src.History(ctx, q, 10)
	require.NoError(t, err)
	require.Empty(t, hist)
	require.False(t, src.QueueConfig(q).Paused)
}
