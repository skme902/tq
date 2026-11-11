package taskqueue_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	taskqueue "github.com/skme902/tq"
	"github.com/skme902/tq/store"
	"github.com/skme902/tq/store/memstore"
)

func putRaw(t *testing.T, st *memstore.Store, expect int64, blob string) {
	t.Helper()
	_, err := st.PutQueueConfig(context.Background(), &store.PutQueueConfigRequest{Key: q, ExpectedVersion: expect,
		Config: []byte(blob), UpdatedBy: "raw", UpdateTime: time.Now()})
	require.NoError(t, err)
}

// A bad newest version (undecodable, or invalid like a negative rate) stands for the newest
// earlier valid version on every path: a running peer, a process started afterwards, and the
// base an Update builds on (review config r2 P0, P2 validation).
func TestBadNewestConfigKeepsLastValid(t *testing.T) {
	for name, blob := range map[string]string{"undecodable": "{bad", "invalid": `{"RateLimit":-1}`} {
		t.Run(name, func(t *testing.T) {
			st := memstore.New()
			ctx := context.Background()
			a := manualStoreSource(t, st)
			_, err := a.Update(ctx, q, taskqueue.ConfigChange{Identity: "a"}, pause(true))
			require.NoError(t, err)
			putRaw(t, st, 1, blob)
			require.NoError(t, a.Refresh(ctx))
			require.True(t, a.QueueConfig(q).Paused, "running peer keeps the last valid config")
			b := manualStoreSource(t, st)
			require.True(t, b.QueueConfig(q).Paused, "a new process serves the last valid config, not defaults")
			got, err := b.Update(ctx, q, taskqueue.ConfigChange{Identity: "b"}, func(*taskqueue.QueueConfig) error { return nil })
			require.NoError(t, err)
			require.True(t, got.Paused, "Update builds on the last valid config")
		})
	}
}

// Rewriting an unchanged config does not notify watchers, so loaded queues do not reprocess
// every task (review config r2 P1).
func TestUnchangedRewriteDoesNotNotify(t *testing.T) {
	st := memstore.New()
	ctx := context.Background()
	a, b := manualStoreSource(t, st), manualStoreSource(t, st)
	_, err := a.Update(ctx, q, taskqueue.ConfigChange{Identity: "a"}, pause(true))
	require.NoError(t, err)
	require.NoError(t, b.Refresh(ctx))
	var na, nb atomic.Int32
	defer a.WatchQueueConfig(q, func() { na.Add(1) })()
	defer b.WatchQueueConfig(q, func() { nb.Add(1) })()
	_, err = a.Update(ctx, q, taskqueue.ConfigChange{Identity: "a"}, pause(true))
	require.NoError(t, err)
	require.NoError(t, b.Refresh(ctx))
	require.Zero(t, na.Load())
	require.Zero(t, nb.Load())
	_, err = a.Update(ctx, q, taskqueue.ConfigChange{Identity: "a"}, pause(false))
	require.NoError(t, err)
	require.NoError(t, b.Refresh(ctx))
	require.EqualValues(t, 1, na.Load())
	require.EqualValues(t, 1, nb.Load())
}

// Close called from a watcher that the poll goroutine is running returns instead of waiting
// for that goroutine (review config r2 P1).
func TestCloseFromPollerWatcher(t *testing.T) {
	st := memstore.New()
	ctx := context.Background()
	a := manualStoreSource(t, st)
	b, err := taskqueue.NewStoreQueueConfigSource(ctx, st, taskqueue.StoreQueueConfigSourceOptions{PollInterval: 10 * time.Millisecond})
	require.NoError(t, err)
	closed := make(chan struct{})
	b.WatchQueueConfig(q, func() { b.Close(); close(closed) })
	_, err = a.Update(ctx, q, taskqueue.ConfigChange{Identity: "a"}, pause(true))
	require.NoError(t, err)
	select {
	case <-closed:
	case <-time.After(5 * time.Second):
		t.Fatal("Close from the poller's watcher deadlocked")
	}
	b.Close() // idempotent, and the poller has stopped or is stopping
}

// A panicking watcher does not kill the poll goroutine: later changes still arrive.
func TestPanickingWatcherKeepsPolling(t *testing.T) {
	st := memstore.New()
	ctx := context.Background()
	a := manualStoreSource(t, st)
	b, err := taskqueue.NewStoreQueueConfigSource(ctx, st, taskqueue.StoreQueueConfigSourceOptions{PollInterval: 10 * time.Millisecond})
	require.NoError(t, err)
	t.Cleanup(b.Close)
	b.WatchQueueConfig(q, func() { panic("watcher bug") })
	_, err = a.Update(ctx, q, taskqueue.ConfigChange{Identity: "a"}, pause(true))
	require.NoError(t, err)
	require.Eventually(t, func() bool { return b.QueueConfig(q).Paused }, 5*time.Second, 5*time.Millisecond)
	_, err = a.Update(ctx, q, taskqueue.ConfigChange{Identity: "a"}, pause(false))
	require.NoError(t, err)
	require.Eventually(t, func() bool { return !b.QueueConfig(q).Paused }, 5*time.Second, 5*time.Millisecond)
}
