package taskqueue_test

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	taskqueue "github.com/skme902/tq"
	"github.com/skme902/tq/store"
	"github.com/skme902/tq/store/memstore"
)

func manualStoreSource(t *testing.T, st *memstore.Store) *taskqueue.StoreQueueConfigSource {
	t.Helper()
	src, err := taskqueue.NewStoreQueueConfigSource(context.Background(), st, taskqueue.StoreQueueConfigSourceOptions{PollInterval: time.Hour})
	require.NoError(t, err)
	t.Cleanup(src.Close)
	return src
}

// A peer that missed a resume and a new pause moves straight to the newest version: its
// watchers never see the superseded resume (review config r1 P0).
func TestRefreshSkipsSupersededVersions(t *testing.T) {
	st := memstore.New()
	ctx := context.Background()
	a, b := manualStoreSource(t, st), manualStoreSource(t, st)
	_, err := a.Update(ctx, q, taskqueue.ConfigChange{Identity: "a"}, pause(true))
	require.NoError(t, err)
	require.NoError(t, b.Refresh(ctx))
	require.True(t, b.QueueConfig(q).Paused)

	var mu sync.Mutex
	var seen []bool
	cancel := b.WatchQueueConfig(q, func() {
		mu.Lock()
		defer mu.Unlock()
		seen = append(seen, b.QueueConfig(q).Paused)
	})
	defer cancel()
	_, err = a.Update(ctx, q, taskqueue.ConfigChange{Identity: "a"}, pause(false))
	require.NoError(t, err)
	_, err = a.Update(ctx, q, taskqueue.ConfigChange{Identity: "a"}, pause(true))
	require.NoError(t, err)
	require.NoError(t, b.Refresh(ctx))
	mu.Lock()
	defer mu.Unlock()
	// The net change is none (paused, then paused again), so with r2's unchanged-config skip
	// there is no notification at all, and never one at the superseded resume.
	require.Empty(t, seen, "no notification: superseded resume skipped, newest equals cached")
	require.True(t, b.QueueConfig(q).Paused)
}

// A watcher can call back into the source (here Refresh and QueueConfig) without deadlocking:
// watchers are notified with no lock of the source held (review config r1 P1).
func TestWatcherMayCallBackIntoSource(t *testing.T) {
	st := memstore.New()
	ctx := context.Background()
	a, b := manualStoreSource(t, st), manualStoreSource(t, st)
	cancel := b.WatchQueueConfig(q, func() {
		_ = b.Refresh(ctx)
		_ = b.QueueConfig(q)
	})
	defer cancel()
	_, err := a.Update(ctx, q, taskqueue.ConfigChange{Identity: "a"}, pause(true))
	require.NoError(t, err)
	done := make(chan error, 1)
	go func() { done <- b.Refresh(ctx) }()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("Refresh deadlocked in a watcher callback")
	}
	require.True(t, b.QueueConfig(q).Paused)
}

// What Update caches does not alias anything the mutate callback still holds (review config
// r1 P1).
func TestUpdateDoesNotAliasCallbackValues(t *testing.T) {
	src := manualStoreSource(t, memstore.New())
	w := map[string]float32{"k": 2}
	keys := []string{"x"}
	got, err := src.Update(context.Background(), q, taskqueue.ConfigChange{Identity: "a"}, func(c *taskqueue.QueueConfig) error {
		c.FairnessWeightOverrides = w
		c.PausedFairnessKeys = keys
		return nil
	})
	require.NoError(t, err)
	w["k"] = 9
	keys[0] = "y"
	got.FairnessWeightOverrides["k"] = 7
	require.Equal(t, float32(2), src.QueueConfig(q).FairnessWeightOverrides["k"])
	require.Equal(t, []string{"x"}, src.QueueConfig(q).PausedFairnessKeys)
}

// Update applies SetFairnessKeyPaused's rules to PausedFairnessKeys (review config r1 P1).
func TestUpdateValidatesPausedKeys(t *testing.T) {
	src := manualStoreSource(t, memstore.New())
	ctx := context.Background()
	set := func(keys ...string) error {
		_, err := src.Update(ctx, q, taskqueue.ConfigChange{Identity: "a"}, func(c *taskqueue.QueueConfig) error {
			c.PausedFairnessKeys = keys
			return nil
		})
		return err
	}
	require.Error(t, set(""), "empty key")
	require.Error(t, set(strings.Repeat("k", 65)), "too long")
	require.NoError(t, set("b", "a", "b"))
	require.Equal(t, []string{"a", "b"}, src.QueueConfig(q).PausedFairnessKeys, "sorted and deduplicated")
}

// feedCounter counts the change-feed rows a source reads.
type feedCounter struct {
	*memstore.Store
	mu   sync.Mutex
	rows int
}

func (f *feedCounter) ListQueueConfigChanges(ctx context.Context, after int64, limit int) ([]store.QueueConfigRecord, error) {
	recs, err := f.Store.ListQueueConfigChanges(ctx, after, limit)
	f.mu.Lock()
	f.rows += len(recs)
	f.mu.Unlock()
	return recs, err
}

// A new source loads the current rows, not the whole history, and then reads only later
// changes (review config r1 P1).
func TestStartupReadsCurrentRowsNotHistory(t *testing.T) {
	st := memstore.New()
	ctx := context.Background()
	a := manualStoreSource(t, st)
	for i := range 30 {
		_, err := a.Update(ctx, q, taskqueue.ConfigChange{Identity: "a"}, pause(i%2 == 0))
		require.NoError(t, err)
	}
	fc := &feedCounter{Store: st}
	b, err := taskqueue.NewStoreQueueConfigSource(ctx, fc, taskqueue.StoreQueueConfigSourceOptions{PollInterval: time.Hour})
	require.NoError(t, err)
	t.Cleanup(b.Close)
	require.Equal(t, a.QueueConfig(q), b.QueueConfig(q))
	require.Zero(t, fc.rows, "no history replay at startup")
	_, err = a.Update(ctx, q, taskqueue.ConfigChange{Identity: "a"}, pause(true))
	require.NoError(t, err)
	require.NoError(t, b.Refresh(ctx))
	require.True(t, b.QueueConfig(q).Paused)
	require.Equal(t, 1, fc.rows, "only the later change")
}

// An undecodable stored version neither hides the other versions from History nor blocks
// Update from replacing it (review config r1 P2).
func TestUndecodableConfigRowRepairable(t *testing.T) {
	st := memstore.New()
	ctx := context.Background()
	src := manualStoreSource(t, st)
	_, err := src.Update(ctx, q, taskqueue.ConfigChange{Identity: "a"}, pause(true))
	require.NoError(t, err)
	_, err = st.PutQueueConfig(ctx, &store.PutQueueConfigRequest{Key: q, ExpectedVersion: 1,
		Config: []byte("{bad"), UpdatedBy: "corrupt", UpdateTime: time.Now()})
	require.NoError(t, err)
	hist, err := src.History(ctx, q, 10)
	require.NoError(t, err)
	require.Len(t, hist, 2)
	require.Error(t, hist[0].Err)
	require.NoError(t, hist[1].Err)
	require.True(t, hist[1].Config.Paused)
	got, err := src.Update(ctx, q, taskqueue.ConfigChange{Identity: "fix"}, pause(false))
	require.NoError(t, err)
	require.False(t, got.Paused)
	rec, err := st.GetQueueConfig(ctx, q)
	require.NoError(t, err)
	require.EqualValues(t, 3, rec.Version)
}
