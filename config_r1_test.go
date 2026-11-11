package taskqueue_test

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	taskqueue "github.com/skme902/tq"
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
	require.Equal(t, []bool{true}, seen, "one notification, at the newest version")
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
