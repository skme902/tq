package matching

import (
	"context"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/skme902/tq/store"
)

// A4: when the last lease is released in the same beat as ctx ending, Shutdown
// returns nil. The hook readies both signals before the wait select so a version
// that trusts the select's choice fails about half the time; the loop makes that
// miss negligible. The task must not come back on a new engine.
func TestShutdownFinishedDrainBeatsExpiredCtx(t *testing.T) {
	for range 16 {
		ts := newTestStore()
		e := newPortEngine(t, ts, nil)
		matched, err := e.AddTask(context.Background(), portQueue, store.Task{Payload: []byte("acked")})
		require.NoError(t, err)
		require.False(t, matched)
		d, err := e.PollTask(context.Background(), portQueue, PollOptions{})
		require.NoError(t, err)

		ctx, cancel := context.WithCancel(context.Background())
		var once sync.Once
		e.testHookBeforeDrainSelect = func() {
			once.Do(func() {
				require.NoError(t, d.Ack())
				cancel()
			})
		}
		require.NoError(t, e.Shutdown(ctx))
		cancel()

		e2 := newPortEngine(t, ts, nil)
		_, err = e2.PollTask(context.Background(), portQueue, PollOptions{NoWait: true})
		require.ErrorIs(t, err, ErrNoTasks)
		e2.Stop()
	}
}
