package matching

// A5: RedriveDeadTasks snapshots the highest dead task id and must not redrive a task that
// dies after that snapshot, and must return rather than scan forever.

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/skme902/tq/store"
)

func TestRedriveSkipsTasksDeadLetteredAfterSnapshot(t *testing.T) {
	ts := newTestStore()
	e := newPortEngine(t, ts, func(c *Config) {
		c.MaxAttempts = 1
		c.LongPollExpirationInterval = 2 * time.Second
	})
	ctx := context.Background()

	oldID := deadLetterPayload(t, e, "old")
	listed, _, err := e.ListDeadTasks(ctx, portQueue, 10, nil)
	require.NoError(t, err)
	require.Len(t, listed, 1)
	require.Equal(t, oldID, listed[0].Task.ID)

	e.testHookAfterDeadSnapshot = func() {
		newID := deadLetterPayload(t, e, "new")
		require.Greater(t, newID, oldID, "a task that dies during redrive gets a newer id")
	}

	// A loop that keeps scanning while the post-snapshot task stays dead would hit this.
	rctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	n, err := e.RedriveDeadTasks(rctx, portQueue, RedriveOptions{})
	require.NoError(t, err)
	require.Equal(t, 1, n, "only the pre-snapshot task is redriven")

	after, _, err := e.ListDeadTasks(ctx, portQueue, 10, nil)
	require.NoError(t, err)
	require.Len(t, after, 1)
	require.Equal(t, "new", string(after[0].Task.Data.Payload))
	require.Greater(t, after[0].Task.ID, oldID)

	d, err := e.PollTask(ctx, portQueue, PollOptions{})
	require.NoError(t, err)
	require.Equal(t, "old", string(d.Task.Payload))
	require.NotEqual(t, oldID, d.TaskID)
	require.Equal(t, int32(1), d.Task.Attempts)
	require.NoError(t, d.Ack())

	still, _, err := e.ListDeadTasks(ctx, portQueue, 10, nil)
	require.NoError(t, err)
	require.Len(t, still, 1)
	require.Equal(t, "new", string(still[0].Task.Data.Payload))
}

// deadLetterPayload spools payload, delivers it once and Nacks it into the dead set.
// The engine's MaxAttempts must be 1.
func deadLetterPayload(t *testing.T, e *Engine, payload string) int64 {
	t.Helper()
	matched, err := e.AddTask(context.Background(), portQueue, store.Task{Payload: []byte(payload)})
	require.NoError(t, err)
	require.False(t, matched)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	d, err := e.PollTask(ctx, portQueue, PollOptions{})
	require.NoError(t, err)
	require.Equal(t, payload, string(d.Task.Payload))
	id := d.TaskID
	require.NoError(t, d.Nack())
	return id
}
