package taskqueue_test

// Regression tests for REVIEW_2026-11-04 H1 and H2: backlog tasks held under a delivery lease
// must neither stall the reader (H1) nor be handed to a second poller while the lease is live (H2).

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	taskqueue "github.com/skme902/tq"
	"github.com/skme902/tq/store/memstore"
)

func pollWithin(e *taskqueue.Engine, d time.Duration) (*taskqueue.Delivery, error) {
	ctx, cancel := context.WithTimeout(context.Background(), d)
	defer cancel()
	return e.PollTask(ctx, q, taskqueue.PollOptions{})
}

// H1: more than GetTasksBatchSize leases may be held at once while the backlog is non-empty.
func TestHeldLeasesDoNotStallBacklog(t *testing.T) {
	e := newEngine(t, memstore.New(), func(c *taskqueue.Config) {
		c.GetTasksBatchSize = 50
		c.GetTasksReloadAt = 10
		c.DeliveryLeaseDuration = 10 * time.Minute
	})
	for range 200 {
		add(t, e, task("x"))
	}
	seen := map[int64]bool{}
	for i := range 200 {
		d, err := pollWithin(e, 2*time.Second)
		require.NoError(t, err, "poll %d of 200 with %d leases held; status %+v", i, len(seen), status(e))
		require.False(t, seen[d.TaskID], "task %d delivered twice", d.TaskID)
		seen[d.TaskID] = true
	}
	// Every task is now leased; nothing else is deliverable.
	_, err := pollWithin(e, 200*time.Millisecond)
	require.Error(t, err)
}

// H2: a lease on a backlog task survives the reader trimming that task from memory.
func TestHeldLeaseNotRedeliveredAfterTrim(t *testing.T) {
	e := newEngine(t, memstore.New(), func(c *taskqueue.Config) {
		c.GetTasksBatchSize = 50
		c.GetTasksReloadAt = 10
		c.DeliveryLeaseDuration = 10 * time.Minute
	})
	// Key A at a tiny weight gets very high passes; key B tasks then sort below them and push
	// the held A tasks out of the reader's in-memory window.
	for range 5 {
		add(t, e, fairTask("A", 0.01, "a"))
	}
	held := map[int64]*taskqueue.Delivery{}
	for range 5 {
		d, err := pollWithin(e, 2*time.Second)
		require.NoError(t, err)
		held[d.TaskID] = d
	}
	for range 60 {
		add(t, e, fairTask("B", 1, "b"))
	}
	for i := range 60 {
		d, err := pollWithin(e, 2*time.Second)
		require.NoError(t, err, "poll %d", i)
		require.NotContains(t, held, d.TaskID, "task %d delivered while its first lease is held", d.TaskID)
		require.NoError(t, d.Ack())
	}
	_, err := pollWithin(e, 200*time.Millisecond)
	require.Error(t, err, "only the held A tasks remain")

	// Nack the held tasks: each is delivered exactly once more, then the queue is empty.
	for _, d := range held {
		require.NoError(t, d.Nack())
	}
	again := map[int64]bool{}
	for range 5 {
		d, err := pollWithin(e, 2*time.Second)
		require.NoError(t, err)
		require.Contains(t, held, d.TaskID)
		require.False(t, again[d.TaskID])
		again[d.TaskID] = true
		require.NoError(t, d.Ack())
	}
	_, err = pollWithin(e, 200*time.Millisecond)
	require.Error(t, err)
}

func status(e *taskqueue.Engine) any {
	s, _ := e.QueueStatus(q)
	return s
}
