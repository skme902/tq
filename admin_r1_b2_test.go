package taskqueue_test

// Regression tests for peek/admin review batch B2.
//
// Pre-fix:
//   - a non-owner PeekTasks has no reader, so a dead-lettered backlog row stuck behind a
//     lease (ack level not yet past it) was returned;
//   - every scanned row, including acked-but-not-GC'd rows, spent the FairnessKey scan
//     budget, so a full budget of those rows yielded a short page;
//   - peekNext dropped a json.Marshal error and PeekTasks reported success;
//   - DeleteTask on an already dead-lettered task returns false and leaves the dead copy,
//     and PurgeQueue's count is rows removed (including a dead-lettered row not yet GC'd)
//     while DeadTasks stays put. The last of these was already true; the test locks it.

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	taskqueue "github.com/skme902/tq"
	"github.com/skme902/tq/internal/matching"
	"github.com/skme902/tq/store"
	"github.com/skme902/tq/store/memstore"
)

// skipComplete turns ack-level GC into a no-op while hold is set, so a test can see backlog
// rows that the reader has already acked.
type skipComplete struct {
	*memstore.Store
	hold atomic.Bool
}

func (s *skipComplete) CompleteTasksLessThan(ctx context.Context, req *store.CompleteTasksLessThanRequest) (int, error) {
	if s.hold.Load() {
		return 0, nil
	}
	return s.Store.CompleteTasksLessThan(ctx, req)
}

func quietAdminConfig(c *taskqueue.Config) {
	c.UpdateAckInterval = time.Hour
	c.TaskDeleteInterval = time.Hour
	c.MetadataUpdateOnAppendInterval = time.Hour
	c.MaxTaskQueueIdleTime = time.Hour
	c.LongPollExpirationInterval = 15 * time.Second
}

// F2: MaxAttempts=1, a dead-lettered task behind a leased task. The owner hides it (acked
// in the reader). A non-owner has no reader and must hide it too, because the row is in
// the dead-letter set. The task behind it stays visible.
func TestPeekNonOwnerHidesDeadLetterBehindLease(t *testing.T) {
	st := memstore.New()
	cfg := func(c *taskqueue.Config) {
		quietAdminConfig(c)
		c.MaxAttempts = 1
	}
	e := newEngine(t, st, cfg)
	add(t, e, task("leased"))
	add(t, e, task("dead"))
	add(t, e, task("live"))

	leased := poll(t, e)
	require.Equal(t, "leased", string(leased.Task.Payload))
	dead := poll(t, e)
	require.Equal(t, "dead", string(dead.Task.Payload))
	require.NoError(t, dead.Nack())
	require.Len(t, deadTasks(t, e), 1)

	ctx := context.Background()
	page, _, err := e.PeekTasks(ctx, q, taskqueue.PeekOptions{})
	require.NoError(t, err)
	require.Equal(t, []string{"leased", "live"}, peekPayloads(page), "owner peek hides the dead-lettered row")
	require.True(t, page[0].Leased)
	require.False(t, page[1].Leased)

	e2 := newEngine(t, st, cfg)
	require.False(t, loaded(e2, q))
	page2, _, err := e2.PeekTasks(ctx, q, taskqueue.PeekOptions{})
	require.NoError(t, err)
	require.Equal(t, []string{"leased", "live"}, peekPayloads(page2), "non-owner peek hides the dead-lettered row")
	require.False(t, page2[0].Leased, "lease state is only known on the owner")
	require.False(t, loaded(e2, q), "peek must not take the queue over")
	require.Len(t, deadTasks(t, e2), 1)
}

// F9: acked rows that GC has not deleted sit in front of the live page. They must not
// spend the scan budget (pageSize * 10), or the live page comes back short.
func TestPeekAckedRowsDoNotConsumeScanBudget(t *testing.T) {
	st := &skipComplete{Store: memstore.New()}
	st.hold.Store(true)
	const pageSize = 5
	const ackedN = pageSize*10 + 5 // more than the scan budget of pageSize*10
	e := newEngine(t, st, quietAdminConfig)
	for i := range ackedN {
		add(t, e, task(fmt.Sprintf("old-%03d", i)))
	}
	var want []string
	for i := range pageSize {
		p := fmt.Sprintf("live-%d", i)
		want = append(want, p)
		add(t, e, task(p))
	}
	for range ackedN {
		require.NoError(t, poll(t, e).Ack())
	}

	ctx := context.Background()
	resp, err := st.GetQueue(ctx, q)
	require.NoError(t, err)
	require.Equal(t, store.Level{}, resp.Info.Subqueues[0].FairAckLevel,
		"persisted ack level must still be in front of the acked rows")
	require.Equal(t, ackedN+pageSize, st.TotalTaskCount(q), "acked rows must still be stored")

	page, _, err := e.PeekTasks(ctx, q, taskqueue.PeekOptions{PageSize: pageSize})
	require.NoError(t, err)
	require.Equal(t, want, peekPayloads(page), "acked rows in front must not shrink the page")
}

// F12: a page-token marshal error is returned from PeekTasks.
func TestPeekMarshalError(t *testing.T) {
	matching.SetPeekMarshalForTest(func(any) ([]byte, error) {
		return nil, errors.New("peek marshal")
	})
	t.Cleanup(func() { matching.SetPeekMarshalForTest(nil) })

	e := newEngine(t, memstore.New(), nil)
	add(t, e, task("a"))
	add(t, e, task("b"))
	_, _, err := e.PeekTasks(context.Background(), q, taskqueue.PeekOptions{PageSize: 1})
	require.ErrorContains(t, err, "peek marshal")
}

// F3/F7: DeleteTask on a dead-lettered task returns false and leaves the dead copy.
// PurgeQueue's return value is rows removed, which includes a dead-lettered backlog row
// not yet GC'd, and is therefore larger than ApproximateBacklogCount. DeadTasks is unchanged.
func TestDeleteDeadLetterAndPurgeRowCount(t *testing.T) {
	st := &skipComplete{Store: memstore.New()}
	st.hold.Store(true)
	e := newEngine(t, st, func(c *taskqueue.Config) {
		quietAdminConfig(c)
		c.MaxAttempts = 1
	})
	add(t, e, task("dead"))
	add(t, e, task("live"))
	d := poll(t, e)
	require.Equal(t, "dead", string(d.Task.Payload))
	require.NoError(t, d.Nack())

	ctx := context.Background()
	dead := deadTasks(t, e)
	require.Len(t, dead, 1)
	require.Equal(t, []string{"dead", "live"}, payloadsOf(storedTasks(t, st, q)))

	ok, err := e.DeleteTask(ctx, q, taskqueue.TaskRef{Subqueue: 0, Pass: dead[0].Task.Pass, ID: dead[0].Task.ID})
	require.NoError(t, err)
	require.False(t, ok, "DeleteTask on an already dead-lettered task")
	require.Equal(t, []string{"dead", "live"}, payloadsOf(storedTasks(t, st, q)),
		"DeleteTask must not remove the backlog row of a dead-lettered task")
	require.Equal(t, []string{"dead"}, payloads(deadTasks(t, e)), "the dead-letter copy stays")

	backlogBefore := backlogCount(t, e)
	rowsBefore := len(storedTasks(t, st, q))
	desc, err := e.DescribeQueue(ctx, q)
	require.NoError(t, err)
	deadBefore := desc.DeadTasks
	require.Equal(t, int64(1), deadBefore)
	require.Greater(t, rowsBefore, int(backlogBefore),
		"the approximate count already excludes the dead-lettered task; its backlog row is still there")

	st.hold.Store(false)
	n, err := e.PurgeQueue(ctx, q)
	require.NoError(t, err)
	require.Equal(t, rowsBefore, n, "PurgeQueue returns rows removed, including the not-yet-GC'd dead-letter row")
	require.Greater(t, int64(n), backlogBefore, "that count is higher than ApproximateBacklogCount")

	desc, err = e.DescribeQueue(ctx, q)
	require.NoError(t, err)
	require.Equal(t, deadBefore, desc.DeadTasks, "purge does not change DeadTasks")
	require.Equal(t, []string{"dead"}, payloads(deadTasks(t, e)))
}
