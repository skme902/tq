package taskqueue_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	taskqueue "github.com/skme902/tq"
	"github.com/skme902/tq/internal/testdb"
	"github.com/skme902/tq/metrics"
	"github.com/skme902/tq/metrics/metricstest"
	"github.com/skme902/tq/store"
	"github.com/skme902/tq/store/memstore"
)

func maxAttempts(n int32) func(*taskqueue.Config) {
	return func(c *taskqueue.Config) { c.MaxAttempts = n }
}

func pollNoWait(e *taskqueue.Engine) (*taskqueue.Delivery, error) {
	return e.PollTask(context.Background(), q, taskqueue.PollOptions{NoWait: true})
}

func deadTasks(t *testing.T, e *taskqueue.Engine) []taskqueue.DeadTask {
	t.Helper()
	var all []taskqueue.DeadTask
	var token []byte
	for {
		page, next, err := e.ListDeadTasks(context.Background(), q, 2, token)
		require.NoError(t, err)
		all = append(all, page...)
		if next == nil {
			return all
		}
		token = next
	}
}

func deadLetteredCount(rec *metricstest.Recorder, reason string) int64 {
	return rec.CounterValue(metrics.TasksDeadLettered, qTags(metrics.Tag{Key: metrics.TagReason, Value: reason})...)
}

// Nack on the last attempt dead-letters the backlog task: it is not redelivered, and listed
// with its attempts and the Nack reason. Ack-level GC removes the backlog row.
func TestMaxAttemptsNackDeadLetters(t *testing.T) {
	rec := metricstest.New()
	st := memstore.New()
	e := newEngine(t, st, maxAttempts(3), withMetrics(rec))
	add(t, e, task("a"))
	var id int64
	for i := int32(1); i <= 3; i++ {
		d := poll(t, e)
		require.Equal(t, i, d.Task.Attempts)
		id = d.TaskID
		require.NoError(t, d.Nack(taskqueue.NackReason("boom")))
	}
	_, err := pollNoWait(e)
	require.ErrorIs(t, err, taskqueue.ErrNoTasks)

	dead := deadTasks(t, e)
	require.Len(t, dead, 1)
	require.Equal(t, id, dead[0].Task.ID)
	require.Equal(t, "a", string(dead[0].Task.Data.Payload))
	require.Equal(t, int32(3), dead[0].Task.Data.Attempts)
	require.Equal(t, "nack: boom", dead[0].Reason)
	require.Equal(t, int64(1), deadLetteredCount(rec, taskqueue.DeadReasonNack))
	require.Eventually(t, func() bool { return st.TotalTaskCount(q) == 0 }, 3*time.Second, 10*time.Millisecond,
		"ack-level GC removes the backlog row")

	desc, err := e.DescribeQueue(context.Background(), q)
	require.NoError(t, err)
	require.Equal(t, int64(1), desc.DeadTasks)
	require.Zero(t, desc.Stats.ApproximateBacklogCount)
}

func TestMaxAttemptsLeaseExpiryDeadLetters(t *testing.T) {
	rec := metricstest.New()
	ts, withClock := fakeClock()
	e := newEngine(t, memstore.New(), maxAttempts(1), withClock, withMetrics(rec))
	add(t, e, task("a"))
	d, err := e.PollTask(context.Background(), q, taskqueue.PollOptions{LeaseDuration: 100 * time.Millisecond})
	require.NoError(t, err)
	ts.Advance(150 * time.Millisecond)
	require.Eventually(t, func() bool { return len(deadTasks(t, e)) == 1 }, 3*time.Second, time.Millisecond)
	require.Equal(t, taskqueue.DeadReasonLeaseExpired, deadTasks(t, e)[0].Reason)
	require.ErrorIs(t, d.Ack(), taskqueue.ErrLeaseLost)
	_, err = pollNoWait(e)
	require.ErrorIs(t, err, taskqueue.ErrNoTasks)
	require.Equal(t, int64(1), deadLetteredCount(rec, taskqueue.DeadReasonLeaseExpired))
}

// The default (0) keeps today's behaviour: unlimited redelivery and no extra store writes.
func TestMaxAttemptsDefaultUnlimited(t *testing.T) {
	st := memstore.New()
	e := newEngine(t, st, nil)
	add(t, e, task("a"))
	for i := int32(1); i <= 5; i++ {
		d := poll(t, e)
		require.Equal(t, i, d.Task.Attempts, "the in-memory count is always reported")
		require.NoError(t, d.Nack())
	}
	require.Empty(t, deadTasks(t, e))
	stored := storedTasks(t, st, q)
	require.Len(t, stored, 1)
	require.Zero(t, stored[0].Data.Attempts, "not persisted without a limit")
	require.NoError(t, poll(t, e).Ack())
}

// Each backlog delivery's count is written before PollTask returns, so deliveries that end in
// a crash (here Stop, which does not count the attempt's outcome) still use up attempts, and a
// reload dead-letters the task once they are gone.
func TestMaxAttemptsSurviveRestart(t *testing.T) {
	forEachStore(t, func(t *testing.T, k testdb.Kind, open testdb.Opener) {
		st := open(t)
		defer closeStore(st)
		e1 := newEngine(t, st, maxAttempts(2))
		add(t, e1, task("poison"))
		d := poll(t, e1)
		require.Equal(t, int32(1), d.Task.Attempts)
		stored := storedTasks(t, st, q)
		require.Len(t, stored, 1)
		require.Equal(t, int32(1), stored[0].Data.Attempts)
		e1.Stop()

		e2 := newEngine(t, st, maxAttempts(2))
		d = poll(t, e2)
		require.Equal(t, int32(2), d.Task.Attempts)
		e2.Stop()

		e3 := newEngine(t, st, maxAttempts(2))
		// NoWait can run before the reload's backlog read: retry until the task was matched.
		require.Eventually(t, func() bool {
			_, err := pollNoWait(e3)
			require.ErrorIs(t, err, taskqueue.ErrNoTasks, "never delivered")
			return len(deadTasks(t, e3)) == 1
		}, 3*time.Second, 5*time.Millisecond)
		dead := deadTasks(t, e3)
		require.Len(t, dead, 1)
		require.Equal(t, taskqueue.DeadReasonAttemptsExhausted, dead[0].Reason)
		require.Equal(t, int32(2), dead[0].Task.Data.Attempts)
		require.Eventually(t, func() bool { return len(storedTasks(t, st, q)) == 0 }, 3*time.Second, 10*time.Millisecond,
			"ack-level GC removes the dead-lettered row")
	})
}

// A sync-matched task has no backlog row: a failed last attempt spools it with its count, and
// it is dead-lettered when next matched.
func TestMaxAttemptsSyncMatched(t *testing.T) {
	e := newEngine(t, memstore.New(), maxAttempts(1))
	ch := startPoller(t, e, taskqueue.PollOptions{})
	require.True(t, add(t, e, task("s")))
	d := <-ch
	require.True(t, d.SyncMatched)
	require.Equal(t, int32(1), d.Task.Attempts)
	require.NoError(t, d.Nack())
	_, err := pollNoWait(e)
	require.ErrorIs(t, err, taskqueue.ErrNoTasks)
	dead := deadTasks(t, e)
	require.Len(t, dead, 1)
	require.Equal(t, "s", string(dead[0].Task.Data.Payload))
	require.Equal(t, taskqueue.DeadReasonAttemptsExhausted, dead[0].Reason)
}

// AddTask ignores a caller-set Attempts.
func TestAddTaskIgnoresAttempts(t *testing.T) {
	e := newEngine(t, memstore.New(), maxAttempts(1))
	tk := task("a")
	tk.Attempts = 7
	add(t, e, tk)
	d := poll(t, e)
	require.Equal(t, int32(1), d.Task.Attempts)
	require.NoError(t, d.Ack())
}

// Per-queue MaxAttempts overrides the engine default, and lowering it applies to the next
// delivery.
func TestMaxAttemptsQueueConfig(t *testing.T) {
	src := taskqueue.NewMemoryQueueConfigSource(0)
	e := newEngine(t, memstore.New(), maxAttempts(10), func(o *taskqueue.Options) { o.QueueConfigSource = src })
	add(t, e, task("a"))
	require.NoError(t, poll(t, e).Nack())
	require.NoError(t, poll(t, e).Nack())
	one := int32(2)
	require.NoError(t, src.SetMaxAttempts(q, &one))
	// Attempts is 2 = the new max: dead-lettered when matched, not delivered.
	_, err := pollNoWait(e)
	require.ErrorIs(t, err, taskqueue.ErrNoTasks)
	require.Len(t, deadTasks(t, e), 1)

	neg := int32(-1)
	require.Error(t, src.SetMaxAttempts(q, &neg))
	_, err = taskqueue.New(taskqueue.Options{Store: memstore.New(), Config: taskqueue.Config{MaxAttempts: -1}})
	require.Error(t, err)
}

// A failed dead-letter write never loses the task: it is redelivered as if unlimited.
func TestDeadLetterFailureRedelivers(t *testing.T) {
	rec := metricstest.New()
	st := memstore.New()
	e := newEngine(t, st, maxAttempts(1), withMetrics(rec))
	add(t, e, task("a"))
	d := poll(t, e)
	st.InjectError(memstore.OpDeadLetterTask, errors.New("boom")) // non-transient: no retry
	require.NoError(t, d.Nack())
	require.Equal(t, int64(1), rec.CounterValue(metrics.DeadLetterFailures, qTags()...))
	// Matched again with Attempts = max: the poll path dead-letters it now.
	_, err := pollNoWait(e)
	require.ErrorIs(t, err, taskqueue.ErrNoTasks)
	require.Len(t, deadTasks(t, e), 1)

	// And if that fails too, the task is delivered rather than dropped.
	add(t, e, task("b"))
	d = poll(t, e)
	st.InjectError(memstore.OpDeadLetterTask, errors.New("boom"), errors.New("boom"))
	require.NoError(t, d.Nack())
	d, err = pollNoWait(e)
	require.NoError(t, err)
	require.Equal(t, "b", string(d.Task.Payload))
	require.Equal(t, int64(3), rec.CounterValue(metrics.DeadLetterFailures, qTags()...))
	require.NoError(t, d.Ack())
}

// A failed attempt write is counted and the task still delivered.
func TestAttemptRecordFailureDelivers(t *testing.T) {
	rec := metricstest.New()
	st := memstore.New()
	e := newEngine(t, st, maxAttempts(3), withMetrics(rec))
	add(t, e, task("a"))
	st.InjectError(memstore.OpUpdateTask, errors.New("boom"))
	d := poll(t, e)
	require.Equal(t, int64(1), rec.CounterValue(metrics.TaskAttemptRecordFailures, qTags()...))
	require.NoError(t, d.Ack())
}

func deadN(t *testing.T, e *taskqueue.Engine, payloads ...string) []int64 {
	t.Helper()
	before := len(deadTasks(t, e))
	var ids []int64
	for _, p := range payloads {
		add(t, e, task(p))
		d := poll(t, e)
		ids = append(ids, d.TaskID)
		require.NoError(t, d.Nack())
	}
	require.Len(t, deadTasks(t, e), before+len(ids))
	return ids
}

func TestRedriveDeadTasks(t *testing.T) {
	rec := metricstest.New()
	e := newEngine(t, memstore.New(), maxAttempts(1), withMetrics(rec))
	ctx := context.Background()
	ids := deadN(t, e, "a", "b", "c")

	n, err := e.RedriveDeadTasks(ctx, q, taskqueue.RedriveOptions{TaskIDs: []int64{ids[1]}})
	require.NoError(t, err)
	require.Equal(t, 1, n)
	d := poll(t, e)
	require.Equal(t, "b", string(d.Task.Payload))
	require.Equal(t, int32(1), d.Task.Attempts, "attempts reset")
	require.NotEqual(t, ids[1], d.TaskID, "redriven as a new task")
	require.NoError(t, d.Ack())

	n, err = e.RedriveDeadTasks(ctx, q, taskqueue.RedriveOptions{Max: 1})
	require.NoError(t, err)
	require.Equal(t, 1, n)
	require.Equal(t, []string{"c"}, payloads(deadTasks(t, e)))

	n, err = e.RedriveDeadTasks(ctx, q, taskqueue.RedriveOptions{})
	require.NoError(t, err)
	require.Equal(t, 1, n)
	require.Empty(t, deadTasks(t, e))
	got := map[string]bool{}
	for range 2 {
		d := poll(t, e)
		got[string(d.Task.Payload)] = true
		require.NoError(t, d.Ack())
	}
	require.Equal(t, map[string]bool{"a": true, "c": true}, got)
	require.Equal(t, int64(3), rec.CounterValue(metrics.TasksRedriven, qTags()...))
}

func payloads(ts []taskqueue.DeadTask) []string {
	var out []string
	for _, dt := range ts {
		out = append(out, string(dt.Task.Data.Payload))
	}
	return out
}

func TestDeleteAndPurgeDeadTasks(t *testing.T) {
	ts, withClock := fakeClock()
	e := newEngine(t, memstore.New(), maxAttempts(1), withClock)
	ctx := context.Background()
	ids := deadN(t, e, "a", "b")
	cut := ts.Now().Add(time.Second)
	ts.Advance(2 * time.Second)
	deadN(t, e, "c", "d")

	n, err := e.PurgeDeadTasks(ctx, q, cut)
	require.NoError(t, err)
	require.Equal(t, 2, n)
	require.Equal(t, []string{"c", "d"}, payloads(deadTasks(t, e)))

	dead := deadTasks(t, e)
	n, err = e.DeleteDeadTasks(ctx, q, []int64{dead[0].Task.ID, ids[0]})
	require.NoError(t, err)
	require.Equal(t, 1, n, "only existing dead tasks count")
	require.Equal(t, []string{"d"}, payloads(deadTasks(t, e)))

	n, err = e.PurgeDeadTasks(ctx, q, time.Time{})
	require.NoError(t, err)
	require.Equal(t, 1, n)
	require.Empty(t, deadTasks(t, e))
}

// Listing, describing, deleting and purging dead tasks read or write the store only: an engine
// that does not own the queue neither loads it nor bumps its rangeID. Redrive adds tasks, so
// it takes the queue over like AddTask; the old owner's lease is then fenced.
func TestDeadLetterAdminOwnership(t *testing.T) {
	forEachStore(t, func(t *testing.T, k testdb.Kind, open testdb.Opener) {
		if !k.Durable {
			t.Skip("needs two handles on one database")
		}
		ctx := context.Background()
		st1, st2 := open(t), open(t)
		defer closeStore(st1)
		defer closeStore(st2)
		e1 := newEngine(t, st1, maxAttempts(1))
		deadN(t, e1, "a", "b")
		add(t, e1, task("live"))
		live := poll(t, e1)
		before, err := st1.GetQueue(ctx, q)
		require.NoError(t, err)

		e2 := newEngine(t, st2, nil)
		require.Len(t, deadTasks(t, e2), 2)
		desc, err := e2.DescribeQueue(ctx, q)
		require.NoError(t, err)
		require.Equal(t, int64(2), desc.DeadTasks)
		require.False(t, desc.Loaded)
		dead := deadTasks(t, e2)
		n, err := e2.DeleteDeadTasks(ctx, q, []int64{dead[0].Task.ID})
		require.NoError(t, err)
		require.Equal(t, 1, n)
		require.False(t, loaded(e2, q))
		after, err := st1.GetQueue(ctx, q)
		require.NoError(t, err)
		require.Equal(t, before.RangeID, after.RangeID, "no takeover")
		require.NoError(t, live.Ack(), "owner unaffected")

		n, err = e2.RedriveDeadTasks(ctx, q, taskqueue.RedriveOptions{})
		require.NoError(t, err)
		require.Equal(t, 1, n)
		require.True(t, loaded(e2, q), "redrive loads the queue like AddTask")
		after, err = st1.GetQueue(ctx, q)
		require.NoError(t, err)
		require.Greater(t, after.RangeID, before.RangeID, "redrive takes the queue over")
		d := poll(t, e2)
		require.Equal(t, "b", string(d.Task.Payload))
		require.NoError(t, d.Ack())
	})
}

// noDLQStore hides the store's DeadLetterStore methods.
type noDLQStore struct{ store.Store }

func TestDeadLetterUnsupportedStore(t *testing.T) {
	e := newEngine(t, noDLQStore{memstore.New()}, maxAttempts(1))
	add(t, e, task("a"))
	d := poll(t, e)
	require.NoError(t, d.Nack())
	d = poll(t, e) // limit ignored: redelivered
	require.Equal(t, int32(2), d.Task.Attempts)
	require.NoError(t, d.Ack())
	_, _, err := e.ListDeadTasks(context.Background(), q, 10, nil)
	var ia *store.InvalidArgumentError
	require.ErrorAs(t, err, &ia)
	_, err = e.RedriveDeadTasks(context.Background(), q, taskqueue.RedriveOptions{})
	require.ErrorAs(t, err, &ia)
}
