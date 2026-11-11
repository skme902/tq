package taskqueue_test

// Regression tests for the feat/ops-dlq review round (redrive durability, attempt-write
// ordering, fencing, backlog count after dead-letter, non-default priority).
//
// Each case is aimed at the pre-fix behaviour:
//   - redrive used AddTask, so a waiting poller sync-matched and the dead row was the only copy;
//   - the lease timer was armed before the attempt write, and a retry rewrote a snapshotted count;
//   - a fenced UpdateTask still returned a delivery;
//   - dead-letter deleted the backlog row before the ack, so the count survived reload and an
//     evicted lease;
//   - no test put a task on a non-default subqueue.

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	taskqueue "github.com/skme902/tq"
	"github.com/skme902/tq/store"
	"github.com/skme902/tq/store/memstore"
)

// updateGate intercepts DeadLetterStore.UpdateTask. The first call signals entered, waits for
// releaseFirst, then returns firstErr (when set) or falls through and records the attempt.
type updateGate struct {
	*memstore.Store

	mu           sync.Mutex
	calls        int
	writes       []int32
	entered      chan struct{}
	releaseFirst chan struct{}
	firstErr     error
}

func (g *updateGate) UpdateTask(ctx context.Context, req *store.UpdateTaskRequest) error {
	g.mu.Lock()
	g.calls++
	n := g.calls
	g.mu.Unlock()
	if n == 1 && g.entered != nil {
		close(g.entered)
		if g.releaseFirst != nil {
			<-g.releaseFirst
		}
		if g.firstErr != nil {
			return g.firstErr
		}
	}
	g.mu.Lock()
	g.writes = append(g.writes, req.Task.Data.Attempts)
	g.mu.Unlock()
	return g.Store.UpdateTask(ctx, req)
}

func (g *updateGate) attemptWrites() []int32 {
	g.mu.Lock()
	defer g.mu.Unlock()
	return append([]int32(nil), g.writes...)
}

// releaseFirstUnblock closes releaseFirst if the test fails before it does, so a poll
// blocked in UpdateTask cannot deadlock Engine.Stop.
func releaseFirstUnblock(t *testing.T, ch chan struct{}) {
	t.Helper()
	t.Cleanup(func() {
		select {
		case <-ch:
		default:
			close(ch)
		}
	})
}

// failUpdate fails UpdateTask with err once, then delegates.
type failUpdate struct {
	*memstore.Store
	err   error
	once  sync.Once
	fired atomic.Bool // the injected failure was returned
}

func (f *failUpdate) UpdateTask(ctx context.Context, req *store.UpdateTaskRequest) error {
	fail := false
	f.once.Do(func() { fail = true })
	if fail {
		f.fired.Store(true)
		return f.err
	}
	return f.Store.UpdateTask(ctx, req)
}

// blockComplete stalls ack-level GC so a test can observe the backlog row after dead-letter.
type blockComplete struct {
	*memstore.Store
	hold chan struct{}
}

func (b *blockComplete) CompleteTasksLessThan(ctx context.Context, req *store.CompleteTasksLessThanRequest) (int, error) {
	select {
	case <-b.hold:
	case <-ctx.Done():
		return 0, ctx.Err()
	}
	return b.Store.CompleteTasksLessThan(ctx, req)
}

func backlogCount(t *testing.T, e *taskqueue.Engine) int64 {
	t.Helper()
	desc, err := e.DescribeQueue(context.Background(), q)
	require.NoError(t, err)
	return desc.Stats.ApproximateBacklogCount
}

func queueStatusCount(t *testing.T, e *taskqueue.Engine) int64 {
	t.Helper()
	status, ok := e.QueueStatus(q)
	require.True(t, ok, "queue should be loaded")
	var n int64
	for _, s := range status {
		n += s.ApproximateBacklogCount
	}
	return n
}

func persistedBacklogCount(t *testing.T, st store.Store) int64 {
	t.Helper()
	resp, err := st.GetQueue(context.Background(), q)
	require.NoError(t, err)
	var n int64
	for _, sq := range resp.Info.Subqueues {
		n += sq.ApproximateBacklogCount
	}
	return n
}

// subqueuePriorityOf returns the subqueue priority key that holds payload.
func subqueuePriorityOf(t *testing.T, st store.Store, payload string) int32 {
	t.Helper()
	ctx := context.Background()
	resp, err := st.GetQueue(ctx, q)
	require.NoError(t, err)
	for i, sq := range resp.Info.Subqueues {
		page, err := st.GetTasks(ctx, &store.GetTasksRequest{
			Key: q, Subqueue: i, InclusiveMin: store.Level{Pass: 1}, PageSize: 100,
		})
		require.NoError(t, err)
		for _, task := range page.Tasks {
			if string(task.Data.Payload) == payload {
				return sq.Key.Priority
			}
		}
	}
	t.Fatalf("payload %q is not in the backlog", payload)
	return 0
}

func payloadsOf(rows []store.TaskInfo) []string {
	out := make([]string, len(rows))
	for i, row := range rows {
		out[i] = string(row.Data.Payload)
	}
	return out
}

type pollResult struct {
	d   *taskqueue.Delivery
	err error
}

func waitPoll(t *testing.T, ch <-chan pollResult) pollResult {
	t.Helper()
	select {
	case r := <-ch:
		return r
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for poll")
		return pollResult{}
	}
}

// finishPoller waits until a startPoller goroutine has sent its result.
func finishPoller(t *testing.T, ch <-chan *taskqueue.Delivery) *taskqueue.Delivery {
	t.Helper()
	select {
	case d := <-ch:
		return d
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for poller")
		return nil
	}
}

func slowConfig(c *taskqueue.Config) {
	// Keep periodic sync, idle unload and GC off the fake-clock advances and the
	// post-dead-letter observation window. A zero last-GC time still schedules one GC
	// on the first ack; tests that need the row block CompleteTasksLessThan.
	c.UpdateAckInterval = time.Hour
	c.TaskDeleteInterval = time.Hour
	c.MaxTaskQueueIdleTime = time.Hour
	c.MetadataUpdateOnAppendInterval = time.Hour
	c.LongPollExpirationInterval = 15 * time.Second
}

// A1: a waiting poller must not sync-match a redrive. The task is already in the backlog
// when RedriveDeadTasks returns, so Stop before Ack leaves it for the next engine, and the
// dead row is gone. A sync-matched redrive would have deleted the only durable copy.
func TestRedriveWaitingPollerStaysDurable(t *testing.T) {
	st := memstore.New()
	e := newEngine(t, st, func(c *taskqueue.Config) {
		c.MaxAttempts = 1
		c.TaskDeleteInterval = 10 * time.Millisecond
		c.UpdateAckInterval = 20 * time.Millisecond
		c.LongPollExpirationInterval = 15 * time.Second
	})
	add(t, e, task("again"))
	d := poll(t, e)
	require.NoError(t, d.Nack())
	require.Eventually(t, func() bool {
		return st.TotalTaskCount(q) == 0 && e.MatcherTasks(q) == 0
	}, 3*time.Second, 5*time.Millisecond, "original row is GC'd and the matcher is empty")
	require.Len(t, deadTasks(t, e), 1)

	ch := startPoller(t, e, taskqueue.PollOptions{})
	n, err := e.RedriveDeadTasks(context.Background(), q, taskqueue.RedriveOptions{})
	require.NoError(t, err)
	require.Equal(t, 1, n)
	require.Empty(t, deadTasks(t, e))

	rows := storedTasks(t, st, q)
	require.Len(t, rows, 1, "redrive must spool before it returns; a sync match would leave no row")
	require.Equal(t, "again", string(rows[0].Data.Payload))
	// The waiting poller may already have recorded this delivery. The row is still the
	// durable copy either way; a sync match would have left no row at all.
	require.LessOrEqual(t, rows[0].Data.Attempts, int32(1))
	id := rows[0].ID

	// Stop before Ack. Drain the poller first-class so its attempt write cannot race
	// the next engine, and so a sync-matched delivery (TaskID 0) is observed.
	e.Stop()
	if got := finishPoller(t, ch); got != nil {
		require.False(t, got.SyncMatched, "redrive must not sync-match a waiting poller")
		require.Equal(t, id, got.TaskID)
	}

	rows = storedTasks(t, st, q)
	require.Len(t, rows, 1)
	require.Equal(t, id, rows[0].ID)
	require.LessOrEqual(t, rows[0].Data.Attempts, int32(1))

	// MaxAttempts above the raced record: a delivery the first engine already counted
	// must still be handed out, not treated as exhausted.
	e2 := newEngine(t, st, maxAttempts(5))
	require.Empty(t, deadTasks(t, e2))
	got := poll(t, e2)
	require.Equal(t, "again", string(got.Task.Payload))
	require.False(t, got.SyncMatched)
	require.Equal(t, id, got.TaskID)
	require.Equal(t, rows[0].Data.Attempts+1, got.Task.Attempts)
	require.NoError(t, got.Ack())
	require.Empty(t, deadTasks(t, e2))
}

// A2: a lease shorter than the attempt write must not expire before that write commits.
// The pre-fix timer was armed first, so advancing past the lease during a blocked UpdateTask
// settled the delivery (and, on the last attempt, dead-lettered it) before PollTask returned.
func TestShortLeaseDoesNotExpireDuringAttemptWrite(t *testing.T) {
	ts, withClock := fakeClock()
	g := &updateGate{
		Store:        memstore.New(),
		entered:      make(chan struct{}),
		releaseFirst: make(chan struct{}),
	}
	e := newEngine(t, g, func(c *taskqueue.Config) {
		slowConfig(c)
		c.MaxAttempts = 4
	}, withClock)
	// After newEngine, so a failed assertion closes the gate before Stop takes the db lock.
	releaseFirstUnblock(t, g.releaseFirst)
	add(t, e, task("slow"))

	ch := make(chan pollResult, 1)
	go func() {
		d, err := e.PollTask(context.Background(), q, taskqueue.PollOptions{LeaseDuration: 100 * time.Millisecond})
		ch <- pollResult{d, err}
	}()
	<-g.entered

	// The write is still blocked. The lease (100ms) is already over on the fake clock.
	ts.Advance(time.Second)
	rows := storedTasks(t, g, q)
	require.Len(t, rows, 1)
	require.Zero(t, rows[0].Data.Attempts, "attempt must not be persisted until UpdateTask returns")
	require.Empty(t, deadTasks(t, e))
	select {
	case r := <-ch:
		t.Fatalf("poll returned while the attempt write was blocked: delivery=%v err=%v", r.d, r.err)
	default:
	}

	close(g.releaseFirst)
	r := waitPoll(t, ch)
	require.NoError(t, r.err)
	require.NotNil(t, r.d)
	require.Equal(t, int32(1), r.d.Task.Attempts)
	require.Equal(t, []int32{1}, g.attemptWrites())
	require.Equal(t, int32(1), storedTasks(t, g, q)[0].Data.Attempts)
	require.NoError(t, r.d.Ack(), "lease must still be held; it is armed only after the attempt write")
	require.Empty(t, deadTasks(t, e))
}

// A2: a retry of the attempt write must not persist an older Attempts over a higher one.
// Pre-fix, the UpdateTask body was snapshotted once, the lease could expire during the
// retry backoff, and the stale write followed a later delivery's higher count.
func TestAttemptRetryDoesNotOverwriteHigherCount(t *testing.T) {
	ts, withClock := fakeClock()
	g := &updateGate{
		Store:        memstore.New(),
		entered:      make(chan struct{}),
		releaseFirst: make(chan struct{}),
		firstErr:     &store.TransientError{Err: errors.New("once")},
	}
	e := newEngine(t, g, func(c *taskqueue.Config) {
		slowConfig(c)
		c.MaxAttempts = 4
	}, withClock)
	// After newEngine, so a failed assertion closes the gate before Stop takes the db lock.
	releaseFirstUnblock(t, g.releaseFirst)
	add(t, e, task("retry"))

	ch := make(chan pollResult, 1)
	go func() {
		d, err := e.PollTask(context.Background(), q, taskqueue.PollOptions{LeaseDuration: time.Second})
		ch <- pollResult{d, err}
	}()
	<-g.entered
	// Park a second poller while the first attempt write still holds the queue lock, so a
	// lease expiry (pre-fix: timer already armed) hands it the task before the retry.
	_ = startPoller(t, e, taskqueue.PollOptions{})
	ts.Advance(2 * time.Second) // past the 1s lease, still during the blocked first write
	close(g.releaseFirst)

	r := waitPoll(t, ch)
	require.NoError(t, r.err)
	require.NotNil(t, r.d)
	writes := g.attemptWrites()
	require.NotEmpty(t, writes)
	for i := 1; i < len(writes); i++ {
		require.GreaterOrEqual(t, writes[i], writes[i-1], "stale attempt write %v", writes)
	}
	require.Equal(t, r.d.Task.Attempts, writes[len(writes)-1])
	rows := storedTasks(t, g, q)
	require.Len(t, rows, 1)
	require.Equal(t, r.d.Task.Attempts, rows[0].Data.Attempts, "persisted Attempts must match the delivery, writes=%v", writes)
	require.NoError(t, r.d.Ack())
}

// A3: ConditionFailed on the attempt write yields no delivery, leaves the backlog row
// alone, unloads the queue, and a later engine can still deliver the task.
func TestAttemptFenceReturnsNoDelivery(t *testing.T) {
	inner := memstore.New()
	st := &failUpdate{Store: inner, err: &store.ConditionFailedError{Msg: "injected"}}
	e := newEngine(t, st, func(c *taskqueue.Config) {
		slowConfig(c)
		c.MaxAttempts = 3
	})
	add(t, e, task("fenced"))
	before := storedTasks(t, st, q)
	require.Len(t, before, 1)
	require.Zero(t, before[0].Data.Attempts)

	// A waiting poll, not NoWait: right after add() loads the queue its initial backlog read
	// can still be in flight, holding the new task back from the matcher, and a NoWait poll
	// then returns ErrNoTasks without ever reaching the attempt write. This poll returns as
	// soon as the fenced attempt write refuses the delivery; the timeout only bounds a bug.
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	d, err := e.PollTask(ctx, q, taskqueue.PollOptions{})
	require.True(t, st.fired.Load(), "the poll must reach the attempt write")
	require.Nil(t, d)
	require.ErrorIs(t, err, taskqueue.ErrNoTasks)
	require.Empty(t, deadTasks(t, e))
	require.Equal(t, before, storedTasks(t, st, q), "fenced attempt write must not change the backlog row")
	require.Eventually(t, func() bool { return !loaded(e, q) }, 3*time.Second, time.Millisecond, "queue unloads on the lost fence")

	e.Stop()
	e2 := newEngine(t, st, maxAttempts(3))
	got := poll(t, e2)
	require.Equal(t, "fenced", string(got.Task.Payload))
	require.Equal(t, before[0].ID, got.TaskID)
	require.Equal(t, int32(1), got.Task.Attempts)
	require.False(t, got.SyncMatched)
	require.NoError(t, got.Ack())
}

// A4(a): after dead-letter, Stop persists a backlog count that excludes the dead task, and a
// new engine on the same store does not deliver it. The backlog row is still there until
// ack-level GC (pre-fix deleted it inside the dead-letter write, before the count moved).
func TestDeadLetterReloadBacklogCount(t *testing.T) {
	hold := make(chan struct{})
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(hold) }) }
	t.Cleanup(release)
	st := &blockComplete{Store: memstore.New(), hold: hold}
	e := newEngine(t, st, func(c *taskqueue.Config) {
		slowConfig(c)
		c.MaxAttempts = 1
	})
	add(t, e, task("dead"))
	add(t, e, task("live"))
	require.Equal(t, int64(2), backlogCount(t, e))

	d := poll(t, e)
	require.Equal(t, "dead", string(d.Task.Payload))
	require.NoError(t, d.Nack())

	require.ElementsMatch(t, []string{"dead", "live"}, payloadsOf(storedTasks(t, st, q)),
		"dead-letter must leave the backlog row for ack-level GC")
	require.Equal(t, int64(1), backlogCount(t, e), "in-memory count excludes the dead task")
	require.Equal(t, int64(1), queueStatusCount(t, e))
	release()

	e.Stop()
	require.Equal(t, int64(1), persistedBacklogCount(t, st))

	e2 := newEngine(t, st, func(c *taskqueue.Config) {
		slowConfig(c)
		c.MaxAttempts = 1
	})
	desc, err := e2.DescribeQueue(context.Background(), q)
	require.NoError(t, err)
	require.False(t, desc.Loaded, "describe of a queue this engine has not loaded")
	require.Equal(t, int64(1), desc.Stats.ApproximateBacklogCount)
	require.Equal(t, int64(1), desc.DeadTasks)

	got := poll(t, e2)
	require.Equal(t, "live", string(got.Task.Payload))
	require.Equal(t, int64(1), backlogCount(t, e2))
	require.Equal(t, int64(1), queueStatusCount(t, e2))
	require.NoError(t, got.Ack())
	_, err = pollNoWait(e2)
	require.ErrorIs(t, err, taskqueue.ErrNoTasks, "dead task must not be redelivered")
	require.Equal(t, int64(0), backlogCount(t, e2))
	require.Len(t, deadTasks(t, e2), 1)
}

// A4(b): dead-letter a leased task that the reader has evicted (batch larger than
// GetTasksBatchSize). It is not redelivered, and the backlog count drops without waiting
// for a read that reaches the end of the subqueue. Pre-fix deleted the row, so the evicted
// ack was never replayed and the count kept the task until atEnd.
func TestDeadLetterEvictedLeaseDropsBacklogCount(t *testing.T) {
	const batch = 5
	const nB = 20
	const nC = 30
	st := memstore.New()
	e := newEngine(t, st, func(c *taskqueue.Config) {
		slowConfig(c)
		c.MaxAttempts = 1
		c.GetTasksBatchSize = batch
		c.GetTasksReloadAt = 1
		c.DeliveryLeaseDuration = time.Hour
	})
	// A tiny weight sorts after every normal task and before the even smaller one, so the
	// held lease is pushed out of the in-memory window once more than batchSize tasks land
	// in front of it. Tasks behind it keep the reader from treating a partial read as atEnd.
	add(t, e, fairTask("A", 0.01, "held"))
	held := poll(t, e)
	require.Equal(t, "held", string(held.Task.Payload))
	for i := range nB {
		add(t, e, fairTask("B", 1, fmt.Sprintf("b%02d", i)))
	}
	for i := range nC {
		add(t, e, fairTask("C", 0.001, fmt.Sprintf("c%02d", i)))
	}
	total := int64(1 + nB + nC)
	require.Equal(t, total, backlogCount(t, e))
	require.Equal(t, total, queueStatusCount(t, e))

	require.NoError(t, held.Nack())
	require.Equal(t, total, backlogCount(t, e), "evicted dead-letter records an ack but does not drop the count yet")
	require.Len(t, deadTasks(t, e), 1)
	require.Equal(t, "held", string(deadTasks(t, e)[0].Task.Data.Payload))

	for i := range nB {
		d := poll(t, e)
		require.Truef(t, strings.HasPrefix(string(d.Task.Payload), "b"), "poll %d delivered %q", i, d.Task.Payload)
		require.NoError(t, d.Ack())
	}
	require.Eventually(t, func() bool { return backlogCount(t, e) == int64(nC) }, 3*time.Second, 5*time.Millisecond,
		"backlog count should drop to the tasks behind the dead-lettered one")
	require.Equal(t, int64(nC), queueStatusCount(t, e))

	// The dead task stays dead. What is deliverable now is the higher-pass remainder.
	d := poll(t, e)
	require.Truef(t, strings.HasPrefix(string(d.Task.Payload), "c"), "delivered %q", d.Task.Payload)
	require.NotEqual(t, "held", string(d.Task.Payload))
	require.NoError(t, d.Ack())
	require.Equal(t, int64(nC-1), backlogCount(t, e))
}

// A14: a non-default priority (its own subqueue) is kept on the dead row and on redrive.
func TestDeadLetterRedrivePreservesPriority(t *testing.T) {
	st := memstore.New()
	e := newEngine(t, st, func(c *taskqueue.Config) {
		c.MaxAttempts = 1
		c.TaskDeleteInterval = 10 * time.Millisecond
		c.UpdateAckInterval = 20 * time.Millisecond
		c.LongPollExpirationInterval = 15 * time.Second
	})
	p1 := task("p1")
	p1.Priority.PriorityKey = 1
	p5 := task("p5")
	p5.Priority.PriorityKey = 5
	add(t, e, p1)
	add(t, e, p5)

	d := poll(t, e)
	require.Equal(t, "p1", string(d.Task.Payload))
	require.Equal(t, int32(1), d.Task.Priority.PriorityKey)
	p1ID := d.TaskID
	require.NoError(t, d.Nack())
	d = poll(t, e)
	require.Equal(t, "p5", string(d.Task.Payload))
	require.Equal(t, int32(5), d.Task.Priority.PriorityKey)
	require.NoError(t, d.Nack())

	dead := deadTasks(t, e)
	require.Len(t, dead, 2)
	byPayload := map[string]taskqueue.DeadTask{}
	for _, dt := range dead {
		byPayload[string(dt.Task.Data.Payload)] = dt
	}
	require.Equal(t, int32(1), byPayload["p1"].Task.Data.Priority.PriorityKey)
	require.Equal(t, p1ID, byPayload["p1"].Task.ID)
	require.Equal(t, int32(5), byPayload["p5"].Task.Data.Priority.PriorityKey)

	require.Eventually(t, func() bool {
		return st.TotalTaskCount(q) == 0 && e.MatcherTasks(q) == 0
	}, 3*time.Second, 5*time.Millisecond)

	ch := startPoller(t, e, taskqueue.PollOptions{})
	n, err := e.RedriveDeadTasks(context.Background(), q, taskqueue.RedriveOptions{TaskIDs: []int64{p1ID}})
	require.NoError(t, err)
	require.Equal(t, 1, n)
	require.Equal(t, []string{"p5"}, payloads(deadTasks(t, e)))
	require.Equal(t, int32(1), subqueuePriorityOf(t, st, "p1"), "redrive spools back onto the priority-1 subqueue")
	rows := storedTasks(t, st, q)
	require.Len(t, rows, 1, "redrive must spool before it returns; a sync match would leave no row")
	require.Equal(t, int32(1), rows[0].Data.Priority.PriorityKey)
	require.LessOrEqual(t, rows[0].Data.Attempts, int32(1))
	id := rows[0].ID
	require.NotEqual(t, p1ID, id)

	e.Stop()
	if got := finishPoller(t, ch); got != nil {
		require.False(t, got.SyncMatched, "redrive must not sync-match a waiting poller")
		require.Equal(t, id, got.TaskID)
		require.Equal(t, int32(1), got.Task.Priority.PriorityKey)
	}

	rows = storedTasks(t, st, q)
	require.Len(t, rows, 1)
	require.Equal(t, id, rows[0].ID)
	require.Equal(t, int32(1), rows[0].Data.Priority.PriorityKey)

	e2 := newEngine(t, st, maxAttempts(5))
	got := poll(t, e2)
	require.Equal(t, "p1", string(got.Task.Payload))
	require.Equal(t, int32(1), got.Task.Priority.PriorityKey)
	require.False(t, got.SyncMatched)
	require.Equal(t, id, got.TaskID)
	require.Equal(t, rows[0].Data.Attempts+1, got.Task.Attempts)
	require.NoError(t, got.Ack())
	require.Equal(t, []string{"p5"}, payloads(deadTasks(t, e2)))
	_, err = pollNoWait(e2)
	require.ErrorIs(t, err, taskqueue.ErrNoTasks)
}
