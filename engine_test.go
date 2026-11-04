package taskqueue_test

// Behavior tests for slice 2 (not upstream). They drive the public API against memstore.

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	taskqueue "github.com/skme902/tq"
	"github.com/skme902/tq/clock"
	"github.com/skme902/tq/metrics"
	"github.com/skme902/tq/store"
	"github.com/skme902/tq/store/memstore"
)

var q = taskqueue.QueueKey{Namespace: "ns", Name: "orders"}

func newEngine(t *testing.T, st store.Store, mod func(*taskqueue.Config), opts ...func(*taskqueue.Options)) *taskqueue.Engine {
	t.Helper()
	cfg := taskqueue.Config{
		LongPollExpirationInterval: 2 * time.Second,
		TaskDeleteInterval:         10 * time.Millisecond,
		UpdateAckInterval:          50 * time.Millisecond, // also drives the periodic GC check
	}
	if mod != nil {
		mod(&cfg)
	}
	o := taskqueue.Options{Store: st, Config: cfg}
	for _, f := range opts {
		f(&o)
	}
	e, err := taskqueue.New(o)
	require.NoError(t, err)
	t.Cleanup(e.Stop)
	return e
}

func task(payload string) taskqueue.Task {
	return taskqueue.Task{Payload: []byte(payload)}
}

func add(t *testing.T, e *taskqueue.Engine, tk taskqueue.Task) bool {
	t.Helper()
	m, err := e.AddTask(context.Background(), q, tk)
	require.NoError(t, err)
	return m
}

func poll(t *testing.T, e *taskqueue.Engine) *taskqueue.Delivery {
	t.Helper()
	d, err := e.PollTask(context.Background(), q, taskqueue.PollOptions{})
	require.NoError(t, err)
	return d
}

func loaded(e *taskqueue.Engine, k taskqueue.QueueKey) bool {
	for _, l := range e.LoadedQueues() {
		if l == k {
			return true
		}
	}
	return false
}

// startPoller starts a poll in the background and waits until it is parked in the matcher.
func startPoller(t *testing.T, e *taskqueue.Engine, opts taskqueue.PollOptions) <-chan *taskqueue.Delivery {
	t.Helper()
	before := e.WaitingPollers(q)
	ch := make(chan *taskqueue.Delivery, 1)
	go func() {
		d, err := e.PollTask(context.Background(), q, opts)
		if err != nil {
			ch <- nil
			return
		}
		ch <- d
	}()
	waitPollers(t, e, before+1)
	return ch
}

// waitPollers waits until n pollers are parked in the queue's matcher.
func waitPollers(t *testing.T, e *taskqueue.Engine, n int) {
	t.Helper()
	require.Eventually(t, func() bool { return e.WaitingPollers(q) >= n }, 3*time.Second, time.Millisecond,
		"poller should park in the matcher")
}

// fakeClock returns an engine option that runs the engine on a fake clock.
func fakeClock() (*clock.EventTimeSource, func(*taskqueue.Options)) {
	ts := clock.NewEventTimeSource().Update(time.Now())
	ts.UseAsyncTimers(true)
	return ts, func(o *taskqueue.Options) { o.TimeSource = ts }
}

func TestLazyCreate(t *testing.T) {
	st := memstore.New()
	e := newEngine(t, st, nil)
	ctx := context.Background()

	_, err := st.GetQueue(ctx, q)
	require.True(t, store.IsNotFound(err), "queue must not exist before first use")
	require.Empty(t, e.LoadedQueues())

	add(t, e, task("a"))
	require.True(t, loaded(e, q))
	resp, err := st.GetQueue(ctx, q)
	require.NoError(t, err)
	require.Equal(t, int64(1), resp.RangeID)

	// first poll on another queue creates it too
	q2 := taskqueue.QueueKey{Namespace: "ns", Name: "other"}
	_, err = e.PollTask(ctx, q2, taskqueue.PollOptions{NoWait: true})
	require.ErrorIs(t, err, taskqueue.ErrNoTasks)
	require.True(t, loaded(e, q2))
	_, err = st.GetQueue(ctx, q2)
	require.NoError(t, err)
}

func TestIdleUnloadAndReloadNoLoss(t *testing.T) {
	st := memstore.New()
	ts, withClock := fakeClock()
	e := newEngine(t, st, func(c *taskqueue.Config) { c.MaxTaskQueueIdleTime = 200 * time.Millisecond }, withClock)

	const n = 50
	for i := range n {
		require.False(t, add(t, e, task(fmt.Sprint(i))))
	}
	ts.Advance(100 * time.Millisecond)
	require.True(t, loaded(e, q), "not idle long enough yet")
	require.Eventually(t, func() bool {
		ts.Advance(50 * time.Millisecond) // timers fire asynchronously; keep time moving
		return !loaded(e, q)
	}, 3*time.Second, 10*time.Millisecond, "queue should unload when idle")
	require.Equal(t, n, st.TotalTaskCount(q))

	resp, err := st.GetQueue(context.Background(), q)
	require.NoError(t, err)
	rangeBefore := resp.RangeID

	got := map[string]bool{}
	for range n {
		d := poll(t, e)
		require.False(t, got[string(d.Task.Payload)], "duplicate %s", d.Task.Payload)
		got[string(d.Task.Payload)] = true
		require.NoError(t, d.Ack())
	}
	require.Len(t, got, n)
	resp, err = st.GetQueue(context.Background(), q)
	require.NoError(t, err)
	require.Greater(t, resp.RangeID, rangeBefore, "reload takes a new lease")
	require.Eventually(t, func() bool {
		ts.Advance(50 * time.Millisecond) // the GC check runs on the UpdateAckInterval timer
		return st.TotalTaskCount(q) == 0
	}, 3*time.Second, 10*time.Millisecond, "acked tasks are GCed")
}

func TestSyncMatch(t *testing.T) {
	st := memstore.New()
	e := newEngine(t, st, nil)
	add(t, e, task("warmup")) // load
	require.NoError(t, poll(t, e).Ack())
	require.Eventually(t, func() bool { return st.TotalTaskCount(q) == 0 }, 3*time.Second, 10*time.Millisecond)

	ch := startPoller(t, e, taskqueue.PollOptions{})
	require.True(t, add(t, e, task("sync")), "should sync match a waiting poller")
	d := <-ch
	require.NotNil(t, d)
	require.True(t, d.SyncMatched)
	require.Zero(t, d.TaskID)
	require.Equal(t, "sync", string(d.Task.Payload))
	require.NoError(t, d.Ack())
	require.Equal(t, 0, st.TotalTaskCount(q), "sync-matched task never hits the store")
}

func TestBacklogDrainsInPriorityOrder(t *testing.T) {
	st := memstore.New()
	e := newEngine(t, st, nil)
	type item struct {
		pri int32
		seq int
	}
	var items []item
	for seq := range 40 {
		p := int32(rand.Intn(5) + 1)
		items = append(items, item{p, seq})
		tk := task(fmt.Sprintf("%d:%02d", p, seq))
		tk.Priority.PriorityKey = p
		require.False(t, add(t, e, tk))
	}
	require.Eventually(t, func() bool { return e.MatcherTasks(q) == len(items) }, 3*time.Second, time.Millisecond)

	sort.SliceStable(items, func(i, j int) bool { return items[i].pri < items[j].pri })
	for _, it := range items {
		d := poll(t, e)
		require.Equal(t, fmt.Sprintf("%d:%02d", it.pri, it.seq), string(d.Task.Payload))
		require.NoError(t, d.Ack())
	}
}

func TestDefaultPriorityIsMiddleLevel(t *testing.T) {
	st := memstore.New()
	e := newEngine(t, st, nil)
	lo := task("p4")
	lo.Priority.PriorityKey = 4
	add(t, e, lo)
	add(t, e, task("default")) // key 0 → default 3
	hi := task("p2")
	hi.Priority.PriorityKey = 2
	add(t, e, hi)
	require.Eventually(t, func() bool { return e.MatcherTasks(q) == 3 }, 3*time.Second, time.Millisecond)
	for _, want := range []string{"p2", "default", "p4"} {
		d := poll(t, e)
		require.Equal(t, want, string(d.Task.Payload))
		require.NoError(t, d.Ack())
	}
	_, err := e.AddTask(context.Background(), q, taskqueue.Task{Priority: taskqueue.Priority{PriorityKey: -1}})
	require.Error(t, err, "negative priority key is rejected (keys above PriorityLevels are clamped, see L3)")
}

// The 5s rule: a sync match is refused while the in-memory backlog has a task older than
// BacklogNegligibleAge, so new tasks cannot jump an old backlog.
func TestSyncMatchRefusedWhileBacklogIsOld(t *testing.T) {
	st := memstore.New()
	e := newEngine(t, st, nil)

	old := task("old-backlog")
	old.Priority.PriorityKey = 5
	old.CreateTime = time.Now().Add(-time.Minute)
	require.False(t, add(t, e, old))
	require.Eventually(t, func() bool { return e.MatcherTasks(q) == 1 }, 3*time.Second, time.Millisecond)

	// A poller that only accepts priority 1 waits even though a (priority 5) backlog exists.
	ch := startPoller(t, e, taskqueue.PollOptions{MinPriority: 1})
	hi := task("new")
	hi.Priority.PriorityKey = 1
	require.False(t, add(t, e, hi), "must not sync match while backlog is older than 5s")
	d := <-ch
	require.NotNil(t, d)
	require.Equal(t, "new", string(d.Task.Payload))
	require.False(t, d.SyncMatched, "delivered through the backlog")
	require.NoError(t, d.Ack())

	// Drain the old task; with a young (empty) backlog sync match works again.
	d = poll(t, e)
	require.Equal(t, "old-backlog", string(d.Task.Payload))
	require.NoError(t, d.Ack())
	ch = startPoller(t, e, taskqueue.PollOptions{MinPriority: 1})
	require.True(t, add(t, e, hi))
	require.True(t, (<-ch).SyncMatched)
}

func TestSyncMatchAllowedWithYoungBacklog(t *testing.T) {
	st := memstore.New()
	e := newEngine(t, st, nil)
	young := task("young")
	young.Priority.PriorityKey = 5
	require.False(t, add(t, e, young))
	require.Eventually(t, func() bool { return e.MatcherTasks(q) == 1 }, 3*time.Second, time.Millisecond)
	ch := startPoller(t, e, taskqueue.PollOptions{MinPriority: 1})
	hi := task("new")
	hi.Priority.PriorityKey = 1
	require.True(t, add(t, e, hi), "backlog younger than 5s does not block sync match")
	require.True(t, (<-ch).SyncMatched)
}

func TestLongPollTimeout(t *testing.T) {
	st := memstore.New()
	// The long-poll interval runs on the engine clock, so check it on a fake clock.
	ts, withClock := fakeClock()
	ef := newEngine(t, memstore.New(), func(c *taskqueue.Config) { c.LongPollExpirationInterval = 1500 * time.Millisecond }, withClock)
	_, err := ef.PollTask(context.Background(), q, taskqueue.PollOptions{NoWait: true}) // load
	require.ErrorIs(t, err, taskqueue.ErrNoTasks)
	errc := make(chan error, 1)
	go func() {
		_, err := ef.PollTask(context.Background(), q, taskqueue.PollOptions{})
		errc <- err
	}()
	waitPollers(t, ef, 1)
	ts.Advance(1400 * time.Millisecond)
	select {
	case err := <-errc:
		t.Fatalf("long poll returned before the interval: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	ts.Advance(100 * time.Millisecond)
	select {
	case err := <-errc:
		require.ErrorIs(t, err, taskqueue.ErrNoTasks, "long poll interval (no jitter below 10s)")
	case <-time.After(3 * time.Second):
		t.Fatal("long poll did not expire at the interval")
	}

	// The caller-deadline path compares against the caller's real-clock context deadline, so it
	// stays on the real clock.
	e := newEngine(t, st, func(c *taskqueue.Config) { c.LongPollExpirationInterval = 1500 * time.Millisecond })
	add(t, e, task("x"))
	require.NoError(t, poll(t, e).Ack())

	// caller deadline shorter than the interval: return empty 1s (returnEmptyTaskTimeBudget) early
	ctx, cancel := context.WithTimeout(context.Background(), 1200*time.Millisecond)
	defer cancel()
	start := time.Now()
	_, err = e.PollTask(ctx, q, taskqueue.PollOptions{})
	el := time.Since(start)
	require.ErrorIs(t, err, taskqueue.ErrNoTasks)
	require.InDelta(t, 200, el.Milliseconds(), 150)
	require.NoError(t, ctx.Err(), "returned before the caller's deadline")

	// NoWait
	start = time.Now()
	_, err = e.PollTask(context.Background(), q, taskqueue.PollOptions{NoWait: true})
	require.ErrorIs(t, err, taskqueue.ErrNoTasks)
	require.Less(t, time.Since(start), 100*time.Millisecond)
}

func TestLongPollJitterAboveTenSeconds(t *testing.T) {
	st := memstore.New()
	e := newEngine(t, st, func(c *taskqueue.Config) { c.LongPollExpirationInterval = time.Minute })
	// caller deadline 3s: child deadline = 2s regardless of jitter (interval ≥ 50s after jitter)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	start := time.Now()
	_, err := e.PollTask(ctx, q, taskqueue.PollOptions{})
	require.ErrorIs(t, err, taskqueue.ErrNoTasks)
	require.InDelta(t, 2000, time.Since(start).Milliseconds(), 300)
}

func TestLeaseAck(t *testing.T) {
	st := memstore.New()
	e := newEngine(t, st, nil)
	add(t, e, task("a"))
	d := poll(t, e)
	require.NotZero(t, d.TaskID)
	require.WithinDuration(t, time.Now().Add(time.Minute), d.LeaseDeadline(), 2*time.Second, "default lease 1m")
	require.NoError(t, d.Ack())
	require.ErrorIs(t, d.Ack(), taskqueue.ErrLeaseLost)
	require.ErrorIs(t, d.Nack(), taskqueue.ErrLeaseLost)
	require.ErrorIs(t, d.ExtendLease(time.Second), taskqueue.ErrLeaseLost)
	require.Eventually(t, func() bool { return st.TotalTaskCount(q) == 0 }, 3*time.Second, 10*time.Millisecond)
	_, err := e.PollTask(context.Background(), q, taskqueue.PollOptions{NoWait: true})
	require.ErrorIs(t, err, taskqueue.ErrNoTasks, "acked task is not redelivered")
}

func TestRedeliveryAfterUnackedLease(t *testing.T) {
	st := memstore.New()
	ts, withClock := fakeClock()
	e := newEngine(t, st, nil, withClock)
	add(t, e, task("a"))
	lease := taskqueue.PollOptions{LeaseDuration: 200 * time.Millisecond}

	d1, err := e.PollTask(context.Background(), q, lease)
	require.NoError(t, err)
	ch := startPoller(t, e, lease) // blocks until d1's lease expires
	ts.Advance(150 * time.Millisecond)
	select {
	case <-ch:
		t.Fatal("redelivered before the lease expired")
	case <-time.After(20 * time.Millisecond):
	}
	ts.Advance(60 * time.Millisecond)
	d2 := <-ch
	require.NotNil(t, d2)
	require.Equal(t, d1.TaskID, d2.TaskID, "same backlog task redelivered")
	require.ErrorIs(t, d1.Ack(), taskqueue.ErrLeaseLost, "expired lease cannot ack")

	// extend keeps it leased past the original deadline
	require.NoError(t, d2.ExtendLease(time.Second))
	_, err = e.PollTask(context.Background(), q, taskqueue.PollOptions{NoWait: true})
	ts.Advance(300 * time.Millisecond)
	_, err2 := e.PollTask(context.Background(), q, taskqueue.PollOptions{NoWait: true})
	require.ErrorIs(t, err, taskqueue.ErrNoTasks)
	require.ErrorIs(t, err2, taskqueue.ErrNoTasks)
	require.NoError(t, d2.Ack())
	require.Eventually(t, func() bool { return st.TotalTaskCount(q) == 0 }, 3*time.Second, 10*time.Millisecond)
}

func TestNackRedeliversImmediately(t *testing.T) {
	st := memstore.New()
	e := newEngine(t, st, nil)
	add(t, e, task("a"))
	d1 := poll(t, e)
	require.NoError(t, d1.Nack())
	d2, err := e.PollTask(context.Background(), q, taskqueue.PollOptions{NoWait: true})
	require.NoError(t, err)
	require.Equal(t, d1.TaskID, d2.TaskID)
	require.NoError(t, d2.Ack())
}

func TestSyncMatchedLeaseExpiryRespools(t *testing.T) {
	st := memstore.New()
	e := newEngine(t, st, nil)
	add(t, e, task("warmup"))
	require.NoError(t, poll(t, e).Ack())

	ch := startPoller(t, e, taskqueue.PollOptions{LeaseDuration: 150 * time.Millisecond})
	require.True(t, add(t, e, task("s")))
	d1 := <-ch
	require.True(t, d1.SyncMatched)
	// not acked: after expiry it is written to the backlog and redelivered
	d2 := poll(t, e)
	require.Equal(t, "s", string(d2.Task.Payload))
	require.False(t, d2.SyncMatched)
	require.NotZero(t, d2.TaskID)
	require.ErrorIs(t, d1.Ack(), taskqueue.ErrLeaseLost)
	require.NoError(t, d2.Ack())
}

func TestStopRespoolsSyncLeasesAndBacklogSurvivesRestart(t *testing.T) {
	st := memstore.New()
	e1, err := taskqueue.New(taskqueue.Options{Store: st, Config: taskqueue.Config{LongPollExpirationInterval: 2 * time.Second}})
	require.NoError(t, err)
	_, err = e1.AddTask(context.Background(), q, task("warmup"))
	require.NoError(t, err)
	d, err := e1.PollTask(context.Background(), q, taskqueue.PollOptions{})
	require.NoError(t, err)
	require.NoError(t, d.Ack())

	ch := make(chan *taskqueue.Delivery, 1)
	go func() {
		d, _ := e1.PollTask(context.Background(), q, taskqueue.PollOptions{})
		ch <- d
	}()
	waitPollers(t, e1, 1)
	m, err := e1.AddTask(context.Background(), q, task("leased-sync"))
	require.NoError(t, err)
	require.True(t, m)
	require.NotNil(t, <-ch)
	for i := range 5 {
		_, err := e1.AddTask(context.Background(), q, task(fmt.Sprint("b", i)))
		require.NoError(t, err)
	}
	e1.Stop()
	_, err = e1.AddTask(context.Background(), q, task("late"))
	require.ErrorIs(t, err, taskqueue.ErrEngineStopped)

	e2 := newEngine(t, st, nil)
	got := map[string]bool{}
	for range 6 {
		d := poll(t, e2)
		got[string(d.Task.Payload)] = true
		require.NoError(t, d.Ack())
	}
	require.Equal(t, map[string]bool{"leased-sync": true, "b0": true, "b1": true, "b2": true, "b3": true, "b4": true}, got)
}

type countingMetrics struct {
	metrics.Handler
	mu     sync.Mutex
	causes map[string]int
}

func (c *countingMetrics) Counter(name string, _ int64, tags ...metrics.Tag) {
	if name != metrics.TaskQueueStopped {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, t := range tags {
		c.causes[t.Value]++
	}
}

func (c *countingMetrics) count(cause string) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.causes[cause]
}

func stealOwnership(t *testing.T, st store.Store) int64 {
	t.Helper()
	resp, err := st.GetQueue(context.Background(), q)
	require.NoError(t, err)
	require.NoError(t, st.UpdateQueue(context.Background(), &store.UpdateQueueRequest{
		RangeID: resp.RangeID + 1, PrevRangeID: resp.RangeID, Info: resp.Info,
	}))
	return resp.RangeID + 1
}

func TestRangeIDConflictUnloadsQueue(t *testing.T) {
	st := memstore.New()
	m := &countingMetrics{Handler: metrics.Noop, causes: map[string]int{}}
	e := newEngine(t, st, nil, func(o *taskqueue.Options) { o.Metrics = m })
	add(t, e, task("before"))
	stolen := stealOwnership(t, st)

	// The write is fenced: CreateTasks fails with ConditionFailed, the queue unloads, and AddTask
	// retries on a freshly loaded queue (which takes over with a higher rangeID).
	add(t, e, task("after"))
	require.Eventually(t, func() bool { return m.count("Conflict") == 1 }, 3*time.Second, 10*time.Millisecond)
	resp, err := st.GetQueue(context.Background(), q)
	require.NoError(t, err)
	require.Greater(t, resp.RangeID, stolen)

	got := []string{}
	for range 2 {
		d := poll(t, e)
		got = append(got, string(d.Task.Payload))
		require.NoError(t, d.Ack())
	}
	require.ElementsMatch(t, []string{"before", "after"}, got)
}

func TestRangeIDConflictDetectedBySyncState(t *testing.T) {
	st := memstore.New()
	m := &countingMetrics{Handler: metrics.Noop, causes: map[string]int{}}
	e := newEngine(t, st, func(c *taskqueue.Config) { c.UpdateAckInterval = 50 * time.Millisecond },
		func(o *taskqueue.Options) { o.Metrics = m })
	add(t, e, task("x"))
	stealOwnership(t, st)
	require.Eventually(t, func() bool { return !loaded(e, q) }, 3*time.Second, 10*time.Millisecond)
	// The cause metric is emitted by Stop, just after the queue leaves the engine map.
	require.Eventually(t, func() bool { return m.count("Conflict") == 1 }, 3*time.Second, 10*time.Millisecond)
	require.Equal(t, 1, st.TotalTaskCount(q), "fenced owner deleted nothing")
}

type starterFunc func(ctx context.Context, req *taskqueue.StartRequest) error

func (f starterFunc) Start(ctx context.Context, req *taskqueue.StartRequest) error {
	return f(ctx, req)
}

func TestStarterErrorClassification(t *testing.T) {
	st := memstore.New()
	var calls sync.Map // payload -> count
	starter := starterFunc(func(_ context.Context, req *taskqueue.StartRequest) error {
		v, _ := calls.LoadOrStore(string(req.Task.Payload), new(atomic.Int32))
		n := v.(*atomic.Int32).Add(1)
		switch string(req.Task.Payload) {
		case "drop":
			return fmt.Errorf("bad: %w", taskqueue.ErrDropTask)
		case "retry":
			if n == 1 {
				return taskqueue.Retryable(errors.New("busy"))
			}
		case "respool":
			if n == 1 {
				return errors.New("permanent-ish")
			}
		}
		return nil
	})
	e := newEngine(t, st, nil, func(o *taskqueue.Options) { o.Starter = starter })
	for _, p := range []string{"drop", "retry", "respool"} {
		add(t, e, task(p))
	}
	got := map[string]int64{}
	for range 2 {
		d := poll(t, e)
		got[string(d.Task.Payload)] = d.TaskID
		require.NoError(t, d.Ack())
	}
	_, err := e.PollTask(context.Background(), q, taskqueue.PollOptions{NoWait: true})
	require.ErrorIs(t, err, taskqueue.ErrNoTasks, "dropped task is not delivered")
	require.Contains(t, got, "retry")
	require.Contains(t, got, "respool")
	require.Greater(t, got["respool"], int64(3), "re-spooled with a new task ID")
	require.LessOrEqual(t, got["retry"], int64(3), "retryable error keeps the original task")
	require.Eventually(t, func() bool { return st.TotalTaskCount(q) == 0 }, 3*time.Second, 10*time.Millisecond)
}

func TestExpiredTasksAreNotDelivered(t *testing.T) {
	st := memstore.New()
	ts, withClock := fakeClock()
	e := newEngine(t, st, nil, withClock)
	exp := task("expired")
	exp.ExpiryTime = ts.Now().Add(50 * time.Millisecond)
	add(t, e, exp)
	add(t, e, task("live"))
	ts.Advance(100 * time.Millisecond)
	d := poll(t, e)
	require.Equal(t, "live", string(d.Task.Payload))
	require.NoError(t, d.Ack())
	_, err := e.PollTask(context.Background(), q, taskqueue.PollOptions{NoWait: true})
	require.ErrorIs(t, err, taskqueue.ErrNoTasks)
}

// No loss and no duplicate acks under concurrent producers/consumers, with mixed priorities,
// random nacks and a small in-memory batch / ID range (forces re-reads, eviction and lease renewal).
func TestConcurrentProducersConsumersNoLossNoDup(t *testing.T) {
	for _, tc := range []struct {
		name         string
		mod          func(*taskqueue.Config)
		nackPct      int
		backlogFirst bool // consumers start after producers finish: everything goes through the store
	}{
		{"defaults", nil, 0, false},
		{"backlog-first", nil, 0, true},
		{"backlog-first-small-batches-with-nacks", func(c *taskqueue.Config) {
			c.GetTasksBatchSize = 20
			c.GetTasksReloadAt = 5
			c.MaxTaskBatchSize = 7
			c.RangeSize = 50
		}, 10, true},
		{"small-batches-with-nacks", func(c *taskqueue.Config) {
			c.GetTasksBatchSize = 20
			c.GetTasksReloadAt = 5
			c.MaxTaskBatchSize = 7
			c.RangeSize = 50 // exercise task ID block renewal
		}, 10, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			st := memstore.New()
			e := newEngine(t, st, tc.mod)
			const producers, perProducer, consumers = 8, 300, 8
			total := producers * perProducer

			var acked sync.Map
			var ackCount, deliveries, nacks, syncMatched atomic.Int64
			ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
			defer cancel()

			var cwg sync.WaitGroup
			var pwg sync.WaitGroup
			startConsumers := func() {
				for range consumers {
					cwg.Go(func() {
						for ackCount.Load() < int64(total) && ctx.Err() == nil {
							d, err := e.PollTask(ctx, q, taskqueue.PollOptions{})
							if errors.Is(err, taskqueue.ErrNoTasks) {
								continue
							}
							if err != nil {
								return
							}
							deliveries.Add(1)
							if d.SyncMatched {
								syncMatched.Add(1)
							}
							if tc.nackPct > 0 && rand.Intn(100) < tc.nackPct {
								nacks.Add(1)
								assert.NoError(t, d.Nack())
								continue
							}
							if _, dup := acked.LoadOrStore(string(d.Task.Payload), true); dup {
								t.Errorf("duplicate delivery of acked task %s", d.Task.Payload)
							}
							assert.NoError(t, d.Ack())
							ackCount.Add(1)
						}
					})
				}
			}
			if !tc.backlogFirst {
				startConsumers()
			}
			for p := range producers {
				pwg.Go(func() {
					for i := range perProducer {
						tk := task(fmt.Sprintf("%d-%d", p, i))
						tk.Priority.PriorityKey = int32(rand.Intn(6)) // 0..5
						for {
							_, err := e.AddTask(ctx, q, tk)
							if errors.Is(err, taskqueue.ErrResourceExhausted) {
								time.Sleep(time.Millisecond)
								continue
							}
							assert.NoError(t, err)
							break
						}
					}
				})
			}
			pwg.Wait()
			if tc.backlogFirst {
				require.Equal(t, total, st.TotalTaskCount(q))
				startConsumers()
			}
			require.Eventually(t, func() bool { return ackCount.Load() == int64(total) }, 60*time.Second, 10*time.Millisecond,
				"acked %d of %d", ackCount.Load(), total)
			cancel()
			cwg.Wait()

			n := 0
			acked.Range(func(any, any) bool { n++; return true })
			require.Equal(t, total, n)
			require.Equal(t, deliveries.Load(), int64(total)+nacks.Load(), "every delivery is an ack or a nack")
			require.Eventually(t, func() bool { return st.TotalTaskCount(q) == 0 }, 5*time.Second, 10*time.Millisecond, "all GCed")
			t.Logf("deliveries=%d nacks=%d syncMatched=%d", deliveries.Load(), nacks.Load(), syncMatched.Load())
		})
	}
}
