package matching

// Regression tests for the feat/ops-dlq review round 2 batch 1:
//   - Stop during a successful attempt write must roll the durable count back;
//   - the lease deadline is the moment the timer is armed, not the pre-write snapshot;
//   - an attempts_exhausted dead-letter is not a dispatch;
//   - a lost attempt-write fence is not a task_attempt_record_failures.

import (
	"context"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/skme902/tq/clock"
	"github.com/skme902/tq/metrics"
	"github.com/skme902/tq/metrics/metricstest"
	"github.com/skme902/tq/store"
	"github.com/skme902/tq/store/memstore"
)

// updateGate blocks the first UpdateTask until release is closed, then delegates.
// Later calls (the attempt-count rollback) go straight through.
type updateGate struct {
	*testStore

	mu      sync.Mutex
	once    sync.Once
	writes  []int32
	entered chan struct{}
	release chan struct{}
}

func newUpdateGate() *updateGate {
	return &updateGate{
		testStore: newTestStore(),
		entered:   make(chan struct{}),
		release:   make(chan struct{}),
	}
}

func (g *updateGate) UpdateTask(ctx context.Context, req *store.UpdateTaskRequest) error {
	block := false
	g.once.Do(func() { block = true })
	if block {
		close(g.entered)
		select {
		case <-g.release:
		case <-ctx.Done():
			return ctx.Err()
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

// unblockOnCleanup closes ch if the test fails while an UpdateTask is still blocked,
// so Engine.Stop can take the db lock the write holds.
func unblockOnCleanup(t *testing.T, ch chan struct{}) {
	t.Helper()
	t.Cleanup(func() {
		select {
		case <-ch:
		default:
			close(ch)
		}
	})
}

func newR2Engine(t *testing.T, st store.Store, ts clock.TimeSource, h metrics.Handler, mut func(*Config)) *Engine {
	t.Helper()
	if ts == nil {
		ts = clock.NewRealTimeSource()
	}
	cfg := DefaultConfig()
	// Keep periodic sync, idle unload, GC and backlog gauges off the fake-clock advances.
	cfg.LongPollExpirationInterval = 15 * time.Second
	cfg.UpdateAckInterval = time.Hour
	cfg.TaskDeleteInterval = time.Hour
	cfg.MaxTaskQueueIdleTime = time.Hour
	cfg.MetadataUpdateOnAppendInterval = time.Hour
	cfg.BacklogMetricsEmitInterval = time.Hour
	if mut != nil {
		mut(&cfg)
	}
	e, err := NewEngine(Options{Config: cfg, Store: st, TimeSource: ts, Logger: discardLogger(), Metrics: h})
	require.NoError(t, err)
	t.Cleanup(e.Stop)
	return e
}

func portMetricTags() []metrics.Tag {
	return []metrics.Tag{
		{Key: metrics.TagNamespace, Value: portQueue.Namespace},
		{Key: metrics.TagTaskQueue, Value: portQueue.Name},
		{Key: metrics.TagPartition, Value: strconv.Itoa(portQueue.Partition)},
	}
}

func backlogRows(t *testing.T, st store.Store) []store.TaskInfo {
	t.Helper()
	ctx := context.Background()
	resp, err := st.GetQueue(ctx, portQueue)
	require.NoError(t, err)
	require.NotNil(t, resp.Info)
	var out []store.TaskInfo
	for i := range resp.Info.Subqueues {
		page, err := st.GetTasks(ctx, &store.GetTasksRequest{
			Key: portQueue, Subqueue: i, InclusiveMin: store.Level{Pass: 1}, PageSize: 100,
		})
		require.NoError(t, err)
		out = append(out, page.Tasks...)
	}
	return out
}

type pollOutcome struct {
	d   *Delivery
	err error
}

func waitOutcome(t *testing.T, ch <-chan pollOutcome) pollOutcome {
	t.Helper()
	select {
	case r := <-ch:
		return r
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for poll")
		return pollOutcome{}
	}
}

func dispatchCount(t *testing.T, e *Engine) int {
	t.Helper()
	e.partitionsLock.RLock()
	pm := e.partitions[portQueue]
	e.partitionsLock.RUnlock()
	require.NotNil(t, pm, "queue should be loaded")
	pqm := pm.defaultQueue
	pqm.taskTrackerLock.Lock()
	defer pqm.taskTrackerLock.Unlock()
	n := 0
	for _, tt := range pqm.tasksDispatched {
		n += tt.tasks.totalTasks()
	}
	return n
}

// Stop during a committed attempt write must not leave Attempts incremented: PollTask
// returns ErrEngineStopped and the stored count is back to 0, so a new engine with
// MaxAttempts=1 delivers the task instead of dead-lettering it.
func TestStopDuringAttemptWriteRollsBackCount(t *testing.T) {
	g := newUpdateGate()
	e := newR2Engine(t, g, nil, nil, func(c *Config) { c.MaxAttempts = 1 })
	unblockOnCleanup(t, g.release)

	_, err := e.AddTask(context.Background(), portQueue, store.Task{Payload: []byte("stop-rollback")})
	require.NoError(t, err)

	ch := make(chan pollOutcome, 1)
	go func() {
		d, err := e.PollTask(context.Background(), portQueue, PollOptions{})
		ch <- pollOutcome{d, err}
	}()
	select {
	case <-g.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("attempt write did not start")
	}

	stopDone := make(chan struct{})
	go func() {
		e.Stop()
		close(stopDone)
	}()
	require.Eventually(t, func() bool {
		e.leasesLock.Lock()
		defer e.leasesLock.Unlock()
		return e.leasesClosed
	}, 3*time.Second, time.Millisecond, "Stop should close leasing while the attempt write is blocked")

	close(g.release)
	r := waitOutcome(t, ch)
	require.ErrorIs(t, r.err, ErrEngineStopped)
	require.Nil(t, r.d)
	select {
	case <-stopDone:
	case <-time.After(5 * time.Second):
		t.Fatal("Stop did not return")
	}

	rows := backlogRows(t, g)
	require.Len(t, rows, 1)
	require.Equal(t, int32(0), rows[0].Data.Attempts, "committed attempt must be rolled back when the lease is not returned")
	require.Equal(t, []int32{1, 0}, g.attemptWrites(), "rollback persists the previous count with the same fenced write")

	e2 := newR2Engine(t, g, nil, nil, func(c *Config) { c.MaxAttempts = 1 })
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	d, err := e2.PollTask(ctx, portQueue, PollOptions{})
	require.NoError(t, err)
	require.Equal(t, "stop-rollback", string(d.Task.Payload))
	require.False(t, d.SyncMatched)
	require.Equal(t, int32(1), d.Task.Attempts, "a rolled-back task is delivered, not dead-lettered")
	dead, _, err := e2.ListDeadTasks(ctx, portQueue, 10, nil)
	require.NoError(t, err)
	require.Empty(t, dead)
	require.NoError(t, d.Ack())
}

// The lease deadline is stamped when the timer is armed, which is after the attempt
// write. Time that passes during that write must not put the deadline in the past.
func TestLeaseDeadlineAfterSlowAttemptWrite(t *testing.T) {
	src := clock.NewEventTimeSource().Update(time.Now())
	src.UseAsyncTimers(true)
	g := newUpdateGate()
	const lease = time.Second
	e := newR2Engine(t, g, src, nil, func(c *Config) { c.MaxAttempts = 4 })
	unblockOnCleanup(t, g.release)

	_, err := e.AddTask(context.Background(), portQueue, store.Task{Payload: []byte("slow-lease")})
	require.NoError(t, err)

	ch := make(chan pollOutcome, 1)
	go func() {
		d, err := e.PollTask(context.Background(), portQueue, PollOptions{LeaseDuration: lease})
		ch <- pollOutcome{d, err}
	}()
	select {
	case <-g.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("attempt write did not start")
	}

	src.Advance(lease + time.Second)
	select {
	case r := <-ch:
		t.Fatalf("poll returned while the attempt write was blocked: delivery=%v err=%v", r.d != nil, r.err)
	default:
	}
	close(g.release)

	r := waitOutcome(t, ch)
	require.NoError(t, r.err)
	require.NotNil(t, r.d)
	require.Equal(t, src.Now().Add(lease), r.d.LeaseDeadline())
	require.NoError(t, r.d.Ack())
}

// A task reloaded with Attempts == MaxAttempts is dead-lettered and must not move
// DescribeQueue's dispatch count or rate. A delivery that is actually returned still does.
func TestExhaustedTaskNotCountedAsDispatched(t *testing.T) {
	ts := newTestStore()
	e1 := newR2Engine(t, ts, nil, nil, func(c *Config) { c.MaxAttempts = 1 })
	_, err := e1.AddTask(context.Background(), portQueue, store.Task{Payload: []byte("spent")})
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	d, err := e1.PollTask(ctx, portQueue, PollOptions{})
	cancel()
	require.NoError(t, err)
	require.Equal(t, int32(1), d.Task.Attempts)
	e1.Stop() // lease ended without ack; the row stays with Attempts == 1

	rows := backlogRows(t, ts)
	require.Len(t, rows, 1)
	require.Equal(t, int32(1), rows[0].Data.Attempts)

	src := clock.NewEventTimeSource().Update(time.Now())
	src.UseAsyncTimers(true)
	e2 := newR2Engine(t, ts, src, nil, func(c *Config) { c.MaxAttempts = 1 })

	var delivered *Delivery
	require.Eventually(t, func() bool {
		got, perr := e2.PollTask(context.Background(), portQueue, PollOptions{NoWait: true})
		if perr == nil {
			delivered = got
			return true
		}
		dead, _, lerr := e2.ListDeadTasks(context.Background(), portQueue, 10, nil)
		return lerr == nil && len(dead) == 1
	}, 3*time.Second, time.Millisecond)
	require.Nil(t, delivered, "Attempts == MaxAttempts must be dead-lettered, not delivered")
	dead, _, err := e2.ListDeadTasks(context.Background(), portQueue, 10, nil)
	require.NoError(t, err)
	require.Len(t, dead, 1)
	require.Equal(t, DeadReasonAttemptsExhausted, dead[0].Reason)

	// The tracker, if wrongly incremented at match time, was created before this advance,
	// so the rate over the 4s window is non-zero. A missing increment stays at 0.
	src.Advance(4 * time.Second)
	require.Zero(t, dispatchCount(t, e2), "dead-lettered task must not be counted as dispatched")
	desc, err := e2.DescribeQueue(context.Background(), portQueue)
	require.NoError(t, err)
	require.Zero(t, desc.Stats.TasksDispatchRate)
	for pri, s := range desc.StatsByPriority {
		require.Zerof(t, s.TasksDispatchRate, "priority %d", pri)
	}

	_, err = e2.AddTask(context.Background(), portQueue, store.Task{Payload: []byte("fresh")})
	require.NoError(t, err)
	require.Eventually(t, func() bool { return e2.MatcherTasks(portQueue) == 1 }, 3*time.Second, time.Millisecond)
	src.Advance(4 * time.Second)
	ctx, cancel = context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	got, err := e2.PollTask(ctx, portQueue, PollOptions{})
	require.NoError(t, err)
	require.Equal(t, "fresh", string(got.Task.Payload))
	require.Equal(t, 1, dispatchCount(t, e2), "a returned delivery is still counted")
	desc, err = e2.DescribeQueue(context.Background(), portQueue)
	require.NoError(t, err)
	require.Greater(t, desc.Stats.TasksDispatchRate, float32(0))
	require.NoError(t, got.Ack())
}

// A ConditionFailed attempt write unloads the queue and emits condition_failed.
// It must not also increment task_attempt_record_failures.
func TestAttemptFenceNotCountedAsRecordFailure(t *testing.T) {
	rec := metricstest.New()
	ts := newTestStore()
	ts.InjectError(memstore.OpUpdateTask, &store.ConditionFailedError{Msg: "fence"})
	e := newR2Engine(t, ts, nil, rec, func(c *Config) { c.MaxAttempts = 3 })

	_, err := e.AddTask(context.Background(), portQueue, store.Task{Payload: []byte("fenced")})
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	d, err := e.PollTask(ctx, portQueue, PollOptions{})
	require.Nil(t, d)
	require.Error(t, err)

	tags := portMetricTags()
	require.Equal(t, int64(0), rec.CounterValue(metrics.TaskAttemptRecordFailures, tags...))
	require.GreaterOrEqual(t, rec.CounterValue(metrics.ConditionFailedErrors, tags...), int64(1))
}
