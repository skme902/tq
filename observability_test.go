package taskqueue_test

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	taskqueue "github.com/skme902/tq"
	"github.com/skme902/tq/metrics"
	"github.com/skme902/tq/metrics/metricstest"
	"github.com/skme902/tq/store"
	"github.com/skme902/tq/store/memstore"
)

// qTags are the per-queue tags of the shared test queue q.
func qTags(extra ...metrics.Tag) []metrics.Tag {
	return append([]metrics.Tag{
		{Key: metrics.TagNamespace, Value: q.Namespace},
		{Key: metrics.TagTaskQueue, Value: q.Name},
		{Key: metrics.TagPartition, Value: "0"},
	}, extra...)
}

func withMetrics(r metrics.Handler) func(*taskqueue.Options) {
	return func(o *taskqueue.Options) { o.Metrics = r }
}

func TestMetricsCarryQueueTagsNotFairnessKeys(t *testing.T) {
	rec := metricstest.New()
	e := newEngine(t, memstore.New(), nil, withMetrics(rec))
	add(t, e, taskqueue.Task{Payload: []byte("a"), Priority: taskqueue.Priority{FairnessKey: "tenant-secret-1"}})
	d := poll(t, e)
	require.NoError(t, d.Ack())

	require.Equal(t, int64(1), rec.CounterValue(metrics.TasksAdded, qTags()...))
	require.Equal(t, int64(1), rec.CounterValue(metrics.TasksSpooled, qTags()...))
	require.Equal(t, int64(1), rec.CounterValue(metrics.PollSuccess, qTags()...))
	require.Equal(t, int64(1), rec.CounterValue(metrics.DeliveryAcked, qTags()...))
	require.Len(t, rec.TimerValues(metrics.TaskWriteLatency, qTags()...), 1, "write latency is tagged per queue")

	// Every series of every queue-level metric carries the queue's tags...
	for _, name := range []string{metrics.TasksAdded, metrics.PollSuccess, metrics.TaskWriteLatency,
		metrics.TaskDispatchLatency, metrics.DeliveryAckLatency, metrics.PendingPolls, metrics.TaskQueueStarted} {
		series := rec.Series(name)
		require.NotEmpty(t, series, name)
		for _, s := range series {
			require.Contains(t, s, "taskqueue=orders", s)
			require.Contains(t, s, "namespace=ns", s)
			require.Contains(t, s, "partition=0", s)
		}
	}
	// ...and never a fairness key.
	require.False(t, rec.HasTagValue("tenant-secret-1"), "fairness keys must never become tag values")
	for _, k := range rec.TagKeys() {
		require.Contains(t, []string{metrics.TagNamespace, metrics.TagTaskQueue, metrics.TagPartition,
			metrics.TagTaskPriority, metrics.TagSource, metrics.TagForwarded, "cause", "reason"}, k)
	}
}

func TestMetricsQueueTagCap(t *testing.T) {
	q2 := taskqueue.QueueKey{Namespace: "ns", Name: "second"}
	q3 := taskqueue.QueueKey{Namespace: "ns", Name: "third"}
	for _, tc := range []struct {
		name  string
		limit int
		real  []string
	}{
		{"cap of 1", 1, []string{q.Name}},
		{"disabled", -1, nil},
		{"default", 0, []string{q.Name, q2.Name, q3.Name}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rec := metricstest.New()
			e := newEngine(t, memstore.New(), func(c *taskqueue.Config) {
				c.MetricsQueueTagLimit = tc.limit
				c.BacklogMetricsEmitInterval = -1 // the ack-level sync emits approximate_backlog_count
			}, withMetrics(rec))
			ctx := context.Background()
			for _, k := range []taskqueue.QueueKey{q, q2, q3} {
				_, err := e.AddTask(ctx, k, task("x"))
				require.NoError(t, err)
			}
			_, err := e.AddTask(ctx, q, task("y")) // a granted queue keeps its tag
			require.NoError(t, err)

			omitted := 3 - len(tc.real)
			for _, name := range tc.real {
				n := int64(1)
				if name == q.Name {
					n = 2
				}
				require.Equal(t, n, rec.CounterValue(metrics.TasksAdded,
					metrics.Tag{Key: metrics.TagNamespace, Value: "ns"},
					metrics.Tag{Key: metrics.TagTaskQueue, Value: name},
					metrics.Tag{Key: metrics.TagPartition, Value: "0"}))
			}
			omittedAdds := int64(omitted)
			if !slices.Contains(tc.real, q.Name) {
				omittedAdds++ // the second add on q
			}
			require.Equal(t, omittedAdds, rec.CounterValue(metrics.TasksAdded,
				metrics.Tag{Key: metrics.TagNamespace, Value: "ns"},
				metrics.Tag{Key: metrics.TagTaskQueue, Value: metrics.OmittedValue},
				metrics.Tag{Key: metrics.TagPartition, Value: "0"}))
			require.Len(t, rec.Series(metrics.TasksAdded), len(tc.real)+min(omitted, 1), "series count is bounded by the cap")

			// Per-queue gauges only for queues with a real tag (upstream: no gauges without the
			// breakdown).
			if len(tc.real) > 0 {
				require.Eventually(t, func() bool {
					for _, s := range rec.Series(metrics.ApproximateBacklogCount) {
						if strings.Contains(s, "taskqueue="+tc.real[len(tc.real)-1]) {
							return true
						}
					}
					return false
				}, 3*time.Second, 10*time.Millisecond)
			} else {
				time.Sleep(200 * time.Millisecond) // several ack-level syncs
			}
			for _, s := range rec.Series(metrics.ApproximateBacklogCount) {
				require.NotContains(t, s, metrics.OmittedValue, s)
			}
		})
	}
}

func TestDeliveryMetricsAndLatenciesFakeClock(t *testing.T) {
	ts, clockOpt := fakeClock()
	rec := metricstest.New()
	e := newEngine(t, memstore.New(), nil, clockOpt, withMetrics(rec))

	// Backlog task: created at t0, dispatched at t0+3s, acked at t0+5s.
	require.False(t, add(t, e, task("backlog")))
	ts.Advance(3 * time.Second)
	d := poll(t, e)
	require.Equal(t, []time.Duration{3 * time.Second}, rec.TimerValues(metrics.TaskDispatchLatency, qTags(
		metrics.Tag{Key: metrics.TagSource, Value: metrics.SourceDBBacklog},
		metrics.Tag{Key: metrics.TagForwarded, Value: "false"},
		metrics.TaskPriorityTag(0))...))
	require.Equal(t, []time.Duration{3 * time.Second}, rec.TimerValues(metrics.AsyncMatchLatency, qTags()...))
	ts.Advance(2 * time.Second)
	require.NoError(t, d.Ack())
	require.Equal(t, []time.Duration{2 * time.Second}, rec.TimerValues(metrics.DeliveryAckLatency, qTags()...))
	require.Equal(t, int64(1), rec.CounterValue(metrics.DeliveryAcked, qTags()...))

	// Sync match: dispatch latency 0, source History, priority tagged; syncmatch_latency recorded.
	ch := startPoller(t, e, taskqueue.PollOptions{})
	require.True(t, add(t, e, taskqueue.Task{Payload: []byte("sync"), Priority: taskqueue.Priority{PriorityKey: 2}}))
	d = <-ch
	require.NotNil(t, d)
	require.Equal(t, []time.Duration{0}, rec.TimerValues(metrics.TaskDispatchLatency, qTags(
		metrics.Tag{Key: metrics.TagSource, Value: metrics.SourceHistory},
		metrics.Tag{Key: metrics.TagForwarded, Value: "false"},
		metrics.TaskPriorityTag(2))...))
	require.Len(t, rec.TimerValues(metrics.SyncMatchLatency, qTags()...), 1)
	require.Empty(t, rec.TimerValues(metrics.AsyncMatchLatency, qTags()...)[1:], "asyncmatch is backlog-only")

	// Nack, extend, expiry.
	require.NoError(t, d.Nack())
	require.Equal(t, int64(1), rec.CounterValue(metrics.DeliveryNacked, qTags()...))
	d, err := e.PollTask(context.Background(), q, taskqueue.PollOptions{LeaseDuration: time.Second})
	require.NoError(t, err)
	require.NoError(t, d.ExtendLease(2*time.Second))
	require.Equal(t, int64(1), rec.CounterValue(metrics.DeliveryLeaseExtended, qTags()...))
	ts.Advance(3 * time.Second)
	require.Eventually(t, func() bool { return rec.CounterValue(metrics.DeliveryLeaseExpired, qTags()...) == 1 },
		3*time.Second, time.Millisecond)
	require.ErrorIs(t, d.Ack(), taskqueue.ErrLeaseLost)
	require.Equal(t, int64(1), rec.CounterValue(metrics.DeliveryAcked, qTags()...), "a lost ack is not counted")
}

func TestBacklogGaugesFakeClock(t *testing.T) {
	ts, clockOpt := fakeClock()
	rec := metricstest.New()
	e := newEngine(t, memstore.New(), func(c *taskqueue.Config) {
		c.BacklogMetricsEmitInterval = 10 * time.Second
	}, clockOpt, withMetrics(rec))
	for _, p := range []int32{1, 1, 4} {
		add(t, e, taskqueue.Task{Payload: []byte("x"), Priority: taskqueue.Priority{PriorityKey: p}})
	}
	require.Eventually(t, func() bool { return e.MatcherTasks(q) == 3 }, 3*time.Second, time.Millisecond)

	ts.Advance(15 * time.Second) // one periodic emission (interval 10 s ± 5%)
	require.Eventually(t, func() bool {
		v, ok := rec.GaugeValue(metrics.ApproximateBacklogAgeSeconds, qTags()...)
		return ok && v > 14.9
	}, 3*time.Second, time.Millisecond)
	age, _ := rec.GaugeValue(metrics.ApproximateBacklogAgeSeconds, qTags()...)
	require.InDelta(t, 15, age, 0.6, "age is measured on the injected clock")
	c1, ok := rec.GaugeValue(metrics.ApproximateBacklogCount, qTags(metrics.TaskPriorityTag(1))...)
	require.True(t, ok)
	require.Equal(t, 2.0, c1)
	c4, _ := rec.GaugeValue(metrics.ApproximateBacklogCount, qTags(metrics.TaskPriorityTag(4))...)
	require.Equal(t, 1.0, c4)
	// The ack-level sync emits the physical_* variant of the count.
	require.Eventually(t, func() bool {
		v, ok := rec.GaugeValue(metrics.PhysicalApproximateBacklogCount, qTags(metrics.TaskPriorityTag(1))...)
		return ok && v == 2
	}, 3*time.Second, time.Millisecond)

	// Unloading zeroes the gauges (upstream), so stale values do not linger.
	e.Stop()
	age, _ = rec.GaugeValue(metrics.ApproximateBacklogAgeSeconds, qTags()...)
	require.Zero(t, age)
	c1, _ = rec.GaugeValue(metrics.ApproximateBacklogCount, qTags(metrics.TaskPriorityTag(1))...)
	require.Zero(t, c1)
}

func TestDescribeLoadedQueue(t *testing.T) {
	ts, clockOpt := fakeClock()
	cs := taskqueue.NewMemoryQueueConfigSource(0)
	rps := float32(50)
	require.NoError(t, cs.SetQueueRateLimit(q, &rps))
	e := newEngine(t, memstore.New(), nil, clockOpt, func(o *taskqueue.Options) { o.QueueConfigSource = cs })
	for _, p := range []int32{1, 1, 1, 5} {
		add(t, e, taskqueue.Task{Payload: []byte("x"), Priority: taskqueue.Priority{PriorityKey: p}})
	}
	require.Eventually(t, func() bool { return e.MatcherTasks(q) == 4 }, 3*time.Second, time.Millisecond)
	ts.Advance(4 * time.Second)
	d := poll(t, e) // a priority-1 task; its lease is held
	require.Equal(t, int32(1), d.Task.Priority.PriorityKey)

	desc, err := e.DescribeQueue(context.Background(), q)
	require.NoError(t, err)
	require.True(t, desc.Loaded)
	require.Equal(t, q, desc.Queue)
	require.Equal(t, int64(1), desc.RangeID)
	require.Equal(t, int64(4), desc.Stats.ApproximateBacklogCount, "a leased task is still in the backlog")
	require.Equal(t, int64(3), desc.StatsByPriority[1].ApproximateBacklogCount)
	require.Equal(t, int64(1), desc.StatsByPriority[5].ApproximateBacklogCount)
	require.Equal(t, 4*time.Second, desc.Stats.ApproximateBacklogAge)
	require.InDelta(t, 1.0, desc.Stats.TasksAddRate, 1e-6, "4 adds over 4 s")
	require.InDelta(t, 0.75, desc.StatsByPriority[1].TasksAddRate, 1e-6)
	require.InDelta(t, 0.25, desc.Stats.TasksDispatchRate, 1e-6, "1 dispatch over 4 s")
	require.Equal(t, 50.0, desc.EffectiveRateLimit)
	require.Equal(t, taskqueue.RateLimitSourceAPI, desc.RateLimitSource)
	require.Equal(t, int64(1), desc.OutstandingLeases)
	require.Equal(t, 3, desc.MatcherTasks)
	require.Zero(t, desc.WaitingPollers)
	require.NoError(t, d.Ack())

	// Waiting pollers on an empty queue.
	q2 := taskqueue.QueueKey{Namespace: "ns", Name: "empty"}
	go func() { _, _ = e.PollTask(context.Background(), q2, taskqueue.PollOptions{}) }()
	require.Eventually(t, func() bool { return e.WaitingPollers(q2) == 1 }, 3*time.Second, time.Millisecond)
	desc, err = e.DescribeQueue(context.Background(), q2)
	require.NoError(t, err)
	require.Equal(t, 1, desc.WaitingPollers)
	require.Zero(t, desc.Stats.ApproximateBacklogCount)
	require.Zero(t, desc.Stats.ApproximateBacklogAge)
	require.True(t, desc.EffectiveRateLimit > 0)
}

func TestDescribeUnloadedQueueDoesNotTakeOwnership(t *testing.T) {
	st := memstore.New()
	ts, clockOpt := fakeClock()
	e1 := newEngine(t, st, nil, clockOpt)
	for _, p := range []int32{2, 2, 2} {
		add(t, e1, taskqueue.Task{Payload: []byte("x"), Priority: taskqueue.Priority{PriorityKey: p}})
	}
	e1.Stop()
	ctx := context.Background()
	before, err := st.GetQueue(ctx, q)
	require.NoError(t, err)

	ts.Advance(10 * time.Second)
	e2 := newEngine(t, st, nil, clockOpt)
	desc, err := e2.DescribeQueue(ctx, q)
	require.NoError(t, err)
	require.False(t, desc.Loaded)
	require.Equal(t, before.RangeID, desc.RangeID)
	require.Equal(t, int64(3), desc.Stats.ApproximateBacklogCount)
	require.Equal(t, int64(3), desc.StatsByPriority[2].ApproximateBacklogCount)
	require.Equal(t, 10*time.Second, desc.Stats.ApproximateBacklogAge, "age from the persisted tasks")
	require.Equal(t, taskqueue.RateLimitSourceSystem, desc.RateLimitSource)
	require.Equal(t, 1000.0, desc.EffectiveRateLimit, "default admin queue rate")
	require.Zero(t, desc.Stats.TasksAddRate)

	after, err := st.GetQueue(ctx, q)
	require.NoError(t, err)
	require.Equal(t, before.RangeID, after.RangeID, "describe must not bump the rangeID")
	require.Equal(t, before.Info, after.Info, "describe must not write metadata")
	require.Empty(t, e2.LoadedQueues(), "describe must not load the queue")

	_, err = e2.DescribeQueue(ctx, taskqueue.QueueKey{Namespace: "ns", Name: "never-used"})
	require.True(t, store.IsNotFound(err), "%v", err)
}

func TestListQueuesPaged(t *testing.T) {
	st := memstore.New()
	e := newEngine(t, st, nil)
	ctx := context.Background()
	for i := range 5 {
		_, err := e.AddTask(ctx, taskqueue.QueueKey{Namespace: "ns", Name: fmt.Sprintf("q%d", i)}, task("x"))
		require.NoError(t, err)
	}
	var got []taskqueue.QueueListing
	var tok []byte
	for pages := 0; ; pages++ {
		require.Less(t, pages, 10)
		page, err := e.ListQueues(ctx, 2, tok)
		require.NoError(t, err)
		require.LessOrEqual(t, len(page.Queues), 2)
		got = append(got, page.Queues...)
		if page.NextPageToken == nil {
			break
		}
		tok = page.NextPageToken
	}
	require.Len(t, got, 5)
	for i, l := range got {
		require.Equal(t, fmt.Sprintf("q%d", i), l.Queue.Name)
		require.True(t, l.Loaded)
		require.Positive(t, l.RangeID)
	}
}
