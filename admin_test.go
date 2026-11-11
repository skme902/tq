package taskqueue_test

import (
	"context"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"strings"
	"sync"
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

func peekAll(t *testing.T, e *taskqueue.Engine, opts taskqueue.PeekOptions) []taskqueue.PeekedTask {
	t.Helper()
	var all []taskqueue.PeekedTask
	for {
		page, next, err := e.PeekTasks(context.Background(), q, opts)
		require.NoError(t, err)
		all = append(all, page...)
		if next == nil {
			return all
		}
		opts.PageToken = next
	}
}

func peekPayloads(ts []taskqueue.PeekedTask) []string {
	var out []string
	for _, p := range ts {
		out = append(out, string(p.Task.Payload))
	}
	return out
}

func prioTask(payload string, pri int32, key string) taskqueue.Task {
	tk := task(payload)
	tk.Priority = store.Priority{PriorityKey: pri, FairnessKey: key}
	return tk
}

// Peek pages through every priority in dispatch order, filters by priority and fairness key,
// and reports leases on a queue loaded here.
func TestPeekTasks(t *testing.T) {
	e := newEngine(t, memstore.New(), nil)
	for i := range 5 {
		add(t, e, prioTask(fmt.Sprint("lo", i), 5, "k"))
	}
	add(t, e, prioTask("hi0", 1, "x"))
	add(t, e, prioTask("hi1", 1, "k"))

	all := peekAll(t, e, taskqueue.PeekOptions{PageSize: 2})
	require.Equal(t, []string{"lo0", "lo1", "lo2", "lo3", "lo4", "hi0", "hi1"}, peekPayloads(all),
		"subqueue order (first-used priority first), then fair order")
	require.Equal(t, int32(5), all[0].Priority)
	require.Equal(t, int32(1), all[5].Priority)

	require.Equal(t, []string{"hi0", "hi1"}, peekPayloads(peekAll(t, e, taskqueue.PeekOptions{Priority: 1})))
	require.Equal(t, []string{"lo0", "lo1", "lo2", "lo3", "lo4", "hi1"},
		peekPayloads(peekAll(t, e, taskqueue.PeekOptions{FairnessKey: "k", PageSize: 1})))

	d := poll(t, e) // hi0: priority 1 first
	require.Equal(t, "hi0", string(d.Task.Payload))
	got := peekAll(t, e, taskqueue.PeekOptions{Priority: 1})
	require.True(t, got[0].Leased)
	require.False(t, got[1].Leased)
	require.NoError(t, d.Ack())
	require.Equal(t, []string{"hi1"}, peekPayloads(peekAll(t, e, taskqueue.PeekOptions{Priority: 1})),
		"acked tasks are hidden before GC deletes them")

	_, _, err := e.PeekTasks(context.Background(), taskqueue.QueueKey{Namespace: "ns", Name: "nope"}, taskqueue.PeekOptions{})
	require.True(t, store.IsNotFound(err))
	_, _, err = e.PeekTasks(context.Background(), q, taskqueue.PeekOptions{PageToken: []byte("junk")})
	var ia *store.InvalidArgumentError
	require.ErrorAs(t, err, &ia)
}

func TestDeleteTask(t *testing.T) {
	rec := metricstest.New()
	st := memstore.New()
	e := newEngine(t, st, nil, withMetrics(rec))
	for _, p := range []string{"a", "b", "c"} {
		add(t, e, task(p))
	}
	peeked := peekAll(t, e, taskqueue.PeekOptions{})
	require.Len(t, peeked, 3)

	// A loaded (matcher) task.
	ok, err := e.DeleteTask(context.Background(), q, peeked[1].Ref)
	require.NoError(t, err)
	require.True(t, ok)
	ok, err = e.DeleteTask(context.Background(), q, peeked[1].Ref)
	require.NoError(t, err)
	require.False(t, ok, "already deleted")
	require.Equal(t, []string{"a", "c"}, peekPayloads(peekAll(t, e, taskqueue.PeekOptions{})))

	// A leased task: its lease ends and it is not redelivered.
	d := poll(t, e)
	require.Equal(t, "a", string(d.Task.Payload))
	ok, err = e.DeleteTask(context.Background(), q, peeked[0].Ref)
	require.NoError(t, err)
	require.True(t, ok)
	require.ErrorIs(t, d.Ack(), taskqueue.ErrLeaseLost)
	require.ErrorIs(t, d.Nack(), taskqueue.ErrLeaseLost)

	d = poll(t, e)
	require.Equal(t, "c", string(d.Task.Payload))
	require.NoError(t, d.Ack())
	_, err = pollNoWait(e)
	require.ErrorIs(t, err, taskqueue.ErrNoTasks)
	require.Eventually(t, func() bool { return st.TotalTaskCount(q) == 0 }, 3*time.Second, 10*time.Millisecond)
	require.Equal(t, int64(2), rec.CounterValue(metrics.TasksAdminDeleted, qTags()...))
	desc, err := e.DescribeQueue(context.Background(), q)
	require.NoError(t, err)
	require.Zero(t, desc.Stats.ApproximateBacklogCount)

	_, err = e.DeleteTask(context.Background(), q, taskqueue.TaskRef{})
	var ia *store.InvalidArgumentError
	require.ErrorAs(t, err, &ia)
	_, err = e.DeleteTask(context.Background(), taskqueue.QueueKey{Namespace: "ns", Name: "nope"}, peeked[0].Ref)
	require.True(t, store.IsNotFound(err), "no queue is created")
	require.False(t, loaded(e, taskqueue.QueueKey{Namespace: "ns", Name: "nope"}))
}

// A task beyond the reader's loaded window (and a re-read after a restart) stays deleted.
func TestDeleteTaskNotLoaded(t *testing.T) {
	st := memstore.New()
	cfg := func(c *taskqueue.Config) { c.GetTasksBatchSize = 4; c.GetTasksReloadAt = 1 }
	e := newEngine(t, st, cfg)
	for i := range 10 {
		add(t, e, task(fmt.Sprint(i)))
	}
	e.Stop()
	e = newEngine(t, st, cfg)
	peeked := peekAll(t, e, taskqueue.PeekOptions{})
	require.Len(t, peeked, 10)
	ok, err := e.DeleteTask(context.Background(), q, peeked[8].Ref)
	require.NoError(t, err)
	require.True(t, ok)
	var got []string
	for range 9 {
		d := poll(t, e)
		got = append(got, string(d.Task.Payload))
		require.NoError(t, d.Ack())
	}
	require.Equal(t, []string{"0", "1", "2", "3", "4", "5", "6", "7", "9"}, got)
	_, err = pollNoWait(e)
	require.ErrorIs(t, err, taskqueue.ErrNoTasks)
}

func TestPurgeQueue(t *testing.T) {
	forEachStore(t, func(t *testing.T, k testdb.Kind, open testdb.Opener) {
		rec := metricstest.New()
		st := open(t)
		defer closeStore(st)
		e := newEngine(t, st, func(c *taskqueue.Config) { c.GetTasksBatchSize = 5; c.GetTasksReloadAt = 1 }, withMetrics(rec))
		for i := range 20 {
			add(t, e, prioTask(fmt.Sprint(i), int32(1+i%2*4), "k"))
		}
		d := poll(t, e)
		n, err := e.PurgeQueue(context.Background(), q)
		require.NoError(t, err)
		require.Equal(t, 20, n)
		require.ErrorIs(t, d.Ack(), taskqueue.ErrLeaseLost)
		_, err = pollNoWait(e)
		require.ErrorIs(t, err, taskqueue.ErrNoTasks)
		require.Empty(t, storedTasks(t, st, q))
		require.Empty(t, peekAll(t, e, taskqueue.PeekOptions{}))
		desc, err := e.DescribeQueue(context.Background(), q)
		require.NoError(t, err)
		require.Zero(t, desc.Stats.ApproximateBacklogCount)
		require.Equal(t, int64(1), rec.CounterValue(metrics.QueuePurges, qTags()...))

		// Tasks added afterwards are kept and delivered, here and after a restart.
		add(t, e, task("after"))
		d = poll(t, e)
		require.Equal(t, "after", string(d.Task.Payload))
		require.NoError(t, d.Ack())
		add(t, e, task("after2"))
		e.Stop()
		e2 := newEngine(t, st, nil)
		d = poll(t, e2)
		require.Equal(t, "after2", string(d.Task.Payload))
		require.NoError(t, d.Ack())
	})
}

// Peek is read-only: another engine can peek a queue it does not own without loading it or
// bumping its rangeID. DeleteTask and PurgeQueue go through the owner, so they take the queue
// over like AddTask; the old owner then loses its leases on the next fenced write.
func TestTaskAdminOwnership(t *testing.T) {
	forEachStore(t, func(t *testing.T, k testdb.Kind, open testdb.Opener) {
		if !k.Durable {
			t.Skip("needs two handles on one database")
		}
		ctx := context.Background()
		st1, st2 := open(t), open(t)
		defer closeStore(st1)
		defer closeStore(st2)
		e1 := newEngine(t, st1, nil)
		for _, p := range []string{"a", "b", "c"} {
			add(t, e1, task(p))
		}
		before, err := st1.GetQueue(ctx, q)
		require.NoError(t, err)

		e2 := newEngine(t, st2, nil)
		peeked := peekAll(t, e2, taskqueue.PeekOptions{})
		require.Equal(t, []string{"a", "b", "c"}, peekPayloads(peeked))
		require.False(t, peeked[0].Leased, "lease state is only known on the owner")
		require.False(t, loaded(e2, q))
		after, err := st1.GetQueue(ctx, q)
		require.NoError(t, err)
		require.Equal(t, before.RangeID, after.RangeID)

		ok, err := e2.DeleteTask(ctx, q, peeked[1].Ref)
		require.NoError(t, err)
		require.True(t, ok)
		require.True(t, loaded(e2, q))
		after, err = st1.GetQueue(ctx, q)
		require.NoError(t, err)
		require.Greater(t, after.RangeID, before.RangeID, "delete took the queue over")
		var got []string
		for range 2 {
			d := poll(t, e2)
			got = append(got, string(d.Task.Payload))
			require.NoError(t, d.Ack())
		}
		require.Equal(t, []string{"a", "c"}, got)
	})
}

type noAdminStore struct{ store.Store }

func TestDeleteTaskUnsupportedStore(t *testing.T) {
	e := newEngine(t, noAdminStore{memstore.New()}, nil)
	add(t, e, task("a"))
	_, err := e.DeleteTask(context.Background(), q, taskqueue.TaskRef{Pass: 1, ID: 1})
	var ia *store.InvalidArgumentError
	require.ErrorAs(t, err, &ia)
	n, err := e.PurgeQueue(context.Background(), q) // core store API only
	require.NoError(t, err)
	require.Equal(t, 1, n)
}

// softAssertHandler records "soft assertion failed" log lines.
type softAssertHandler struct {
	slog.Handler
	mu   sync.Mutex
	msgs []string
}

func (h *softAssertHandler) Handle(ctx context.Context, r slog.Record) error {
	if strings.Contains(r.Message, "soft assertion") {
		h.mu.Lock()
		h.msgs = append(h.msgs, r.Message)
		h.mu.Unlock()
	}
	return nil
}
func (h *softAssertHandler) Enabled(context.Context, slog.Level) bool { return true }
func (h *softAssertHandler) WithAttrs([]slog.Attr) slog.Handler      { return h }
func (h *softAssertHandler) WithGroup(string) slog.Handler           { return h }

// Deletes and a purge racing producers and consumers keep the reader's bookkeeping sound (no
// soft assertion fires) and never deliver a task after its delete returned.
func TestTaskAdminConcurrent(t *testing.T) {
	h := &softAssertHandler{}
	e := newEngine(t, memstore.New(), func(c *taskqueue.Config) { c.GetTasksBatchSize = 8; c.GetTasksReloadAt = 2 },
		func(o *taskqueue.Options) { o.Logger = slog.New(h) })
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	var deletedMu sync.Mutex
	deleted := map[string]time.Time{} // payload -> when its DeleteTask returned
	for p := range 2 {
		wg.Go(func() {
			for i := 0; ctx.Err() == nil; i++ {
				_, _ = e.AddTask(ctx, q, task(fmt.Sprintf("p%d-%d", p, i)))
			}
		})
	}
	for range 3 {
		wg.Go(func() {
			for ctx.Err() == nil {
				start := time.Now()
				pctx, pcancel := context.WithTimeout(ctx, 50*time.Millisecond)
				d, err := e.PollTask(pctx, q, taskqueue.PollOptions{})
				pcancel()
				if err != nil {
					continue
				}
				deletedMu.Lock()
				at, dead := deleted[string(d.Task.Payload)]
				deletedMu.Unlock()
				// Only a poll already running when the delete returned may deliver the task.
				require.False(t, dead && at.Before(start), "task %s (id %d attempts %d) delivered after its delete returned (%v before poll)", d.Task.Payload, d.TaskID, d.Task.Attempts, start.Sub(at))
				if rand.IntN(4) == 0 {
					_ = d.Nack()
				} else {
					_ = d.Ack()
				}
			}
		})
	}
	wg.Go(func() {
		for i := 0; ctx.Err() == nil; i++ {
			page, _, err := e.PeekTasks(ctx, q, taskqueue.PeekOptions{PageSize: 20})
			if err != nil || len(page) == 0 {
				continue
			}
			pt := page[rand.IntN(len(page))]
			if ok, err := e.DeleteTask(ctx, q, pt.Ref); err == nil && ok {
				deletedMu.Lock()
				deleted[string(pt.Task.Payload)] = time.Now()
				deletedMu.Unlock()
			}
			if i == 50 {
				_, _ = e.PurgeQueue(ctx, q)
			}
		}
	})
	time.Sleep(time.Second)
	cancel()
	wg.Wait()
	h.mu.Lock()
	defer h.mu.Unlock()
	require.Empty(t, h.msgs)
}
