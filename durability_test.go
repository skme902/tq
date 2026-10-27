package taskqueue_test

// Engine-level durability tests (slice 5, not upstream). Each runs against every store kind
// from internal/testdb: memstore always, sqlite always, postgres when TQ_TEST_POSTGRES_DSN is
// set. A "restart" is a new Engine on a new store handle (its own connection pools) over the
// same database; "two processes" are two engines on two handles.

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
	"github.com/skme902/tq/internal/testdb"
	"github.com/skme902/tq/metrics"
	"github.com/skme902/tq/store"
)

func forEachStore(t *testing.T, f func(t *testing.T, k testdb.Kind, open testdb.Opener)) {
	for _, k := range testdb.Kinds() {
		t.Run(k.Name, func(t *testing.T) {
			f(t, k, k.NewDB(t))
		})
	}
}

func closeStore(st store.Store) {
	if c, ok := st.(interface{ Close() error }); ok {
		_ = c.Close()
	}
}

// storedTasks returns every task row of queue k across its persisted subqueues, in
// (subqueue, pass, id) order.
func storedTasks(t testing.TB, st store.Store, k store.QueueKey) []store.TaskInfo {
	t.Helper()
	ctx := context.Background()
	resp, err := st.GetQueue(ctx, k)
	if store.IsNotFound(err) {
		return nil
	}
	require.NoError(t, err)
	var out []store.TaskInfo
	for sq := range resp.Info.Subqueues {
		min := store.Level{Pass: 1}
		for {
			page, err := st.GetTasks(ctx, &store.GetTasksRequest{Key: k, Subqueue: sq, InclusiveMin: min, PageSize: 500})
			require.NoError(t, err)
			out = append(out, page.Tasks...)
			if len(page.Tasks) < 500 {
				break
			}
			last := page.Tasks[len(page.Tasks)-1]
			min = store.Level{Pass: last.Pass, ID: last.ID + 1}
		}
	}
	return out
}

func TestDurableBacklogSurvivesRestart(t *testing.T) {
	forEachStore(t, func(t *testing.T, _ testdb.Kind, open testdb.Opener) {
		st1 := open(t)
		e1, err := taskqueue.New(taskqueue.Options{Store: st1, Config: taskqueue.Config{
			LongPollExpirationInterval: 2 * time.Second, UpdateAckInterval: 50 * time.Millisecond,
		}})
		require.NoError(t, err)
		const n, ackFirst = 200, 50
		want := map[string]int32{}
		for i := range n {
			tk := task(fmt.Sprint("t", i))
			tk.Priority.PriorityKey = int32(1 + i%5)
			want[string(tk.Payload)] = tk.Priority.PriorityKey
			_, err := e1.AddTask(context.Background(), q, tk)
			require.NoError(t, err)
		}
		// Ack a contiguous prefix of the dispatch order, so the ack levels cover it.
		for range ackFirst {
			d, err := e1.PollTask(context.Background(), q, taskqueue.PollOptions{})
			require.NoError(t, err)
			delete(want, string(d.Task.Payload))
			require.NoError(t, d.Ack())
		}
		e1.Stop() // final SyncState persists ack levels
		closeStore(st1)

		st2 := open(t)
		e2 := newEngine(t, st2, nil)
		// Let every subqueue's reader hand its backlog to the matcher before asserting order.
		require.NoError(t, poll(t, e2).Nack())
		require.Eventually(t, func() bool { return e2.MatcherTasks(q) >= n-ackFirst }, 5*time.Second, time.Millisecond)
		var prev int32
		for range n - ackFirst {
			d := poll(t, e2)
			p := string(d.Task.Payload)
			pk, ok := want[p]
			require.True(t, ok, "unexpected or duplicate delivery %q", p)
			require.GreaterOrEqual(t, pk, prev, "backlog drains in priority order after restart")
			prev = pk
			delete(want, p)
			require.NoError(t, d.Ack())
		}
		require.Empty(t, want)
		_, err = e2.PollTask(context.Background(), q, taskqueue.PollOptions{NoWait: true})
		require.ErrorIs(t, err, taskqueue.ErrNoTasks, "acked tasks are not redelivered")
		// Rows the first engine acked but had not GCed before Stop stay in the store below
		// their subqueue's persisted ack level: invisible, and deleted the next time that
		// subqueue's ack level advances (upstream fairTaskReader GCs only on ack progress).
		for _, pk := range []int32{1, 2, 3, 4, 5} {
			tk := task(fmt.Sprint("gc", pk))
			tk.Priority.PriorityKey = pk
			add(t, e2, tk)
			d := poll(t, e2)
			require.Equal(t, tk.Payload, d.Task.Payload)
			require.NoError(t, d.Ack())
		}
		deadline := time.Now().Add(10 * time.Second)
		for len(storedTasks(t, st2, q)) > 0 {
			if time.Now().After(deadline) {
				resp, err := st2.GetQueue(context.Background(), q)
				require.NoError(t, err)
				for _, r := range storedTasks(t, st2, q) {
					t.Logf("left: pass=%d id=%d payload=%s prio=%d", r.Pass, r.ID, r.Data.Payload, r.Data.Priority.PriorityKey)
				}
				for i, sq := range resp.Info.Subqueues {
					t.Logf("subqueue %d: %+v", i, sq)
				}
				t.Fatal("acked tasks are not GCed")
			}
			time.Sleep(10 * time.Millisecond)
		}
	})
}

func TestDurableRangeIDFencingTwoEngines(t *testing.T) {
	forEachStore(t, func(t *testing.T, _ testdb.Kind, open testdb.Opener) {
		ctx := context.Background()
		st1, st2 := open(t), open(t)
		m1 := &countingMetrics{Handler: metrics.Noop, causes: map[string]int{}}
		m2 := &countingMetrics{Handler: metrics.Noop, causes: map[string]int{}}
		e1 := newEngine(t, st1, nil, func(o *taskqueue.Options) { o.Metrics = m1 })

		const nA, nAck, nB = 100, 30, 50
		for i := range nA {
			require.False(t, add(t, e1, task(fmt.Sprint("a", i))))
		}
		acked := map[string]bool{}
		for range nAck {
			d := poll(t, e1)
			acked[string(d.Task.Payload)] = true
			require.NoError(t, d.Ack())
		}
		// Acked rows are only deleted (GC) after the ack level moved past them, so once they
		// are gone no owner can redeliver them.
		require.Eventually(t, func() bool { return len(storedTasks(t, st2, q)) == nA-nAck }, 5*time.Second,
			10*time.Millisecond, "e1 GCs its acked prefix")
		before, err := st2.GetQueue(ctx, q)
		require.NoError(t, err)

		// Second process takes over by touching the queue.
		e2 := newEngine(t, st2, nil, func(o *taskqueue.Options) { o.Metrics = m2 })
		for i := range nB {
			add(t, e2, task(fmt.Sprint("b", i)))
		}
		after, err := st2.GetQueue(ctx, q)
		require.NoError(t, err)
		require.Greater(t, after.RangeID, before.RangeID, "takeover bumps the rangeID")

		// The first engine finds out on its next fenced operation (periodic SyncState here),
		// gets ConditionFailed and unloads without deleting anything.
		require.Eventually(t, func() bool { return !loaded(e1, q) }, 5*time.Second, 10*time.Millisecond,
			"fenced engine unloads")
		// The cause metric is emitted by Stop, just after the queue leaves the engine map.
		require.Eventually(t, func() bool { return m1.count("Conflict") == 1 }, 5*time.Second, 10*time.Millisecond)
		require.Equal(t, 0, m2.count("Conflict"))
		require.True(t, loaded(e2, q))

		got := map[string]bool{}
		for range nA - nAck + nB {
			d := poll(t, e2)
			p := string(d.Task.Payload)
			require.False(t, got[p], "duplicate delivery %q", p)
			require.False(t, acked[p], "acked task %q redelivered", p)
			got[p] = true
			require.NoError(t, d.Ack())
		}
		_, err = e2.PollTask(ctx, q, taskqueue.PollOptions{NoWait: true})
		require.ErrorIs(t, err, taskqueue.ErrNoTasks)
		require.Len(t, got, nA-nAck+nB, "no loss")
		require.Eventually(t, func() bool { return len(storedTasks(t, st2, q)) == 0 }, 5*time.Second, 10*time.Millisecond)
		require.Equal(t, 1, m1.count("Conflict"), "fenced engine stays unloaded")
	})
}

func TestDurableFairnessSurvivesRestart(t *testing.T) {
	forEachStore(t, func(t *testing.T, k testdb.Kind, open testdb.Opener) {
		ts, withClock := fakeClock()
		st1 := open(t)
		e1 := newEngine(t, st1, nil, withClock)

		// Part 1: pass order of a mixed-weight backlog is what the store holds, and a new
		// engine dispatches exactly that order.
		weights := map[string]float32{"x": 1, "y": 2, "z": 4}
		addAll(t, e1, interleave(60, []string{"x", "y", "z"}, weights))
		writerBarrier(t, e1)
		e1.Stop()
		var stored []store.TaskInfo
		for _, ti := range storedTasks(t, st1, q) {
			if keyOf(string(ti.Data.Payload)) != "barrier" {
				stored = append(stored, ti)
			}
		}
		require.Len(t, stored, 180)
		require.True(t, sort.SliceIsSorted(stored, func(i, j int) bool {
			return store.Level{Pass: stored[i].Pass, ID: stored[i].ID}.Less(store.Level{Pass: stored[j].Pass, ID: stored[j].ID})
		}))
		passes := map[int64]bool{}
		for _, ti := range stored {
			passes[ti.Pass] = true
		}
		require.Greater(t, len(passes), 60, "fairness assigned distinct passes (not FIFO)")
		closeStore(st1)

		st2 := open(t)
		e2 := newEngine(t, st2, nil, withClock)
		require.NoError(t, poll(t, e2).Nack()) // loads the queue; the task goes back to the matcher
		require.Eventually(t, func() bool { return e2.MatcherTasks(q) >= 181 }, 5*time.Second, time.Millisecond)
		var got []string
		for _, p := range drain(t, e2, 181) {
			if p != "barrier-0" { // other priority level
				got = append(got, p)
			}
		}
		require.Len(t, got, len(stored))
		for i, ti := range stored {
			require.Equal(t, string(ti.Data.Payload), got[i], "dispatch %d follows persisted (pass, id) order", i)
		}
		e2.Stop()
		closeStore(st2)

		// Part 2: TopK fairness counts persist and seed the counter after restart (as
		// TestFairnessPersistsAcrossReloadViaTopK, but across store handles). Fresh database,
		// so part 1's ack levels and counts do not mix in.
		open = k.NewDB(t)
		st3 := open(t)
		e3 := newEngine(t, st3, nil, withClock)
		var old []taskqueue.Task
		for i := range 1000 {
			old = append(old, fairTask("a", 1, fmt.Sprintf("aold-%d", i)))
		}
		addAll(t, e3, old)
		ts.Advance(time.Second)
		add(t, e3, fairTask("a", 1, "aold-1000"))
		writerBarrier(t, e3)
		e3.Stop()
		resp, err := st3.GetQueue(context.Background(), q)
		require.NoError(t, err)
		var topA int64
		for _, c := range resp.Info.Subqueues[0].TopKFairnessCounts {
			if c.Key == "a" {
				topA = c.Count
			}
		}
		require.Equal(t, int64(1001*1000), topA, "%s: TopK count for a persisted", k.Name)
		closeStore(st3)

		st4 := open(t)
		e4 := newEngine(t, st4, nil, withClock)
		var fresh []taskqueue.Task
		for i := range 10 {
			fresh = append(fresh, fairTask("b", 1, fmt.Sprintf("b-%d", i)))
			fresh = append(fresh, fairTask("a", 1, fmt.Sprintf("anew-%d", i)))
		}
		addAll(t, e4, fresh)
		// Let the reload's first read hand its batch to the matcher before polling, so the
		// fresh writes (held while that read is pending) are merged in first.
		require.Eventually(t, func() bool { return e4.MatcherTasks(q) >= 1000 }, 5*time.Second, time.Millisecond)
		got = drain(t, e4, 1022) // 1001 old a + barrier + 20 fresh
		firstNew, lastB, lastOld := -1, -1, -1
		for i, p := range got {
			switch keyOf(p) {
			case "b":
				lastB = i
			case "aold":
				lastOld = i
			case "anew":
				if firstNew < 0 {
					firstNew = i
				}
			}
		}
		t.Logf("after restart: last b at %d, last old-a at %d, first new-a at %d", lastB, lastOld, firstNew)
		// b's exact position depends on how many old-a tasks the matcher dispatched before the
		// fresh writes were merged in (62-118 idle, up to ~260 under the full -race suite); the
		// property is that b is not queued behind a's old backlog.
		require.Less(t, lastB, lastOld/2, "new key b is served from near the ack level")
		require.Greater(t, firstNew, lastOld, "restored TopK keeps a's new tasks behind its old backlog")
	})
}

func TestDurableConcurrentProducersConsumersNoLoss(t *testing.T) {
	for _, tc := range []struct {
		name         string
		mod          func(*taskqueue.Config)
		nackPct      int
		backlogFirst bool
	}{
		{"defaults", nil, 0, false},
		{"backlog-first-small-batches-with-nacks", func(c *taskqueue.Config) {
			c.GetTasksBatchSize = 20
			c.GetTasksReloadAt = 5
			c.MaxTaskBatchSize = 7
			c.RangeSize = 50
		}, 10, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			forEachStore(t, func(t *testing.T, _ testdb.Kind, open testdb.Opener) {
				st := open(t)
				e := newEngine(t, st, tc.mod)
				const producers, perProducer, consumers = 8, 200, 8
				total := producers * perProducer

				var acked sync.Map
				var ackCount, deliveries, nacks, syncMatched atomic.Int64
				ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
				defer cancel()
				var cwg, pwg sync.WaitGroup
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
							tk.Priority.PriorityKey = int32(rand.Intn(6))
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
					require.Len(t, storedTasks(t, st, q), total, "everything persisted before consumers start")
					startConsumers()
				}
				require.Eventually(t, func() bool { return ackCount.Load() == int64(total) }, 90*time.Second,
					10*time.Millisecond, "acked %d of %d", ackCount.Load(), total)
				cancel()
				cwg.Wait()
				n := 0
				acked.Range(func(any, any) bool { n++; return true })
				require.Equal(t, total, n)
				require.Equal(t, deliveries.Load(), int64(total)+nacks.Load())
				require.Eventually(t, func() bool { return len(storedTasks(t, st, q)) == 0 }, 10*time.Second,
					10*time.Millisecond, "all GCed")
				t.Logf("deliveries=%d nacks=%d syncMatched=%d", deliveries.Load(), nacks.Load(), syncMatched.Load())
			})
		})
	}
}
