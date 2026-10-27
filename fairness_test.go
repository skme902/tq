package taskqueue_test

// Fairness behavior tests (slice 3, not upstream). They drive the public API against memstore
// with a saturated backlog (all tasks written before any poller arrives, so nothing is
// sync-matched) and measure dispatch order.

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	taskqueue "github.com/skme902/tq"
	"github.com/skme902/tq/store"
	"github.com/skme902/tq/store/memstore"
)

func fairTask(key string, weight float32, payload string) taskqueue.Task {
	return taskqueue.Task{
		Payload:  []byte(payload),
		Priority: taskqueue.Priority{FairnessKey: key, FairnessWeight: weight},
	}
}

// addAll writes tasks with a few concurrent producers. Per-key pass order follows each key's
// write order, which is all these tests depend on.
func addAll(t *testing.T, e *taskqueue.Engine, tasks []taskqueue.Task) {
	t.Helper()
	const workers = 8
	var wg sync.WaitGroup
	errs := make(chan error, workers)
	for w := range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := w; i < len(tasks); i += workers {
				if matched, err := e.AddTask(context.Background(), q, tasks[i]); err != nil || matched {
					errs <- fmt.Errorf("add %d: matched=%v err=%v", i, matched, err)
					return
				}
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}
	// Dispatch-order assertions need the backlog to be in the matcher, not still on its way from
	// the reader (which loads up to GetTasksBatchSize = 1000 tasks).
	want := min(len(tasks), 1000)
	require.Eventually(t, func() bool { return e.MatcherTasks(q) >= want }, 5*time.Second, time.Millisecond,
		"backlog should reach the matcher")
}

// interleave returns n tasks per key, round-robin across keys.
func interleave(n int, keys []string, weights map[string]float32) []taskqueue.Task {
	var out []taskqueue.Task
	for i := range n {
		for _, k := range keys {
			out = append(out, fairTask(k, weights[k], fmt.Sprintf("%s-%d", k, i)))
		}
	}
	return out
}

// drain polls n tasks one at a time, acks each, and returns their payloads in dispatch order.
func drain(t *testing.T, e *taskqueue.Engine, n int) []string {
	t.Helper()
	out := make([]string, 0, n)
	for range n {
		d := poll(t, e)
		out = append(out, string(d.Task.Payload))
		require.NoError(t, d.Ack())
	}
	return out
}

func keyOf(payload string) string { return payload[:strings.LastIndexByte(payload, '-')] }

func shares(payloads []string) map[string]float64 {
	m := map[string]float64{}
	for _, p := range payloads {
		m[keyOf(p)]++
	}
	for k := range m {
		m[k] /= float64(len(payloads))
	}
	return m
}

func TestFairnessWeightedSharesConvergeToWeightRatio(t *testing.T) {
	weights := map[string]float32{"w1": 1, "w2": 2, "w5": 5}
	keys := []string{"w1", "w2", "w5"}
	expected := map[string]float64{"w1": 1.0 / 8, "w2": 2.0 / 8, "w5": 5.0 / 8}
	const perKey = 2000

	t.Run("single poller", func(t *testing.T) {
		e := newEngine(t, memstore.New(), nil)
		addAll(t, e, interleave(perKey, keys, weights))
		got := drain(t, e, 1600)
		for _, n := range []int{80, 400, 800, 1600} {
			s := shares(got[:n])
			t.Logf("first %4d dispatches: w1=%.4f w2=%.4f w5=%.4f (expected %.4f %.4f %.4f)",
				n, s["w1"], s["w2"], s["w5"], expected["w1"], expected["w2"], expected["w5"])
			for _, k := range keys {
				require.InDelta(t, expected[k], s[k], 0.02, "key %s after %d dispatches", k, n)
			}
		}
		// sliding windows of 80 stay close to the ratio throughout
		for start := 0; start+80 <= len(got); start += 80 {
			s := shares(got[start : start+80])
			for _, k := range keys {
				require.InDelta(t, expected[k], s[k], 0.03, "window [%d,%d) key %s", start, start+80, k)
			}
		}
	})

	t.Run("8 concurrent pollers", func(t *testing.T) {
		e := newEngine(t, memstore.New(), nil)
		addAll(t, e, interleave(perKey, keys, weights))
		const total = 1600
		var mu sync.Mutex
		var got []string
		var wg sync.WaitGroup
		for range 8 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for {
					d, err := e.PollTask(context.Background(), q, taskqueue.PollOptions{})
					if err != nil {
						t.Errorf("poll: %v", err)
						return
					}
					mu.Lock()
					done := len(got) >= total
					if !done {
						got = append(got, string(d.Task.Payload))
					}
					mu.Unlock()
					if done {
						_ = d.Nack()
						return
					}
					_ = d.Ack()
				}
			}()
		}
		wg.Wait()
		s := shares(got)
		t.Logf("first %d dispatches (8 pollers): w1=%.4f w2=%.4f w5=%.4f", total, s["w1"], s["w2"], s["w5"])
		for _, k := range keys {
			require.InDelta(t, expected[k], s[k], 0.02, "key %s", k)
		}
	})
}

func TestFairnessSkewedArrivalDoesNotStarveLightKeys(t *testing.T) {
	e := newEngine(t, memstore.New(), nil)
	var heavy []taskqueue.Task
	for i := range 5000 {
		heavy = append(heavy, fairTask("heavy", 0, fmt.Sprintf("heavy-%d", i)))
	}
	addAll(t, e, heavy)
	// light keys arrive after the heavy backlog is fully written
	var light []taskqueue.Task
	for i := range 10 {
		light = append(light, fairTask(fmt.Sprintf("light%d", i), 0, fmt.Sprintf("light%d-0", i)))
	}
	light = append(light, interleave(5, []string{"lightmulti"}, nil)...)
	addAll(t, e, light)

	got := drain(t, e, 40)
	lastLight := -1
	nLight := 0
	for i, p := range got {
		if keyOf(p) != "heavy" {
			lastLight = i
			nLight++
		}
	}
	t.Logf("all %d light tasks dispatched by position %d (of 5015 backlog)", nLight, lastLight)
	require.Equal(t, 15, nLight)
	// 11 light keys each start at pass 1000; lightmulti steps by 1000 alongside heavy, so all
	// light tasks are out within the first ~20 dispatches.
	require.Less(t, lastLight, 25)

	t.Run("light key arriving mid-drain starts at the ack level", func(t *testing.T) {
		// keep draining heavy, then add a new light key; it must be served immediately
		_ = drain(t, e, 500)
		addAll(t, e, interleave(3, []string{"late"}, nil))
		got := drain(t, e, 10)
		var pos []int
		for i, p := range got {
			if keyOf(p) == "late" {
				pos = append(pos, i)
			}
		}
		t.Logf("late key positions in next 10 dispatches: %v", pos)
		require.Equal(t, []int{0, 2, 4}, pos, "late key interleaves 1:1 with heavy from the ack level")
	})
}

func TestFairnessNewKeyStartsAtAckLevelPass(t *testing.T) {
	for _, dither := range []bool{false, true} {
		t.Run(fmt.Sprintf("dither=%v", dither), func(t *testing.T) {
			st := memstore.New()
			e := newEngine(t, st, func(c *taskqueue.Config) { c.FairnessPassDither = dither })
			addAll(t, e, interleave(100, []string{"a"}, map[string]float32{"a": 1}))
			_ = drain(t, e, 40)

			// wait until the in-memory ack level reflects the 40 acks
			var ack store.Level
			require.Eventually(t, func() bool {
				s, ok := e.QueueStatus(q)
				if !ok || len(s) == 0 {
					return false
				}
				ack = s[0].AckLevel
				return ack.Pass == 40*1000
			}, 5*time.Second, time.Millisecond)

			const weightB = 0.5 // inc = 2000
			add(t, e, fairTask("b", weightB, "b-0"))
			add(t, e, fairTask("b", weightB, "b-1"))

			resp, err := st.GetTasks(context.Background(), &store.GetTasksRequest{
				Key: q, Subqueue: 0, InclusiveMin: store.Level{Pass: 1}, PageSize: 1000})
			require.NoError(t, err)
			var bPasses []int64
			for _, ti := range resp.Tasks {
				if ti.Data.Priority.FairnessKey == "b" {
					bPasses = append(bPasses, ti.Pass)
				}
			}
			require.Len(t, bPasses, 2)
			t.Logf("ack level pass=%d, b passes=%v", ack.Pass, bPasses)
			if dither {
				// upstream ditherPass: [base, base+inc)
				require.GreaterOrEqual(t, bPasses[0], ack.Pass)
				require.Less(t, bPasses[0], ack.Pass+2000)
			} else {
				// counter.GetPass on a new key: max(base, inc) == base here
				require.Equal(t, ack.Pass, bPasses[0])
			}
			require.Equal(t, bPasses[0]+2000, bPasses[1])

			// b-0 is at (or within one stride of) the ack level, so it is served ahead of
			// a's remaining backlog (a's next pass is ack+1000).
			next := drain(t, e, 3)
			if !dither {
				require.Equal(t, "b-0", next[0])
			} else {
				require.Contains(t, next[:2], "b-0")
			}
		})
	}
}

func jain(xs []float64) float64 {
	var s, s2 float64
	for _, x := range xs {
		s += x
		s2 += x * x
	}
	return s * s / (float64(len(xs)) * s2)
}

func TestFairnessManyKeysBeyondMapLimit(t *testing.T) {
	st := memstore.New()
	ts, withClock := fakeClock()
	e := newEngine(t, st, nil, withClock)
	const nKeys, perKey = 300, 20
	keys := make([]string, nKeys)
	for i := range keys {
		keys[i] = fmt.Sprintf("k%03d", i)
	}
	addAll(t, e, interleave(perKey, keys, nil))

	got := drain(t, e, nKeys*perKey/2)
	counts := map[string]float64{}
	for _, p := range got {
		counts[keyOf(p)]++
	}
	xs := make([]float64, 0, nKeys)
	for _, k := range keys {
		xs = append(xs, counts[k])
	}
	sort.Float64s(xs)
	j := jain(xs)
	t.Logf("%d keys x %d tasks, first %d dispatches: per-key min=%v p10=%v median=%v p90=%v max=%v (ideal %d), Jain=%.4f",
		nKeys, perKey, len(got), xs[0], xs[nKeys/10], xs[nKeys/2], xs[nKeys*9/10], xs[nKeys-1], perKey/2, j)
	require.Greater(t, xs[0], 0.0, "no key starved")
	require.Greater(t, j, 0.9)

	// TopK persisted on the UpdateAckInterval cadence is capped at MapLimit (100).
	ts.Advance(60 * time.Millisecond) // > UpdateAckInterval (50ms) so the next write persists TopK
	add(t, e, fairTask("k000", 0, "k000-x"))
	writerBarrier(t, e)
	e.Stop()
	resp, err := st.GetQueue(context.Background(), q)
	require.NoError(t, err)
	topK := resp.Info.Subqueues[0].TopKFairnessCounts
	t.Logf("persisted TopK entries: %d", len(topK))
	require.Len(t, topK, taskqueue.DefaultCounterParams.MapLimit)
}

func TestFairnessWeightOverrideChangeMidStream(t *testing.T) {
	src := taskqueue.NewMemoryQueueConfigSource(0)
	e := newEngine(t, memstore.New(), nil, func(o *taskqueue.Options) { o.QueueConfigSource = src })
	keys := []string{"a", "b"}

	// phase 1: equal weights (b's task weight 1, no override)
	addAll(t, e, interleave(400, keys, map[string]float32{"a": 1, "b": 1}))
	s1 := shares(drain(t, e, 400))
	_ = drain(t, e, 400)

	// phase 2: override b to 4 (tasks still say 1; the override wins, as upstream)
	require.NoError(t, src.UpdateFairnessWeightOverrides(q, map[string]float32{"b": 4}, nil))
	addAll(t, e, interleave(400, keys, map[string]float32{"a": 1, "b": 1}))
	s2 := shares(drain(t, e, 400))
	_ = drain(t, e, 400)

	// phase 3: unset the override
	require.NoError(t, src.UpdateFairnessWeightOverrides(q, nil, []string{"b"}))
	addAll(t, e, interleave(400, keys, map[string]float32{"a": 1, "b": 1}))
	s3 := shares(drain(t, e, 400))

	t.Logf("b share: no override=%.4f (exp 0.5), override b=4: %.4f (exp 0.8), unset: %.4f (exp 0.5)", s1["b"], s2["b"], s3["b"])
	require.InDelta(t, 0.5, s1["b"], 0.02)
	require.InDelta(t, 0.8, s2["b"], 0.02)
	require.InDelta(t, 0.5, s3["b"], 0.02)

	// upstream validation and limits
	require.Error(t, src.UpdateFairnessWeightOverrides(q, map[string]float32{"": 1}, nil))
	require.Error(t, src.UpdateFairnessWeightOverrides(q, map[string]float32{"x": 0}, nil))
	require.Error(t, src.UpdateFairnessWeightOverrides(q, map[string]float32{"x": 1}, []string{"x"}))
	small := taskqueue.NewMemoryQueueConfigSource(2)
	require.NoError(t, small.UpdateFairnessWeightOverrides(q, map[string]float32{"x": 1, "y": 2}, nil))
	require.ErrorIs(t, small.UpdateFairnessWeightOverrides(q, map[string]float32{"z": 1}, nil),
		taskqueue.ErrFairnessOverridesUpdateRejected)
}

// writerBarrier makes sure the task writer has finished the iteration for the previous add,
// including the TopK persist check that runs after it answers the add (as upstream). It writes a
// task to the lowest priority level, whose subqueue has its own counter, so subqueue 0's TopK and
// the higher-priority dispatch order are not affected.
func writerBarrier(t *testing.T, e *taskqueue.Engine) {
	t.Helper()
	tk := fairTask("barrier", 1, "barrier-0")
	tk.Priority.PriorityKey = 5
	add(t, e, tk)
}

func TestFairnessPersistsAcrossReloadViaTopK(t *testing.T) {
	st := memstore.New()
	ts, withClock := fakeClock()

	e1 := newEngine(t, st, nil, withClock)
	var old []taskqueue.Task
	for i := range 1000 {
		old = append(old, fairTask("a", 1, fmt.Sprintf("aold-%d", i)))
	}
	addAll(t, e1, old)
	// TopK is persisted after a write batch once UpdateAckInterval has elapsed (upstream
	// persistFairnessKeys ticker), and reaches the store on the next SyncState (final one on Stop).
	ts.Advance(time.Second)
	add(t, e1, fairTask("a", 1, "aold-1000"))
	writerBarrier(t, e1)
	e1.Stop()

	resp, err := st.GetQueue(context.Background(), q)
	require.NoError(t, err)
	topK := resp.Info.Subqueues[0].TopKFairnessCounts
	require.Equal(t, []store.FairnessKeyCount{{Key: "a", Count: 1001 * 1000}}, topK)

	e2 := newEngine(t, st, nil, withClock)
	var fresh []taskqueue.Task
	for i := range 10 {
		fresh = append(fresh, fairTask("b", 1, fmt.Sprintf("b-%d", i)))
		fresh = append(fresh, fairTask("a", 1, fmt.Sprintf("anew-%d", i)))
	}
	addAll(t, e2, fresh)
	require.Eventually(t, func() bool { return e2.MatcherTasks(q) >= 1000 }, 5*time.Second, time.Millisecond)
	got := drain(t, e2, 1021)
	var firstNew, lastB, lastOld int
	for i, p := range got {
		switch keyOf(p) {
		case "anew":
		case "b":
			lastB = i
		case "aold":
			lastOld = i
		}
		if keyOf(p) == "anew" && firstNew == 0 {
			firstNew = i
		}
	}
	// payload "anew-N" has key "anew" by keyOf; its fairness key is "a"
	t.Logf("after reload: last b at %d, last old-a at %d, first new-a at %d", lastB, lastOld, firstNew)
	// b is a new key, so it starts at the ack level and should be near the front. It is not
	// exactly in the first ~20: these writes race with the reload's first 1000-task read, and
	// (as upstream fairTaskReader) writes that land while a read is pending are held until that
	// read has handed its whole batch to the matcher, by which time some polls have happened
	// (up to ~280 seen under a loaded -race run). The property is b is not behind a's backlog.
	require.Less(t, lastB, lastOld/2, "new key b is served from near the ack level")
	require.Greater(t, firstNew, lastOld, "restored counter keeps a's new tasks behind its old backlog")
}

func TestFairnessPriorityDominatesFairness(t *testing.T) {
	e := newEngine(t, memstore.New(), nil)
	var tasks []taskqueue.Task
	for i := range 300 {
		tasks = append(tasks,
			taskqueue.Task{Payload: []byte(fmt.Sprintf("p1light-%d", i)),
				Priority: taskqueue.Priority{PriorityKey: 1, FairnessKey: "light", FairnessWeight: 1}},
			taskqueue.Task{Payload: []byte(fmt.Sprintf("p3heavy-%d", i)),
				Priority: taskqueue.Priority{PriorityKey: 3, FairnessKey: "heavy", FairnessWeight: 100}},
			taskqueue.Task{Payload: []byte(fmt.Sprintf("p3x-%d", i)),
				Priority: taskqueue.Priority{PriorityKey: 3, FairnessKey: "x", FairnessWeight: 1}},
		)
	}
	addAll(t, e, tasks)
	got := drain(t, e, 600)
	for i, p := range got[:300] {
		require.Equal(t, "p1light", keyOf(p), "dispatch %d", i)
	}
	// within priority 3 fairness still applies: heavy (w100) vs x (w1)
	s := shares(got[300:400])
	t.Logf("priority 3 first 100: heavy=%.2f x=%.2f", s["p3heavy"], s["p3x"])
	require.Greater(t, s["p3heavy"], 0.95)
}
