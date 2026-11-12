package taskqueue_test

// Rate limiting behavior tests (slice 4, not upstream). They drive the public API on a fake
// clock: the backlog is written first, pollers run in goroutines and record the fake time of
// each delivery, and the test advances the clock in 1ms steps. Expected counts follow from the
// limiter primitive (matcher_data.go): starting from an idle limiter, R+1 tasks pass at once
// (burst = 1s of tokens, plus the one that is ready now), then one every 1/R seconds.

import (
	"context"
	"fmt"
	"math"
	"os"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	taskqueue "github.com/skme902/tq"
	"github.com/skme902/tq/clock"
	"github.com/skme902/tq/store/memstore"
)

func f32p(v float32) *float32 { return &v }
func f64p(v float64) *float64 { return &v }

type delivery struct {
	at      time.Time
	payload string
}

// rlHarness is an engine on a fake clock with a MemoryQueueConfigSource.
type rlHarness struct {
	t   *testing.T
	ts  *clock.EventTimeSource
	e   *taskqueue.Engine
	src *taskqueue.MemoryQueueConfigSource
	t0  time.Time

	mu  sync.Mutex
	got []delivery

	cancel context.CancelFunc
	wg     sync.WaitGroup
}

func newRLHarness(t *testing.T, mod func(*taskqueue.Config), opts ...func(*taskqueue.Options)) *rlHarness {
	t.Helper()
	ts, withClock := fakeClock()
	src := taskqueue.NewMemoryQueueConfigSource(0)
	all := append([]func(*taskqueue.Options){withClock, func(o *taskqueue.Options) { o.QueueConfigSource = src }}, opts...)
	h := &rlHarness{t: t, ts: ts, src: src}
	h.e = newEngine(t, memstore.New(), mod, all...)
	t.Cleanup(h.stop)
	return h
}

// backlog writes n tasks per key (round-robin) before any poller exists, so nothing is
// sync-matched, and waits for them to reach the matcher.
func (h *rlHarness) backlog(n int, keys map[string]float32) {
	h.t.Helper()
	var tasks []taskqueue.Task
	for i := range n {
		for _, k := range sortedKeys(keys) {
			tasks = append(tasks, fairTask(k, keys[k], fmt.Sprintf("%s-%d", k, i)))
		}
	}
	addAll(h.t, h.e, tasks)
}

func sortedKeys(m map[string]float32) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	// deterministic order
	for i := range out {
		for j := i + 1; j < len(out); j++ {
			if out[j] < out[i] {
				out[i], out[j] = out[j], out[i]
			}
		}
	}
	return out
}

// startPollers starts n pollers that record the fake time of each delivery and ack it.
func (h *rlHarness) startPollers(n int, opts taskqueue.PollOptions) {
	ctx, cancel := context.WithCancel(context.Background())
	h.cancel = cancel
	h.t0 = h.ts.Now()
	for range n {
		h.wg.Add(1)
		go func() {
			defer h.wg.Done()
			for ctx.Err() == nil {
				d, err := h.e.PollTask(ctx, q, opts)
				if err != nil {
					continue
				}
				h.mu.Lock()
				h.got = append(h.got, delivery{at: h.ts.Now(), payload: string(d.Task.Payload)})
				h.mu.Unlock()
				_ = d.Ack()
			}
		}()
	}
}

func (h *rlHarness) stop() {
	if h.cancel != nil {
		h.cancel()
		h.cancel = nil
		// unblock pollers waiting on fake-clock timers
		done := make(chan struct{})
		go func() { h.wg.Wait(); close(done) }()
		for {
			select {
			case <-done:
				return
			default:
				h.ts.Advance(time.Second)
				time.Sleep(time.Millisecond)
			}
		}
	}
}

// run advances the fake clock by d in 1ms steps, yielding so pollers and timers keep up.
func (h *rlHarness) run(d time.Duration) {
	end := h.ts.Now().Add(d)
	for h.ts.Now().Before(end) {
		h.ts.Advance(time.Millisecond)
		for range 4 {
			runtime.Gosched()
		}
		time.Sleep(20 * time.Microsecond)
	}
	// let in-flight deliveries for the last step be recorded
	time.Sleep(20 * time.Millisecond)
}

// runUntil steps the fake clock like run until want deliveries are recorded or d of fake
// time has passed, then settles.
func (h *rlHarness) runUntil(want int, d time.Duration) {
	end := h.ts.Now().Add(d)
	for h.total() < want && h.ts.Now().Before(end) {
		h.ts.Advance(time.Millisecond)
		for range 4 {
			runtime.Gosched()
		}
		time.Sleep(20 * time.Microsecond)
	}
	h.settle(want)
}

// settle waits in real time, without moving the fake clock, until want deliveries are recorded
// or 300ms pass, so assertions about a single fake instant do not depend on scheduler speed.
func (h *rlHarness) settle(want int) {
	deadline := time.Now().Add(300 * time.Millisecond)
	for h.total() < want && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	time.Sleep(10 * time.Millisecond)
}

// count returns deliveries in [from, to) relative to t0, optionally filtered by key.
func (h *rlHarness) count(from, to time.Duration, key string) int {
	h.mu.Lock()
	defer h.mu.Unlock()
	n := 0
	for _, d := range h.got {
		off := d.at.Sub(h.t0)
		if off >= from && off < to && (key == "" || keyOf(d.payload) == key) {
			n++
		}
	}
	return n
}

func (h *rlHarness) total() int { return h.count(-time.Hour, time.Hour*1000, "") }

var one = map[string]float32{"k": 1}

func TestRateLimitQueueRateOverWindow(t *testing.T) {
	const rate = 50
	h := newRLHarness(t, nil)
	require.NoError(t, h.src.SetQueueRateLimit(q, f32p(rate)))
	h.backlog(1500, one)
	h.startPollers(4, taskqueue.PollOptions{})
	h.run(10 * time.Second)

	// Steady state, excluding the initial burst: [2s, 10s) should hold 8s*R tasks.
	steady := h.count(2*time.Second, 10*time.Second, "")
	require.InDelta(t, 8*rate, steady, 4, "steady-state rate")
	// Whole window: R+1 burst at t0 plus R per second.
	require.InDelta(t, rate+1+10*rate, h.total(), 6, "total in window incl. burst")
	t.Logf("rate=%d steady[2s,10s)=%d (%.1f/s) total=%d", rate, steady, float64(steady)/8, h.total())

	rps, src, ok := h.e.RateLimit(q)
	require.True(t, ok)
	require.Equal(t, taskqueue.RateLimitSourceAPI, src)
	require.InDelta(t, rate, rps, 1e-6)
}

func TestRateLimitBurst(t *testing.T) {
	const rate = 20
	h := newRLHarness(t, nil)
	require.NoError(t, h.src.SetQueueRateLimit(q, f32p(rate)))
	h.backlog(500, one)
	h.startPollers(4, taskqueue.PollOptions{})
	// With the clock frozen at t0, exactly one burst passes: R tokens (1s at rate R) plus the
	// one that is ready now.
	h.settle(rate + 1)
	require.Equal(t, rate+1, h.total(), "initial burst at a single instant")
	h.ts.Advance(49 * time.Millisecond)
	h.settle(rate + 2)
	require.Equal(t, rate+1, h.total(), "nothing more before the next interval")
	h.ts.Advance(time.Millisecond)
	h.settle(rate + 2)
	require.Equal(t, rate+2, h.total(), "next token exactly one interval (50ms) later")
	h.run(2*time.Second - 50*time.Millisecond)
	// then it is paced: no new burst while the backlog keeps the limiter busy
	require.InDelta(t, 2*rate, h.count(50*time.Millisecond, 2*time.Second, ""), 2)
}

func TestRateLimitBurstRefillsAfterIdle(t *testing.T) {
	const rate = 10
	h := newRLHarness(t, nil)
	require.NoError(t, h.src.SetQueueRateLimit(q, f32p(rate)))
	h.backlog(rate+1, one) // exactly one burst
	h.startPollers(2, taskqueue.PollOptions{})
	h.settle(rate + 1)
	require.Equal(t, rate+1, h.total())
	h.run(3 * time.Second) // idle long enough to refill the bucket
	before := h.total()
	for i := range 100 { // pollers are waiting, so some of these sync-match
		_, err := h.e.AddTask(context.Background(), q, fairTask("k", 1, fmt.Sprintf("late-%d", i)))
		require.NoError(t, err)
	}
	h.settle(before + rate + 1)
	require.Equal(t, rate+1, h.total()-before, "full burst again after idle (ready time clipped to now-burst)")
}

func TestRateLimitPerKeyWeights(t *testing.T) {
	const perKey = 10
	h := newRLHarness(t, nil)
	require.NoError(t, h.src.SetFairnessKeyRateLimitDefault(q, f32p(perKey)))
	h.backlog(400, map[string]float32{"a": 1, "b": 2})
	h.startPollers(4, taskqueue.PollOptions{})
	h.run(10 * time.Second)

	a := h.count(2*time.Second, 10*time.Second, "a")
	b := h.count(2*time.Second, 10*time.Second, "b")
	t.Logf("per-key default=%d: a(w=1)=%d (%.1f/s) b(w=2)=%d (%.1f/s)", perKey, a, float64(a)/8, b, float64(b)/8)
	require.InDelta(t, 8*perKey, a, 3)
	require.InDelta(t, 8*2*perKey, b, 4)
	require.InDelta(t, 2.0, float64(b)/float64(a), 0.15)
}

func TestRateLimitThrottledKeyDoesNotBlockOthers(t *testing.T) {
	h := newRLHarness(t, nil)
	// per-key 1 rps; "cold" has weight 100 so its own limit is 100 rps
	require.NoError(t, h.src.SetFairnessKeyRateLimitDefault(q, f32p(1)))
	h.backlog(50, map[string]float32{"hot": 1, "cold": 100})
	h.startPollers(4, taskqueue.PollOptions{})
	h.settle(52) // clock frozen at t0
	require.Equal(t, 50, h.count(0, time.Millisecond, "cold"), "cold key drains past the throttled hot key")
	require.Equal(t, 2, h.count(0, time.Millisecond, "hot"), "hot key: burst 1 + ready 1")
}

func TestRateLimitWholeQueueStallsWithoutPerKey(t *testing.T) {
	h := newRLHarness(t, nil)
	require.NoError(t, h.src.SetQueueRateLimit(q, f32p(0)))
	h.backlog(20, map[string]float32{"a": 1, "b": 1})
	h.startPollers(2, taskqueue.PollOptions{})
	h.run(5 * time.Second)
	require.Zero(t, h.total(), "rate 0 never dispatches")
}

func TestRateLimitLiveOverrideUpAndDown(t *testing.T) {
	h := newRLHarness(t, nil)
	require.NoError(t, h.src.SetQueueRateLimit(q, f32p(10)))
	h.backlog(1500, one)
	h.startPollers(4, taskqueue.PollOptions{})
	h.run(5 * time.Second)
	require.NoError(t, h.src.SetQueueRateLimit(q, f32p(100))) // up
	h.run(5 * time.Second)
	require.NoError(t, h.src.SetQueueRateLimit(q, f32p(5))) // down
	h.run(5 * time.Second)
	require.NoError(t, h.src.SetQueueRateLimit(q, nil)) // cleared: back to unlimited (no worker rate, quirk)
	h.run(100 * time.Millisecond)

	s1 := h.count(2*time.Second, 5*time.Second, "")
	s2 := h.count(7*time.Second, 10*time.Second, "")
	s3 := h.count(12*time.Second, 15*time.Second, "")
	t.Logf("override: 10rps→%.1f/s, 100rps→%.1f/s, 5rps→%.1f/s, cleared→%d remaining", float64(s1)/3, float64(s2)/3, float64(s3)/3, 1500-h.total())
	require.InDelta(t, 30, s1, 2)
	require.InDelta(t, 300, s2, 4)
	require.InDelta(t, 15, s3, 2)
	// lowering the rate clips the ready time to at most interval+burst ahead, no long stall
	require.Positive(t, h.count(10*time.Second, 11500*time.Millisecond, ""))

	rps, src, ok := h.e.RateLimit(q)
	require.True(t, ok)
	require.Equal(t, taskqueue.RateLimitSourceSystem, src)
	require.True(t, math.IsInf(rps, 1) || rps == 1000, "rps=%v", rps)
}

func TestRateLimitPollerMaxTasksPerSecond(t *testing.T) {
	h := newRLHarness(t, nil)
	h.backlog(500, one)
	h.startPollers(3, taskqueue.PollOptions{MaxTasksPerSecond: f64p(20)})
	h.run(5 * time.Second)
	steady := h.count(2*time.Second, 5*time.Second, "")
	require.InDelta(t, 60, steady, 3)
	rps, src, ok := h.e.RateLimit(q)
	require.True(t, ok)
	require.Equal(t, taskqueue.RateLimitSourceWorker, src)
	require.InDelta(t, 20, rps, 1e-6)

	// API rate wins over the worker rate
	require.NoError(t, h.src.SetQueueRateLimit(q, f32p(5)))
	h.run(3 * time.Second)
	_, src, _ = h.e.RateLimit(q)
	require.Equal(t, taskqueue.RateLimitSourceAPI, src)
	require.InDelta(t, 10, h.count(6*time.Second, 8*time.Second, ""), 2)
}

func TestRateLimitZeroAndTinyRate(t *testing.T) {
	for _, rate := range []float32{0, 1e-10} {
		t.Run(fmt.Sprint(rate), func(t *testing.T) {
			h := newRLHarness(t, nil)
			require.NoError(t, h.src.SetQueueRateLimit(q, f32p(rate)))
			h.backlog(10, one)
			h.startPollers(2, taskqueue.PollOptions{})
			h.run(3 * time.Second)
			require.Zero(t, h.total())
			// raising the rate wakes the stalled matcher (ready time clipped, tasks reprocessed)
			require.NoError(t, h.src.SetQueueRateLimit(q, f32p(10)))
			// SetQueueRateLimit clips the ready time synchronously at the frozen fake now; the
			// 10th task is then due exactly 2s later. The matcher re-arms its rate-limit timer
			// with a relative delay, so clock steps between its Now() snapshot and the arm push
			// that deadline past 2s by however long the arming goroutine was descheduled. Keep
			// stepping the clock until all 10 arrive (only 10 exist, so this cannot
			// over-deliver), bounded at 2s of extra fake time.
			h.runUntil(10, 4*time.Second)
			require.Equal(t, 10, h.total())
		})
	}
	t.Run("negative rejected", func(t *testing.T) {
		h := newRLHarness(t, nil)
		require.Error(t, h.src.SetQueueRateLimit(q, f32p(-1)))
		require.Error(t, h.src.SetFairnessKeyRateLimitDefault(q, f32p(-1)))
	})
}

func TestRateLimitSystemDefaultQuirk(t *testing.T) {
	for _, applyOnLoad := range []bool{false, true} {
		t.Run(fmt.Sprintf("ApplySystemRateLimitOnLoad=%v", applyOnLoad), func(t *testing.T) {
			h := newRLHarness(t, func(c *taskqueue.Config) {
				c.ApplySystemRateLimitOnLoad = applyOnLoad
				c.AdminNamespaceTaskQueueToPartitionDispatchRate = 20 // small so the cap is visible
			})
			h.backlog(200, one)
			h.startPollers(4, taskqueue.PollOptions{})
			h.run(2 * time.Second)
			rps, src, ok := h.e.RateLimit(q)
			require.True(t, ok)
			require.Equal(t, taskqueue.RateLimitSourceSystem, src)
			require.InDelta(t, 20, rps, 1e-6, "effective rate is reported either way")
			if applyOnLoad {
				require.InDelta(t, 21+2*20, h.total(), 3, "system rate enforced from load")
			} else {
				require.Equal(t, 200, h.total(), "upstream quirk: system rate not installed until the effective rate changes")
			}
		})
	}
}

// The admin (system) rate caps the API rate. Upstream installs a new whole-queue limit only
// when the effective rate changes, so an admin cap that is already the minimum when the queue
// loads is not enforced unless ApplySystemRateLimitOnLoad is set; a live change always applies.
func TestRateLimitAdminNamespaceRate(t *testing.T) {
	t.Run("live change", func(t *testing.T) {
		h := newRLHarness(t, nil)
		require.NoError(t, h.src.SetQueueRateLimit(q, f32p(100)))
		h.backlog(1000, one)
		h.startPollers(4, taskqueue.PollOptions{})
		h.run(2 * time.Second)
		h.src.SetAdminNamespaceDispatchRate(q.Namespace, f64p(10)) // system = min(ns, ns+queue)
		rps, src, _ := h.e.RateLimit(q)
		require.Equal(t, taskqueue.RateLimitSourceSystem, src)
		require.InDelta(t, 10, rps, 1e-6)
		h.run(4 * time.Second)
		require.InDelta(t, 20, h.count(4*time.Second, 6*time.Second, ""), 2)
		h.src.SetAdminNamespaceDispatchRate(q.Namespace, nil)
		_, src, _ = h.e.RateLimit(q)
		require.Equal(t, taskqueue.RateLimitSourceAPI, src)
	})
	for _, applyOnLoad := range []bool{false, true} {
		t.Run(fmt.Sprintf("set before load, ApplySystemRateLimitOnLoad=%v", applyOnLoad), func(t *testing.T) {
			h := newRLHarness(t, func(c *taskqueue.Config) { c.ApplySystemRateLimitOnLoad = applyOnLoad })
			require.NoError(t, h.src.SetQueueRateLimit(q, f32p(100)))
			h.src.SetAdminNamespaceDispatchRate(q.Namespace, f64p(10))
			h.backlog(300, one)
			h.startPollers(4, taskqueue.PollOptions{})
			h.run(2 * time.Second)
			rps, src, _ := h.e.RateLimit(q)
			require.Equal(t, taskqueue.RateLimitSourceSystem, src)
			require.InDelta(t, 10, rps, 1e-6, "reported either way")
			if applyOnLoad {
				require.InDelta(t, 11+2*10, h.total(), 2)
			} else {
				require.Equal(t, 300, h.total(), "upstream quirk: cap not installed (effective rate never changed)")
			}
		})
	}
}

func TestRateLimitTokenRecycledForUnusedTask(t *testing.T) {
	var drops atomic.Int32
	starter := starterFunc(func(_ context.Context, req *taskqueue.StartRequest) error {
		if keyOf(string(req.Task.Payload)) == "drop" {
			drops.Add(1)
			return taskqueue.ErrDropTask
		}
		return nil
	})
	h := newRLHarness(t, nil, func(o *taskqueue.Options) { o.Starter = starter })
	require.NoError(t, h.src.SetQueueRateLimit(q, f32p(1)))
	var tasks []taskqueue.Task
	for i := range 10 {
		tasks = append(tasks, fairTask("drop", 1, fmt.Sprintf("drop-%d", i)))
	}
	tasks = append(tasks, fairTask("keep", 1, "keep-0"))
	for _, tk := range tasks { // sequential: keep is last in dispatch order
		add(t, h.e, tk)
	}
	require.Eventually(t, func() bool { return h.e.MatcherTasks(q) == 11 }, 5*time.Second, time.Millisecond)
	h.startPollers(1, taskqueue.PollOptions{})
	h.settle(1) // clock frozen at t0
	// without recycling, rate 1 (burst 1) would deliver keep-0 after ~9s
	require.Equal(t, int32(10), drops.Load())
	require.Equal(t, 1, h.count(0, time.Millisecond, "keep"))
}

// TestRateLimitRealClockThroughput is a real-clock sanity run (TEMPORAL_TEST_RATE_REALCLOCK=1).
func TestRateLimitRealClockThroughput(t *testing.T) {
	if os.Getenv("TEMPORAL_TEST_RATE_REALCLOCK") == "" {
		t.Skip("set TEMPORAL_TEST_RATE_REALCLOCK=1")
	}
	const (
		rate     = 200
		duration = 10 * time.Second
		warmup   = 2 * time.Second
	)
	src := taskqueue.NewMemoryQueueConfigSource(0)
	require.NoError(t, src.SetQueueRateLimit(q, f32p(rate)))
	e := newEngine(t, memstore.New(), nil, func(o *taskqueue.Options) { o.QueueConfigSource = src })
	const n = 4000 // > rate*duration + burst, so the backlog never runs dry
	tasks := make([]taskqueue.Task, n)
	for i := range tasks {
		tasks[i] = fairTask("k", 1, fmt.Sprintf("k-%d", i))
	}
	addAll(t, e, tasks)

	ctx, cancel := context.WithCancel(context.Background())
	var mu sync.Mutex
	var times []time.Time
	var wg sync.WaitGroup
	t0 := time.Now()
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for ctx.Err() == nil {
				d, err := e.PollTask(ctx, q, taskqueue.PollOptions{})
				if err != nil {
					continue
				}
				mu.Lock()
				times = append(times, time.Now())
				mu.Unlock()
				_ = d.Ack()
			}
		}()
	}
	time.Sleep(duration)
	cancel()
	wg.Wait()

	inWindow := func(a, b time.Duration) int {
		c := 0
		for _, tm := range times {
			if off := tm.Sub(t0); off >= a && off < b {
				c++
			}
		}
		return c
	}
	first10ms := inWindow(0, 10*time.Millisecond)
	steady := inWindow(warmup, duration)
	steadyRate := float64(steady) / (duration - warmup).Seconds()
	t.Logf("real clock: target=%d rps, total=%d in %v, burst(first 10ms)=%d, steady [%v,%v)=%d → %.2f rps, backlog left=%d",
		rate, len(times), duration, first10ms, warmup, duration, steady, steadyRate, n-len(times))
	require.InDelta(t, rate, steadyRate, rate*0.02)
	require.Greater(t, n-len(times), 0, "backlog never ran dry, so the limiter (not supply) bounded the rate")
}
