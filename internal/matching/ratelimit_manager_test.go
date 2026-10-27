// Ported from temporalio/temporal v1.32.1 af08a5e5, service/matching/ratelimit_manager_test.go
// and rate_limit_fraction_provider_test.go. Changes: mockUserDataManager -> a nil or
// MemoryQueueConfigSource; NewConfig(dynamicconfig) -> DefaultConfig(); the fraction provider
// is Config.RateLimitFraction; Start takes the change callback. Tests after the "Library
// tests" marker are not upstream.
// Copyright (c) 2025 Temporal Technologies Inc. MIT License; see UPSTREAM_LICENSE and NOTICE.

package matching

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"github.com/skme902/tq/clock"
	"github.com/skme902/tq/store"
)

type RateLimitManagerSuite struct {
	suite.Suite
	*require.Assertions
}

func TestRateLimitManagerSuite(t *testing.T) {
	suite.Run(t, new(RateLimitManagerSuite))
}

func (s *RateLimitManagerSuite) SetupTest() {
	s.Assertions = require.New(s.T())
}

var testRLQueue = store.QueueKey{Namespace: "test-namespace", Name: "test-task-queue"}

func (s *RateLimitManagerSuite) TestUpdatePerKeySimpleRateLimitLocked_WhenFairnessKeyRateLimitDefaultIsNil() {
	config := newTaskQueueConfig(DefaultConfig())
	rateLimitManager := newRateLimitManager(nil, testRLQueue, config, clock.NewRealTimeSource())
	rateLimitManager.Start(nil)
	rateLimitManager.mu.Lock()
	// Simulate the condition where fairnessKeyRateLimitDefault is nil
	rateLimitManager.fairnessKeyRateLimitDefault = nil
	// Add per-key ready entries to verify they get cleared
	rateLimitManager.perKeyReady.Put("key1", simpleLimiter(1000))
	rateLimitManager.perKeyReady.Put("key2", simpleLimiter(2000))
	// Set per-key limit to verify it gets cleared
	rateLimitManager.perKeyLimit = simpleLimiterParams{
		interval: time.Second,
		burst:    10,
	}
	// Verify initial state
	s.Equal(2, rateLimitManager.perKeyReady.Size())
	s.True(rateLimitManager.perKeyLimit.limited())
	// Update the per-key simple rate limit with fairnessKeyRateLimitDefault as nil
	rateLimitManager.updatePerKeySimpleRateLimitWithBurstLocked(time.Second)
	// Verify that clearPerKeyRateLimitsLocked was called
	// The cache should be replaced with a new empty cache
	s.Equal(0, rateLimitManager.perKeyReady.Size(), "All per-key ready entries should be cleared")
	s.False(rateLimitManager.perKeyLimit.limited(), "Per-key limit should be cleared")
	rateLimitManager.mu.Unlock()
}

// Additions to rateLimitManager for use by other unit tests:

func (r *rateLimitManager) UpdateSimpleRateLimitWithBurstForTesting(burstDuration time.Duration) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.updateSimpleRateLimitWithBurstLocked(burstDuration)
}

func (r *rateLimitManager) UpdatePerKeySimpleRateLimitWithBurstForTesting(burstDuration time.Duration) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.updatePerKeySimpleRateLimitWithBurstLocked(burstDuration)
}

func (r *rateLimitManager) SetAdminRateForTesting(rps float64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.adminNsRate = rps
	r.adminTqRate = rps
}

func (r *rateLimitManager) SetEffectiveRPSAndSourceForTesting(rps float64, source RateLimitSource) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.effectiveRPS = rps
	r.fairnessKeyRateLimitDefault = &rps
	r.rateLimitSource = source
}

func (r *rateLimitManager) SetFairnessKeyRateLimitDefaultForTesting(rps float64, source RateLimitSource) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.fairnessKeyRateLimitDefault = &rps
}

func (r *rateLimitManager) SetAPIConfigRPSForTesting(rps float64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.apiConfigRPS = &rps
	r.computeEffectiveRPSAndSourceLocked()
}

func (r *rateLimitManager) SetWorkerRPSForTesting(rps float64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.workerRPS = &rps
	r.computeEffectiveRPSAndSourceLocked()
}

// GetFairnessKeyRateLimitDefaultForTesting returns the stored per-partition fairness-key rate and
// whether it is set. It is the value stored after fraction and partition scaling.
func (r *rateLimitManager) GetFairnessKeyRateLimitDefaultForTesting() (float64, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.fairnessKeyRateLimitDefault == nil {
		return 0, false
	}
	return *r.fairnessKeyRateLimitDefault, true
}

func (r *rateLimitManager) GetNumReadPartitionsForTesting() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.numReadPartitions
}

// newConfigWithFraction returns a Config whose RateLimitFraction always returns the given fraction.
func newConfigWithFraction(fraction float64) Config {
	cfg := DefaultConfig()
	cfg.RateLimitFraction = func(store.QueueKey) float64 {
		return fraction
	}
	return cfg
}

// TestFractionScaling_ApiConfigRPS verifies that apiConfigRPS is scaled by the fraction
// before being divided by the number of read partitions.
func (s *RateLimitManagerSuite) TestFractionScaling_ApiConfigRPS() {
	cfg := newConfigWithFraction(0.5)
	config := newTaskQueueConfig(cfg)
	rlm := newRateLimitManager(nil, testRLQueue, config, clock.NewRealTimeSource())
	rlm.Start(nil)
	defer rlm.Stop()

	rlm.SetAPIConfigRPSForTesting(100.0)

	rps, source := rlm.GetEffectiveRPSAndSource()
	// effectiveRPS = apiRPS * fraction / numPartitions; GetEffectiveRPSAndSource multiplies back by numPartitions → apiRPS * fraction
	s.InEpsilon(50.0, rps, 1e-9)
	s.Equal(RateLimitSourceAPI, source)
}

// TestFractionScaling_WorkerRPS verifies that workerRPS is scaled by the fraction.
func (s *RateLimitManagerSuite) TestFractionScaling_WorkerRPS() {
	cfg := newConfigWithFraction(0.5)
	config := newTaskQueueConfig(cfg)
	rlm := newRateLimitManager(nil, testRLQueue, config, clock.NewRealTimeSource())
	rlm.Start(nil)
	defer rlm.Stop()

	rlm.SetWorkerRPSForTesting(100.0)

	rps, source := rlm.GetEffectiveRPSAndSource()
	s.InEpsilon(50.0, rps, 1e-9)
	s.Equal(RateLimitSourceWorker, source)
}

// TestFractionScaling_FairnessKeyRateLimitDefault verifies that the per-key fairness rate default
// is also scaled by the fraction (Fix 3 coverage).
func (s *RateLimitManagerSuite) TestFractionScaling_FairnessKeyRateLimitDefault() {
	const fraction = 0.5
	const fairnessKeyRPS = float32(100)
	cfg := newConfigWithFraction(fraction)
	cfg.NumReadPartitions = 4 // upstream default, so the partition division is exercised
	config := newTaskQueueConfig(cfg)
	src := NewMemoryQueueConfigSource(0)
	rps := fairnessKeyRPS
	s.NoError(src.SetFairnessKeyRateLimitDefault(testRLQueue, &rps))
	rlm := newRateLimitManager(src, testRLQueue, config, clock.NewRealTimeSource())
	rlm.Start(nil)
	defer rlm.Stop()

	rlm.UserDataChanged()

	got, ok := rlm.GetFairnessKeyRateLimitDefaultForTesting()
	s.True(ok)
	// val = fairnessKeyRPS * fraction / numPartitions
	numPartitions := rlm.GetNumReadPartitionsForTesting()
	want := float64(fairnessKeyRPS) * fraction / float64(numPartitions)
	s.InEpsilon(want, got, 1e-9)
}

// TestFractionScaling_ZeroFraction verifies that a zero fraction (fully drained) reduces
// the effective RPS to zero even when an API rate limit is set.
func (s *RateLimitManagerSuite) TestFractionScaling_ZeroFraction() {
	cfg := newConfigWithFraction(0.0)
	config := newTaskQueueConfig(cfg)
	rlm := newRateLimitManager(nil, testRLQueue, config, clock.NewRealTimeSource())
	rlm.Start(nil)
	defer rlm.Stop()

	rlm.SetAPIConfigRPSForTesting(100.0)

	rps, source := rlm.GetEffectiveRPSAndSource()
	s.InDelta(0.0, rps, 0.0)
	s.Equal(RateLimitSourceAPI, source)
}

// rate_limit_fraction_provider_test.go

func TestNewTaskQueueRateLimitFractionProvider_ClampedToMax(t *testing.T) {
	require.InEpsilon(t, maxRateLimitFraction, clampRateLimitFraction(2.0), 0)
}

func TestNewTaskQueueRateLimitFractionProvider_ClampedToMin(t *testing.T) {
	require.InDelta(t, minRateLimitFraction, clampRateLimitFraction(-0.5), 0)
}

func TestNewTaskQueueRateLimitFractionProvider_PassthroughInRange(t *testing.T) {
	require.InEpsilon(t, 0.5, clampRateLimitFraction(0.5), 0)
}

// ---- Library tests (not upstream) ----

func newTestRLM(t *testing.T, mod func(*Config), src QueueConfigSource) (*rateLimitManager, *clock.EventTimeSource) {
	t.Helper()
	cfg := DefaultConfig()
	if mod != nil {
		mod(&cfg)
	}
	ts := clock.NewEventTimeSource().Update(time.Unix(1_700_000_000, 0))
	rlm := newRateLimitManager(src, testRLQueue, newTaskQueueConfig(cfg.WithDefaults()), ts)
	rlm.Start(func() { rlm.UserDataChanged() })
	rlm.UserDataChanged()
	t.Cleanup(rlm.Stop)
	return rlm, ts
}

func f32(v float32) *float32 { return &v }
func f64(v float64) *float64 { return &v }

func TestRateLimitManager_SystemRateQuirk(t *testing.T) {
	t.Run("default: system rate computed but not installed at load", func(t *testing.T) {
		rlm, _ := newTestRLM(t, nil, nil)
		rps, src := rlm.GetEffectiveRPSAndSource()
		require.InDelta(t, 1000, rps, 0) // min(10000, 1000)
		require.Equal(t, RateLimitSourceSystem, src)
		ready, _ := rlm.rateLimitState()
		require.False(t, rlm.wholeQueueLimit.limited(), "upstream quirk: unlimited until the effective rate changes")
		require.Zero(t, ready)
	})
	t.Run("ApplySystemRateLimitOnLoad installs it", func(t *testing.T) {
		rlm, _ := newTestRLM(t, func(c *Config) { c.ApplySystemRateLimitOnLoad = true }, nil)
		require.Equal(t, makeSimpleLimiterParams(1000, time.Second), rlm.wholeQueueLimit)
	})
	t.Run("default: installed after a change and change back", func(t *testing.T) {
		rlm, _ := newTestRLM(t, nil, nil)
		rlm.InjectWorkerRPS(&pollMetadata{maxTasksPerSecond: f64(5)})
		require.Equal(t, makeSimpleLimiterParams(5, time.Second), rlm.wholeQueueLimit)
		rlm.InjectWorkerRPS(&pollMetadata{})
		require.Equal(t, makeSimpleLimiterParams(1000, time.Second), rlm.wholeQueueLimit)
	})
}

func TestRateLimitManager_EffectiveRateSelection(t *testing.T) {
	src := NewMemoryQueueConfigSource(0)
	rlm, _ := newTestRLM(t, nil, src)

	// worker below system
	rlm.InjectWorkerRPS(&pollMetadata{maxTasksPerSecond: f64(50)})
	rps, s := rlm.GetEffectiveRPSAndSource()
	require.Equal(t, [2]any{50.0, RateLimitSourceWorker}, [2]any{rps, s})

	// API beats worker even when higher
	require.NoError(t, src.SetQueueRateLimit(testRLQueue, f32(200)))
	rps, s = rlm.GetEffectiveRPSAndSource()
	require.Equal(t, [2]any{200.0, RateLimitSourceAPI}, [2]any{rps, s})

	// API above system: system wins
	require.NoError(t, src.SetQueueRateLimit(testRLQueue, f32(5000)))
	rps, s = rlm.GetEffectiveRPSAndSource()
	require.Equal(t, [2]any{1000.0, RateLimitSourceSystem}, [2]any{rps, s})

	// system = min(namespace admin, queue admin)
	src.SetAdminNamespaceDispatchRate(testRLQueue.Namespace, f64(300))
	rps, s = rlm.GetEffectiveRPSAndSource()
	require.Equal(t, [2]any{300.0, RateLimitSourceSystem}, [2]any{rps, s})
	src.SetAdminQueueDispatchRate(testRLQueue, f64(100))
	rps, _ = rlm.GetEffectiveRPSAndSource()
	require.InDelta(t, 100, rps, 0)
	// other namespaces are unaffected
	src.SetAdminNamespaceDispatchRate("other", f64(1))
	rps, _ = rlm.GetEffectiveRPSAndSource()
	require.InDelta(t, 100, rps, 0)

	// unset API: worker rate (50) applies again
	require.NoError(t, src.SetQueueRateLimit(testRLQueue, nil))
	rps, s = rlm.GetEffectiveRPSAndSource()
	require.Equal(t, [2]any{50.0, RateLimitSourceWorker}, [2]any{rps, s})

	// a poll without MaxTasksPerSecond clears the worker rate (last poller wins)
	rlm.InjectWorkerRPS(&pollMetadata{})
	rps, s = rlm.GetEffectiveRPSAndSource()
	require.Equal(t, [2]any{100.0, RateLimitSourceSystem}, [2]any{rps, s})

	// after Stop, changes are no longer applied
	rlm.Stop()
	require.NoError(t, src.SetQueueRateLimit(testRLQueue, f32(1)))
	rps, _ = rlm.GetEffectiveRPSAndSource()
	require.InDelta(t, 100, rps, 0)
}

func TestRateLimitManager_NumReadPartitions(t *testing.T) {
	src := NewMemoryQueueConfigSource(0)
	require.NoError(t, src.SetQueueRateLimit(testRLQueue, f32(400)))
	rlm, _ := newTestRLM(t, func(c *Config) { c.NumReadPartitions = 4 }, src)
	// per-partition limit is 100, reported total 400
	require.Equal(t, makeSimpleLimiterParams(100, time.Second), rlm.wholeQueueLimit)
	rps, _ := rlm.GetEffectiveRPSAndSource()
	require.InDelta(t, 400, rps, 1e-9)
}

func TestRateLimitManager_RejectsNegativeRates(t *testing.T) {
	src := NewMemoryQueueConfigSource(0)
	var ia *store.InvalidArgumentError
	require.ErrorAs(t, src.SetQueueRateLimit(testRLQueue, f32(-1)), &ia)
	require.ErrorAs(t, src.SetFairnessKeyRateLimitDefault(testRLQueue, f32(-1)), &ia)
	require.NoError(t, src.SetQueueRateLimit(testRLQueue, f32(0)))
}

func TestRateLimitManager_PerKeyWeightScalesInterval(t *testing.T) {
	src := NewMemoryQueueConfigSource(0)
	require.NoError(t, src.SetFairnessKeyRateLimitDefault(testRLQueue, f32(10)))
	rlm, ts := newTestRLM(t, nil, src)
	now := ts.Now().UnixNano()
	mk := func(key string, w float32) *internalTask {
		return newInternalTaskForSyncMatch(&store.Task{Priority: store.Priority{FairnessKey: key, FairnessWeight: w}}, nil)
	}
	// burst is 1s worth of tokens; consume 30 tokens each so both are well past burst
	a, b := mk("a", 1), mk("b", 2)
	rlm.consumeTokens(now, a, 30)
	rlm.consumeTokens(now, b, 30)
	// a: 10/s -> ready at now + 30*100ms - 1s burst = +2s; b: 20/s -> +1.5s - 1s = +0.5s
	require.Equal(t, 2*time.Second, rlm.readyTimeForTask(a).delay(now))
	require.Equal(t, 500*time.Millisecond, rlm.readyTimeForTask(b).delay(now))

	// override the weight of "a" to 4 via config: interval 25ms
	require.NoError(t, src.UpdateFairnessWeightOverrides(testRLQueue, map[string]float32{"c": 4}, nil))
	c := mk("c", 0)
	rlm.consumeTokens(now, c, 80) // 80*25ms - 1s = 1s
	require.Equal(t, time.Second, rlm.readyTimeForTask(c).delay(now))
}

func TestRateLimitManager_ForwardedTasksNotCounted(t *testing.T) {
	src := NewMemoryQueueConfigSource(0)
	require.NoError(t, src.SetQueueRateLimit(testRLQueue, f32(1)))
	require.NoError(t, src.SetFairnessKeyRateLimitDefault(testRLQueue, f32(1)))
	rlm, ts := newTestRLM(t, nil, src)
	now := ts.Now().UnixNano()
	fwd := newInternalTaskForSyncMatch(&store.Task{Priority: store.Priority{FairnessKey: "k"}},
		&forwardInfo{sourcePartition: 1})
	require.True(t, fwd.isForwarded())
	before := rlm.wholeQueueReady
	rlm.consumeTokens(now, fwd, 5)
	require.Equal(t, before, rlm.wholeQueueReady)
	require.Equal(t, 0, rlm.perKeyReady.Size())
}

func TestRateLimitManager_PerKeyLRUBounded(t *testing.T) {
	src := NewMemoryQueueConfigSource(0)
	require.NoError(t, src.SetFairnessKeyRateLimitDefault(testRLQueue, f32(1)))
	rlm, ts := newTestRLM(t, nil, src)
	now := ts.Now().UnixNano()
	for i := range 2500 {
		rlm.consumeTokens(now, newInternalTaskForSyncMatch(&store.Task{Priority: store.Priority{FairnessKey: fmt.Sprint(i)}}, nil), 5)
	}
	require.Equal(t, 2000, rlm.perKeyReady.Size())
	// the oldest keys were evicted and are therefore ready again (only the whole-queue limit applies)
	old := newInternalTaskForSyncMatch(&store.Task{Priority: store.Priority{FairnessKey: "0"}}, nil)
	recent := newInternalTaskForSyncMatch(&store.Task{Priority: store.Priority{FairnessKey: "2499"}}, nil)
	require.LessOrEqual(t, rlm.readyTimeForTask(old).delay(now), time.Duration(0))
	require.Positive(t, rlm.readyTimeForTask(recent).delay(now))
}

func TestRateLimitManager_ZeroAndTinyRates(t *testing.T) {
	src := NewMemoryQueueConfigSource(0)
	rlm, ts := newTestRLM(t, nil, src)
	now := ts.Now().UnixNano()
	for _, r := range []float32{0, 1e-10} {
		require.NoError(t, src.SetQueueRateLimit(testRLQueue, f32(r)))
		require.Equal(t, simpleLimiterParams{interval: -1}, rlm.wholeQueueLimit, "rate %v never", r)
		ready, _ := rlm.rateLimitState()
		require.Equal(t, simpleLimiterNever, ready)
		require.Greater(t, ready.delay(now), 100*365*24*time.Hour)
	}
	// increasing again clips the far-future ready time to now + maxTokens*interval + burst
	require.NoError(t, src.SetQueueRateLimit(testRLQueue, f32(2)))
	ready, _ := rlm.rateLimitState()
	require.Equal(t, 1500*time.Millisecond, ready.delay(ts.Now().UnixNano()))
}
