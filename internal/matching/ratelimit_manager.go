// Ported from temporalio/temporal v1.32.1 af08a5e5, service/matching/ratelimit_manager.go and
// rate_limit_fraction_provider.go.
// Changes:
//   - userDataManager -> QueueConfigSource (QueueConfig.RateLimit / FairnessKeyRateLimitDefault /
//     FairnessWeightOverrides); the admin dispatch-rate dynamic config subscriptions
//     (AdminNamespaceToPartitionRateSub / AdminNamespaceTaskQueueToPartitionRateSub) are read
//     from Config with QueueConfig.Admin* overrides, re-read on every config change.
//   - NumReadPartitions and RateLimitFraction come from Config (static).
//   - The quotas.DynamicRateLimiter / MutableRateBurst (used only by the classic, non-priority
//     matcher, which is not ported) and MinTaskThrottlingBurstSize are dropped.
//   - The time source is injected (upstream uses clock.NewRealTimeSource()).
//   - common/cache LRU -> internal/cache.LRU (same count-limited LRU semantics).
//   - Config.ApplySystemRateLimitOnLoad optionally installs the computed limit in Start
//     (default false = upstream: the limit is only installed when the effective rate changes).
// Copyright (c) 2025 Temporal Technologies Inc. MIT License; see UPSTREAM_LICENSE and NOTICE.

package matching

import (
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/skme902/tq/clock"
	"github.com/skme902/tq/internal/cache"
	"github.com/skme902/tq/store"
)

// RateLimitSource reports where a queue's effective rate limit comes from (upstream
// enumspb.RateLimitSource).
type RateLimitSource int

const (
	RateLimitSourceUnspecified RateLimitSource = iota
	// RateLimitSourceAPI: QueueConfig.RateLimit.
	RateLimitSourceAPI
	// RateLimitSourceWorker: the last poller's PollOptions.MaxTasksPerSecond.
	RateLimitSourceWorker
	// RateLimitSourceSystem: the admin dispatch rates.
	RateLimitSourceSystem
)

func (s RateLimitSource) String() string {
	switch s {
	case RateLimitSourceAPI:
		return "api"
	case RateLimitSourceWorker:
		return "worker"
	case RateLimitSourceSystem:
		return "system"
	default:
		return "unspecified"
	}
}

type (
	rateLimitManager struct {
		mu sync.Mutex

		// Dependencies
		configSource QueueConfigSource // replaces userDataManager
		queue        store.QueueKey
		config       *taskQueueConfig // Dynamic configuration for task queues set by system.
		timeSource   clock.TimeSource

		// Sources of the effective RPS.
		workerRPS                   *float64 // RPS set by worker at the time of polling, if available.
		apiConfigRPS                *float64 // RPS set via API, if available.
		fairnessKeyRateLimitDefault *float64 // per-partition fairnessKeyRateLimitDefault set via API, if available
		adminNsRate                 float64
		adminTqRate                 float64
		numReadPartitions           int

		// Derived from the above sources.
		effectiveRPS    float64         // Min of api/worker set RPS and system defaults. Always reflects the per-partition wise task queue RPS.
		systemRPS       float64         // Min of partition level dispatch rates times the number of read partitions.
		rateLimitSource RateLimitSource // Source of the rate limit, can be set via API, worker or system default.
		// Fairness tasks rate limiter.
		wholeQueueLimit simpleLimiterParams
		wholeQueueReady simpleLimiter
		// Rate limiter for individual fairness keys.
		// Note that we currently have only one limit for all keys, which is scaled by the key's
		// weight. If we do this, we can assume that all keys are either at or below their rate
		// limit at the same time, so that if the head of the queue is at its limit, then the rest
		// must also be, and so we don't have to "skip over" the head of the queue due to rate
		// limits. This isn't true in situations where weights have changed in between writing and
		// reading. We'll handle that situation better in the future.
		perKeyLimit     simpleLimiterParams
		perKeyReady     *cache.LRU[string, simpleLimiter]
		perKeyOverrides fairnessWeightOverrides // TODO(fairness): get this from config

		// Deviation (pause, library addition): QueueConfig.Paused / PausedFairnessKeys,
		// read lock-free by findMatch. pausedKeys is nil when no key is paused.
		pause   atomic.Pointer[pauseSnapshot]
		cancels []func()
		stopped bool // set by Stop; guarded by mu
	}
)

const (
	// How much rate limit a task queue can use up in an instant. E.g., for a rate of
	// 100/second and burst duration of 2 seconds, the capacity of a bucket-type limiting
	// algorithm would be 200.
	defaultBurstDuration = time.Second

	// rate_limit_fraction_provider.go
	minRateLimitFraction     = 0.0
	maxRateLimitFraction     = 1.0
	defaultRateLimitFraction = maxRateLimitFraction
)

// clampRateLimitFraction is NewTaskQueueRateLimitFractionProvider's [0, 1] clamp.
func clampRateLimitFraction(f float64) float64 {
	return max(min(f, maxRateLimitFraction), minRateLimitFraction)
}

// Create a new rate limit manager for the task queue partition. This only allocates;
// config subscriptions are registered in Start, so an unstarted manager holds
// no external references and can simply be garbage collected.
func newRateLimitManager(
	configSource QueueConfigSource,
	queue store.QueueKey,
	config *taskQueueConfig,
	timeSource clock.TimeSource,
) *rateLimitManager {
	return &rateLimitManager{
		configSource: configSource,
		queue:        queue,
		config:       config,
		perKeyReady:  cache.New[string, simpleLimiter](config.FairnessKeyRateLimitCacheSize),
		timeSource:   timeSource,
	}
}

// rateLimitFraction replaces config.RateLimitFraction() (the fraction provider).
func (r *rateLimitManager) rateLimitFraction() float64 {
	if r.config.RateLimitFraction == nil {
		return defaultRateLimitFraction
	}
	return clampRateLimitFraction(r.config.RateLimitFraction(r.queue))
}

func (r *rateLimitManager) queueConfig() QueueConfig {
	if r.configSource == nil {
		return QueueConfig{}
	}
	return r.configSource.QueueConfig(r.queue)
}

// adminRates replaces the two admin dynamic config values.
func (r *rateLimitManager) adminRates(cfg QueueConfig) (ns, tq float64) {
	ns, tq = r.config.AdminNamespaceToPartitionDispatchRate, r.config.AdminNamespaceTaskQueueToPartitionDispatchRate
	if cfg.AdminNamespaceDispatchRate != nil {
		ns = *cfg.AdminNamespaceDispatchRate
	}
	if cfg.AdminQueueDispatchRate != nil {
		tq = *cfg.AdminQueueDispatchRate
	}
	return ns, tq
}

// Start reads the system config and computes the initial rate limits. onChange (may be nil)
// is subscribed to QueueConfigWatcher changes; the partition manager passes its
// userDataChanged, which calls UserDataChanged.
func (r *rateLimitManager) Start(onChange func()) {
	r.mu.Lock()
	defer r.mu.Unlock()

	// Overall system rate limit will be the min of the two configs that are partition wise times the number of partitons.
	cfg := r.queueConfig()
	r.adminNsRate, r.adminTqRate = r.adminRates(cfg)
	r.setPause(cfg)
	// Defaulting to 1 partition if misconfigured (upstream does this in setNumReadPartitions).
	r.numReadPartitions = max(r.config.NumReadPartitions, 1)
	if w, ok := r.configSource.(QueueConfigWatcher); ok && onChange != nil && !r.stopped {
		r.cancels = append(r.cancels, w.WatchQueueConfig(r.queue, onChange))
	}
	r.computeEffectiveRPSAndSourceLocked()
	if r.config.ApplySystemRateLimitOnLoad {
		// Not upstream: install the computed (system) limit now instead of waiting for the
		// effective rate to change.
		r.updateSimpleRateLimitWithBurstLocked(defaultBurstDuration)
	}
}

// Computes the effectiveRPS and its source by evaluating all possible rate limit configurations.
// - If an API-level RPS is configured, effectiveRPS = min(system default RPS, API-configured RPS)
// - Else if a worker-level RPS is configured, effectiveRPS = min(system default RPS, worker-configured RPS)
// - Otherwise, fall back to the system default RPS from dynamic config.
func (r *rateLimitManager) computeEffectiveRPSAndSourceLocked() {

	var (
		effectiveRPS    = math.Inf(1)
		rateLimitSource RateLimitSource
	)
	// Overall system rate limit will be the min of the two configs that are partition wise times the number of partions.
	systemRPS := min(
		r.adminNsRate,
		r.adminTqRate,
	)
	r.systemRPS = systemRPS
	fraction := r.rateLimitFraction()
	switch {
	case r.apiConfigRPS != nil:
		effectiveRPS = *r.apiConfigRPS * fraction / float64(r.numReadPartitions)
		rateLimitSource = RateLimitSourceAPI
	case r.workerRPS != nil:
		effectiveRPS = *r.workerRPS * fraction / float64(r.numReadPartitions)
		rateLimitSource = RateLimitSourceWorker
	}

	if effectiveRPS < r.systemRPS {
		r.effectiveRPS = effectiveRPS
		r.rateLimitSource = rateLimitSource
	} else {
		r.effectiveRPS = r.systemRPS
		r.rateLimitSource = RateLimitSourceSystem
	}
}

func (r *rateLimitManager) computeAndApplyRateLimitLocked() {
	oldRPS := r.effectiveRPS
	r.computeEffectiveRPSAndSourceLocked()
	newRPS := r.effectiveRPS
	// If the effective RPS has changed, we need to update the rate limiters.
	if oldRPS != newRPS {
		r.updateSimpleRateLimitWithBurstLocked(defaultBurstDuration)
	}
	// Internally, checks if the per-key rate limit has changed and updates it accordingly.
	r.updatePerKeySimpleRateLimitWithBurstLocked(defaultBurstDuration)
}

// Lazy injection of poll metadata.
// Called whenever a new poll request comes in.
// This allows the rate limit manager to adjust its rate limits based on any updates right before polling happens.
// Like upstream, a poll without MaxTasksPerSecond clears the worker rate (last poller wins).
func (r *rateLimitManager) InjectWorkerRPS(meta *pollMetadata) {
	r.mu.Lock()
	defer r.mu.Unlock()
	var rps *float64
	if meta != nil && meta.maxTasksPerSecond != nil {
		value := *meta.maxTasksPerSecond
		rps = &value
	}
	r.workerRPS = rps
	r.computeAndApplyRateLimitLocked()
}

// Return the effective RPS and its source together.
// The source can be API, worker or system default.
// It defaults to the system dynamic config.
func (r *rateLimitManager) GetEffectiveRPSAndSource() (float64, RateLimitSource) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.effectiveRPS * float64(r.numReadPartitions), r.rateLimitSource
}

// Updates the API-configured RPS based on the latest user data
// and applies the new rate limit if the effective RPS has changed.
func (r *rateLimitManager) UserDataChanged() {
	r.mu.Lock()
	defer r.mu.Unlock()
	// Fetch the latest user data and update the API-configured RPS.
	r.trySetRPSFromUserDataLocked()
	r.computeAndApplyRateLimitLocked()
}

// trySetRPSFromUserDataLocked sets the apiConfigRPS from user data.
// Called exclusively in response to updates in user data.
// Port: also re-reads the admin rates (upstream: setAdminNsRate / setAdminTqRate callbacks,
// which end in the same computeAndApplyRateLimitLocked).
func (r *rateLimitManager) trySetRPSFromUserDataLocked() {
	config := r.queueConfig()
	r.adminNsRate, r.adminTqRate = r.adminRates(config)
	// If rate limit is an empty message, it means rate limit could have been unset via API.
	// In this case, the apiConfigRPS will need to be unset.
	if config.RateLimit == nil {
		r.apiConfigRPS = nil
	} else {
		val := float64(*config.RateLimit)
		r.apiConfigRPS = &val
	}
	if config.FairnessKeyRateLimitDefault == nil {
		r.fairnessKeyRateLimitDefault = nil
	} else {
		// Maintain the fairnessKeyRateLimitDefault as per-partition rate, scaled by the same
		// fraction applied to the whole-queue effectiveRPS.
		fraction := r.rateLimitFraction()
		val := float64(*config.FairnessKeyRateLimitDefault) * fraction / float64(r.numReadPartitions)
		r.fairnessKeyRateLimitDefault = &val
	}
	r.perKeyOverrides = config.FairnessWeightOverrides
	r.setPause(config)
}

// pauseSnapshot is one immutable QueueConfig pause setting. It is published as a single
// pointer, so a reader never sees the queue flag of one update with the key set of another.
type pauseSnapshot struct {
	paused bool
	keys   map[string]struct{} // nil when no key is paused
}

// setPause caches the queue's pause settings (library addition).
func (r *rateLimitManager) setPause(cfg QueueConfig) {
	p := &pauseSnapshot{paused: cfg.Paused}
	if len(cfg.PausedFairnessKeys) > 0 {
		p.keys = make(map[string]struct{}, len(cfg.PausedFairnessKeys))
		for _, k := range cfg.PausedFairnessKeys {
			p.keys[k] = struct{}{}
		}
	}
	r.pause.Store(p)
}

// pauseState reports whether the queue is paused and the paused fairness keys (nil if none),
// from one snapshot.
func (r *rateLimitManager) pauseState() (bool, map[string]struct{}) {
	if p := r.pause.Load(); p != nil {
		return p.paused, p.keys
	}
	return false, nil
}

// UpdateSimpleRateLimit updates the overall queue rate limits for the simpleRateLimiter implementation
func (r *rateLimitManager) updateSimpleRateLimitWithBurstLocked(burstDuration time.Duration) {
	newRPS := r.effectiveRPS
	r.wholeQueueLimit = makeSimpleLimiterParams(newRPS, burstDuration)

	// Clip to handle the case where we have increased from a zero or very low limit and had
	// ready times in the far future.
	now := r.timeSource.Now().UnixNano()
	r.wholeQueueReady = r.wholeQueueReady.clip(r.wholeQueueLimit, now, maxTokens)
}

// UpdatePerKeySimpleRateLimit updates the per-key rate limit for the simpleRateLimit implementation
// UpdateTaskQueueConfig api is the single source for the per-key rate limit.
func (r *rateLimitManager) updatePerKeySimpleRateLimitWithBurstLocked(burstDuration time.Duration) {
	if r.fairnessKeyRateLimitDefault == nil {
		r.clearPerKeyRateLimitsLocked()
		return
	}
	rate := *r.fairnessKeyRateLimitDefault
	slp := makeSimpleLimiterParams(rate, burstDuration)
	if slp == r.perKeyLimit {
		// No change in per-key rate limit, no need to update.
		return
	}
	r.perKeyLimit = slp

	// Clip to handle the case where we have increased from a zero or very low limit and had
	// ready times in the far future.
	var updates map[string]simpleLimiter
	now := r.timeSource.Now().UnixNano()
	for _, e := range r.perKeyReady.Entries() {
		sl := e.Value
		if clipped := sl.clip(r.perKeyLimit, now, maxTokens); clipped != sl {
			if updates == nil {
				updates = make(map[string]simpleLimiter)
			}
			updates[e.Key] = clipped
		}
	}

	// This messes up the LRU order, but we can't avoid that here without adding new
	// functionality to Cache.
	for key, clipped := range updates {
		r.perKeyReady.Put(key, clipped)
	}
}

// clearPerKeyRateLimitsLocked removes all fairness per-key rate limits.
func (r *rateLimitManager) clearPerKeyRateLimitsLocked() {
	r.perKeyReady = cache.New[string, simpleLimiter](r.config.FairnessKeyRateLimitCacheSize)
	r.perKeyLimit = simpleLimiterParams{}
}

// rateLimitState returns the whole-queue ready time and whether a per-key limit is in effect.
func (r *rateLimitManager) rateLimitState() (wholeQueueReady simpleLimiter, perKeyLimited bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.wholeQueueReady, r.perKeyLimit.limited()
}

func (r *rateLimitManager) readyTimeForTask(task *internalTask) simpleLimiter {
	r.mu.Lock()
	defer r.mu.Unlock()
	// TODO(pri): after we have task-specific ready time, we can re-enable this
	// if task.isForwarded() {
	// 	// don't count any rate limit for forwarded tasks, it was counted on the child
	// 	return 0
	// }
	ready := r.wholeQueueReady

	if r.perKeyLimit.limited() {
		key := task.getPriority().FairnessKey
		if v, ok := r.perKeyReady.Get(key); ok {
			ready = max(ready, v)
		}
	}

	return ready
}

func (r *rateLimitManager) consumeTokens(now int64, task *internalTask, tokens int64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if task.isForwarded() {
		// don't count any rate limit for forwarded tasks, it was counted on the child
		return
	}

	r.wholeQueueReady = r.wholeQueueReady.consume(r.wholeQueueLimit, now, tokens)

	if r.perKeyLimit.limited() {
		pri := task.getPriority()
		key := pri.FairnessKey
		weight := getEffectiveWeight(r.perKeyOverrides, pri)
		p := r.perKeyLimit
		p.interval = time.Duration(float32(p.interval) / weight) // scale by weight
		var sl simpleLimiter
		if v, ok := r.perKeyReady.Get(key); ok {
			sl = v
		}
		r.perKeyReady.Put(key, sl.consume(p, now, tokens))
	}
}

// GetFairnessWeightOverrides returns the current fairness weight overrides.
func (r *rateLimitManager) GetFairnessWeightOverrides() fairnessWeightOverrides {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.perKeyOverrides
}

func (r *rateLimitManager) Stop() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.stopped = true // a Start after Stop must not register a watch (REVIEW M3)
	for _, cancel := range r.cancels {
		cancel()
	}
	r.cancels = nil
}
