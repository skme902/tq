// Adapted from temporalio/temporal v1.32.1 af08a5e5, service/matching/config.go and
// common/dynamicconfig/constants.go (defaults). Dynamic config is replaced by a plain struct.
// Copyright (c) 2025 Temporal Technologies Inc. MIT License; see UPSTREAM_LICENSE and NOTICE.

package matching

import (
	"fmt"
	"math"
	"time"

	"github.com/skme902/tq/internal/counter"
	"github.com/skme902/tq/store"
)

// Config holds the queue tuning knobs. Zero values are replaced by DefaultConfig values in
// WithDefaults. Defaults are upstream's (dynamicconfig/constants.go line refs in comments).
type Config struct {
	// LongPollExpirationInterval is the maximum time a poll waits (matching.longPollExpirationInterval, 1m).
	LongPollExpirationInterval time.Duration
	// MaxTaskQueueIdleTime unloads a queue after this long without polls/adds (matching.maxTaskQueueIdleTime, 5m).
	MaxTaskQueueIdleTime time.Duration
	// GetTasksBatchSize is the max tasks read per store call and kept in memory per subqueue (1000).
	GetTasksBatchSize int
	// GetTasksReloadAt triggers a read when at most this many tasks are loaded (100). It must be
	// in [0, GetTasksBatchSize).
	GetTasksReloadAt int
	// UpdateAckInterval is how often ack levels / metadata are synced to the store (1m).
	UpdateAckInterval time.Duration
	// MetadataUpdateOnAppendInterval piggybacks metadata on CreateTasks at most this often (5s).
	MetadataUpdateOnAppendInterval time.Duration
	// OutstandingTaskAppendsThreshold is the writer channel capacity; full → ErrResourceExhausted (250).
	OutstandingTaskAppendsThreshold int
	// MaxTaskBatchSize is the max tasks per CreateTasks call (100).
	MaxTaskBatchSize int
	// MaxTaskDeleteBatchSize triggers GC once this many tasks are acked (100).
	MaxTaskDeleteBatchSize int
	// TaskDeleteInterval triggers GC after this long even if few tasks were acked (15s).
	TaskDeleteInterval time.Duration
	// BacklogNegligibleAge: sync match is allowed only while the oldest in-memory backlog task is younger than this (5s).
	BacklogNegligibleAge time.Duration
	// MaxWaitForPollerBeforeFwd gates forwarding when a backlog exists (200ms). Unused: partitions are not implemented.
	MaxWaitForPollerBeforeFwd time.Duration
	// BacklogTaskForwardTimeout caps the validator retry interval (60s).
	BacklogTaskForwardTimeout time.Duration
	// PriorityLevels is the number of priority keys [1, PriorityLevels] (5). Default key is (levels+1)/2.
	PriorityLevels int
	// FairnessPassDither spreads new keys' initial pass (false).
	FairnessPassDither bool
	// FairnessCounter configures the per-subqueue fairness pass counter (upstream
	// matching.fairnessCounter, default counter.DefaultCounterParams()): an exact map for up to
	// MapLimit (100) keys, then a count-min sketch (W=100, D=5, growth up to W=10000, shadow-row
	// reseed every 250000 increments). The zero value means the default; a non-zero value is
	// used as given (so e.g. Grow.Threshold=0 disables growth like upstream).
	FairnessCounter counter.CounterParams
	// RangeSize is the task ID block size per rangeID (100000).
	RangeSize int64

	// DeliveryLeaseDuration is how long a polled task stays leased before it is redelivered
	// if not acked. Library-specific (upstream hands tasks to history instead), default 1m.
	DeliveryLeaseDuration time.Duration

	// Rate limiting (upstream ratelimit_manager.go inputs). The effective per-partition rate
	// is min(system, user) where system = min(namespace admin rate, namespace+queue admin rate)
	// and user = API rate (QueueConfig.RateLimit) else the last poller's MaxTasksPerSecond,
	// times RateLimitFraction / NumReadPartitions. QueueConfig.Admin* override the two admin
	// rates per queue at runtime (upstream: dynamic config subscriptions).

	// AdminNamespaceToPartitionDispatchRate is the max rps of any queue partition in a namespace
	// (admin.matchingNamespaceToPartitionDispatchRate, 10000). "Namespace" is
	// store.QueueKey.Namespace; queues sharing it share this default but not a budget (upstream
	// applies it per partition too).
	AdminNamespaceToPartitionDispatchRate float64
	// AdminNamespaceTaskQueueToPartitionDispatchRate is the max rps of one queue partition
	// (admin.matchingNamespaceTaskqueueToPartitionDispatchRate, 1000).
	AdminNamespaceTaskQueueToPartitionDispatchRate float64
	// NumReadPartitions divides user-set rates between partitions
	// (matching.numTaskqueueReadPartitions). Deviation: default 1, not upstream's 4, because
	// the library is single-partition (partitions are not implemented).
	NumReadPartitions int
	// RateLimitFraction scales user-set rates for a queue (upstream
	// TaskQueueRateLimitFractionProvider); the result is clamped to [0, 1]. nil means 1.
	RateLimitFraction func(queue store.QueueKey) float64
	// FairnessKeyRateLimitCacheSize is the LRU size of per-fairness-key limiter state
	// (matching.fairnessKeyRateLimitCacheSize, 2000).
	FairnessKeyRateLimitCacheSize int
	// ApplySystemRateLimitOnLoad, if true, enforces the system (admin) rate as soon as a queue
	// loads. Upstream (and the default, false) computes it at load but only installs a limit
	// when the effective rate later changes, so a queue with no API/poller rate and default
	// admin rates is unlimited rather than capped at 1000/s.
	ApplySystemRateLimitOnLoad bool

	// MetricsQueueTagLimit caps the number of distinct queues (namespace + name) whose metrics
	// carry their real name in the "taskqueue" tag (default 1000). Queues beyond the cap, or all
	// queues when the value is negative, are tagged "__omitted__" and emit no per-queue gauges
	// (backlog count and age), like upstream with metrics.breakdownByTaskQueue=false. The cap is
	// per Engine and first come, first served; a queue keeps its tag for the Engine's lifetime.
	// Fairness keys are never used as tags.
	MetricsQueueTagLimit int
	// BacklogMetricsEmitInterval is how often a loaded queue emits approximate_backlog_count and
	// approximate_backlog_age_seconds from its live stats (matching.backlogMetricsEmitInterval,
	// 1m, 5% jitter); the ack-level sync then emits the physical_* variants, as upstream. A
	// negative value disables the periodic emission, and the sync emits the unprefixed names
	// instead (upstream's interval 0).
	BacklogMetricsEmitInterval time.Duration

	// MaxAttempts is the default number of deliveries a task gets before it is dead-lettered
	// (library addition; QueueConfig.MaxAttempts overrides it per queue). 0, the default,
	// means unlimited: tasks are redelivered forever, as before. When a queue's limit is
	// positive, every backlog delivery also records its attempt count in the store before
	// PollTask returns (one extra write), so the count survives crashes. Needs a store that
	// implements store.DeadLetterStore; with any other store the limit is ignored.
	MaxAttempts int32

	// MaxTaskAge, if positive, caps every new task's ExpiryTime at AddTask time + MaxTaskAge
	// (library addition; QueueConfig.MaxTaskAge overrides it per queue). The cap is applied
	// by AddTask, so it covers tasks added after it is set; expired tasks are then dropped
	// like any task past its ExpiryTime. 0, the default, adds no cap.
	MaxTaskAge time.Duration

	// RetryBackoff, if Initial is positive, delays redelivery of a nacked or expired backlog
	// task by an exponential backoff on its attempt count (library addition). A Nack with
	// NackDelay overrides it. The zero value, the default, redelivers immediately as before.
	// The delay is written to the task row (VisibleAt) when the store implements
	// store.DeadLetterStore, so it survives a reload; otherwise it applies in memory only.
	RetryBackoff RetryBackoff

	// MaxDelayedInMemory is how many delayed tasks (Task.VisibleAt in the future) one
	// subqueue reader holds without counting them toward GetTasksBatchSize (library
	// addition). Past it they count as loaded, so a backlog with more delayed tasks than
	// this plus GetTasksBatchSize ahead of ready ones delays those ready ones too.
	// Default 10000.
	MaxDelayedInMemory int
}

// DefaultConfig returns upstream defaults.
func DefaultConfig() Config {
	return Config{
		LongPollExpirationInterval:      time.Minute,
		MaxDelayedInMemory:              10000,
		MaxTaskQueueIdleTime:            5 * time.Minute,
		GetTasksBatchSize:               1000,
		GetTasksReloadAt:                100,
		UpdateAckInterval:               time.Minute,
		MetadataUpdateOnAppendInterval:  5 * time.Second,
		OutstandingTaskAppendsThreshold: 250,
		MaxTaskBatchSize:                100,
		MaxTaskDeleteBatchSize:          100,
		TaskDeleteInterval:              15 * time.Second,
		BacklogNegligibleAge:            5 * time.Second,
		MaxWaitForPollerBeforeFwd:       200 * time.Millisecond,
		BacklogTaskForwardTimeout:       time.Minute,
		PriorityLevels:                  5,
		FairnessPassDither:              false,
		FairnessCounter:                 counter.DefaultCounterParams(),
		RangeSize:                       100000,
		DeliveryLeaseDuration:           time.Minute,

		AdminNamespaceToPartitionDispatchRate:          10000,
		AdminNamespaceTaskQueueToPartitionDispatchRate: 1000,
		NumReadPartitions:                              1,
		FairnessKeyRateLimitCacheSize:                  2000,
		MetricsQueueTagLimit:                           1000,
		BacklogMetricsEmitInterval:                     time.Minute,
	}
}

// WithDefaults fills zero fields from DefaultConfig.
func (c Config) WithDefaults() Config {
	d := DefaultConfig()
	setD := func(v *time.Duration, def time.Duration) {
		if *v == 0 {
			*v = def
		}
	}
	setI := func(v *int, def int) {
		if *v == 0 {
			*v = def
		}
	}
	setD(&c.LongPollExpirationInterval, d.LongPollExpirationInterval)
	setD(&c.MaxTaskQueueIdleTime, d.MaxTaskQueueIdleTime)
	setI(&c.GetTasksBatchSize, d.GetTasksBatchSize)
	setI(&c.GetTasksReloadAt, d.GetTasksReloadAt)
	setI(&c.MaxDelayedInMemory, d.MaxDelayedInMemory)
	setD(&c.UpdateAckInterval, d.UpdateAckInterval)
	setD(&c.MetadataUpdateOnAppendInterval, d.MetadataUpdateOnAppendInterval)
	setI(&c.OutstandingTaskAppendsThreshold, d.OutstandingTaskAppendsThreshold)
	setI(&c.MaxTaskBatchSize, d.MaxTaskBatchSize)
	setI(&c.MaxTaskDeleteBatchSize, d.MaxTaskDeleteBatchSize)
	setD(&c.TaskDeleteInterval, d.TaskDeleteInterval)
	setD(&c.BacklogNegligibleAge, d.BacklogNegligibleAge)
	setD(&c.MaxWaitForPollerBeforeFwd, d.MaxWaitForPollerBeforeFwd)
	setD(&c.BacklogTaskForwardTimeout, d.BacklogTaskForwardTimeout)
	setI(&c.PriorityLevels, d.PriorityLevels)
	if c.FairnessCounter == (counter.CounterParams{}) {
		c.FairnessCounter = d.FairnessCounter
	}
	if c.RangeSize == 0 {
		c.RangeSize = d.RangeSize
	}
	setD(&c.DeliveryLeaseDuration, d.DeliveryLeaseDuration)
	if c.AdminNamespaceToPartitionDispatchRate == 0 {
		c.AdminNamespaceToPartitionDispatchRate = d.AdminNamespaceToPartitionDispatchRate
	}
	if c.AdminNamespaceTaskQueueToPartitionDispatchRate == 0 {
		c.AdminNamespaceTaskQueueToPartitionDispatchRate = d.AdminNamespaceTaskQueueToPartitionDispatchRate
	}
	setI(&c.NumReadPartitions, d.NumReadPartitions)
	setI(&c.FairnessKeyRateLimitCacheSize, d.FairnessKeyRateLimitCacheSize)
	setI(&c.MetricsQueueTagLimit, d.MetricsQueueTagLimit)
	setD(&c.BacklogMetricsEmitInterval, d.BacklogMetricsEmitInterval)
	return c
}

// validate rejects settings the backlog reader cannot run with (REVIEW L7). It runs after
// WithDefaults, so zero fields have their defaults.
func (c Config) validate() error {
	if c.GetTasksBatchSize < 1 {
		return &store.InvalidArgumentError{Msg: fmt.Sprintf("GetTasksBatchSize must be positive, got %d", c.GetTasksBatchSize)}
	}
	if b := c.RetryBackoff; b.Initial < 0 || b.Max < 0 || math.IsNaN(b.Multiplier) || math.IsInf(b.Multiplier, 0) || (b.Multiplier != 0 && b.Multiplier < 1) {
		return &store.InvalidArgumentError{Msg: fmt.Sprintf("RetryBackoff needs non-negative durations and Multiplier 0 or >= 1, got %+v", b)}
	}
	if c.MaxDelayedInMemory < 0 {
		return &store.InvalidArgumentError{Msg: fmt.Sprintf("MaxDelayedInMemory must be non-negative, got %d", c.MaxDelayedInMemory)}
	}
	if c.MaxTaskAge < 0 {
		return &store.InvalidArgumentError{Msg: fmt.Sprintf("MaxTaskAge must be non-negative, got %v", c.MaxTaskAge)}
	}
	if c.MaxAttempts < 0 {
		return &store.InvalidArgumentError{Msg: fmt.Sprintf("MaxAttempts must be non-negative, got %d", c.MaxAttempts)}
	}
	if c.GetTasksReloadAt < 0 || c.GetTasksReloadAt >= c.GetTasksBatchSize {
		return &store.InvalidArgumentError{Msg: fmt.Sprintf(
			"GetTasksReloadAt must be in [0, GetTasksBatchSize=%d), got %d", c.GetTasksBatchSize, c.GetTasksReloadAt)}
	}
	return nil
}

// taskQueueConfig is the per-queue resolved config (upstream config.go taskQueueConfig).
type taskQueueConfig struct {
	Config
	PriorityLevels     priorityKey
	DefaultPriorityKey priorityKey
	// BreakdownMetricsByTaskQueue is true when this queue's metrics carry its real taskqueue
	// tag (upstream BreakdownMetricsByTaskQueue && BreakdownMetricsByPartition). Per-queue
	// gauges are emitted only then.
	BreakdownMetricsByTaskQueue bool
}

func newTaskQueueConfig(c Config) *taskQueueConfig {
	priorityLevels := priorityKey(c.PriorityLevels)
	// Deviation: upstream (config.go:403-405) computes max(p, min(p, maxPriorityLevels), 1),
	// which is just max(p, 1), so the 60-level cap is ineffective. We apply the intended clamp.
	priorityLevels = max(min(priorityLevels, maxPriorityLevels), 1)
	return &taskQueueConfig{
		Config:             c,
		PriorityLevels:     priorityLevels,
		DefaultPriorityKey: (priorityLevels + 1) / 2,
	}
}

// Ported from config.go clipPriority.
func (c *taskQueueConfig) clipPriority(priority priorityKey) priorityKey {
	if priority == 0 {
		priority = c.DefaultPriorityKey
	}
	priority = max(priority, 1)
	priority = min(priority, c.PriorityLevels)
	return priority
}

// Ported from config.go setDefaultPriority.
func (c *taskQueueConfig) setDefaultPriority(task *internalTask) {
	if task.effectivePriority == 0 {
		task.effectivePriority = effectivePriorityFactor * c.DefaultPriorityKey
	}
}
