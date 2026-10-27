// Adapted from temporalio/temporal v1.32.1 af08a5e5, service/matching/config.go and
// common/dynamicconfig/constants.go (defaults). Dynamic config is replaced by a plain struct.
// Copyright (c) 2025 Temporal Technologies Inc. MIT License; see UPSTREAM_LICENSE and NOTICE.

package matching

import (
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
	// GetTasksReloadAt triggers a read when fewer tasks than this are loaded (100).
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
	// matching.fairnessCounter, default counter.DefaultCounterParams): an exact map for up to
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
}

// DefaultConfig returns upstream defaults.
func DefaultConfig() Config {
	return Config{
		LongPollExpirationInterval:      time.Minute,
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
		FairnessCounter:                 counter.DefaultCounterParams,
		RangeSize:                       100000,
		DeliveryLeaseDuration:           time.Minute,

		AdminNamespaceToPartitionDispatchRate:          10000,
		AdminNamespaceTaskQueueToPartitionDispatchRate: 1000,
		NumReadPartitions:                              1,
		FairnessKeyRateLimitCacheSize:                  2000,
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
	return c
}

// taskQueueConfig is the per-queue resolved config (upstream config.go taskQueueConfig).
type taskQueueConfig struct {
	Config
	PriorityLevels     priorityKey
	DefaultPriorityKey priorityKey
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
