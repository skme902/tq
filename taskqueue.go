// Package taskqueue is an embeddable task queue extracted from Temporal's matching service
// (temporalio/temporal v1.32.1): lazily-loaded named queues with long-poll consumers, sync match
// when a consumer is waiting, a durable fair backlog otherwise, priority levels, rangeID-fenced
// ownership, leased deliveries with ack/nack/redelivery, upstream's weighted fairness
// (stride scheduling per fairness key within each priority level) and upstream's rate
// limiting (whole-queue and per-fairness-key next-ready-time limiters).
//
// See README.md for usage and docs/UPSTREAM_MAP.md for the upstream mapping.
package taskqueue

import (
	"github.com/skme902/tq/internal/counter"
	"github.com/skme902/tq/internal/matching"
	"github.com/skme902/tq/store"
)

type (
	// Engine owns the loaded queues. Create with New, release with Stop.
	Engine = matching.Engine
	// Options configures an Engine.
	Options = matching.Options
	// Config holds tuning knobs; zero fields take upstream defaults (DefaultConfig).
	Config = matching.Config
	// PollOptions are per-poll options.
	PollOptions = matching.PollOptions
	// Delivery is a leased task returned by Poll; call Ack, Nack or ExtendLease.
	Delivery = matching.Delivery
	// Starter is an optional hook run after a match, before Poll returns.
	Starter = matching.Starter
	// StartRequest is passed to Starter.Start.
	StartRequest = matching.StartRequest
	// Validator optionally expires stale backlog tasks.
	Validator = matching.Validator
	// QueueConfig is per-queue runtime config.
	QueueConfig = matching.QueueConfig
	// QueueConfigSource supplies per-queue config (fairness weight overrides, rate limits).
	QueueConfigSource = matching.QueueConfigSource
	// QueueConfigWatcher is optionally implemented by a QueueConfigSource to apply changes
	// to loaded queues live.
	QueueConfigWatcher = matching.QueueConfigWatcher
	// RateLimitSource says where a queue's effective rate limit comes from (Engine.RateLimit).
	RateLimitSource = matching.RateLimitSource
	// MemoryQueueConfigSource is a mutable in-process QueueConfigSource with upstream's
	// override validation, merge and size-limit semantics.
	MemoryQueueConfigSource = matching.MemoryQueueConfigSource

	// CounterParams configures the fairness pass counter (Config.FairnessCounter).
	CounterParams = counter.CounterParams
	// CMSketchParams configures the count-min sketch used beyond CounterParams.MapLimit keys.
	CMSketchParams = counter.CMSketchParams
	// CMSGrowParams configures count-min sketch growth.
	CMSGrowParams = counter.CMSGrowParams
	// CMSReseedParams configures count-min sketch shadow-row reseeding.
	CMSReseedParams = counter.CMSReseedParams
	// PartitionRouter forwards between partitions (unused until multi-partition support).
	PartitionRouter = matching.PartitionRouter
	// SubqueueStatus is a debugging snapshot of one subqueue.
	SubqueueStatus = matching.SubqueueStatus

	// QueueKey identifies a queue partition.
	QueueKey = store.QueueKey
	// Task is the task payload and scheduling attributes.
	Task = store.Task
	// Priority holds priority key and fairness attributes.
	Priority = store.Priority
)

const (
	RateLimitSourceUnspecified = matching.RateLimitSourceUnspecified
	RateLimitSourceAPI         = matching.RateLimitSourceAPI
	RateLimitSourceWorker      = matching.RateLimitSourceWorker
	RateLimitSourceSystem      = matching.RateLimitSourceSystem
)

var (
	// ErrNoTasks is returned by Engine.PollTask when the long poll expires.
	ErrNoTasks = matching.ErrNoTasks
	// ErrResourceExhausted is returned by AddTask when too many writes are outstanding.
	ErrResourceExhausted = matching.ErrResourceExhausted
	// ErrEngineStopped is returned after Stop.
	ErrEngineStopped = matching.ErrEngineStopped
	// ErrLeaseLost is returned by Delivery methods after the lease expired or was settled.
	ErrLeaseLost = matching.ErrLeaseLost
	// ErrDropTask may be returned (wrapped) by a Starter to drop a task.
	ErrDropTask = matching.ErrDropTask
	// ErrFairnessOverridesUpdateRejected is returned by
	// MemoryQueueConfigSource.UpdateFairnessWeightOverrides when the merged map is too large.
	ErrFairnessOverridesUpdateRejected = matching.ErrFairnessOverridesUpdateRejected
	// DefaultCounterParams are upstream's fairness counter defaults.
	DefaultCounterParams = counter.DefaultCounterParams
	// NoopRouter never forwards.
	NoopRouter = matching.NoopRouter
)

// New returns an Engine. Options.Store is required.
func New(opts Options) (*Engine, error) { return matching.NewEngine(opts) }

// DefaultConfig returns upstream defaults.
func DefaultConfig() Config { return matching.DefaultConfig() }

// NewMemoryQueueConfigSource returns an empty MemoryQueueConfigSource; maxOverrides <= 0
// means the upstream default (1000).
func NewMemoryQueueConfigSource(maxOverrides int) *MemoryQueueConfigSource {
	return matching.NewMemoryQueueConfigSource(maxOverrides)
}

// Retryable marks a Starter error as transient (redeliver instead of re-spool).
func Retryable(err error) error { return matching.Retryable(err) }
