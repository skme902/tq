// Package taskqueue is an embeddable task queue extracted from Temporal's matching service
// (temporalio/temporal v1.32.1): lazily-loaded named queues with long-poll consumers, sync match
// when a consumer is waiting, a durable fair backlog otherwise, priority levels, rangeID-fenced
// ownership, leased deliveries with ack/nack/redelivery, upstream's weighted fairness
// (stride scheduling per fairness key within each priority level) and upstream's rate
// limiting (whole-queue and per-fairness-key next-ready-time limiters).
//
// Stop ends an engine immediately. Shutdown(ctx) drains outstanding leases and in-flight
// polls, then stops; if ctx ends first it stops anyway and returns ctx's error.
//
// See README.md for usage and docs/UPSTREAM_MAP.md for the upstream mapping.
//
// The package name is taskqueue while the module path ends in tq; import it as
// taskqueue "github.com/skme902/tq". The name is kept for API stability.
package taskqueue

import (
	"time"

	"github.com/skme902/tq/internal/counter"
	"github.com/skme902/tq/internal/matching"
	"github.com/skme902/tq/store"
)

type (
	// Engine owns the loaded queues. Create with New; release with Stop, or Shutdown(ctx) to drain first.
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
	// A loaded queue re-reads it only when notified through QueueConfigWatcher.
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
	// QueueDescription is Engine.DescribeQueue's snapshot (upstream DescribeTaskQueue stats).
	QueueDescription = matching.QueueDescription
	// DeadTask is a dead-lettered task (Engine.ListDeadTasks).
	DeadTask = matching.DeadTask
	// RedriveOptions selects the dead tasks Engine.RedriveDeadTasks moves back.
	RedriveOptions = matching.RedriveOptions
	// NackOption configures Delivery.Nack.
	NackOption = matching.NackOption
	// RetryBackoff is Config.RetryBackoff: an exponential delay before redelivering a
	// nacked or expired backlog task.
	RetryBackoff = matching.RetryBackoff
	// SweepOptions configures Engine.SweepIdleQueues; SweepResult is what it did.
	SweepOptions = matching.SweepOptions
	SweepResult  = matching.SweepResult
	// HealthReport is Engine.Health's result.
	HealthReport = matching.HealthReport
	// QueueProblem is one degraded queue in a HealthReport.
	QueueProblem = matching.QueueProblem
	// TaskRef identifies one backlog task (PeekedTask.Ref, Engine.DeleteTask).
	TaskRef = matching.TaskRef
	// PeekOptions selects the tasks Engine.PeekTasks returns.
	PeekOptions = matching.PeekOptions
	// PeekedTask is one backlog task as Engine.PeekTasks sees it.
	PeekedTask = matching.PeekedTask
	// QueueStats are backlog count/age and add/dispatch rates (upstream TaskQueueStats).
	QueueStats = matching.QueueStats
	// QueueListing is one row of Engine.ListQueues.
	QueueListing = matching.QueueListing
	// ListQueuesPage is one page of Engine.ListQueues.
	ListQueuesPage = matching.ListQueuesPage

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
	// ErrEngineStopped is returned after Stop, including by Delivery methods on leases Stop ended.
	ErrEngineStopped = matching.ErrEngineStopped
	// ErrEngineDraining is returned by Engine.PollTask while Engine.Shutdown drains.
	ErrEngineDraining = matching.ErrEngineDraining
	// ErrLeaseLost is returned by Delivery methods after the lease expired or was settled.
	ErrLeaseLost = matching.ErrLeaseLost
	// ErrDropTask may be returned (wrapped) by a Starter to drop a task.
	ErrDropTask = matching.ErrDropTask
	// ErrFairnessOverridesUpdateRejected is returned by
	// MemoryQueueConfigSource.UpdateFairnessWeightOverrides when the merged map is too large.
	ErrFairnessOverridesUpdateRejected = matching.ErrFairnessOverridesUpdateRejected
	// NoopRouter never forwards.
	NoopRouter = matching.NoopRouter
)

// New returns an Engine. Options.Store is required.
func New(opts Options) (*Engine, error) { return matching.NewEngine(opts) }

// DefaultCounterParams returns upstream's fairness counter defaults (Config.FairnessCounter).
// It is a function, not a variable, so callers cannot change the library's defaults.
func DefaultCounterParams() CounterParams { return counter.DefaultCounterParams() }

// DefaultConfig returns upstream defaults.
func DefaultConfig() Config { return matching.DefaultConfig() }

// NewMemoryQueueConfigSource returns an empty MemoryQueueConfigSource; maxOverrides <= 0
// means the upstream default (1000).
func NewMemoryQueueConfigSource(maxOverrides int) *MemoryQueueConfigSource {
	return matching.NewMemoryQueueConfigSource(maxOverrides)
}

// Retryable marks a Starter error as transient (redeliver instead of re-spool).
func Retryable(err error) error { return matching.Retryable(err) }

// NackDelay redelivers the task no earlier than d from now (see Delivery.Nack).
func NackDelay(d time.Duration) NackOption { return matching.NackDelay(d) }

// NackReason records why a consumer gave up on a task; it is kept if the Nack dead-letters it.
func NackReason(reason string) NackOption { return matching.NackReason(reason) }

// MinSweepIdle is the shortest SweepOptions.IdleFor accepted.
const MinSweepIdle = matching.MinSweepIdle

// Dead-letter reasons (DeadTask.Reason prefix, tasks_dead_lettered "reason" tag).
const (
	DeadReasonNack              = matching.DeadReasonNack
	DeadReasonLeaseExpired      = matching.DeadReasonLeaseExpired
	DeadReasonAttemptsExhausted = matching.DeadReasonAttemptsExhausted
)
