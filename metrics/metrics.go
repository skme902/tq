// Package metrics is the minimal metrics surface used by the queue. It replaces upstream
// Temporal's go.temporal.io/server/common/metrics.Handler; plug in Prometheus/OTel by
// implementing Handler.
package metrics

import (
	"slices"
	"strconv"
	"time"
)

// Tag is a metric dimension.
type Tag struct {
	Key, Value string
}

// Handler records metrics. Implementations must be safe for concurrent use.
type Handler interface {
	Counter(name string, delta int64, tags ...Tag)
	Gauge(name string, value float64, tags ...Tag)
	Timer(name string, d time.Duration, tags ...Tag)
}

// TagBinder is optionally implemented by handlers that can pre-bind tags more cheaply than a
// per-call append (upstream metrics.Handler.WithTags). WithTags uses it when present.
type TagBinder interface {
	WithTags(tags ...Tag) Handler
}

// WithTags returns a handler that adds tags to every metric recorded through it, before the
// call's own tags. With no tags it returns h unchanged.
func WithTags(h Handler, tags ...Tag) Handler {
	if len(tags) == 0 {
		return h
	}
	if b, ok := h.(TagBinder); ok {
		return b.WithTags(tags...)
	}
	if t, ok := h.(*tagged); ok {
		return &tagged{h: t.h, tags: slices.Clip(append(slices.Clone(t.tags), tags...))}
	}
	if _, ok := h.(noop); ok {
		return h
	}
	return &tagged{h: h, tags: slices.Clip(slices.Clone(tags))}
}

type tagged struct {
	h    Handler
	tags []Tag
}

func (t *tagged) with(tags []Tag) []Tag {
	if len(tags) == 0 {
		return t.tags
	}
	all := make([]Tag, 0, len(t.tags)+len(tags))
	return append(append(all, t.tags...), tags...)
}

func (t *tagged) Counter(name string, delta int64, tags ...Tag) {
	t.h.Counter(name, delta, t.with(tags)...)
}
func (t *tagged) Gauge(name string, value float64, tags ...Tag) {
	t.h.Gauge(name, value, t.with(tags)...)
}
func (t *tagged) Timer(name string, d time.Duration, tags ...Tag) {
	t.h.Timer(name, d, t.with(tags)...)
}

// Tag keys and special values. They are upstream's (common/metrics/tags.go, task_queues.go).
const (
	// TagNamespace is the queue's QueueKey.Namespace.
	TagNamespace = "namespace"
	// TagTaskQueue is the queue's QueueKey.Name, or OmittedValue when the per-queue breakdown
	// is disabled or its cardinality cap is reached.
	TagTaskQueue = "taskqueue"
	// TagReason is why a task was dead-lettered (library-only; tasks_dead_lettered).
	TagReason = "reason"
	// TagPartition is the queue's QueueKey.Partition as a decimal string.
	TagPartition = "partition"
	// TagTaskPriority is the task's priority key; "" for the default priority (upstream
	// MatchingTaskPriorityTag). Set only on the metrics where upstream sets it.
	TagTaskPriority = "task_priority"
	// TagSource is where a dispatched task came from: SourceHistory (sync match) or
	// SourceDBBacklog (read from the store), as upstream's enumsspb.TaskSource names.
	TagSource = "source"
	// TagForwarded is "true"/"false" (always "false": partitions are not implemented).
	TagForwarded = "forwarded"

	OmittedValue    = "__omitted__"
	SourceHistory   = "History"
	SourceDBBacklog = "DbBacklog"
)

// TaskPriorityTag is upstream MatchingTaskPriorityTag: the priority key, or "" for 0 (default).
func TaskPriorityTag(priorityKey int32) Tag {
	v := ""
	if priorityKey != 0 {
		v = strconv.FormatInt(int64(priorityKey), 10)
	}
	return Tag{Key: TagTaskPriority, Value: v}
}

// Noop discards all metrics.
var Noop Handler = noop{}

type noop struct{}

func (noop) Counter(string, int64, ...Tag)       {}
func (noop) Gauge(string, float64, ...Tag)       {}
func (noop) Timer(string, time.Duration, ...Tag) {}

// Metric names emitted by the queue. Names follow upstream Temporal where one exists.
const (
	TasksAdded              = "tasks_added"
	SyncMatch               = "sync_match"
	TasksSpooled            = "tasks_spooled"
	PollSuccess             = "poll_success"
	PollSuccessWithSync     = "poll_success_sync"
	PollTimeout             = "poll_timeouts"
	TasksDropped            = "tasks_dropped"
	TaskRewrites            = "task_rewrites"
	TaskRetryTransient      = "task_retry_transient"
	TaskCompletedMissing    = "task_completed_missing"
	ConditionFailedErrors   = "condition_failed_errors"
	LeaseRequests           = "lease_requests"
	LeaseFailures           = "lease_failures"
	TaskWriteThrottle       = "task_write_throttle"
	TaskWriteLatency        = "task_write_latency"
	FairReaderStuckDetected = "fair_reader_stuck_detected"
	DeliveryLeaseExpired    = "delivery_lease_expired"
	TaskQueueStarted        = "task_queue_started"
	TaskQueueStopped        = "task_queue_stopped"
	ApproximateBacklogCount = "approximate_backlog_count"
	TasksRateLimited        = "tasks_rate_limited"

	// Dead letters (library-only), per queue. tasks_dead_lettered is tagged reason (nack,
	// lease_expired, attempts_exhausted).
	TasksDeadLettered         = "tasks_dead_lettered"
	DeadLetterFailures        = "dead_letter_failures"
	TasksRedriven             = "tasks_redriven"
	TaskAttemptRecordFailures = "task_attempt_record_failures"

	// Task admin (library-only), per queue.
	TasksAdminDeleted = "tasks_admin_deleted"
	QueuePurges       = "queue_purges"

	// Engine-level (no queue tags), library-only.
	// EngineDraining is 1 while Engine.Shutdown drains and 0 once it stops.
	EngineDraining = "engine_draining"
	// EngineDrainLeases is the number of leases still outstanding when a drain ends (0 for a
	// clean drain); EngineDrainDuration is how long the drain took.
	EngineDrainLeases   = "engine_drain_leases_remaining"
	EngineDrainDuration = "engine_drain_duration"

	// Per-queue gauges (upstream names; approximate_backlog_count is above), tagged
	// task_priority for counts. Emitted only when the queue has a real taskqueue tag (upstream
	// emits them only with metrics.breakdownByTaskQueue and ...ByPartition).
	ApproximateBacklogAgeSeconds = "approximate_backlog_age_seconds"
	// The physical_* variants come from the ack-level sync (stored count, age of the oldest
	// task the reader had in memory at the last ack) when the periodic emission is on.
	PhysicalApproximateBacklogCount      = "physical_approximate_backlog_count"
	PhysicalApproximateBacklogAgeSeconds = "physical_approximate_backlog_age_seconds"
	// PendingPolls is the number of polls waiting in the queue (upstream pending_polls).
	PendingPolls = "pending_polls"

	// Latencies (upstream names). TaskDispatchLatency is create time → handed to a poller,
	// tagged source / forwarded / task_priority; AsyncMatchLatency is the same for backlog
	// tasks only; SyncMatchLatency is the duration of an AddTask that sync-matched.
	TaskDispatchLatency = "task_dispatch_latency"
	AsyncMatchLatency   = "asyncmatch_latency"
	SyncMatchLatency    = "syncmatch_latency"

	// Delivery lease outcomes (library-specific: upstream has no leases).
	DeliveryAcked         = "delivery_acked"
	DeliveryNacked        = "delivery_nacked"
	DeliveryLeaseExtended = "delivery_lease_extended"
	// DeliveryAckLatency is delivery → successful Ack (consumer processing time).
	DeliveryAckLatency = "delivery_ack_latency"
)
