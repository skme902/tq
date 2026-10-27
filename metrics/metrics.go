// Package metrics is the minimal metrics surface used by the queue. It replaces upstream
// Temporal's go.temporal.io/server/common/metrics.Handler; plug in Prometheus/OTel by
// implementing Handler.
package metrics

import "time"

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
)
