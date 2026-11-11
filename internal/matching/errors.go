// Error values adapted from temporalio/temporal v1.32.1 af08a5e5 service/matching
// (task_writer.go, pri_matcher.go, matching_engine.go, task_queue_partition_manager.go).
// Copyright (c) 2025 Temporal Technologies Inc. MIT License; see UPSTREAM_LICENSE and NOTICE.

package matching

import (
	"context"
	"errors"

	"github.com/skme902/tq/store"
)

var (
	// ErrNoTasks is returned by Poll when the long poll expires without a task.
	ErrNoTasks = errors.New("no tasks")
	// ErrResourceExhausted is returned by AddTask when the writer queue is full.
	ErrResourceExhausted = &store.ResourceExhaustedError{Msg: "too many outstanding appends to the task queue"}
	// ErrEngineStopped is returned after Stop, including by Delivery methods on leases Stop ended.
	ErrEngineStopped = errors.New("engine stopped")
	// ErrEngineDraining is returned by PollTask while Engine.Shutdown drains, including to
	// polls that were already waiting when the drain started, unless the caller's own ctx
	// was already done (those keep returning ErrNoTasks).
	ErrEngineDraining = errors.New("engine draining")
	// ErrLeaseLost is returned by Ack/Nack/Extend after the delivery lease expired or was settled.
	ErrLeaseLost = errors.New("delivery lease lost")
	// ErrFairnessOverridesUpdateRejected is returned when a weight override update would
	// exceed the MemoryQueueConfigSource limit (upstream user_data_manager.go).
	ErrFairnessOverridesUpdateRejected = errFairnessOverridesUpdateRejected

	errFairnessOverridesUpdateRejected = &store.InvalidArgumentError{Msg: "fairness weight overrides update rejected: exceeding maximum key size"}

	// upstream task_writer.go: errShutdown is a ConditionFailedError
	errShutdown            = &store.ConditionFailedError{Msg: "task queue shutting down"}
	errNonContiguousBlocks = errors.New("previous block end is not equal to current block")
	errTaskQueueClosed     = errors.New("task queue closed")
	errMatcherStopped      = Retryable(errors.New("matcher stopped"))
	errReprocessTask       = Retryable(errors.New("reprocess task"))
	errInternalMatchError  = errors.New("internal matcher error")
	errLeaseExpired        = Retryable(errors.New("delivery lease expired"))
	errNacked              = Retryable(errors.New("delivery nacked"))
	errLeaseRefused        = Retryable(errors.New("delivery refused: engine stopped"))
	// errAttemptFenced: the attempt write lost the rangeID fence. The delivery must not be
	// handed to the poller; the backlog row is unchanged.
	errAttemptFenced = errors.New("attempt record lost ownership")
)

// isTransientMatchError replaces common.IsServiceClientTransientError ||
// IsContextDeadlineExceededErr || IsContextCanceledErr in fairTaskReader.completeTask.
func isTransientMatchError(err error) bool {
	return IsRetryable(err) || store.IsTransient(err) ||
		errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled)
}

// isPersistenceTransient replaces common.IsPersistenceTransientError.
func isPersistenceTransient(err error) bool {
	return store.IsTransient(err)
}

// isShutdownOrConflict reports errors after which an AddTask may be retried on a reloaded queue.
func isShutdownOrConflict(err error) bool {
	return errors.Is(err, errShutdown) || errors.Is(err, errTaskQueueClosed) || store.IsConditionFailed(err)
}

// physical_task_queue_manager.go: serviceerror.NewCanceled("remote sync match failed")
var errRemoteSyncMatchFailed = Retryable(errors.New("remote sync match failed"))
