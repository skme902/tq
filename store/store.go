// Adapted from temporalio/temporal v1.32.1 af08a5e5, common/persistence/data_interfaces.go
// (TaskManager / FairTaskManager and their request types, fair (v2) subset only).
// Copyright (c) 2025 Temporal Technologies Inc. MIT License; see UPSTREAM_LICENSE and NOTICE.

// Package store defines the persistence contract for the queue. It mirrors the subset of
// upstream Temporal's FairTaskManager that the matching fair path uses: a queue metadata row
// fenced by a rangeID (compare-and-swap), and tasks ordered by (pass, id) within a subqueue.
package store

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// UnknownNumRowsAffected is returned by CompleteTasksLessThan when the store cannot report how
// many rows were deleted. Callers treat it as "everything below the bound was deleted".
const UnknownNumRowsAffected = -1

type (
	// QueueKey identifies one physical queue (one partition of a named queue).
	QueueKey struct {
		Namespace string
		Name      string
		Partition int
	}

	// Level is a position in a fair subqueue: tasks are ordered by Pass, then ID.
	Level struct {
		Pass int64
		ID   int64
	}

	// Priority mirrors upstream commonpb.Priority.
	Priority struct {
		// PriorityKey: lower is dispatched first. 0 means the queue default; keys above the
		// queue's PriorityLevels are dispatched as the lowest level. Negative keys are rejected.
		PriorityKey int32
		// FairnessKey groups tasks for fair dispatch within a priority level.
		FairnessKey string
		// FairnessWeight scales the key's share; <= 0 means 1.0.
		FairnessWeight float32
	}

	// Task is the user payload plus scheduling attributes (replaces persistencespb.TaskInfo).
	Task struct {
		Payload    []byte
		Priority   Priority
		CreateTime time.Time
		// ExpiryTime: zero means never expires.
		ExpiryTime time.Time
		// Attempts is the number of deliveries of this task that have started (set by the
		// queue; AddTask ignores it). See Delivery.Task.Attempts and Config.MaxAttempts.
		Attempts int32 `json:",omitzero"`
		// VisibleAt, if in the future, keeps the task from being delivered before then (library
		// addition: delayed enqueue and Nack backoff). Zero or past means deliverable now.
		VisibleAt time.Time `json:",omitzero"`
	}

	// TaskInfo is an allocated (persisted) task (replaces persistencespb.AllocatedTaskInfo).
	TaskInfo struct {
		ID   int64
		Pass int64
		Data Task
	}

	// SubqueueKey identifies a subqueue within a physical queue.
	SubqueueKey struct {
		Priority int32
	}

	// FairnessKeyCount is a persisted top-K fairness counter entry.
	FairnessKeyCount struct {
		Key   string
		Count int64
	}

	// SubqueueInfo is the persisted state of one subqueue.
	SubqueueInfo struct {
		Key                     SubqueueKey
		FairAckLevel            Level
		FairMaxReadLevel        Level
		ApproximateBacklogCount int64
		TopKFairnessCounts      []FairnessKeyCount
	}

	// QueueInfo is the queue metadata row (replaces persistencespb.TaskQueueInfo).
	QueueInfo struct {
		Key            QueueKey
		Subqueues      []SubqueueInfo
		LastUpdateTime time.Time
		// ExpiryTime: zero means the row does not expire.
		ExpiryTime time.Time
	}

	CreateQueueRequest struct {
		RangeID int64
		Info    *QueueInfo
	}

	GetQueueResponse struct {
		RangeID int64
		Info    *QueueInfo
	}

	UpdateQueueRequest struct {
		RangeID     int64
		PrevRangeID int64
		Info        *QueueInfo
	}

	DeleteQueueRequest struct {
		Key     QueueKey
		RangeID int64
	}

	CreateTasksRequest struct {
		// RangeID must equal the stored rangeID or the whole request fails with
		// *ConditionFailedError.
		RangeID int64
		Info    *QueueInfo
		Tasks   []TaskInfo
		// Subqueues, if non-nil, has the same length as Tasks and holds each task's subqueue
		// index. nil means subqueue 0 for all tasks.
		Subqueues []int
		// UpdateMetadata asks the store to also write Info (still conditioned on RangeID).
		UpdateMetadata bool
	}

	CreateTasksResponse struct {
		// UpdatedMetadata reports whether Info was written along with the tasks.
		UpdatedMetadata bool
	}

	GetTasksRequest struct {
		Key      QueueKey
		Subqueue int
		// Return tasks >= InclusiveMin in (Pass, ID) order with no upper bound.
		// InclusiveMin.Pass must be >= 1.
		InclusiveMin Level
		PageSize     int
	}

	GetTasksResponse struct {
		Tasks []TaskInfo
	}

	CompleteTasksLessThanRequest struct {
		Key      QueueKey
		Subqueue int
		// Delete tasks < ExclusiveMax in (Pass, ID) order.
		ExclusiveMax Level
		// Limit is an upper bound on rows deleted; stores may ignore it.
		Limit int
		// ConditionRangeID, if non-zero, makes the delete conditional on the stored rangeID;
		// a mismatch returns *ConditionFailedError and deletes nothing.
		ConditionRangeID int64
	}

	// Store is the persistence interface (replaces upstream persistence.FairTaskManager).
	//
	// All methods must be safe for concurrent use. Conditional operations must be atomic with
	// respect to each other for the same QueueKey.
	Store interface {
		// CreateQueue creates the queue row. Returns *ConditionFailedError if it exists.
		CreateQueue(ctx context.Context, req *CreateQueueRequest) error
		// GetQueue returns the queue row or *NotFoundError.
		GetQueue(ctx context.Context, key QueueKey) (*GetQueueResponse, error)
		// UpdateQueue writes the row if the stored rangeID == PrevRangeID, setting it to RangeID.
		UpdateQueue(ctx context.Context, req *UpdateQueueRequest) error
		// DeleteQueue deletes the row and its tasks (and dead tasks, for a DeadLetterStore) if
		// the stored rangeID == RangeID.
		DeleteQueue(ctx context.Context, req *DeleteQueueRequest) error
		// CreateTasks writes all tasks or none.
		CreateTasks(ctx context.Context, req *CreateTasksRequest) (*CreateTasksResponse, error)
		// GetTasks reads tasks in (Pass, ID) order from one subqueue.
		GetTasks(ctx context.Context, req *GetTasksRequest) (*GetTasksResponse, error)
		// CompleteTasksLessThan deletes tasks below a level in one subqueue and returns the number
		// deleted, or UnknownNumRowsAffected.
		CompleteTasksLessThan(ctx context.Context, req *CompleteTasksLessThanRequest) (int, error)
	}
)

// ListQueuesRequest pages through every persisted queue (upstream
// persistence.TaskManager.ListTaskQueue). PageSize must be positive; PageToken is nil for the
// first page and otherwise the NextPageToken of the previous page.
type ListQueuesRequest struct {
	PageSize  int
	PageToken []byte
}

// ListedQueue is one persisted queue row.
type ListedQueue struct {
	RangeID int64
	Info    *QueueInfo
}

// ListQueuesResponse is one page. NextPageToken is nil when there are no more pages. The order
// is implementation-defined but stable, and every queue that exists for the whole listing is
// returned exactly once.
type ListQueuesResponse struct {
	Queues        []ListedQueue
	NextPageToken []byte
}

// QueueLister is implemented by stores that can enumerate their queues without loading or
// taking ownership of them. memstore, sqlite and postgres implement it; it is a separate
// interface so custom Store implementations written before it existed keep compiling.
type QueueLister interface {
	ListQueues(ctx context.Context, req *ListQueuesRequest) (*ListQueuesResponse, error)
}

// DeadTask is a dead-lettered task: a task whose deliveries reached its queue's max attempts.
type DeadTask struct {
	// Task is the task as it was dead-lettered (ID and Pass from the backlog; Data.Attempts is
	// the number of deliveries it had).
	Task     TaskInfo
	Reason   string
	DeadTime time.Time
}

// UpdateTaskRequest rewrites the Data of one existing backlog task in place, keeping its
// subqueue, Pass and ID (so its position in fair order does not change).
type UpdateTaskRequest struct {
	Key      QueueKey
	RangeID  int64 // fence: *ConditionFailedError if the stored rangeID differs
	Subqueue int
	Task     TaskInfo
}

// DeadLetterTaskRequest records Task in the queue's dead-letter set. It does not remove the
// backlog row: the engine finishes the task like an ack, and ack-level GC deletes the row.
// A crash after this write and before that ack level is persisted leaves the task both live
// and dead (at-least-once). A backlog row that is already gone is not an error. Recording
// the same task ID again overwrites the dead entry, so a retry does not duplicate it.
type DeadLetterTaskRequest struct {
	Key      QueueKey
	RangeID  int64 // fence: *ConditionFailedError if the stored rangeID differs
	Subqueue int
	Task     TaskInfo
	Reason   string
	DeadTime time.Time
}

// ListDeadTasksRequest pages through a queue's dead tasks in task ID order. PageSize must be
// positive; PageToken is nil for the first page.
type ListDeadTasksRequest struct {
	Key       QueueKey
	PageSize  int
	PageToken []byte
}

// ListDeadTasksResponse is one page; NextPageToken is nil after the last page.
type ListDeadTasksResponse struct {
	Tasks         []DeadTask
	NextPageToken []byte
}

// DeleteDeadTasksRequest deletes dead tasks: those in TaskIDs if it is non-nil, otherwise
// those with DeadTime before DeadBefore (which must then be non-zero).
type DeleteDeadTasksRequest struct {
	Key        QueueKey
	TaskIDs    []int64
	DeadBefore time.Time
}

// StoreHealth is a HealthChecker's report.
type StoreHealth struct {
	// SchemaVersion is the highest migration applied (0 if none or not applicable).
	SchemaVersion int
	// WantSchemaVersion is the highest migration this build embeds (0 if not applicable).
	WantSchemaVersion int
}

// HealthChecker is implemented by stores that can be probed for readiness (memstore, sqlite
// and postgres do). CheckHealth must be cheap: a ping and a small read.
type HealthChecker interface {
	CheckHealth(ctx context.Context) (StoreHealth, error)
}

// DeleteTaskRequest deletes one backlog task, identified by its full key, under the rangeID
// fence.
type DeleteTaskRequest struct {
	Key      QueueKey
	RangeID  int64 // fence: *ConditionFailedError if the stored rangeID differs
	Subqueue int
	Pass     int64
	ID       int64
}

// TaskAdminStore is implemented by stores that can delete a single backlog task (memstore,
// sqlite and postgres do). Engine.DeleteTask needs it.
type TaskAdminStore interface {
	// DeleteTask deletes the task and reports whether it existed.
	DeleteTask(ctx context.Context, req *DeleteTaskRequest) (bool, error)
}

// DeadLetterStore is implemented by stores that support max attempts and dead-lettering
// (memstore, sqlite and postgres do). The dead-letter set belongs to the queue: DeleteQueue
// removes it too. Dead-letter reads and deletes are not fenced, since no engine owns them.
type DeadLetterStore interface {
	UpdateTask(ctx context.Context, req *UpdateTaskRequest) error
	DeadLetterTask(ctx context.Context, req *DeadLetterTaskRequest) error
	ListDeadTasks(ctx context.Context, req *ListDeadTasksRequest) (*ListDeadTasksResponse, error)
	DeleteDeadTasks(ctx context.Context, req *DeleteDeadTasksRequest) (int, error)
	CountDeadTasks(ctx context.Context, key QueueKey) (int64, error)
}

// DeleteQueueIfIdleRequest asks to delete a queue only if nobody took it over since it was
// listed and it holds no work.
type DeleteQueueIfIdleRequest struct {
	Key QueueKey
	// RangeID must equal the stored rangeID (the value ListQueues returned).
	RangeID int64
	// IdleBefore, if non-zero, also requires the stored QueueInfo.LastUpdateTime to be
	// before it, checked in the same transaction (so an owner that wrote after the listing
	// keeps its queue).
	IdleBefore time.Time
	// Now, if non-zero, makes a task above the ack level count as live only while it has not
	// expired (ExpiryTime unset or not before Now, the reader's rule), so a queue holding only
	// expired tasks is deleted with them. Zero: every row above the ack level is live.
	Now time.Time
}

// QueueSweeper is implemented by stores that can delete idle queues (memstore, sqlite and
// postgres do).
type QueueSweeper interface {
	// DeleteQueueIfIdle atomically deletes the queue row and its task rows if the stored
	// rangeID equals req.RangeID, the row is idle per req.IdleBefore, no live task row lies
	// above its subqueue's FairAckLevel (rows at or below it are already acked and only wait
	// for GC; with req.Now set, an expired row is not live and is deleted with the queue), and
	// the queue has no dead tasks. It reports whether it deleted the queue; a missing queue, a
	// rangeID mismatch or live work return (false, nil). A task row that cannot be decoded
	// while checking expiry returns an InvalidArgumentError naming it, and the queue is kept.
	// The expiry check reads every row above the ack level inside the delete transaction.
	DeleteQueueIfIdle(ctx context.Context, req *DeleteQueueIfIdleRequest) (bool, error)
}

// Less reports whether a < b lexicographically by (Pass, ID).
func (a Level) Less(b Level) bool {
	return a.Pass < b.Pass || a.Pass == b.Pass && a.ID < b.ID
}

func (a Level) String() string { return fmt.Sprintf("<%d,%d>", a.Pass, a.ID) }

func (k QueueKey) String() string {
	return fmt.Sprintf("%s/%s#%d", k.Namespace, k.Name, k.Partition)
}

// ConditionFailedError is returned when a rangeID condition does not hold (lost ownership).
type ConditionFailedError struct {
	Msg string
}

func (e *ConditionFailedError) Error() string { return "condition failed: " + e.Msg }

// NotFoundError is returned when a queue row does not exist.
type NotFoundError struct {
	Msg string
}

func (e *NotFoundError) Error() string { return "not found: " + e.Msg }

// InvalidArgumentError is returned for malformed requests.
type InvalidArgumentError struct {
	Msg string
}

func (e *InvalidArgumentError) Error() string { return "invalid argument: " + e.Msg }

// TransientError marks an error as retryable by the queue's persistence retry loops.
// Stores should wrap timeouts/unavailability in it (or return context errors).
type TransientError struct {
	Err error
}

func (e *TransientError) Error() string { return "transient: " + e.Err.Error() }
func (e *TransientError) Unwrap() error { return e.Err }

// ResourceExhaustedError signals throttling; readers back off longer on it.
type ResourceExhaustedError struct {
	Msg string
	// DefinitelyNotWritten is true when the store rejected the write before applying it.
	DefinitelyNotWritten bool
}

func (e *ResourceExhaustedError) Error() string { return "resource exhausted: " + e.Msg }

// IsConditionFailed reports whether err is (or wraps) a *ConditionFailedError.
func IsConditionFailed(err error) bool {
	var c *ConditionFailedError
	return errors.As(err, &c)
}

// IsNotFound reports whether err is (or wraps) a *NotFoundError.
func IsNotFound(err error) bool {
	var c *NotFoundError
	return errors.As(err, &c)
}

// IsTransient reports whether err is a retryable persistence error.
func IsTransient(err error) bool {
	var t *TransientError
	var r *ResourceExhaustedError
	return errors.As(err, &t) || errors.As(err, &r)
}

// IsResourceExhausted reports whether err is (or wraps) a *ResourceExhaustedError.
func IsResourceExhausted(err error) bool {
	var r *ResourceExhaustedError
	return errors.As(err, &r)
}
