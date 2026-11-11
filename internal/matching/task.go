// Ported from temporalio/temporal v1.32.1 af08a5e5, service/matching/task.go
// Query, nexus, started (poll-forwarded) and versioning fields are removed; forwardInfo is kept
// as a placeholder for partition forwarding (not implemented).
// Copyright (c) 2025 Temporal Technologies Inc. MIT License; see UPSTREAM_LICENSE and NOTICE.

package matching

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/skme902/tq/store"
)

// priorityKey is the (effective) priority; lower is dispatched first.
type priorityKey int32

// taskSource replaces enumsspb.TaskSource.
type taskSource int

const (
	taskSourceHistory   taskSource = iota + 1 // added via AddTask (sync match path)
	taskSourceDBBacklog                       // read from the store
)

// syncMatchTaskId is the fake task ID of a not-yet-persisted task (physical_task_queue_manager.go).
const syncMatchTaskId = -137

// dropReason replaces upstream's dropReason metric tag.
type dropReason string

const (
	dropReasonNone          dropReason = ""
	dropReasonExpiredMemory dropReason = "expired_memory"
	dropReasonExpiredRead   dropReason = "expired_read"
	dropReasonInvalid       dropReason = "invalid"
	dropReasonStarter       dropReason = "starter"
)

type (
	// forwardInfo replaces taskqueuespb.TaskForwardInfo (unused: no forwarding).
	forwardInfo struct {
		sourcePartition int
		taskSource      taskSource
	}

	// genericTaskInfo contains the info for a task that's locally generated
	genericTaskInfo struct {
		*store.TaskInfo
		completionFunc func(*internalTask, taskResponse)
		// leaseFunc, if set, is called when a backlog task is handed out under a delivery lease
		// (library addition; see fairTaskReader.leaseTask).
		leaseFunc func(*internalTask)
		// subqueue is the backlog subqueue the task was read from (backlog tasks only).
		subqueue subqueueIndex
	}

	// internalTask represents a task from AddTask (sync match) or from the backlog.
	internalTask struct {
		event       *genericTaskInfo
		source      taskSource
		responseC   chan taskResponse // non-nil only where there is a caller waiting for response (sync match)
		forwardInfo *forwardInfo
		// queued is true while this task is in matcherData.tasks.
		queued            bool
		recycleToken      func(*internalTask)
		removeFromMatcher atomic.Pointer[func()]

		// The following fields are for use by priMatcher/matcherData:
		waitableMatchResult
		forwardCtx context.Context // non-nil for sync match task only
		// effectivePriority is initialized from an explicit task priority if present, or the
		// default for the task queue. The scale of effectivePriority is 10× the normal scale to
		// allow inserting forwards in between priority levels.
		effectivePriority priorityKey
		pollForwarderType pollForwarderType

		// leased is true while a backlog task is held under a delivery lease and tracked by the
		// reader as leased rather than loaded. Guarded by the owning fairTaskReader's lock.
		leased bool
	}

	// taskResponse is used to report the result of either a match with a local poller,
	// or forwarding a task.
	taskResponse struct {
		forwarded  bool
		forwardErr error
		startErr   error
		dropReason dropReason
	}

	// taskFinishResult describes how a task finished. It is passed to internalTask.finish.
	taskFinishResult struct {
		err           error
		consumedToken bool
		dropReason    dropReason
	}
)

var (
	// sentinel values for task.removeFromMatcher
	removeFuncNotAddedYet = func() {}
	removeFuncEvicted     = func() {}
)

func (res taskResponse) err() error {
	if res.forwarded {
		return res.forwardErr
	}
	return res.startErr
}

func newInternalTaskForSyncMatch(info *store.Task, fwd *forwardInfo) *internalTask {
	source := taskSourceHistory
	if fwd != nil {
		source = fwd.taskSource
	}
	return &internalTask{
		event: &genericTaskInfo{
			TaskInfo: &store.TaskInfo{
				Data: *info,
				ID:   syncMatchTaskId,
			},
		},
		forwardInfo:       fwd,
		source:            source,
		responseC:         make(chan taskResponse, 1),
		effectivePriority: effectivePriorityFactor * priorityKey(info.Priority.PriorityKey),
	}
}

func newInternalTaskFromBacklog(
	info *store.TaskInfo,
	completionFunc func(*internalTask, taskResponse),
) *internalTask {
	return &internalTask{
		event: &genericTaskInfo{
			TaskInfo:       info,
			completionFunc: completionFunc,
		},
		source:            taskSourceDBBacklog,
		effectivePriority: effectivePriorityFactor * priorityKey(info.Data.Priority.PriorityKey),
	}
}

func newPollForwarderTask(p priorityKey, t pollForwarderType) *internalTask {
	return &internalTask{effectivePriority: p, pollForwarderType: t}
}

func (task *internalTask) isPollForwarder() bool {
	return task.pollForwarderType != notPollForwarder
}

// isForwarded returns true if the task was forwarded from a child partition.
func (task *internalTask) isForwarded() bool {
	return task.forwardInfo != nil
}

func (task *internalTask) isSyncMatchTask() bool {
	return task.responseC != nil
}

// getResponse waits for a response on the task's response channel.
func (task *internalTask) getResponse() (taskResponse, bool) {
	if task.responseC == nil {
		return taskResponse{}, false
	}
	return <-task.responseC, true
}

func (task *internalTask) getPriority() store.Priority {
	if task.event != nil {
		return task.event.Data.Priority
	}
	return store.Priority{}
}

func (task *internalTask) fairLevel() fairLevel {
	return fairLevelFromAllocatedTask(task.event.TaskInfo)
}

// resetMatcherState must be called before adding or re-adding a backlog task to priMatcher.
func (task *internalTask) resetMatcherState() {
	task.removeFromMatcher.Store(&removeFuncNotAddedYet)
}

// setRemoveFunc sets the function to remove the task from the matcher.
// It returns true if the task is still valid and the function was set,
// false if the task was evicted already and should not be added.
func (task *internalTask) setRemoveFunc(remove func()) bool {
	return task.removeFromMatcher.CompareAndSwap(&removeFuncNotAddedYet, &remove)
}

// setEvicted marks the task as evicted. If it was added to a matcher it will be removed.
func (task *internalTask) setEvicted() {
	remove := task.removeFromMatcher.Swap(&removeFuncEvicted)
	(*remove)()
}

// finish marks a task as finished. Must be called after a poller picks up a task
// and marks it as started. If the task is unable to marked as started, then this
// method should be called with a non-nil error argument.
//
// If the task took a rate limit token and didn't "use" it by actually dispatching the task,
// finish will be called with consumedToken=false so the token is given back.
func (task *internalTask) finish(r taskFinishResult) {
	task.finishInternal(taskResponse{
		startErr:   r.err,
		dropReason: r.dropReason,
	}, r.consumedToken)
}

func (task *internalTask) finishInternal(res taskResponse, consumedToken bool) {
	if !consumedToken && task.recycleToken != nil {
		task.recycleToken(task)
	}

	switch {
	case task.responseC != nil:
		task.responseC <- res
	case task.event.completionFunc != nil:
		task.event.completionFunc(task, res)
	}
}

// droppedTaskExpiryReason replaces metrics_util.go getDroppedTaskExpiryReason: a task the
// validator rejects is reported as expired_memory if it has expired, else invalid.
func droppedTaskExpiryReason(task *internalTask, now time.Time) dropReason {
	if isTaskExpired(task.event.TaskInfo, now) {
		return dropReasonExpiredMemory
	}
	return dropReasonInvalid
}

// isTaskExpired replaces task_validation.go IsTaskExpired (uses the injected clock).
func isTaskExpired(t *store.TaskInfo, now time.Time) bool {
	expiry := t.Data.ExpiryTime
	return expiry.Unix() > 0 && expiry.Before(now)
}
