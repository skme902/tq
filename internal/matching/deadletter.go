package matching

// Max attempts and dead letters are a library addition with no upstream counterpart (upstream
// matching acks a task once history starts it; history owns retries). See README "Dead
// letters".
//
// A task's Attempts counts deliveries started (PollTask returning it). A delivery that ends
// without Ack (Nack, lease expiry, Stop, a crash) used one attempt. When the queue's max is
// positive:
//   - each backlog delivery writes its new count to the store before PollTask returns, so a
//     task that crashes its consumer is still counted;
//   - a backlog delivery that is Nacked or expires on its last attempt moves the task to the
//     dead-letter set instead of redelivering it;
//   - a task matched with Attempts >= max (read back after a crash, a re-spooled sync task, a
//     lowered max) is dead-lettered without being delivered.
// Dead-lettering is fenced by the queue's rangeID like every backlog write. If it fails the
// task is delivered (or redelivered) as if there were no limit, so a store fault never loses
// a task.

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/skme902/tq/internal/backoff"
	"github.com/skme902/tq/metrics"
	"github.com/skme902/tq/store"
)

// Dead-letter reasons (DeadTask.Reason prefix and the tasks_dead_lettered "reason" tag).
const (
	DeadReasonNack              = "nack"
	DeadReasonLeaseExpired      = "lease_expired"
	DeadReasonAttemptsExhausted = "attempts_exhausted"
)

var errDeadLetterUnsupported = &store.InvalidArgumentError{Msg: "store does not implement store.DeadLetterStore"}

// maxAttempts is the queue's effective limit, 0 when unlimited or the store cannot dead-letter.
func (pqm *physicalTaskQueueManagerImpl) maxAttempts() int32 {
	if pqm.partitionMgr.engine.deadLetters == nil {
		return 0
	}
	return pqm.partitionMgr.maxAttempts.Load()
}

// recordAttempt writes a backlog task's new attempt count in place (fenced).
func (db *taskQueueDB) recordAttempt(ctx context.Context, dls store.DeadLetterStore, sub subqueueIndex, t store.TaskInfo) error {
	db.Lock() // serialize with other fenced writes, so an in-process range renewal cannot fail it
	defer db.Unlock()
	return dls.UpdateTask(ctx, &store.UpdateTaskRequest{Key: db.queue, RangeID: db.rangeID, Subqueue: int(sub), Task: t})
}

func (db *taskQueueDB) deadLetterTask(ctx context.Context, dls store.DeadLetterStore, sub subqueueIndex, t store.TaskInfo, reason string, at time.Time) error {
	db.Lock()
	defer db.Unlock()
	return dls.DeadLetterTask(ctx, &store.DeadLetterTaskRequest{
		Key: db.queue, RangeID: db.rangeID, Subqueue: int(sub), Task: t, Reason: reason, DeadTime: at,
	})
}

// recordAttempt persists a backlog delivery's attempt count when the queue has a limit. A
// failure is counted and logged and the task is delivered anyway; a fencing failure also
// unloads the queue (its lease will then be lost on Ack).
func (e *Engine) recordAttempt(pqm *physicalTaskQueueManagerImpl, task *internalTask) {
	ctx, cancel := context.WithTimeout(pqm.tqCtx, persistenceOperationRetryPolicy.Expiration)
	defer cancel()
	info := *task.event.TaskInfo
	db := pqm.backlogMgr.getDB()
	err := backoff.ThrottleRetryContext(ctx, func(ctx context.Context) error {
		return db.recordAttempt(ctx, e.deadLetters, task.event.subqueue, info)
	}, persistenceOperationRetryPolicy, isPersistenceTransient)
	if err != nil {
		pqm.metricsHandler.Counter(metrics.TaskAttemptRecordFailures, 1)
		pqm.logger.Warn("failed to record task attempt", "task_id", info.ID, "attempts", info.Data.Attempts, "error", err)
		pqm.backlogMgr.signalIfFatal(err)
	}
}

// deadLetter moves a backlog task to the dead-letter set and finishes it as acked. It
// returns false (and leaves the task to the caller) if the write failed.
func (e *Engine) deadLetter(pqm *physicalTaskQueueManagerImpl, task *internalTask, reason, detail string, consumedToken bool) bool {
	ctx, cancel := context.WithTimeout(pqm.tqCtx, persistenceOperationRetryPolicy.Expiration)
	defer cancel()
	info := *task.event.TaskInfo
	full := reason
	if detail != "" {
		full += ": " + detail
	}
	db := pqm.backlogMgr.getDB()
	now := e.timeSource.Now()
	err := backoff.ThrottleRetryContext(ctx, func(ctx context.Context) error {
		return db.deadLetterTask(ctx, e.deadLetters, task.event.subqueue, info, full, now)
	}, persistenceOperationRetryPolicy, isPersistenceTransient)
	if err != nil {
		pqm.metricsHandler.Counter(metrics.DeadLetterFailures, 1)
		pqm.logger.Error("failed to dead-letter task; delivering it again",
			"task_id", info.ID, "attempts", info.Data.Attempts, "reason", full, "error", err)
		pqm.backlogMgr.signalIfFatal(err)
		return false
	}
	pqm.metricsHandler.Counter(metrics.TasksDeadLettered, 1, metrics.Tag{Key: metrics.TagReason, Value: reason})
	pqm.logger.Info("task dead-lettered", "task_id", info.ID, "attempts", info.Data.Attempts, "reason", full)
	task.finish(taskFinishResult{consumedToken: consumedToken})
	return true
}

// exhausted reports whether a matched task has used all its attempts and should be
// dead-lettered instead of delivered.
func exhausted(task *internalTask, max int32) bool {
	return max > 0 && !task.isSyncMatchTask() && task.event.Data.Attempts >= max
}

// DeadTask is a dead-lettered task (store.DeadTask).
type DeadTask = store.DeadTask

func (e *Engine) deadLetterStore() (store.DeadLetterStore, error) {
	if e.isStopped() {
		return nil, ErrEngineStopped
	}
	if e.deadLetters == nil {
		return nil, errDeadLetterUnsupported
	}
	return e.deadLetters, nil
}

// ListDeadTasks pages a queue's dead tasks in task ID order. It reads the store only: it does
// not load the queue or take it over. pageSize <= 0 means 100.
func (e *Engine) ListDeadTasks(ctx context.Context, queue store.QueueKey, pageSize int, pageToken []byte) ([]DeadTask, []byte, error) {
	dls, err := e.deadLetterStore()
	if err != nil {
		return nil, nil, err
	}
	if pageSize <= 0 {
		pageSize = 100
	}
	resp, err := dls.ListDeadTasks(ctx, &store.ListDeadTasksRequest{Key: queue, PageSize: pageSize, PageToken: pageToken})
	if err != nil {
		return nil, nil, err
	}
	return resp.Tasks, resp.NextPageToken, nil
}

// DeleteDeadTasks deletes the given dead tasks and returns how many existed. Store only, like
// ListDeadTasks.
func (e *Engine) DeleteDeadTasks(ctx context.Context, queue store.QueueKey, taskIDs []int64) (int, error) {
	dls, err := e.deadLetterStore()
	if err != nil {
		return 0, err
	}
	if len(taskIDs) == 0 {
		return 0, nil
	}
	return dls.DeleteDeadTasks(ctx, &store.DeleteDeadTasksRequest{Key: queue, TaskIDs: taskIDs})
}

// PurgeDeadTasks deletes the queue's dead tasks that died before deadBefore (all of them for
// a zero time) and returns the count. Store only, like ListDeadTasks.
func (e *Engine) PurgeDeadTasks(ctx context.Context, queue store.QueueKey, deadBefore time.Time) (int, error) {
	dls, err := e.deadLetterStore()
	if err != nil {
		return 0, err
	}
	if deadBefore.IsZero() {
		deadBefore = time.Unix(0, 1<<63-1)
	}
	return dls.DeleteDeadTasks(ctx, &store.DeleteDeadTasksRequest{Key: queue, DeadBefore: deadBefore})
}

// RedriveOptions selects the dead tasks RedriveDeadTasks moves back.
type RedriveOptions struct {
	// TaskIDs limits the redrive to these dead tasks; empty means all (in task ID order).
	TaskIDs []int64
	// Max caps how many tasks are redriven; 0 means no cap.
	Max int
}

// RedriveDeadTasks moves dead tasks back to the queue as new tasks (new IDs, Attempts reset
// to 0, the original payload, priority and create time) and returns how many it moved. Each
// task is added like AddTask, so it loads the queue on this Engine (taking it over from
// another process, as AddTask would) and may sync-match a waiting poller. The move is
// at-least-once: a task is deleted from the dead set only after its AddTask succeeded, so a
// crash or error in between can leave it both redriven and dead. On error the count of tasks
// already moved is returned with it.
func (e *Engine) RedriveDeadTasks(ctx context.Context, queue store.QueueKey, opts RedriveOptions) (int, error) {
	dls, err := e.deadLetterStore()
	if err != nil {
		return 0, err
	}
	want := map[int64]bool{}
	for _, id := range opts.TaskIDs {
		want[id] = true
	}
	moved := 0
	var token []byte
	for {
		resp, err := dls.ListDeadTasks(ctx, &store.ListDeadTasksRequest{Key: queue, PageSize: 100, PageToken: token})
		if err != nil {
			return moved, err
		}
		var done []int64
		for _, dt := range resp.Tasks {
			if len(opts.TaskIDs) > 0 && !want[dt.Task.ID] {
				continue
			}
			if opts.Max > 0 && moved+len(done) >= opts.Max {
				break
			}
			t := dt.Task.Data
			t.Attempts = 0
			if _, err := e.AddTask(ctx, queue, t); err != nil {
				return moved + e.finishRedrive(ctx, dls, queue, done), fmt.Errorf("redrive task %d: %w", dt.Task.ID, err)
			}
			done = append(done, dt.Task.ID)
			delete(want, dt.Task.ID)
		}
		n := e.finishRedrive(ctx, dls, queue, done)
		moved += n
		if n < len(done) {
			return moved, errors.New("redrive: failed to delete redriven dead tasks; they may be redriven again")
		}
		if resp.NextPageToken == nil || (opts.Max > 0 && moved >= opts.Max) || (len(opts.TaskIDs) > 0 && len(want) == 0) {
			return moved, nil
		}
		token = resp.NextPageToken
	}
}

// finishRedrive deletes redriven dead tasks and counts them; it returns how many it deleted
// (all or none).
func (e *Engine) finishRedrive(ctx context.Context, dls store.DeadLetterStore, queue store.QueueKey, ids []int64) int {
	if len(ids) == 0 {
		return 0
	}
	if _, err := dls.DeleteDeadTasks(ctx, &store.DeleteDeadTasksRequest{Key: queue, TaskIDs: ids}); err != nil {
		e.logger.Error("failed to delete redriven dead tasks", "queue", queue.String(), "count", len(ids), "error", err)
		return 0
	}
	h, _ := e.queueMetricsHandler(queue)
	h.Counter(metrics.TasksRedriven, int64(len(ids)))
	return len(ids)
}
