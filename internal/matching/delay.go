package matching

// Delayed visibility and retry backoff are a library addition with no upstream counterpart
// (upstream history schedules activity retries with its own timers and adds a new matching
// task when one is due). See README "Delays and retry backoff".
//
// A backlog task whose VisibleAt is in the future is loaded by its reader like any task but
// held as "delayed": it pins the ack level and is armed on a timer (the injected clock), and
// it is added to the matcher when the timer fires. Up to Config.MaxDelayedInMemory delayed
// tasks do not count toward the read batch, so a run of delayed tasks does not stop the reader
// from loading the ready tasks behind them; past that they count like loaded tasks, bounding
// memory (the reader has one cursor, so it cannot skip an unbounded run). A trimmed delayed task is dropped like a loaded one and delayed again when re-read.

import (
	"context"
	"math"
	"time"

	"github.com/skme902/tq/internal/backoff"
	"github.com/skme902/tq/metrics"
)

// maxDelayedInMemory is how many delayed tasks a reader holds without counting them as
// loaded (Config.MaxDelayedInMemory).
func (tr *fairTaskReader) maxDelayedInMemory() int {
	return tr.backlogMgr.config.MaxDelayedInMemory
}

// effectiveLoadedLocked is loadedTasks plus the delayed tasks past maxDelayedInMemory.
func (tr *fairTaskReader) effectiveLoadedLocked() int {
	return tr.loadedTasks + max(0, tr.delayedTasks-tr.maxDelayedInMemory())
}

// armDelayLocked starts the timer that makes a delayed task matchable at visibleAt.
func (tr *fairTaskReader) armDelayLocked(task *internalTask, visibleAt time.Time) {
	d := max(visibleAt.Sub(tr.backlogMgr.timeSource.Now()), 0)
	// Not under the timer callback: the matcher reads the clock, and a fake clock runs
	// callbacks with its lock held.
	task.delayTimer = tr.backlogMgr.timeSource.AfterFunc(d, func() { go tr.delayFired(task) })
}

// delayLocked turns a loaded (just unleased) task into a delayed one. It reports false if
// visibleAt is not in the future (the caller re-matches the task now).
func (tr *fairTaskReader) delayLocked(task *internalTask, visibleAt time.Time) bool {
	if !visibleAt.After(tr.backlogMgr.timeSource.Now()) {
		return false
	}
	if _, found := tr.outstandingTasks.Get(outstandingTask{level: task.fairLevel()}); !found || task.delayed {
		return false
	}
	task.delayed = true
	tr.loadedTasks--
	tr.delayedTasks++
	tr.armDelayLocked(task, visibleAt)
	tr.backlogMgr.metricsHandler.Counter(metrics.TasksDelayed, 1)
	return true
}

// undelayLocked drops a delayed task's delayed state (it is completed, trimmed or deleted).
func (tr *fairTaskReader) undelayLocked(task *internalTask) {
	task.delayed = false
	tr.delayedTasks--
	if task.delayTimer != nil {
		task.delayTimer.Stop()
		task.delayTimer = nil
	}
}

// delayFired makes a delayed task matchable, unless it left the delayed state meanwhile.
func (tr *fairTaskReader) delayFired(task *internalTask) {
	tr.lock.Lock()
	if !task.delayed || tr.backlogMgr.tqCtx.Err() != nil {
		tr.lock.Unlock()
		return
	}
	task.delayed = false
	task.delayTimer = nil
	tr.delayedTasks--
	tr.loadedTasks++
	tr.lock.Unlock()
	tr.addTaskToMatcher(task)
}

// RetryBackoff is an exponential delay before a nacked or expired backlog task is
// redelivered (library addition). The n-th failed delivery waits
// min(Initial * Multiplier^(n-1), Max). The zero value disables it: redelivery is immediate.
type RetryBackoff struct {
	Initial time.Duration
	// Max caps the delay; 0 means no cap.
	Max time.Duration
	// Multiplier is the growth factor; 0 means 2. Must be at least 1.
	Multiplier float64
}

// delay is the backoff after the attempts-th delivery failed.
func (b RetryBackoff) delay(attempts int32) time.Duration {
	if b.Initial <= 0 {
		return 0
	}
	m := b.Multiplier
	if m == 0 {
		m = 2
	}
	d := float64(b.Initial) * math.Pow(m, float64(max(attempts, 1)-1))
	if b.Max > 0 && d > float64(b.Max) {
		return b.Max
	}
	if d > math.MaxInt64 {
		return time.Duration(math.MaxInt64)
	}
	return time.Duration(d)
}

// delayRedelivery records that a failed backlog delivery should be redelivered at visibleAt:
// it writes VisibleAt to the task row (best effort, fenced) and marks the task so its reader
// delays it when the delivery finishes. A failed write is counted and logged; the delay
// still applies in memory, so only a reload in that window redelivers early.
func (e *Engine) delayRedelivery(pqm *physicalTaskQueueManagerImpl, task *internalTask, visibleAt time.Time) {
	task.event.Data.VisibleAt = visibleAt
	if e.deadLetters != nil {
		ctx, cancel := context.WithTimeout(pqm.tqCtx, persistenceOperationRetryPolicy.Expiration)
		info := *task.event.TaskInfo
		db := pqm.backlogMgr.getDB()
		err := backoff.ThrottleRetryContext(ctx, func(ctx context.Context) error {
			return db.recordAttempt(ctx, e.deadLetters, task.event.subqueue, info)
		}, persistenceOperationRetryPolicy, isPersistenceTransient)
		cancel()
		if err != nil {
			pqm.metricsHandler.Counter(metrics.DelayPersistFailures, 1)
			pqm.logger.Warn("failed to persist redelivery delay; applying it in memory", "task_id", info.ID, "error", err)
			pqm.backlogMgr.signalIfFatal(err)
		}
	}
	task.redeliverAt.Store(visibleAt.UnixNano())
}
