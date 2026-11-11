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

// effectiveLoadedLocked is the count the reader limits: loaded tasks, not counting "held" ones
// (delayed tasks and loaded tasks of paused fairness keys, see pause_hold.go) up to
// maxDelayedInMemory.
func (tr *fairTaskReader) effectiveLoadedLocked() int {
	paused := tr.pausedLoadedLocked(tr.pausedKeys())
	return tr.loadedTasks - paused + max(0, tr.delayedTasks+paused-tr.maxDelayedInMemory())
}

// armDelayLocked starts the timer that makes a delayed task matchable at visibleAt. Arming and
// stopping under tr.lock is safe because timer callbacks never run inline on the clock
// (afterFunc). delayGen identifies this delay: a callback for an older one (already
// scheduled when its timer was stopped) is ignored by delayFired.
func (tr *fairTaskReader) armDelayLocked(task *internalTask, visibleAt time.Time) {
	if tr.delayStopped {
		return // the queue is stopping: the row keeps VisibleAt for the next load
	}
	task.delayGen++
	gen := task.delayGen
	if task.delayTimer != nil {
		task.delayTimer.Stop()
	}
	task.delayTimer = afterFuncAt(tr.backlogMgr.timeSource, visibleAt, func() { tr.delayFired(task, gen) })
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
	task.delayGen++ // a pending registration or firing for the old delay is dropped
	tr.delayedTasks--
	if task.delayTimer != nil {
		task.delayTimer.Stop()
		task.delayTimer = nil
	}
}

// stopDelayTimers stops every delayed task's timer when the queue unloads or stops, so neither
// the timers nor the reader, partition and engine their callbacks hold outlive the queue (a
// delay can be hours, or effectively forever with an uncapped RetryBackoff). It runs before
// tqCtx is cancelled, so it also blocks arming new timers.
func (tr *fairTaskReader) stopDelayTimers() {
	tr.lock.Lock()
	defer tr.lock.Unlock()
	tr.delayStopped = true
	tr.outstandingTasks.Scan(func(o outstandingTask) bool {
		if o.task != nil && o.task.delayed {
			o.task.delayGen++ // a callback already scheduled is ignored
			if o.task.delayTimer != nil {
				o.task.delayTimer.Stop()
				o.task.delayTimer = nil
			}
		}
		return true
	})
}

// evictDelayedLocked drops a delayed task that a trim chopped from outstandingTasks. Its
// VisibleAt is kept in evictedDelays, so the re-read delays it again even when the delay
// exists only in memory (no DeadLetterStore, or a failed VisibleAt write).
func (tr *fairTaskReader) evictDelayedLocked(task *internalTask) {
	tr.backlogAge.record(task.event.Data.CreateTime, -1)
	tr.recordEvictedDelayLocked(task.fairLevel(), task.event.Data.VisibleAt)
	tr.undelayLocked(task)
	task.setEvicted()
}

// recordEvictedDelayLocked remembers the visibility time of a task dropped from memory.
// Bounded by evictedAcksCacheSize: past that it drops the highest other level (the furthest
// from the ack level, so the last to be re-read), never the one just written.
func (tr *fairTaskReader) recordEvictedDelayLocked(level fairLevel, at time.Time) {
	if tr.evictedDelays == nil {
		tr.evictedDelays = make(map[fairLevel]time.Time)
	}
	tr.evictedDelays[level] = at
	for len(tr.evictedDelays) > evictedAcksCacheSize {
		victim, found := fairLevel{}, false
		for l := range tr.evictedDelays {
			if l != level && (!found || victim.less(l)) {
				victim, found = l, true
			}
		}
		delete(tr.evictedDelays, victim)
	}
}

// delayFired makes a delayed task matchable, unless it left the delayed state meanwhile.
func (tr *fairTaskReader) delayFired(task *internalTask, gen uint64) {
	tr.lock.Lock()
	if !task.delayed || task.delayGen != gen || tr.delayStopped || tr.backlogMgr.tqCtx.Err() != nil {
		tr.lock.Unlock()
		return
	}
	task.delayed = false
	task.delayTimer = nil
	tr.delayedTasks--
	if o, ok := tr.outstandingTasks.Get(outstandingTask{level: task.fairLevel()}); !ok || o.task != task {
		tr.lock.Unlock() // no longer tracked by this reader: a re-read owns the row
		return
	}
	tr.loadedTasks++
	// Reset the matcher state under the lock: a trim that runs once the lock is dropped then
	// marks the task evicted, and the matcher refuses it. Resetting after the unlock (as
	// addTaskToMatcher does) would wipe that eviction, and the trimmed row would be matched
	// here and again by the re-read that loads it.
	task.resetMatcherState()
	tr.lock.Unlock()
	if testHookDelayFiredUnlocked != nil {
		testHookDelayFiredUnlocked(tr, task)
	}
	tr.addPreparedTaskToMatcher(task)
}

// testHookDelayFiredUnlocked runs in delayFired between dropping the reader lock and adding the
// task to the matcher (tests only).
var testHookDelayFiredUnlocked func(tr *fairTaskReader, task *internalTask)

// maxVisibleAt is the latest redelivery time: redeliverAt holds unix nanos, which cannot
// represent a later time (an uncapped RetryBackoff reaches it after a few dozen attempts).
var maxVisibleAt = time.Unix(0, math.MaxInt64)

// redeliveryTime is now+delay, capped at maxVisibleAt.
func redeliveryTime(now time.Time, delay time.Duration) time.Time {
	if delay >= maxVisibleAt.Sub(now) {
		return maxVisibleAt
	}
	return now.Add(delay)
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
