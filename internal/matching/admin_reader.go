package matching

// Reader-side task admin (library addition): deleting one task and purging a subqueue on the
// owning engine. See admin.go.

import (
	"context"
	"time"

	"github.com/skme902/tq/store"
)

// testHookAfterAdminStoreDelete, if set, runs after a successful store delete and
// before adminDeleteTask retakes the reader lock. Tests only.
var testHookAfterAdminStoreDelete func()

// adminPurgePinWaitHook, if set, runs when adminPurge is about to wait for a writer
// ack-level pin to drop. It must not block on the reader lock. Tests only.
var adminPurgePinWaitHook func()

// adminDeleteTask deletes the task at level: from the store (fenced) and, if loaded, from
// memory. It returns whether the task existed and the task if it was leased (the caller ends
// that lease).
func (tr *fairTaskReader) adminDeleteTask(ctx context.Context, tas store.TaskAdminStore, level fairLevel) (bool, *internalTask, error) {
	tr.lock.Lock()
	if !tr.ackLevel.less(level) {
		tr.lock.Unlock()
		return false, nil, nil // acked: gone, or deleted by the next GC
	}
	if e, ok := tr.outstandingTasks.Get(outstandingTask{level: level}); ok && e.acked() {
		tr.lock.Unlock()
		return false, nil, nil // acked, ack level not past it yet
	}
	// Stop concurrent reads from loading it before the store delete commits.
	if tr.adminDeleted == nil {
		tr.adminDeleted = make(map[fairLevel]struct{})
	}
	tr.adminDeleted[level] = struct{}{}
	if tr.adminInFlight == nil {
		tr.adminInFlight = make(map[fairLevel]*store.TaskInfo)
	}
	tr.adminInFlight[level] = nil // pins the ack level until the delete returns
	tr.lock.Unlock()

	db := tr.backlogMgr.db
	found, err := db.deleteTask(ctx, tas, tr.subqueue, level)
	if err == nil && testHookAfterAdminStoreDelete != nil {
		testHookAfterAdminStoreDelete()
	}
	tr.lock.Lock()
	skipped := tr.adminInFlight[level]
	delete(tr.adminInFlight, level)
	if err != nil {
		delete(tr.adminDeleted, level)
		// A read that ran during the delete skipped this row and may have moved past it.
		// The row is still in the store: merge it back now. The ack level was pinned
		// while the delete ran, so it cannot have moved past the row.
		var restored []*internalTask
		if skipped != nil {
			restored = tr.mergeTasksLocked([]*store.TaskInfo{skipped}, mergeWrite)
		}
		tr.advanceAckLevelLocked()
		tr.maybeReadTasksLocked()
		tr.lock.Unlock()
		for _, task := range restored {
			tr.addTaskToMatcher(task)
		}
		tr.backlogMgr.signalIfFatal(err)
		return false, nil, err
	}
	defer tr.lock.Unlock()
	// The ack level was pinned during the delete; let it advance once we are done.
	defer tr.advanceAckLevelLocked()
	var leased *internalTask
	inMemory := false
	// The task may have been acked while the store delete ran. The completion
	// path owns the backlog delta (already applied, or applied when the ack
	// level advances over the ack entry). Do not apply it again.
	ackedNow := !tr.ackLevel.less(level)
	if e, ok := tr.outstandingTasks.Get(outstandingTask{level: level}); ok {
		if e.acked() {
			ackedNow = true
		} else {
			inMemory = true
			tr.outstandingTasks.Delete(e)
			tr.backlogAge.record(e.task.event.Data.CreateTime, -1)
			if e.task.leased {
				e.task.adminDeleted.Store(true)
				e.task.leased = false
				tr.leasedTasks--
				leased = e.task
			} else if e.task.delayed {
				tr.undelayLocked(e.task)
				e.task.adminRemove()
			} else {
				tr.loadedTasks--
				e.task.adminRemove() // out of the matcher (a poll that already took it still delivers it)
			}
		}
	} else if t, ok := tr.leasedEvicted[level]; ok {
		delete(tr.leasedEvicted, level)
		t.adminDeleted.Store(true)
		leased = t
	}
	if ackedNow {
		if !tr.ackLevel.less(level) {
			delete(tr.adminDeleted, level)
		}
		return found, nil, nil
	}
	if !found && !inMemory && leased == nil {
		delete(tr.adminDeleted, level)
		return false, nil, nil
	}
	db.updateFairAckLevel(tr.subqueue, tr.ackLevel, -1, tr.knownCountLocked(), tr.backlogAge.oldestTime())
	tr.advanceAckLevelLocked()
	tr.maybeReadTasksLocked()
	return true, leased, nil
}

// adminPurge drops every task at or below the subqueue's max written level: it moves the ack
// level there (in memory, then persisted by the caller's SyncState) and returns the leased
// tasks it dropped and the level to delete below.
//
// It first waits, bounded by ctx, while the ack level is pinned (a writer between picking
// passes and committing, newlyWrittenTasks held for a read, or an admin delete in flight).
// Once the pin is clear the reader lock is held until the ack level has moved, and
// CreateFairTasks raises FairMaxReadLevel before it commits, so every row written so far is
// at or below maxLevel and is purged, including a task whose AddTask returned while the
// purge was waiting (like SQS PurgeQueue). Nothing is left below the new ack level. A write
// that pins after this returns gets the new ack level as its pass base, so it lands above.
func (tr *fairTaskReader) adminPurge(ctx context.Context) (leased []*internalTask, exclusiveMax fairLevel, err error) {
	tr.lock.Lock()
	defer tr.lock.Unlock()

	if err = tr.waitAckPinClearLocked(ctx); err != nil {
		return nil, fairLevel{}, err
	}

	maxLevel := tr.backlogMgr.db.GetMaxFairReadLevel(tr.subqueue)
	maxLevel = maxLevel.max(tr.readLevel)
	if maxItem, ok := tr.outstandingTasks.Max(); ok {
		maxLevel = maxLevel.max(maxItem.level)
	}
	var drop []outstandingTask
	tr.outstandingTasks.Scan(func(o outstandingTask) bool {
		if maxLevel.less(o.level) {
			return false
		}
		drop = append(drop, o)
		return true
	})
	for _, o := range drop {
		tr.outstandingTasks.Delete(o)
		if o.task == nil {
			continue
		}
		tr.backlogAge.record(o.task.event.Data.CreateTime, -1)
		if o.task.leased {
			o.task.adminDeleted.Store(true)
			o.task.leased = false
			tr.leasedTasks--
			leased = append(leased, o.task)
		} else if o.task.delayed {
			tr.undelayLocked(o.task)
			o.task.adminRemove()
		} else {
			tr.loadedTasks--
			o.task.adminRemove()
		}
	}
	for level, t := range tr.leasedEvicted {
		if !maxLevel.less(level) {
			delete(tr.leasedEvicted, level)
			t.adminDeleted.Store(true)
			leased = append(leased, t)
		}
	}
	for {
		l, ok := tr.evictedAcks.Min()
		if !ok || maxLevel.less(l) {
			break
		}
		tr.evictedAcks.PopMin()
	}
	ackMoved := tr.ackLevel.less(maxLevel)
	if ackMoved {
		tr.ackLevel = maxLevel
		// A failed purge delete leaves rows behind the new ack level. numToGC == 0
		// would never retry them; ack-level GC runs once this is at least 1.
		if tr.numToGC < 1 {
			tr.numToGC = 1
		}
	}
	if tr.readLevel.less(maxLevel) {
		tr.readLevel = maxLevel
	}
	for level := range tr.adminDeleted {
		if !tr.ackLevel.less(level) {
			delete(tr.adminDeleted, level)
		}
	}
	// What is left in memory is above the purge point; anything else above it is unknown
	// (written concurrently), and the next read to the end corrects the count.
	tr.backlogMgr.db.updateFairAckLevel(tr.subqueue, tr.ackLevel, 0, int64(tr.loadedTasks+tr.leasedTasks+tr.delayedTasks), tr.backlogAge.oldestTime())
	tr.maybeReadTasksLocked()
	return leased, tr.ackLevel.inc(), nil
}

// waitAckPinClearLocked blocks until a writer pin (and any tasks held for a
// concurrent read) has dropped, or ctx is done. The caller holds tr.lock; it is
// released while waiting so the writer can unpin. On return the lock is held.
func (tr *fairTaskReader) waitAckPinClearLocked(ctx context.Context) error {
	notified := false
	for tr.ackLevelPinnedLocked() {
		tr.lock.Unlock()
		if !notified && adminPurgePinWaitHook != nil {
			notified = true
			adminPurgePinWaitHook()
		}
		timer := time.NewTimer(time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			tr.lock.Lock()
			return ctx.Err()
		case <-timer.C:
		}
		tr.lock.Lock()
	}
	return nil
}
