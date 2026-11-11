package matching

// Reader-side task admin (library addition): deleting one task and purging a subqueue on the
// owning engine. See admin.go.

import (
	"context"

	"github.com/skme902/tq/store"
)

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
	tr.lock.Unlock()

	db := tr.backlogMgr.db
	found, err := db.deleteTask(ctx, tas, tr.subqueue, level)
	tr.lock.Lock()
	defer tr.lock.Unlock()
	if err != nil {
		delete(tr.adminDeleted, level)
		tr.backlogMgr.signalIfFatal(err)
		return false, nil, err
	}
	var leased *internalTask
	inMemory := false
	if e, ok := tr.outstandingTasks.Get(outstandingTask{level: level}); ok && !e.acked() {
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
	} else if t, ok := tr.leasedEvicted[level]; ok {
		delete(tr.leasedEvicted, level)
		t.adminDeleted.Store(true)
		leased = t
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
func (tr *fairTaskReader) adminPurge() (leased []*internalTask, exclusiveMax fairLevel) {
	maxLevel := tr.backlogMgr.db.GetMaxFairReadLevel(tr.subqueue)
	tr.lock.Lock()
	defer tr.lock.Unlock()
	maxLevel = maxLevel.max(tr.readLevel)
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
	if tr.ackLevel.less(maxLevel) {
		tr.ackLevel = maxLevel
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
	return leased, tr.ackLevel.inc()
}
