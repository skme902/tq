package matching

// Task admin (library addition; upstream has tdbg list-tasks but no delete or purge): peek at a
// queue's backlog, delete one task, purge the backlog. See README "Task admin".
//
// PeekTasks reads the store only and never takes ownership. DeleteTask and PurgeQueue change
// the backlog, so they run on the owner: they load the queue on this Engine (taking it over
// from another process, like AddTask) and go through its readers, so memory, counts, the ack
// level and the rangeID fence stay consistent.

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"

	"github.com/skme902/tq/metrics"
	"github.com/skme902/tq/store"
)

var errTaskAdminUnsupported = &store.InvalidArgumentError{Msg: "store does not implement store.TaskAdminStore"}

// TaskRef identifies one backlog task (its full store key).
type TaskRef struct {
	Subqueue int
	Pass     int64
	ID       int64
}

// PeekOptions selects the tasks PeekTasks returns.
type PeekOptions struct {
	// PageSize is the max tasks returned (default 100, max 1000).
	PageSize int
	// PageToken continues a previous call; nil starts at the front.
	PageToken []byte
	// Priority, if non-zero, returns only tasks of that priority key.
	Priority int32
	// FairnessKey, if non-empty, returns only tasks with that fairness key. The filter is
	// applied while scanning, so a page may be short (even empty) with a NextPageToken.
	FairnessKey string
}

// PeekedTask is one backlog task as PeekTasks sees it.
type PeekedTask struct {
	Ref TaskRef
	// Priority is the task's effective priority key (its subqueue's).
	Priority int32
	// Task is the stored task. Task.Attempts is the persisted count, which is only kept up
	// to date when the queue has MaxAttempts set.
	Task store.Task
	// Leased is true if the queue is loaded on this Engine and the task is out on a lease.
	Leased bool
}

type peekToken struct {
	Subqueue int   `json:"s"`
	Pass     int64 `json:"p"`
	ID       int64 `json:"i"` // next inclusive min
}

const (
	peekDefaultPageSize = 100
	peekMaxPageSize     = 1000
	peekMaxScanFactor   = 10 // rows scanned per returned slot when filtering
)

// PeekTasks pages a queue's unacked backlog tasks in (subqueue, fair level) order: the order
// each priority level dispatches in. It reads the store only, so it neither loads the queue
// nor takes it over; on a queue loaded here it skips tasks acked but not yet deleted and sets
// Leased. Tasks still being written are not seen. It returns *store.NotFoundError for a queue
// that was never persisted.
func (e *Engine) PeekTasks(ctx context.Context, queue store.QueueKey, opts PeekOptions) ([]PeekedTask, []byte, error) {
	if e.isStopped() {
		return nil, nil, ErrEngineStopped
	}
	pageSize := opts.PageSize
	if pageSize <= 0 {
		pageSize = peekDefaultPageSize
	}
	pageSize = min(pageSize, peekMaxPageSize)
	var tok peekToken
	if len(opts.PageToken) > 0 {
		if err := json.Unmarshal(opts.PageToken, &tok); err != nil {
			return nil, nil, &store.InvalidArgumentError{Msg: "invalid page token"}
		}
	}
	resp, err := e.store.GetQueue(ctx, queue)
	if err != nil {
		return nil, nil, err
	}
	tqConfig := newTaskQueueConfig(e.config)
	readers := e.loadedReaders(queue)
	var out []PeekedTask
	budget := pageSize * peekMaxScanFactor
	for sq := tok.Subqueue; sq < len(resp.Info.Subqueues); sq++ {
		info := resp.Info.Subqueues[sq]
		pri := int32(tqConfig.clipPriority(priorityKey(info.Key.Priority)))
		if opts.Priority != 0 && pri != opts.Priority {
			continue
		}
		from := fairLevelFromStore(info.FairAckLevel).max(fairLevel{pass: 1, id: 0}).inc()
		if sq == tok.Subqueue && len(opts.PageToken) > 0 {
			from = from.max(fairLevel{pass: tok.Pass, id: tok.ID})
		}
		var tr *fairTaskReader
		if sq < len(readers) {
			tr = readers[sq]
		}
		for {
			want := pageSize - len(out)
			if opts.FairnessKey != "" {
				want = min(budget, peekMaxPageSize)
			}
			if want <= 0 || budget <= 0 {
				return out, e.peekNext(sq, from), nil
			}
			res, err := e.store.GetTasks(ctx, &store.GetTasksRequest{
				Key: queue, Subqueue: sq, InclusiveMin: from.toStore(), PageSize: want,
			})
			if err != nil {
				return nil, nil, err
			}
			for _, t := range res.Tasks {
				level := fairLevelFromAllocatedTask(&t)
				from = level.inc()
				budget--
				if opts.FairnessKey != "" && t.Data.Priority.FairnessKey != opts.FairnessKey {
					if budget <= 0 {
						return out, e.peekNext(sq, from), nil
					}
					continue
				}
				leased, acked := tr.peekState(level)
				if acked {
					continue
				}
				out = append(out, PeekedTask{
					Ref:      TaskRef{Subqueue: sq, Pass: t.Pass, ID: t.ID},
					Priority: pri,
					Task:     t.Data,
					Leased:   leased,
				})
				if len(out) == pageSize {
					return out, e.peekNext(sq, from), nil
				}
			}
			if len(res.Tasks) < want {
				break // end of this subqueue
			}
		}
	}
	return out, nil, nil
}

func (e *Engine) peekNext(sq int, from fairLevel) []byte {
	b, _ := json.Marshal(peekToken{Subqueue: sq, Pass: from.pass, ID: from.id})
	return b
}

// loadedReaders returns the readers of a queue loaded on this Engine (nil otherwise), by
// subqueue index. It does not load the queue.
func (e *Engine) loadedReaders(queue store.QueueKey) []*fairTaskReader {
	e.partitionsLock.RLock()
	pm, ok := e.partitions[queue]
	e.partitionsLock.RUnlock()
	if !ok || !pm.defaultQueue.backlogMgr.isInitialized() {
		return nil
	}
	bm := pm.defaultQueue.backlogMgr
	bm.subqueueLock.Lock()
	defer bm.subqueueLock.Unlock()
	return slices.Clone(bm.subqueues)
}

// peekState reports what this reader knows about a stored task: leased, or acked (but not yet
// deleted). A nil reader knows nothing.
func (tr *fairTaskReader) peekState(level fairLevel) (leased, acked bool) {
	if tr == nil {
		return false, false
	}
	tr.lock.Lock()
	defer tr.lock.Unlock()
	if !tr.ackLevel.less(level) {
		return false, true
	}
	if _, ok := tr.adminDeleted[level]; ok {
		return false, true
	}
	if _, ok := tr.leasedEvicted[level]; ok {
		return true, false
	}
	if _, ok := tr.evictedAcks.Get(level); ok {
		return false, true
	}
	if o, ok := tr.outstandingTasks.Get(outstandingTask{level: level}); ok {
		return !o.acked() && o.task.leased, o.acked()
	}
	return false, false
}

func (db *taskQueueDB) deleteTask(ctx context.Context, tas store.TaskAdminStore, sub subqueueIndex, level fairLevel) (bool, error) {
	db.Lock()
	defer db.Unlock()
	return tas.DeleteTask(ctx, &store.DeleteTaskRequest{
		Key: db.queue, RangeID: db.rangeID, Subqueue: int(sub), Pass: level.pass, ID: level.id,
	})
}

// adminQueue loads a persisted queue on this Engine for a mutating admin call. It returns
// *store.NotFoundError, without creating the queue, if it was never persisted.
func (e *Engine) adminQueue(ctx context.Context, queue store.QueueKey) (*physicalTaskQueueManagerImpl, error) {
	if e.isStopped() {
		return nil, ErrEngineStopped
	}
	e.partitionsLock.RLock()
	_, loaded := e.partitions[queue]
	e.partitionsLock.RUnlock()
	if !loaded {
		if _, err := e.store.GetQueue(ctx, queue); err != nil {
			return nil, err
		}
	}
	pm, err := e.getTaskQueuePartitionManager(ctx, queue, true)
	if err != nil {
		return nil, err
	}
	return pm.defaultQueue, nil
}

// DeleteTask deletes one backlog task and reports whether it existed. It loads the queue on
// this Engine (taking it over from another owner, like AddTask). A loaded task is removed from
// memory and the matcher; a leased one has its lease ended (Ack and Nack return ErrLeaseLost).
// A poll that matched the task an instant before the delete still delivers it once.
func (e *Engine) DeleteTask(ctx context.Context, queue store.QueueKey, ref TaskRef) (bool, error) {
	tas, ok := e.store.(store.TaskAdminStore)
	if !ok {
		return false, errTaskAdminUnsupported
	}
	if ref.ID <= 0 || ref.Pass <= 0 || ref.Subqueue < 0 {
		return false, &store.InvalidArgumentError{Msg: fmt.Sprintf("invalid task ref %+v", ref)}
	}
	pqm, err := e.adminQueue(ctx, queue)
	if err != nil {
		return false, err
	}
	bm := pqm.backlogMgr
	bm.subqueueLock.Lock()
	var tr *fairTaskReader
	if ref.Subqueue < len(bm.subqueues) {
		tr = bm.subqueues[ref.Subqueue]
	}
	bm.subqueueLock.Unlock()
	if tr == nil {
		return false, nil
	}
	found, leased, err := tr.adminDeleteTask(ctx, tas, fairLevel{pass: ref.Pass, id: ref.ID})
	if err != nil {
		return false, err
	}
	if leased != nil {
		e.endLeases([]*internalTask{leased})
	}
	if found {
		pqm.metricsHandler.Counter(metrics.TasksAdminDeleted, 1)
		pqm.logger.Info("task deleted by admin", "task_id", ref.ID, "subqueue", ref.Subqueue)
	}
	return found, nil
}

// PurgeQueue deletes every backlog task of a queue and returns how many rows it deleted. It
// loads the queue on this Engine (taking it over from another owner, like AddTask). Tasks added
// while it runs may or may not be purged. A write that already pinned the ack level is kept:
// its passes were chosen before the purge watermark moved, and a lagging fairness key can land
// below the previous max. Tasks added after PurgeQueue returns are kept. Leases on purged tasks
// end (Ack and Nack return ErrLeaseLost). Dead tasks are kept (PurgeDeadTasks); sync-matched
// leases are not in the backlog and are not affected.
func (e *Engine) PurgeQueue(ctx context.Context, queue store.QueueKey) (int, error) {
	pqm, err := e.adminQueue(ctx, queue)
	if err != nil {
		return 0, err
	}
	bm := pqm.backlogMgr
	bm.subqueueLock.Lock()
	readers := slices.Clone(bm.subqueues)
	bm.subqueueLock.Unlock()
	var leased []*internalTask
	bounds := make([]fairLevel, len(readers))
	for i, tr := range readers {
		l, b, err := tr.adminPurge(ctx)
		if err != nil {
			e.endLeases(leased)
			return 0, err
		}
		leased = append(leased, l...)
		bounds[i] = b
	}
	e.endLeases(leased)
	// Persist the new ack levels first: from then on a reload cannot read the purged rows,
	// even if the deletes below fail (ack-level GC retries them).
	db := bm.getDB()
	if err := db.SyncState(ctx); err != nil {
		bm.signalIfFatal(err)
		return 0, err
	}
	total := 0
	for i := range readers {
		n, err := db.CompleteFairTasksLessThan(ctx, bounds[i], 0, subqueueIndex(i))
		if err != nil {
			bm.signalIfFatal(err)
			return total, err
		}
		if n > 0 {
			total += n
		}
	}
	pqm.metricsHandler.Counter(metrics.QueuePurges, 1)
	pqm.metricsHandler.Counter(metrics.TasksAdminDeleted, int64(total))
	pqm.logger.Info("queue purged", "tasks", total, "leases_ended", len(leased))
	return total, nil
}

// endLeases ends the leases on tasks an admin call removed: later Ack/Nack/ExtendLease return
// ErrLeaseLost and nothing is redelivered.
func (e *Engine) endLeases(tasks []*internalTask) {
	if len(tasks) == 0 {
		return
	}
	want := make(map[*internalTask]bool, len(tasks))
	for _, t := range tasks {
		want[t] = true
	}
	e.leasesLock.Lock()
	var ds []*Delivery
	for d := range e.leases {
		if want[d.task] {
			ds = append(ds, d)
		}
	}
	e.leasesLock.Unlock()
	for _, d := range ds {
		if d.settleWith(ErrLeaseLost) {
			d.stopTimer()
			d.release()
		}
	}
}
