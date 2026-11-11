package matching

// Delivery leases are a library addition with no upstream counterpart: upstream matching hands a
// task to history (RecordTaskStarted) and acks it immediately; history then owns retries. Here the
// consumer owns the task until Ack, and an unacked lease is redelivered:
//   - backlog tasks stay unacked in the fairTaskReader for the whole lease, so the ack level
//     cannot pass them, but they are counted as leased rather than loaded so they do not hold
//     back reads (fairTaskReader.leaseTask). If the reader trims a leased task from memory it
//     remembers it in leasedEvicted, so a re-read does not hand it to a second poller.
//     Expiry/Nack finishes them with a retryable error, which the reader turns into
//     addTaskToMatcher (upstream's transient-error path).
//   - sync-matched tasks were never persisted; expiry/Nack spools them to the backlog.
//   - Engine.Stop ends every lease (stopping its timer); later calls return ErrEngineStopped.

import (
	"fmt"
	"sync"
	"time"

	"github.com/skme902/tq/clock"
	"github.com/skme902/tq/metrics"
	"github.com/skme902/tq/store"
)

// Delivery is a leased task returned by PollTask.
type Delivery struct {
	Queue store.QueueKey
	// TaskID is the persisted task ID, or 0 for a sync-matched task.
	TaskID      int64
	Task        store.Task
	SyncMatched bool

	engine *Engine
	pqm    *physicalTaskQueueManagerImpl
	task   *internalTask
	timer  clock.Timer

	deliveredAt time.Time

	mu       sync.Mutex
	settled  bool
	lostErr  error // returned by Ack/Nack/ExtendLease once settled
	deadline time.Time
}

// newDelivery leases a matched task to a poller. It fails with ErrEngineStopped once Stop has
// snapshotted the leases. A refused sync-matched task is finished with that error, so the
// partition manager spools it (or AddTask reports the error) instead of handing out an
// in-memory-only task that Stop would never re-spool (REVIEW M1); a refused backlog task is
// returned to its reader.
func (e *Engine) newDelivery(task *internalTask, pqm *physicalTaskQueueManagerImpl, queue store.QueueKey, lease time.Duration) (*Delivery, error) {
	if lease <= 0 {
		lease = e.config.DeliveryLeaseDuration
	}
	now := e.timeSource.Now()
	d := &Delivery{
		Queue:       queue,
		Task:        task.event.Data,
		SyncMatched: task.isSyncMatchTask(),
		engine:      e,
		pqm:         pqm,
		task:        task,
		deliveredAt: now,
		deadline:    now.Add(lease),
	}
	if !d.SyncMatched {
		d.TaskID = task.event.ID
		if f := task.event.leaseFunc; f != nil {
			f(task) // before the timer is armed, so expiry always follows the lease
		}
	}

	// Register the lease and arm its timer atomically with respect to Stop's snapshot.
	e.leasesLock.Lock()
	if e.leasesClosed {
		e.leasesLock.Unlock()
		if d.SyncMatched {
			task.finish(taskFinishResult{err: ErrEngineStopped})
		} else {
			task.finish(taskFinishResult{err: errLeaseRefused}) // unlease; the queue is unloading
		}
		return nil, ErrEngineStopped
	}
	pqm.outstandingLeases.Add(1)
	d.arm(lease)
	e.leases[d] = struct{}{}
	e.leasesLock.Unlock()
	if d.SyncMatched {
		// The task is "started": let the AddTask caller return (upstream: finish after
		// RecordTaskStarted succeeds).
		task.finish(taskFinishResult{consumedToken: true})
	}
	emitDispatchLatency(pqm.metricsHandler, task, now)
	return d, nil
}

// emitDispatchLatency is upstream's poll-time emission (matching.emitTaskDispatchLatencyAtPoll,
// default true): task create time to the successful start, plus asyncmatch_latency for backlog
// tasks (upstream emits that at RecordTaskStarted).
func emitDispatchLatency(h metrics.Handler, task *internalTask, now time.Time) {
	ct := task.event.Data.CreateTime
	if ct.IsZero() || h == metrics.Noop {
		return // zero: should not happen but for safety; Noop: skip building the tag slice
	}
	lat := now.Sub(ct)
	source := metrics.SourceDBBacklog
	if task.isSyncMatchTask() {
		source = metrics.SourceHistory
	}
	h.Timer(metrics.TaskDispatchLatency, lat,
		metrics.Tag{Key: metrics.TagSource, Value: source},
		metrics.Tag{Key: metrics.TagForwarded, Value: "false"},
		metrics.TaskPriorityTag(task.getPriority().PriorityKey),
	)
	if !task.isSyncMatchTask() {
		h.Timer(metrics.AsyncMatchLatency, lat)
	}
}

func (d *Delivery) arm(lease time.Duration) {
	d.mu.Lock()
	d.timer = d.engine.timeSource.AfterFunc(lease, d.expire)
	d.mu.Unlock()
}

// LeaseDeadline returns when the lease expires.
func (d *Delivery) LeaseDeadline() time.Time {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.deadline
}

// settle marks the delivery settled; returns false if it already was.
func (d *Delivery) settle() bool { return d.settleWith(ErrLeaseLost) }

// settleWith settles the delivery; later calls get lost as their error.
func (d *Delivery) settleWith(lost error) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.settled {
		return false
	}
	d.settled = true
	d.lostErr = lost
	return true
}

func (d *Delivery) lost() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.lostErr
}

func (d *Delivery) stopTimer() {
	d.mu.Lock()
	d.timer.Stop()
	d.mu.Unlock()
}

func (d *Delivery) release() {
	d.pqm.outstandingLeases.Add(-1)
	d.engine.leasesLock.Lock()
	delete(d.engine.leases, d)
	d.engine.leasesLock.Unlock()
	d.pqm.MarkAlive()
}

// Ack completes the task. It returns ErrLeaseLost if the lease already expired or was settled,
// or ErrEngineStopped if Stop ended the lease (the task is then redelivered after a reload).
func (d *Delivery) Ack() error {
	if !d.settle() {
		return d.lost()
	}
	d.stopTimer()
	d.release()
	if !d.SyncMatched {
		d.task.finish(taskFinishResult{consumedToken: true})
	}
	h := d.pqm.metricsHandler
	h.Counter(metrics.DeliveryAcked, 1)
	h.Timer(metrics.DeliveryAckLatency, d.engine.timeSource.Since(d.deliveredAt))
	return nil
}

// Nack releases the task for immediate redelivery.
func (d *Delivery) Nack() error {
	if !d.settle() {
		return d.lost()
	}
	d.stopTimer()
	d.release()
	d.pqm.metricsHandler.Counter(metrics.DeliveryNacked, 1)
	d.redeliver(errNacked)
	return nil
}

// ExtendLease pushes the lease deadline to now+dur. dur must be positive.
func (d *Delivery) ExtendLease(dur time.Duration) error {
	if dur <= 0 {
		return &store.InvalidArgumentError{Msg: fmt.Sprintf("lease extension must be positive, got %v", dur)}
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.settled {
		return d.lostErr
	}
	d.deadline = d.engine.timeSource.Now().Add(dur)
	d.timer.Reset(dur)
	d.pqm.metricsHandler.Counter(metrics.DeliveryLeaseExtended, 1)
	return nil
}

func (d *Delivery) expire() {
	d.mu.Lock()
	// A callback from before an ExtendLease can still run after it: the deadline decides
	// (REVIEW M2). The reset timer fires again at the new deadline.
	if d.settled || d.engine.timeSource.Now().Before(d.deadline) {
		d.mu.Unlock()
		return
	}
	d.settled = true
	d.lostErr = ErrLeaseLost
	d.mu.Unlock()
	d.pqm.metricsHandler.Counter(metrics.DeliveryLeaseExpired, 1)
	d.release()
	go d.redeliver(errLeaseExpired) // not under the timer callback
}

func (d *Delivery) redeliver(err error) {
	if d.SyncMatched {
		d.engine.respoolSyncTaskLocked(d, false)
		return
	}
	d.task.finish(taskFinishResult{err: err, consumedToken: true})
}
