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

import (
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

	mu       sync.Mutex
	settled  bool
	deadline time.Time
}

func (e *Engine) newDelivery(task *internalTask, pqm *physicalTaskQueueManagerImpl, queue store.QueueKey, lease time.Duration) *Delivery {
	if lease <= 0 {
		lease = e.config.DeliveryLeaseDuration
	}
	d := &Delivery{
		Queue:       queue,
		Task:        task.event.Data,
		SyncMatched: task.isSyncMatchTask(),
		engine:      e,
		pqm:         pqm,
		task:        task,
		deadline:    e.timeSource.Now().Add(lease),
	}
	if !d.SyncMatched {
		d.TaskID = task.event.ID
		if f := task.event.leaseFunc; f != nil {
			f(task) // before the timer is armed, so expiry always follows the lease
		}
	}
	pqm.outstandingLeases.Add(1)
	d.mu.Lock()
	d.timer = e.timeSource.AfterFunc(lease, d.expire)
	d.mu.Unlock()
	if d.SyncMatched {
		e.syncLeasesLock.Lock()
		e.syncLeases[d] = struct{}{}
		e.syncLeasesLock.Unlock()
		// The task is "started": let the AddTask caller return (upstream: finish after
		// RecordTaskStarted succeeds).
		task.finish(taskFinishResult{consumedToken: true})
	}
	return d
}

// LeaseDeadline returns when the lease expires.
func (d *Delivery) LeaseDeadline() time.Time {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.deadline
}

// settle marks the delivery settled; returns false if it already was.
func (d *Delivery) settle() bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.settled {
		return false
	}
	d.settled = true
	return true
}

func (d *Delivery) stopTimer() {
	d.mu.Lock()
	d.timer.Stop()
	d.mu.Unlock()
}

func (d *Delivery) release() {
	d.pqm.outstandingLeases.Add(-1)
	if d.SyncMatched {
		d.engine.syncLeasesLock.Lock()
		delete(d.engine.syncLeases, d)
		d.engine.syncLeasesLock.Unlock()
	}
	d.pqm.MarkAlive()
}

// Ack completes the task. It returns ErrLeaseLost if the lease already expired or was settled.
func (d *Delivery) Ack() error {
	if !d.settle() {
		return ErrLeaseLost
	}
	d.stopTimer()
	d.release()
	if !d.SyncMatched {
		d.task.finish(taskFinishResult{consumedToken: true})
	}
	return nil
}

// Nack releases the task for immediate redelivery.
func (d *Delivery) Nack() error {
	if !d.settle() {
		return ErrLeaseLost
	}
	d.stopTimer()
	d.release()
	d.redeliver(errNacked)
	return nil
}

// ExtendLease pushes the lease deadline to now+dur.
func (d *Delivery) ExtendLease(dur time.Duration) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.settled {
		return ErrLeaseLost
	}
	d.deadline = d.engine.timeSource.Now().Add(dur)
	d.timer.Reset(dur)
	return nil
}

func (d *Delivery) expire() {
	if !d.settle() {
		return
	}
	d.engine.metricsHandler.Counter(metrics.DeliveryLeaseExpired, 1)
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
