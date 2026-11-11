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
	TaskID int64
	// Task is the task; Task.Attempts counts deliveries including this one (1 on the first).
	Task        store.Task
	SyncMatched bool

	engine *Engine
	pqm    *physicalTaskQueueManagerImpl
	task   *internalTask
	timer  clock.Timer

	deliveredAt time.Time

	// released is closed by release, after the settling side effect.
	released chan struct{}

	mu       sync.Mutex
	settled  bool
	lostErr  error // returned by Ack/Nack/ExtendLease once settled
	deadline time.Time
}

// newDelivery leases a matched task to a poller. It fails with ErrEngineStopped once leasing is
// closed: Shutdown has decided the drain is over, or Stop has snapshotted the leases. A refused
// sync-matched task is finished with that error, so the partition manager spools it (or AddTask
// reports the error) instead of handing out an in-memory-only task that Stop would never
// re-spool (REVIEW M1); a refused backlog task is returned to its reader.
func (e *Engine) newDelivery(task *internalTask, pqm *physicalTaskQueueManagerImpl, queue store.QueueKey, lease time.Duration) (*Delivery, error) {
	if lease <= 0 {
		lease = e.config.DeliveryLeaseDuration
	}
	now := e.timeSource.Now()
	task.event.Data.Attempts++
	d := &Delivery{
		Queue:       queue,
		Task:        task.event.Data,
		SyncMatched: task.isSyncMatchTask(),
		engine:      e,
		pqm:         pqm,
		task:        task,
		deliveredAt: now,
		deadline:    now.Add(lease),
		released:    make(chan struct{}),
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
		task.event.Data.Attempts-- // not delivered
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
	e := d.engine
	e.leasesLock.Lock()
	delete(e.leases, d)
	e.maybeWakeDrainLocked()
	e.leasesLock.Unlock()
	d.pqm.MarkAlive()
	close(d.released)
}

// Ack completes the task. It returns ErrLeaseLost if the lease already expired or was settled,
// or ErrEngineStopped if Stop ended the lease (the task is then redelivered after a reload).
// The backlog finish runs before release so a waiting Shutdown cannot Stop until the ack is
// recorded; a sync-matched task was finished in newDelivery.
func (d *Delivery) Ack() error {
	if !d.settle() {
		return d.lost()
	}
	d.stopTimer()
	if !d.SyncMatched {
		d.task.finish(taskFinishResult{consumedToken: true})
	}
	h := d.pqm.metricsHandler
	h.Counter(metrics.DeliveryAcked, 1)
	h.Timer(metrics.DeliveryAckLatency, d.engine.timeSource.Since(d.deliveredAt))
	d.release()
	return nil
}

// NackOption configures Nack.
type NackOption func(*nackOptions)

type nackOptions struct {
	reason   string
	delay    time.Duration
	hasDelay bool
}

// NackDelay redelivers the task no earlier than d from now instead of immediately (or after
// Config.RetryBackoff); NackDelay(0) redelivers immediately even with a backoff configured.
// For a backlog task the delay is written to its row (Task.VisibleAt). A sync-matched task is
// spooled with that VisibleAt. It does not apply when this Nack dead-letters the task.
func NackDelay(d time.Duration) NackOption {
	return func(o *nackOptions) { o.delay, o.hasDelay = max(d, 0), true }
}

// NackReason records why the consumer gave up on the task. It is kept only if this Nack
// dead-letters the task (DeadTask.Reason "nack: <reason>").
func NackReason(reason string) NackOption {
	return func(o *nackOptions) { o.reason = reason }
}

// Nack releases the task for immediate redelivery, or, if this was its last attempt
// (QueueConfig.MaxAttempts), dead-letters it. Redeliver (backlog back to the matcher or to the
// dead-letter table, sync-matched spooled) runs before release so Shutdown cannot Stop in
// between.
func (d *Delivery) Nack(opts ...NackOption) error {
	var o nackOptions
	for _, opt := range opts {
		opt(&o)
	}
	if !d.settle() {
		return d.lost()
	}
	d.stopTimer()
	delay := d.engine.config.RetryBackoff.delay(d.Task.Attempts)
	if o.hasDelay {
		delay = o.delay
	}
	d.redeliver(errNacked, DeadReasonNack, o.reason, delay)
	d.pqm.metricsHandler.Counter(metrics.DeliveryNacked, 1)
	d.release()
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
	// Not under the timer callback. Redeliver before release so a waiting Shutdown cannot
	// Stop until the task is back on the backlog (or spooled, for a sync match).
	go func() {
		d.redeliver(errLeaseExpired, DeadReasonLeaseExpired, "", d.engine.config.RetryBackoff.delay(d.Task.Attempts))
		d.release()
	}()
}

// redeliver returns a failed delivery's task to the queue, or dead-letters a backlog task that
// used its last attempt. A sync-matched task has no backlog row to move: it is spooled with its
// attempt count and dead-lettered when next matched (reason attempts_exhausted).
func (d *Delivery) redeliver(err error, reason, detail string, delay time.Duration) {
	var visibleAt time.Time
	if delay > 0 {
		visibleAt = d.engine.timeSource.Now().Add(delay)
	}
	if d.SyncMatched {
		d.engine.respoolSyncTask(d, false, visibleAt)
		return
	}
	if max := d.pqm.maxAttempts(); max > 0 && d.Task.Attempts >= max {
		if d.engine.deadLetter(d.pqm, d.task, reason, detail, true) {
			return
		}
	}
	if !visibleAt.IsZero() {
		d.engine.delayRedelivery(d.pqm, d.task, visibleAt)
	}
	d.task.finish(taskFinishResult{err: err, consumedToken: true})
}
