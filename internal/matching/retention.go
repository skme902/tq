package matching

// Queue retention is a library addition with no upstream counterpart (upstream keeps idle
// task queue rows until their persistence TTL, which normal partitions do not have). See
// README "Retention".

import (
	"context"
	"fmt"
	"time"

	"github.com/skme902/tq/metrics"
	"github.com/skme902/tq/store"
)

// MinSweepIdle is the shortest SweepOptions.IdleFor accepted. A loaded queue rewrites its row
// at least every 12h (plus one UpdateAckInterval) even when nothing changes: SyncState's
// heartbeat is half its 24h default TTL, since normal partitions have persistence TTL 0. 48h
// tolerates a missed heartbeat (DB errors, clock skew), the same grace as upstream's task
// queue scavenger. A shorter window could delete the row under a quiet owner that would then
// share rangeID 1 with whoever recreates the queue.
const MinSweepIdle = 48 * time.Hour

// SweepOptions configures SweepIdleQueues.
type SweepOptions struct {
	// IdleFor is how long a queue's row must have gone unwritten; at least MinSweepIdle.
	IdleFor time.Duration
	// MaxDeletes caps the queues deleted in one sweep; 0 means no cap.
	MaxDeletes int
	// DeadTaskRetention, if positive, first deletes dead tasks older than this from every
	// listed queue (loaded or not), so queues whose dead tasks age out become sweepable.
	DeadTaskRetention time.Duration
	// PageSize is the ListQueues page size (default 100).
	PageSize int
}

// SweepResult is what one SweepIdleQueues call did.
type SweepResult struct {
	Scanned         int
	Deleted         []store.QueueKey
	DeadTasksPurged int
}

// SweepIdleQueues deletes queues that are idle and empty: not loaded on this Engine, row
// unwritten for opts.IdleFor, no task above its ack level, and no dead tasks. Each delete is
// one store transaction conditioned on the listed rangeID and the idle time, so a queue that
// another process loaded or wrote since the listing is kept. It never loads or takes over a
// queue. A queue deleted here is recreated empty by its next AddTask or PollTask. On error the
// work done so far is returned with it. Needs a store implementing store.QueueLister and
// store.QueueSweeper (memstore, sqlite and postgres do).
func (e *Engine) SweepIdleQueues(ctx context.Context, opts SweepOptions) (SweepResult, error) {
	var res SweepResult
	if opts.IdleFor < MinSweepIdle {
		return res, &store.InvalidArgumentError{Msg: fmt.Sprintf("SweepIdleQueues: IdleFor must be at least %v, got %v", MinSweepIdle, opts.IdleFor)}
	}
	if opts.MaxDeletes < 0 || opts.DeadTaskRetention < 0 || opts.PageSize < 0 {
		return res, &store.InvalidArgumentError{Msg: "SweepIdleQueues: MaxDeletes, DeadTaskRetention and PageSize must be non-negative"}
	}
	lister, ok := e.store.(store.QueueLister)
	sweeper, ok2 := e.store.(store.QueueSweeper)
	if !ok || !ok2 {
		return res, &store.InvalidArgumentError{Msg: "SweepIdleQueues: the store does not implement store.QueueLister and store.QueueSweeper"}
	}
	var dls store.DeadLetterStore
	if opts.DeadTaskRetention > 0 {
		var err error
		if dls, err = e.deadLetterStore(); err != nil {
			return res, err
		}
	}
	if e.isStopped() {
		return res, ErrEngineStopped
	}
	pageSize := opts.PageSize
	if pageSize == 0 {
		pageSize = 100
	}
	now := e.timeSource.Now()
	idleBefore := now.Add(-opts.IdleFor)
	defer func() {
		e.metricsHandler.Counter(metrics.QueuesSwept, int64(len(res.Deleted)))
		e.metricsHandler.Counter(metrics.DeadTasksExpired, int64(res.DeadTasksPurged))
	}()
	var token []byte
	for {
		page, err := lister.ListQueues(ctx, &store.ListQueuesRequest{PageSize: pageSize, PageToken: token})
		if err != nil {
			return res, err
		}
		for _, q := range page.Queues {
			if q.Info == nil {
				continue
			}
			res.Scanned++
			if dls != nil {
				n, err := dls.DeleteDeadTasks(ctx, &store.DeleteDeadTasksRequest{Key: q.Info.Key, DeadBefore: now.Add(-opts.DeadTaskRetention)})
				res.DeadTasksPurged += n
				if err != nil {
					return res, err
				}
			}
			if !q.Info.LastUpdateTime.Before(idleBefore) {
				continue
			}
			deleted, err := e.deleteIfIdle(ctx, sweeper, q.Info.Key, q.RangeID, idleBefore)
			if err != nil {
				return res, err
			}
			if deleted {
				res.Deleted = append(res.Deleted, q.Info.Key)
				if opts.MaxDeletes > 0 && len(res.Deleted) >= opts.MaxDeletes {
					return res, nil
				}
			}
		}
		if token = page.NextPageToken; token == nil {
			return res, nil
		}
	}
}

// deleteIfIdle deletes the queue unless it is loaded here. partitionsLock is held (read)
// across the store call so this Engine cannot start loading the queue in between; a load
// elsewhere is excluded by the store's rangeID condition.
func (e *Engine) deleteIfIdle(ctx context.Context, sw store.QueueSweeper, key store.QueueKey, rangeID int64, idleBefore time.Time) (bool, error) {
	e.partitionsLock.RLock()
	defer e.partitionsLock.RUnlock()
	if e.stopped {
		return false, ErrEngineStopped
	}
	if _, loaded := e.partitions[key]; loaded {
		return false, nil
	}
	return sw.DeleteQueueIfIdle(ctx, &store.DeleteQueueIfIdleRequest{Key: key, RangeID: rangeID, IdleBefore: idleBefore})
}
