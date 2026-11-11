package matching

// DescribeQueue and ListQueues are modelled on upstream DescribeTaskQueue (enhanced mode,
// ReportStats) and the admin ListTaskQueue persistence call. A loaded queue is described from
// memory (upstream physicalTaskQueueManager.GetStatsByPriority); an unloaded one from its
// persisted metadata, read without loading the queue, taking ownership or bumping its rangeID.

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"

	"time"

	"github.com/skme902/tq/store"
)

// describeAgePeekTasks bounds the tasks read per subqueue to estimate an unloaded queue's
// backlog age.
const describeAgePeekTasks = 100

// QueueStats are upstream TaskQueueStats.
type QueueStats struct {
	// ApproximateBacklogCount is the persisted-and-maintained approximate number of backlog
	// tasks (upstream's counter, not an exact count).
	ApproximateBacklogCount int64
	// ApproximateBacklogAge is the age of the oldest backlog task known: for a loaded queue the
	// oldest task in memory (as upstream), for an unloaded one the oldest of the first
	// describeAgePeekTasks tasks per subqueue in dispatch order. 0 if the backlog is empty.
	ApproximateBacklogAge time.Duration
	// TasksAddRate and TasksDispatchRate are tasks/s over the last 30 s (upstream task
	// trackers: 5 s buckets). Only loaded queues track them; an unloaded queue reports 0.
	TasksAddRate      float32
	TasksDispatchRate float32
	// RateLimitingActive is true if a rate limit held back a dispatch in the last 30 s.
	RateLimitingActive bool
}

// merge is upstream taskqueue.MergeStats: counts and rates add, age is the max.
func (s *QueueStats) merge(o QueueStats) {
	s.ApproximateBacklogCount += o.ApproximateBacklogCount
	s.ApproximateBacklogAge = max(s.ApproximateBacklogAge, o.ApproximateBacklogAge)
	s.TasksAddRate += o.TasksAddRate
	s.TasksDispatchRate += o.TasksDispatchRate
	s.RateLimitingActive = s.RateLimitingActive || o.RateLimitingActive
}

// QueueDescription is a point-in-time snapshot of one queue.
type QueueDescription struct {
	Queue store.QueueKey
	// Loaded is true if this Engine has the queue loaded (and so owns it). For an unloaded
	// queue the fields below that describe memory (pollers, loaded tasks, leases, rates) are 0.
	Loaded bool
	// RangeID is the queue's ownership epoch: the loaded value, or the persisted one. Another
	// engine instance taking the queue over increments it.
	RangeID int64
	// LastUpdateTime is when the queue metadata was last written (persisted value for an
	// unloaded queue; last successful write by this Engine for a loaded one).
	LastUpdateTime time.Time

	// Stats aggregates StatsByPriority.
	Stats QueueStats
	// StatsByPriority is keyed by priority key (1..PriorityLevels).
	StatsByPriority map[int32]QueueStats

	// EffectiveRateLimit is the queue's effective dispatch rate in tasks/s (+Inf when
	// unlimited) and RateLimitSource where it comes from (upstream
	// RateLimitManager.GetEffectiveRPSAndSource). For an unloaded queue it is computed from the
	// QueueConfigSource and admin rates; a worker rate is unknown until a poll.
	EffectiveRateLimit float64
	RateLimitSource    RateLimitSource
	// Paused and PausedFairnessKeys are the queue's QueueConfig pause settings.
	Paused             bool
	PausedFairnessKeys []string

	// WaitingPollers is the number of polls parked in the matcher.
	WaitingPollers int
	// LoadedTasks is the number of backlog tasks read into memory, not leased and not delayed.
	LoadedTasks int
	// MatcherTasks is the number of tasks in the matcher, ready to be handed out.
	MatcherTasks int
	// OutstandingLeases is the number of deliveries not yet acked, nacked or expired.
	OutstandingLeases int64

	// DeadTasks is the number of dead-lettered tasks (read from the store; 0 if the store
	// does not implement store.DeadLetterStore). Library addition.
	DeadTasks int64
}

// DescribeQueue returns a snapshot of a queue. A queue not loaded on this Engine is described
// from the store without loading it. It returns *store.NotFoundError if the queue has never
// been persisted.
func (e *Engine) DescribeQueue(ctx context.Context, queue store.QueueKey) (*QueueDescription, error) {
	e.partitionsLock.RLock()
	pm, ok := e.partitions[queue]
	stopped := e.stopped
	e.partitionsLock.RUnlock()
	if stopped {
		return nil, ErrEngineStopped
	}
	var (
		d   *QueueDescription
		err error
	)
	if ok && pm.defaultQueue.backlogMgr.isInitialized() {
		d = e.describeLoaded(pm)
	} else if d, err = e.describeUnloaded(ctx, queue); err != nil {
		return nil, err
	}
	if d.Loaded {
		// What dispatch uses: the rate limit manager's cache, refreshed on load and on
		// QueueConfigWatcher notifications (like EffectiveRateLimit).
		var keys map[string]struct{}
		d.Paused, keys = pm.rateLimitManager.pauseState()
		d.PausedFairnessKeys = slices.Sorted(maps.Keys(keys))
	} else if e.configSource != nil {
		cfg := e.configSource.QueueConfig(queue)
		d.Paused, d.PausedFairnessKeys = cfg.Paused, slices.Sorted(slices.Values(cfg.PausedFairnessKeys))
	}
	if e.deadLetters != nil {
		if d.DeadTasks, err = e.deadLetters.CountDeadTasks(ctx, queue); err != nil {
			return nil, err
		}
	}
	return d, nil
}

func (e *Engine) describeLoaded(pm *taskQueuePartitionManagerImpl) *QueueDescription {
	pq := pm.defaultQueue
	d := &QueueDescription{
		Queue:             pm.partition,
		Loaded:            true,
		StatsByPriority:   map[int32]QueueStats{},
		WaitingPollers:    pq.priMatcher.waitingPollers(),
		LoadedTasks:       int(pq.backlogMgr.BacklogCountHint()),
		MatcherTasks:      pq.priMatcher.matcherTasks(),
		OutstandingLeases: pq.outstandingLeases.Load(),
	}
	db := pq.backlogMgr.getDB()
	db.Lock()
	d.RangeID, d.LastUpdateTime = db.rangeID, db.lastWrite
	db.Unlock()
	for pri, s := range pq.statsByPriority() {
		d.StatsByPriority[pri] = *s
		d.Stats.merge(*s)
	}
	d.EffectiveRateLimit, d.RateLimitSource = pm.rateLimitManager.GetEffectiveRPSAndSource()
	return d
}

func (e *Engine) describeUnloaded(ctx context.Context, queue store.QueueKey) (*QueueDescription, error) {
	resp, err := e.store.GetQueue(ctx, queue)
	if err != nil {
		return nil, err
	}
	d := &QueueDescription{
		Queue:           queue,
		RangeID:         resp.RangeID,
		StatsByPriority: map[int32]QueueStats{},
	}
	if resp.Info == nil {
		return nil, fmt.Errorf("DescribeQueue %v: queue row has no metadata", queue)
	}
	d.LastUpdateTime = resp.Info.LastUpdateTime
	tqConfig := newTaskQueueConfig(e.config)
	for i, sq := range resp.Info.Subqueues {
		pri := int32(tqConfig.clipPriority(priorityKey(sq.Key.Priority)))
		s := d.StatsByPriority[pri]
		s.ApproximateBacklogCount += sq.ApproximateBacklogCount
		if sq.ApproximateBacklogCount > 0 {
			oldest, err := e.peekOldestCreateTime(ctx, queue, i, sq.FairAckLevel)
			if err != nil {
				return nil, err
			}
			if !oldest.IsZero() {
				s.ApproximateBacklogAge = max(s.ApproximateBacklogAge, e.timeSource.Since(oldest))
			}
		}
		d.StatsByPriority[pri] = s
		d.Stats.merge(s)
	}
	// A throwaway rate-limit manager computes the effective rate exactly as a load would; it
	// is never started with a watcher, so it holds no references.
	rlm := newRateLimitManager(e.configSource, queue, tqConfig, e.timeSource)
	rlm.Start(nil)
	rlm.UserDataChanged()
	d.EffectiveRateLimit, d.RateLimitSource = rlm.GetEffectiveRPSAndSource()
	return d, nil
}

// peekOldestCreateTime reads up to describeAgePeekTasks unacked tasks of one subqueue (from
// where the reader would start) and returns the oldest create time among them.
func (e *Engine) peekOldestCreateTime(ctx context.Context, queue store.QueueKey, subqueue int, ackLevel store.Level) (time.Time, error) {
	from := fairLevelFromStore(ackLevel).max(fairLevel{pass: 1, id: 0}).inc()
	res, err := e.store.GetTasks(ctx, &store.GetTasksRequest{
		Key:          queue,
		Subqueue:     subqueue,
		InclusiveMin: from.toStore(),
		PageSize:     describeAgePeekTasks,
	})
	if err != nil {
		var ia *store.InvalidArgumentError
		if errors.As(err, &ia) {
			return time.Time{}, nil // a corrupt row: report no age rather than fail (REVIEW L10)
		}
		return time.Time{}, err
	}
	var oldest time.Time
	for _, t := range res.Tasks {
		oldest = minNonZeroTime(oldest, t.Data.CreateTime)
	}
	return oldest, nil
}

// QueueListing is one row of ListQueues.
type QueueListing struct {
	Queue   store.QueueKey
	RangeID int64
	// ApproximateBacklogCount is the persisted value (summed over subqueues). For a queue loaded
	// on this Engine it can lag the live value by up to UpdateAckInterval.
	ApproximateBacklogCount int64
	LastUpdateTime          time.Time
	// Loaded is true if this Engine has the queue loaded.
	Loaded bool
}

// ListQueuesPage is one page of ListQueues.
type ListQueuesPage struct {
	Queues []QueueListing
	// NextPageToken is nil on the last page.
	NextPageToken []byte
}

// ListQueues pages through every queue persisted in the store, loaded or not (upstream
// persistence ListTaskQueue). It requires a store that implements store.QueueLister.
func (e *Engine) ListQueues(ctx context.Context, pageSize int, pageToken []byte) (*ListQueuesPage, error) {
	l, ok := e.store.(store.QueueLister)
	if !ok {
		return nil, &store.InvalidArgumentError{Msg: "ListQueues: the store does not implement store.QueueLister"}
	}
	resp, err := l.ListQueues(ctx, &store.ListQueuesRequest{PageSize: pageSize, PageToken: pageToken})
	if err != nil {
		return nil, err
	}
	page := &ListQueuesPage{NextPageToken: resp.NextPageToken, Queues: make([]QueueListing, 0, len(resp.Queues))}
	e.partitionsLock.RLock()
	defer e.partitionsLock.RUnlock()
	for _, q := range resp.Queues {
		if q.Info == nil {
			continue
		}
		row := QueueListing{Queue: q.Info.Key, RangeID: q.RangeID, LastUpdateTime: q.Info.LastUpdateTime}
		for _, sq := range q.Info.Subqueues {
			row.ApproximateBacklogCount += sq.ApproximateBacklogCount
		}
		_, row.Loaded = e.partitions[q.Info.Key]
		page.Queues = append(page.Queues, row)
	}
	return page, nil
}
