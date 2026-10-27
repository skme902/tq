// Ported from temporalio/temporal v1.32.1 af08a5e5, service/matching/fair_task_writer.go
// (writeTaskRequest/taskIDBlock/noTaskIDs from task_writer.go, rangeIDToTaskIDBlock from
// backlog_manager.go). Changes: protos → store types; the TopK persist ticker is replaced by a
// clock check after each batch; metrics trimmed.
// Copyright (c) 2025 Temporal Technologies Inc. MIT License; see UPSTREAM_LICENSE and NOTICE.

package matching

import (
	"context"
	"hash/maphash"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/skme902/tq/internal/backoff"
	"github.com/skme902/tq/internal/counter"
	"github.com/skme902/tq/metrics"
	"github.com/skme902/tq/store"
)

type (
	writeTaskRequest struct {
		taskInfo   *store.Task
		responseCh chan<- error
		subqueue   subqueueIndex
		fairLevel  // filled in by taskWriterLoop
	}

	taskIDBlock struct {
		start int64
		end   int64
	}

	// fairTaskWriter writes tasks with stride scheduling
	fairTaskWriter struct {
		backlogMgr     *fairBacklogManagerImpl
		config         *taskQueueConfig
		db             *taskQueueDB
		logger         *slog.Logger
		counterFactory func(subqueueIndex) counter.Counter
		appendCh       chan *writeTaskRequest

		// state:
		taskIDBlock        taskIDBlock
		currentTaskIDBlock taskIDBlock                       // copy of taskIDBlock for safe concurrent access via getCurrentTaskIDBlock()
		counters           map[subqueueIndex]counter.Counter // only used in taskWriterLoop.
		ditherSeed         maphash.Seed                      // seed for fairness pass dither (see pickPasses).
	}
)

var (
	noTaskIDs = taskIDBlock{start: 1, end: 0}

	// backlog_manager.go
	persistenceOperationRetryPolicy = backoff.Policy{Initial: 50 * time.Millisecond, Max: time.Second, Expiration: 30 * time.Second}
	foreverRetryPolicy              = backoff.Policy{Initial: time.Second, Max: 10 * time.Second, Expiration: backoff.NoInterval}
)

func rangeIDToTaskIDBlock(rangeID int64, rangeSize int64) taskIDBlock {
	return taskIDBlock{
		start: (rangeID-1)*rangeSize + 1,
		end:   rangeID * rangeSize,
	}
}

func newFairTaskWriter(
	backlogMgr *fairBacklogManagerImpl,
	counterFactory func(subqueueIndex) counter.Counter,
) *fairTaskWriter {
	return &fairTaskWriter{
		backlogMgr:     backlogMgr,
		config:         backlogMgr.config,
		db:             backlogMgr.db,
		logger:         backlogMgr.logger,
		counterFactory: counterFactory,
		appendCh:       make(chan *writeTaskRequest, backlogMgr.config.OutstandingTaskAppendsThreshold),

		taskIDBlock: noTaskIDs,
		counters:    make(map[subqueueIndex]counter.Counter),
		ditherSeed:  maphash.MakeSeed(),
	}
}

// Start fairTaskWriter background goroutine.
func (w *fairTaskWriter) Start() {
	go w.taskWriterLoop()
}

func (w *fairTaskWriter) appendTask(
	subqueue subqueueIndex,
	taskInfo *store.Task,
) error {
	select {
	case <-w.backlogMgr.tqCtx.Done():
		return errShutdown
	default:
		// noop
	}

	startTime := w.backlogMgr.timeSource.Now()
	ch := make(chan error, 1)
	req := &writeTaskRequest{
		taskInfo:   taskInfo,
		responseCh: ch,
		subqueue:   subqueue,
	}

	select {
	case w.appendCh <- req:
		select {
		case err := <-ch:
			w.backlogMgr.metricsHandler.Timer(metrics.TaskWriteLatency, w.backlogMgr.timeSource.Since(startTime))
			return err
		case <-w.backlogMgr.tqCtx.Done():
			// if we are shutting down, this request will never make
			// it to persistence, just bail out and fail this request
			return errShutdown
		}
	default: // channel is full, throttle
		w.backlogMgr.metricsHandler.Counter(metrics.TaskWriteThrottle, 1)
		return ErrResourceExhausted
	}
}

func (w *fairTaskWriter) allocTaskIDs(reqs []*writeTaskRequest) error {
	for i := range reqs {
		if w.taskIDBlock.start > w.taskIDBlock.end {
			// we ran out of current allocation block
			newBlock, err := w.allocTaskIDBlock(w.taskIDBlock.end)
			if err != nil {
				return err
			}
			w.taskIDBlock = newBlock
		}
		reqs[i].id = w.taskIDBlock.start
		w.taskIDBlock.start++
	}
	return nil
}

func (w *fairTaskWriter) pickPasses(tasks []*writeTaskRequest, bases []fairLevel) {
	// Fetch latest fairness weight overrides
	overrides := w.backlogMgr.pqMgr.GetFairnessWeightOverrides()
	dither := w.config.FairnessPassDither

	for i, task := range tasks {
		pri := task.taskInfo.Priority
		key := pri.FairnessKey
		weight := getEffectiveWeight(overrides, pri)
		inc := max(1, int64(strideFactor/weight))
		base := bases[task.subqueue].pass
		if dither {
			base = ditherPass(w.ditherSeed, key, base, inc)
		}
		cntr := w.counters[task.subqueue]
		if cntr == nil {
			cntr = w.counterFactory(task.subqueue)
			w.counters[task.subqueue] = cntr
		}
		pass := cntr.GetPass(key, base, inc)
		softAssert(w.logger, pass >= base, "counter returned pass below base")
		tasks[i].pass = pass
	}
}

func (w *fairTaskWriter) initState() error {
	state, err := w.renewLeaseWithRetry(foreverRetryPolicy, isPersistenceTransient)
	if err != nil {
		w.backlogMgr.initState(taskQueueState{}, err)
		return err
	}
	w.taskIDBlock = rangeIDToTaskIDBlock(state.rangeID, w.config.RangeSize)
	w.currentTaskIDBlock = w.taskIDBlock
	w.backlogMgr.initState(state, nil)
	return nil
}

func (w *fairTaskWriter) taskWriterLoop() {
	if w.initState() != nil {
		return
	}

	// Deviation: upstream uses a time.Ticker(UpdateAckInterval) checked non-blockingly after each
	// batch; we check elapsed time on the injected clock, which is equivalent.
	lastPersistFairnessKeys := w.backlogMgr.timeSource.Now()

	var reqs []*writeTaskRequest
	for {
		atomic.StoreInt64(&w.currentTaskIDBlock.start, w.taskIDBlock.start)
		atomic.StoreInt64(&w.currentTaskIDBlock.end, w.taskIDBlock.end)

		// prepare slice for reuse
		clear(reqs)
		reqs = reqs[:0]

		select {
		case <-w.backlogMgr.tqCtx.Done():
			return
		case req := <-w.appendCh:
			// read a batch of requests from the channel
			reqs = append(reqs, req)
			reqs = w.getWriteBatch(reqs)
		}

		err := w.allocTaskIDs(reqs)
		if err == nil {
			err = w.writeBatch(reqs)
		}

		// maybe persist fairness key counts if it's time. Deviation: done before responding (it
		// is an in-memory update) so a caller whose AddTask returned knows whether its batch's
		// counts were included; upstream checks a ticker after responding.
		if w.backlogMgr.timeSource.Since(lastPersistFairnessKeys) >= w.config.UpdateAckInterval {
			lastPersistFairnessKeys = w.backlogMgr.timeSource.Now()
			for subqueue, cntr := range w.counters {
				w.db.persistTopKFairnessKeys(subqueue, cntr.TopK())
			}
		}

		for _, req := range reqs {
			req.responseCh <- err
		}
	}
}

func (w *fairTaskWriter) getWriteBatch(reqs []*writeTaskRequest) []*writeTaskRequest {
	for range w.config.MaxTaskBatchSize - 1 {
		select {
		case req := <-w.appendCh:
			reqs = append(reqs, req)
		default: // channel is empty, don't block
			return reqs
		}
	}
	return reqs
}

func (w *fairTaskWriter) writeBatch(reqs []*writeTaskRequest) (retErr error) {
	bases, unpin := w.backlogMgr.getAndPinAckLevels()
	defer func() { unpin(retErr) }()

	w.pickPasses(reqs, bases)
	resp, err := w.db.CreateFairTasks(w.backlogMgr.tqCtx, reqs)
	if err == nil {
		w.backlogMgr.wroteNewTasks(resp) // must be called before unpin()
	} else {
		w.logger.Error("Persistent store operation failure", "op", "CreateTasks", "error", err)
		w.backlogMgr.signalIfFatal(err)
	}
	return err
}

func (w *fairTaskWriter) renewLeaseWithRetry(
	retryPolicy backoff.Policy,
	retryErrors func(error) bool,
) (taskQueueState, error) {
	var newState taskQueueState
	op := func(ctx context.Context) (err error) {
		newState, err = w.db.RenewLease(ctx)
		return
	}
	w.backlogMgr.metricsHandler.Counter(metrics.LeaseRequests, 1)
	err := backoff.ThrottleRetryContext(w.backlogMgr.tqCtx, op, retryPolicy, retryErrors)
	if err != nil {
		w.backlogMgr.metricsHandler.Counter(metrics.LeaseFailures, 1)
		return newState, err
	}
	return newState, nil
}

func (w *fairTaskWriter) allocTaskIDBlock(prevBlockEnd int64) (taskIDBlock, error) {
	currBlock := rangeIDToTaskIDBlock(w.db.RangeID(), w.config.RangeSize)
	if currBlock.end != prevBlockEnd {
		return taskIDBlock{}, errNonContiguousBlocks
	}
	state, err := w.renewLeaseWithRetry(persistenceOperationRetryPolicy, isPersistenceTransient)
	if err != nil {
		if w.backlogMgr.signalIfFatal(err) {
			return taskIDBlock{}, errShutdown
		}
		return taskIDBlock{}, err
	}
	return rangeIDToTaskIDBlock(state.rangeID, w.config.RangeSize), nil
}

// getCurrentTaskIDBlock returns the current taskIDBlock. Safe to be called concurrently.
func (w *fairTaskWriter) getCurrentTaskIDBlock() taskIDBlock {
	return taskIDBlock{
		start: atomic.LoadInt64(&w.currentTaskIDBlock.start),
		end:   atomic.LoadInt64(&w.currentTaskIDBlock.end),
	}
}
