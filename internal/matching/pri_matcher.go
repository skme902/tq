// Ported from temporalio/temporal v1.32.1 af08a5e5, service/matching/pri_matcher.go
// Changes: query/nexus offers, poll-forwarding to parents, priority-backlog forwarders and metrics
// are removed (single partition, slice 2). Task forwarding keeps its structure behind
// PartitionRouter but is not started (fwdr is always nil: partitions are not implemented).
// Copyright (c) 2025 Temporal Technologies Inc. MIT License; see UPSTREAM_LICENSE and NOTICE.

package matching

import (
	"context"
	"log/slog"
	"time"

	"github.com/skme902/tq/clock"
	"github.com/skme902/tq/internal/backoff"
	"github.com/skme902/tq/metrics"
	"github.com/skme902/tq/store"
)

// priTaskMatcher matches a task producer with a task consumer
// Producers are usually AddTask calls or the taskReader that drains backlog from db.
// Consumers are the task queue pollers.
type priTaskMatcher struct {
	config *taskQueueConfig

	// holds waiting polls and tasks
	data matcherData

	// Background context used for forwarding tasks. Closed when task queue is closed.
	tqCtx context.Context

	queue          store.QueueKey
	fwdr           PartitionRouter // nil: root / single partition
	validator      Validator
	metricsHandler metrics.Handler
	logger         *slog.Logger
	markAlive      func() // function to mark the physical task queue alive
}

type waitingPoller struct {
	waitableMatchResult
	prev, next        *waitingPoller // intrusive links for pollerList; nil when not queued
	startTime         time.Time
	forwardCtx        context.Context   // non-nil iff poll can be forwarded
	pollMetadata      *pollMetadata     // non-nil iff poll can be forwarded
	queued            bool              // true while in matcherData.pollers
	taskForwarderType taskForwarderType // type of task forwarder (if any)
}

type matchResult struct {
	task      *internalTask
	poller    *waitingPoller
	ctxErr    error // set if context timed out/canceled or reprocess task
	ctxErrIdx int   // index of context that closed first
}

// pollConditions replaces matchingservice.PollConditions.
type pollConditions struct {
	MinPriority int32
	NoWait      bool
}

// pollMetadata replaces upstream pollMetadata (only the fields used without versioning).
type pollMetadata struct {
	identity      string
	conditions    *pollConditions
	forwardedFrom string
	// maxTasksPerSecond replaces taskQueueMetadata.MaxTasksPerSecond (nil = not set).
	maxTasksPerSecond *float64
}

// newPriTaskMatcher returns a task matcher instance
func newPriTaskMatcher(
	tqCtx context.Context,
	config *taskQueueConfig,
	queue store.QueueKey,
	fwdr PartitionRouter,
	validator Validator,
	logger *slog.Logger,
	metricsHandler metrics.Handler,
	timeSource clock.TimeSource,
	rateLimitManager *rateLimitManager,
	onRateLimited func(),
	markAlive func(),
) *priTaskMatcher {
	return &priTaskMatcher{
		config:         config,
		data:           newMatcherData(config, logger, timeSource, fwdr != nil, rateLimitManager, onRateLimited),
		tqCtx:          tqCtx,
		logger:         logger,
		metricsHandler: metricsHandler,
		queue:          queue,
		fwdr:           fwdr,
		validator:      validator,
		markAlive:      markAlive,
	}
}

func (tm *priTaskMatcher) Start() {
	retrier := backoff.NewRetrier(backoff.Policy{
		Initial: time.Second,
		Max:     tm.config.BacklogTaskForwardTimeout,
	})
	if tm.fwdr == nil {
		// Root doesn't forward. But it does need something to validate tasks.
		// Deviation: upstream starts this loop even with a nil validator (and then just
		// re-adds tasks); we only start it when a Validator is configured.
		if tm.validator != nil {
			go tm.validateTasksOnRoot(retrier)
		}
		return
	}
	// Non-root partitions: task/poll forwarding is not implemented.
}

func (tm *priTaskMatcher) Stop() {
	tm.data.Stop()
	// When we're stopping, sync tasks and pollers will be cancelled by tqCtx being canceled.
	// Backlog tasks held in this matcher will be dropped. That's okay: they're still in the
	// store and will be re-read when the queue reloads.
}

func (tm *priTaskMatcher) validateTasksOnRoot(retrier *backoff.Retrier) {
	ctxs := []context.Context{tm.tqCtx}
	poller := &waitingPoller{taskForwarderType: validatorTaskForwarder}
	for {
		res := tm.data.EnqueuePollerAndWait(ctxs, poller)
		if res.ctxErr != nil {
			return // task queue closing
		}
		if !softAssert(tm.logger, res.task != nil, "expected a task from match") {
			continue
		}

		task := res.task
		if !softAssert(tm.logger, task.forwardCtx == nil, "expected non-forwarded task") ||
			!softAssert(tm.logger, !task.isSyncMatchTask(), "expected non-sync match task") ||
			!softAssert(tm.logger, task.source == taskSourceDBBacklog, "expected backlog task") {
			continue
		}

		maybeValid := tm.validator == nil || tm.validator.MaybeValid(tm.queue, task.event.TaskInfo)
		if !maybeValid {
			// We found an invalid one, complete it and go back for another immediately.
			task.finish(taskFinishResult{dropReason: droppedTaskExpiryReason(task, tm.data.timeSource.Now())})

			// Stay alive as long as we're invalidating tasks
			tm.markAlive()

			retrier.Reset()
		} else {
			// Task was valid, put it back and slow down checking.
			task.finish(taskFinishResult{err: errReprocessTask, consumedToken: true})
			// retrier's max interval is backlogTaskForwardTimeout, so for just valid tasks,
			// this loop will essentially be limited to that interval.
			_ = backoff.InterruptibleSleep(tm.tqCtx, retrier.NextBackOff())
		}
	}
}

// Offer offers a task to a potential consumer (poller)
// If the task is successfully matched with a consumer, this
// method will return true and no error. If the task is matched
// but consumer returned error, then this method will return
// true and error message. This method should ONLY be used for sync match.
//
// Cases when this method will block:
//
// Forwarded tasks that originated from db backlog:
// When this method is called with a task that is forwarded from a
// remote partition and if (1) this task queue is root (2) task
// was from db backlog - this method will block until context timeout
// trying to match with a poller. The caller is expected to set the
// correct context timeout.
func (tm *priTaskMatcher) Offer(ctx context.Context, task *internalTask) (syncMatchOutcome, error) {
	finish := func() (syncMatchOutcome, error) {
		res, ok := task.getResponse()
		if !softAssert(tm.logger, ok, "expected a sync match task") {
			return syncMatchUnspecified, nil
		}
		if res.forwarded {
			if res.forwardErr == nil {
				// task was remotely sync matched on the parent partition
				return syncMatchSuccess, nil
			}
			return syncMatchNoPoller, nil // forward error, give up here
		}
		// TODO (upstream): when startErr is non-nil, the task was not actually
		// dispatched. This should return a failure outcome instead of syncMatchSuccess.
		// We keep upstream's outcome; the partition manager spools on startErr (see AddTask).
		return syncMatchSuccess, res.startErr
	}

	// Fast path if we have a waiting poller (or forwarder).
	task.forwardCtx = ctx
	outcome := tm.data.MatchTaskImmediately(task)
	switch outcome {
	case syncMatchSuccess:
		return finish()
	case syncMatchBacklogPresent:
		return outcome, nil
	default:
		// We only block if we are the root and the task is forwarded from a backlog.
		// Otherwise, stop here.
		if tm.isForwardingAllowed() ||
			task.source != taskSourceDBBacklog ||
			!task.isForwarded() {
			return outcome, nil
		}
	}

	res := tm.data.EnqueueTaskAndWait([]context.Context{ctx, tm.tqCtx}, task)
	if res.ctxErr != nil {
		return syncMatchNoPoller, res.ctxErr
	}
	if !softAssert(tm.logger, res.poller != nil, "expeced poller from match") {
		return syncMatchNoPoller, nil
	}

	return finish()
}

func (tm *priTaskMatcher) AddTask(task *internalTask) error {
	if !task.setRemoveFunc(func() { tm.data.RemoveTask(task) }) {
		return nil // handle race where task is evicted from reader before being added
	}
	return tm.data.EnqueueTaskNoWait(task)
}

// Poll blocks until a task is found or context deadline is exceeded
// Returns ErrNoTasks when context deadline is exceeded
func (tm *priTaskMatcher) Poll(ctx context.Context, pollMetadata *pollMetadata) (*internalTask, error) {
	poller := &waitingPoller{
		startTime:    tm.data.timeSource.Now(),
		forwardCtx:   ctx,
		pollMetadata: pollMetadata,
	}

	var res *matchResult
	if pollMetadata.conditions != nil && pollMetadata.conditions.NoWait {
		res = tm.data.MatchPollerImmediately(poller)
	} else {
		ctxs := []context.Context{ctx, tm.tqCtx}
		res = tm.data.EnqueuePollerAndWait(ctxs, poller)
	}

	if res == nil {
		return nil, ErrNoTasks // only possible for MatchPollerImmediately
	} else if res.ctxErr != nil {
		if res.ctxErrIdx == 0 {
			tm.metricsHandler.Counter(metrics.PollTimeout, 1)
		}
		return nil, ErrNoTasks
	}

	if !softAssert(tm.logger, res.task != nil, "expected task from match") {
		return nil, errInternalMatchError
	}

	task := res.task
	if task.isSyncMatchTask() {
		tm.metricsHandler.Counter(metrics.PollSuccessWithSync, 1)
	}
	tm.metricsHandler.Counter(metrics.PollSuccess, 1)
	return task, nil
}

// ReprocessAllTasks re-adds all in-matcher tasks (used on config changes upstream).
func (tm *priTaskMatcher) ReprocessAllTasks() {
	tasks := tm.data.ReprocessTasks(func(task *internalTask) (shouldRemove bool) {
		return true
	})
	// ReprocessTasks will have woken sync tasks, but for backlog we also need to call finish.
	for _, task := range tasks {
		if !task.isSyncMatchTask() {
			task.finish(taskFinishResult{err: errReprocessTask, consumedToken: true})
		}
	}
}

func (tm *priTaskMatcher) HasWaitingPoller() bool {
	return tm.data.HasWaitingPoller()
}

func (tm *priTaskMatcher) isForwardingAllowed() bool {
	return tm.fwdr != nil
}

func (p *waitingPoller) minPriority() priorityKey {
	if p.pollMetadata == nil || p.pollMetadata.conditions == nil {
		return 0
	}
	return priorityKey(p.pollMetadata.conditions.MinPriority)
}

// waitingPollers is a library addition backing Engine.WaitingPollers.
func (tm *priTaskMatcher) waitingPollers() int {
	tm.data.lock.Lock()
	defer tm.data.lock.Unlock()
	return tm.data.pollers.Len()
}

// matcherTasks is a library addition backing Engine.MatcherTasks.
func (tm *priTaskMatcher) matcherTasks() int {
	tm.data.lock.Lock()
	defer tm.data.lock.Unlock()
	return tm.data.tasks.Len()
}
