// Ported from temporalio/temporal v1.32.1 af08a5e5, service/matching/task_queue_partition_manager.go
// Changes: single default physical queue (no versioned queues, user data, scale manager, hooks,
// partition counts); AddTask keeps the sync-match-then-spool structure. User data changes are
// QueueConfigWatcher notifications; the initial user data load is a synchronous
// UserDataChanged in Start (upstream: async onUserDataChanged after userDataManager.Start).
// Copyright (c) 2025 Temporal Technologies Inc. MIT License; see UPSTREAM_LICENSE and NOTICE.

package matching

import (
	"context"
	"log/slog"
	"math/rand/v2"
	"sync/atomic"
	"time"

	"github.com/skme902/tq/metrics"
	"github.com/skme902/tq/store"
)

type taskQueuePartitionManagerImpl struct {
	engine         *Engine
	partition      store.QueueKey
	config         *taskQueueConfig
	logger         *slog.Logger
	metricsHandler metrics.Handler

	// rateLimitManager is used to manage the rate limit for task queues.
	rateLimitManager *rateLimitManager

	defaultQueue *physicalTaskQueueManagerImpl

	// metricsCtx ends the periodic backlog-metrics emission (upstream initCtx).
	metricsCtx    context.Context
	metricsCancel context.CancelFunc

	// maxAttempts is the effective QueueConfig.MaxAttempts / Config.MaxAttempts (0 =
	// unlimited), refreshed on load and on every config notification.
	maxAttempts atomic.Int32
}

func newTaskQueuePartitionManager(e *Engine, partition store.QueueKey, config *taskQueueConfig) *taskQueuePartitionManagerImpl {
	handler, breakdown := e.queueMetricsHandler(partition)
	config.BreakdownMetricsByTaskQueue = breakdown
	pm := &taskQueuePartitionManagerImpl{
		engine:         e,
		partition:      partition,
		config:         config,
		logger:         e.logger.With("queue", partition.String()),
		metricsHandler: handler,
	}
	pm.metricsCtx, pm.metricsCancel = context.WithCancel(context.Background())
	pm.rateLimitManager = newRateLimitManager(e.configSource, partition, config, e.timeSource)
	pm.defaultQueue = newPhysicalTaskQueueManager(pm, partition)
	// The engine default is the limit a negative QueueConfig override must not replace.
	pm.maxAttempts.Store(config.MaxAttempts)
	return pm
}

func (pm *taskQueuePartitionManagerImpl) Start() {
	pm.refreshQueueConfig()
	pm.rateLimitManager.Start(pm.userDataChanged)
	// initial user data load (upstream: userDataManager.Start -> onUserDataChanged)
	pm.rateLimitManager.UserDataChanged()
	pm.defaultQueue.Start()
	if pm.config.BreakdownMetricsByTaskQueue && pm.config.BacklogMetricsEmitInterval > 0 {
		go pm.emitLogicalBacklogMetrics(pm.metricsCtx)
	}
}

func (pm *taskQueuePartitionManagerImpl) Stop(unloadCause unloadCause) {
	pm.metricsCancel()
	pm.rateLimitManager.Stop()
	pm.defaultQueue.Stop(unloadCause)
	pm.emitZeroLogicalBacklog()
}

// emitLogicalBacklogMetrics is upstream's loop: every BacklogMetricsEmitInterval (±5% jitter,
// upstream backoff.Jitter(interval, 0.05)) emit the live per-priority backlog stats.
func (pm *taskQueuePartitionManagerImpl) emitLogicalBacklogMetrics(ctx context.Context) {
	for {
		interval := pm.config.BacklogMetricsEmitInterval
		d := time.Duration(float64(interval) * (0.95 + 0.1*rand.Float64()))
		ch, t := pm.engine.timeSource.NewTimer(d)
		select {
		case <-ctx.Done():
			t.Stop()
			return
		case <-ch:
			pm.fetchAndEmitLogicalBacklogMetrics()
		}
	}
}

// fetchAndEmitLogicalBacklogMetrics is upstream's without version attribution: the backlog
// count per task_priority and the backlog age (oldest across priorities) from the same stats
// DescribeQueue returns.
func (pm *taskQueuePartitionManagerImpl) fetchAndEmitLogicalBacklogMetrics() {
	if !pm.config.BreakdownMetricsByTaskQueue || !pm.defaultQueue.backlogMgr.isInitialized() {
		return
	}
	var age time.Duration
	for pri, stats := range pm.defaultQueue.backlogMgr.BacklogStatsByPriority() {
		pm.metricsHandler.Gauge(metrics.ApproximateBacklogCount, float64(stats.ApproximateBacklogCount), metrics.TaskPriorityTag(pri))
		age = max(age, stats.ApproximateBacklogAge)
	}
	pm.metricsHandler.Gauge(metrics.ApproximateBacklogAgeSeconds, age.Seconds())
}

// emitZeroLogicalBacklog is upstream emitZeroLogicalBacklogForQueue: zero the gauges on unload
// so stale values do not persist.
func (pm *taskQueuePartitionManagerImpl) emitZeroLogicalBacklog() {
	if !pm.config.BreakdownMetricsByTaskQueue || pm.config.BacklogMetricsEmitInterval <= 0 {
		return
	}
	for pri := range pm.defaultQueue.backlogMgr.BacklogStatsByPriority() {
		pm.metricsHandler.Gauge(metrics.ApproximateBacklogCount, 0, metrics.TaskPriorityTag(pri))
	}
	pm.metricsHandler.Gauge(metrics.ApproximateBacklogAgeSeconds, 0)
}

// userDataChanged is called on QueueConfigWatcher notifications.
func (pm *taskQueuePartitionManagerImpl) userDataChanged() {
	pm.refreshQueueConfig()
	// Update rateLimits if any change is userData.
	pm.rateLimitManager.UserDataChanged()
	pm.defaultQueue.UserDataChanged()
}

// refreshQueueConfig caches the library-only QueueConfig fields (the rate limit manager caches
// the upstream ones). A negative MaxAttempts override is ignored: 0 already means unlimited, so
// clamping a negative value would drop the queue's limit.
func (pm *taskQueuePartitionManagerImpl) refreshQueueConfig() {
	n := pm.config.MaxAttempts
	if src := pm.engine.configSource; src != nil {
		if v := src.QueueConfig(pm.partition).MaxAttempts; v != nil {
			if *v < 0 {
				pm.logger.Warn("ignoring negative QueueConfig.MaxAttempts; keeping the previous limit",
					"max_attempts", *v, "limit", pm.maxAttempts.Load())
				return
			}
			n = *v
		}
	}
	pm.maxAttempts.Store(n)
}

// GetRateLimitManager returns the partition's rate limit manager.
func (pm *taskQueuePartitionManagerImpl) GetRateLimitManager() *rateLimitManager {
	return pm.rateLimitManager
}

func (pm *taskQueuePartitionManagerImpl) WaitUntilInitialized(ctx context.Context) error {
	return pm.defaultQueue.WaitUntilInitialized(ctx)
}

// AddTask tries a sync match and otherwise spools the task to the backlog.
func (pm *taskQueuePartitionManagerImpl) AddTask(
	ctx context.Context,
	taskInfo *store.Task,
	fwd *forwardInfo,
) (syncMatched bool, err error) {
	syncMatchQueue := pm.defaultQueue
	var spoolQueue *physicalTaskQueueManagerImpl
	if fwd == nil {
		spoolQueue = pm.defaultQueue // spoolQueue will be nil iff task is forwarded.
	}

	syncMatchTask := newInternalTaskForSyncMatch(taskInfo, fwd)
	pm.config.setDefaultPriority(syncMatchTask)

	pm.metricsHandler.Counter(metrics.TasksAdded, 1)
	outcome, err := syncMatchQueue.TrySyncMatch(ctx, syncMatchTask)
	if outcome == syncMatchSuccess {
		// Deviation: upstream returns startErr to the caller (history retries) and only spools
		// on BUSY_WORKFLOW. With no history behind us, any start error (other than a drop, which
		// is reported as success) means the task was not delivered, so spool it instead.
		if err == nil {
			pm.metricsHandler.Counter(metrics.SyncMatch, 1)
			return true, nil
		}
	}
	// other errors are ignored and we try to spool the task

	if spoolQueue == nil {
		// This means the task is being forwarded. Child partition will persist the task when sync match fails.
		return false, errRemoteSyncMatchFailed
	}

	err = spoolQueue.SpoolTask(taskInfo)
	if err == nil {
		pm.metricsHandler.Counter(metrics.TasksSpooled, 1)
	}
	return false, err
}

// spoolBacklog persists task without a sync match. Redrive uses it so a waiting poller cannot
// hold the only copy. Public AddTask is unchanged.
func (pm *taskQueuePartitionManagerImpl) spoolBacklog(taskInfo *store.Task) error {
	pm.metricsHandler.Counter(metrics.TasksAdded, 1)
	pm.defaultQueue.incTaskTracker(pm.defaultQueue.tasksAdded, priorityKey(taskInfo.Priority.PriorityKey), 1)
	err := pm.defaultQueue.SpoolTask(taskInfo) // also markAlive
	if err == nil {
		pm.metricsHandler.Counter(metrics.TasksSpooled, 1)
	}
	return err
}

func (pm *taskQueuePartitionManagerImpl) PollTask(ctx context.Context, pollMetadata *pollMetadata) (*internalTask, *physicalTaskQueueManagerImpl, error) {
	dbq := pm.defaultQueue

	// The desired global rate limit for the task queue can come from multiple sources:
	// UpdateTaskQueueConfig API call, poller metadata, or system default.
	// In the case of worker set rate limits at the time of poll task :
	// we update the ratelimiter rps if it has changed from the previous poll.
	// Last poller wins if different pollers provide different values
	// Highest priority is given to the rate limit set by the UpdateTaskQueueConfig api call.
	// Followed by the rate limit set by the poller.
	// UpdateRateLimit implicitly handles whether an update is required or not,
	// based on whether the effectiveRPS has changed.
	pm.rateLimitManager.InjectWorkerRPS(pollMetadata)

	task, err := dbq.PollTask(ctx, pollMetadata)
	return task, dbq, err
}

func (pm *taskQueuePartitionManagerImpl) unloadPhysicalQueue(unloadedDbq *physicalTaskQueueManagerImpl, unloadCause unloadCause) {
	// this is the default queue, unload the whole partition
	if pm.defaultQueue == unloadedDbq {
		pm.engine.unloadTaskQueuePartition(pm, unloadCause)
	}
}
