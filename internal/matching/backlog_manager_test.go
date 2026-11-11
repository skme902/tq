// Ported from temporalio/temporal v1.32.1 af08a5e5, service/matching/backlog_manager_test.go
// (TestBacklogManager_Fair_Suite cases). gomock's MockphysicalTaskQueueManager is replaced by
// fakePQM and testTaskManager by testStore (memstore + faults); classic/pri-only cases dropped.
// Copyright (c) 2025 Temporal Technologies Inc. MIT License; see UPSTREAM_LICENSE and NOTICE.

package matching

import (
	"container/list"
	"context"
	"fmt"
	"math"
	"math/rand"
	rand2 "math/rand/v2"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"

	"github.com/skme902/tq/clock"
	"github.com/skme902/tq/internal/backoff"
	"github.com/skme902/tq/internal/counter"
	"github.com/skme902/tq/metrics"
	"github.com/skme902/tq/store"
)

// fakePQM replaces MockphysicalTaskQueueManager.
type fakePQM struct {
	key        store.QueueKey
	addSpooled func(*internalTask) error
	onUnload   func(unloadCause)
	unloads    atomic.Int32
}

func (f *fakePQM) QueueKey() store.QueueKey { return f.key }
func (f *fakePQM) UnloadFromPartitionManager(c unloadCause) {
	f.unloads.Add(1)
	if f.onUnload != nil {
		f.onUnload(c)
	}
}
func (f *fakePQM) AddSpooledTask(t *internalTask) error {
	if f.addSpooled != nil {
		return f.addSpooled(t)
	}
	return nil
}
func (f *fakePQM) GetFairnessWeightOverrides() fairnessWeightOverrides { return nil }
func (f *fakePQM) PausedFairnessKeys() map[string]struct{}             { return nil }

type BacklogManagerTestSuite struct {
	suite.Suite

	cfg        Config
	blm        *fairBacklogManagerImpl
	cancelCtx  context.CancelFunc
	taskMgr    *testStore
	ptqMgr     *fakePQM
	metricsCap *captureMetrics
	// counterFactory overrides the default test counter (library addition, see hybridCounter).
	counterFactory func() counter.Counter

	capturedTasksLock  sync.Mutex
	capturedTasksSlice []*internalTask
}

func TestBacklogManager_Fair_Suite(t *testing.T) {
	t.Parallel()
	suite.Run(t, &BacklogManagerTestSuite{})
}

func (s *BacklogManagerTestSuite) SetupTest() {
	s.capturedTasksLock.Lock()
	s.capturedTasksSlice = nil
	s.capturedTasksLock.Unlock()
	s.taskMgr = newTestStore()
	s.metricsCap = newCaptureMetrics()
	s.cfg = DefaultConfig()
	s.ptqMgr = &fakePQM{key: store.QueueKey{Namespace: "test-namespace", Name: "test-queue"}}
	s.blm = nil
	s.counterFactory = nil
}

// newBLM builds the backlog manager (upstream builds it in SetupTest; dynamic config overrides
// there become edits to s.cfg before calling this).
func (s *BacklogManagerTestSuite) newBLM() *fairBacklogManagerImpl {
	var ctx context.Context
	ctx, s.cancelCtx = context.WithCancel(context.Background())
	s.T().Cleanup(s.cancelCtx)
	cf := s.counterFactory
	if cf == nil {
		cf = func() counter.Counter { return counter.NewMapCounter(1000) }
	}
	s.blm = newFairBacklogManager(
		ctx,
		s.ptqMgr,
		newTaskQueueConfig(s.cfg),
		s.taskMgr,
		discardLogger(),
		s.metricsCap,
		clock.NewRealTimeSource(),
		cf,
	)
	return s.blm
}

func (s *BacklogManagerTestSuite) setupToCaptureTasks() {
	s.ptqMgr.addSpooled = func(t *internalTask) error {
		s.capturedTasksLock.Lock()
		defer s.capturedTasksLock.Unlock()
		s.capturedTasksSlice = append(s.capturedTasksSlice, t)
		return nil
	}
}

func (s *BacklogManagerTestSuite) capturedTasksLen() int {
	s.capturedTasksLock.Lock()
	defer s.capturedTasksLock.Unlock()
	return len(s.capturedTasksSlice)
}

func (s *BacklogManagerTestSuite) capturedTasks() []*internalTask {
	s.capturedTasksLock.Lock()
	defer s.capturedTasksLock.Unlock()
	return slices.Clone(s.capturedTasksSlice)
}

func totalApproximateBacklogCount(c *fairBacklogManagerImpl) int64 {
	return c.db.getTotalApproximateBacklogCount()
}

func newTestTask(expiry time.Duration) *store.Task {
	now := time.Now()
	return &store.Task{ExpiryTime: now.Add(expiry), CreateTime: now}
}

func (s *BacklogManagerTestSuite) TestTaskWriterShutdown() {
	blm := s.newBLM()
	blm.Start()
	defer blm.Stop()
	s.NoError(blm.WaitUntilInitialized(context.Background()))

	err := blm.SpoolTask(&store.Task{})
	s.NoError(err)

	s.cancelCtx()
	err = blm.SpoolTask(&store.Task{})
	s.Error(err)
	// upstream expects UnloadFromPartitionManager(unloadCauseConflict) since errShutdown is a
	// ConditionFailedError
	s.Equal(int32(1), s.ptqMgr.unloads.Load())
}

func (s *BacklogManagerTestSuite) TestApproximateBacklogCount_IncrementedBySpoolTask() {
	blm := s.newBLM()
	blm.Start()
	defer blm.Stop()
	s.NoError(blm.WaitUntilInitialized(context.Background()))

	taskCount := 10
	for range taskCount {
		s.NoError(blm.SpoolTask(newTestTask(3000 * time.Second)))
	}
	s.Equal(int64(taskCount), totalApproximateBacklogCount(blm),
		"backlog count should match the number of tasks")
}

func (s *BacklogManagerTestSuite) TestApproximateBacklogCount_IncrementedBySpoolTask_Unavailable() {
	s.taskMgr.addFault("CreateTasks", nil, 1.0)
	// Disable the reader reloading to make the test reliable (upstream: GetTasksReloadAt=-1).
	s.cfg.GetTasksReloadAt = -1

	blm := s.newBLM()
	blm.Start()
	defer blm.Stop()
	s.NoError(blm.WaitUntilInitialized(context.Background()))

	taskCount := 10
	for range taskCount {
		s.Error(blm.SpoolTask(newTestTask(3000 * time.Second)))
	}
	s.Equal(int64(taskCount), totalApproximateBacklogCount(blm),
		"backlog count should match the number of tasks despite the errors")
}

func (s *BacklogManagerTestSuite) TestApproximateBacklogCount_NotIncrementedBySpoolTask_CondFailedError() {
	s.taskMgr.addFault("CreateTasks", &store.ConditionFailedError{Msg: "injected"}, 1.0)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	s.ptqMgr.onUnload = func(c unloadCause) {
		s.Equal(unloadCauseConflict, c)
		cancel()
	}

	blm := s.newBLM()
	blm.Start()
	defer blm.Stop()
	s.NoError(blm.WaitUntilInitialized(context.Background()))

	s.Error(blm.SpoolTask(newTestTask(3000 * time.Second)))
	<-ctx.Done()
	s.ErrorIs(ctx.Err(), context.Canceled, "should have unloaded")

	s.Equal(int64(0), totalApproximateBacklogCount(blm), "backlog count should not be incremented")
}

func (s *BacklogManagerTestSuite) TestApproximateBacklogCount_NotIncrementedBySpoolTask_PersistenceLimit() {
	// A write dropped by the persistence rate limiter definitely didn't reach the database,
	// so it should not count towards the backlog.
	s.taskMgr.addFault("CreateTasks", &store.ResourceExhaustedError{Msg: "injected", DefinitelyNotWritten: true}, 1.0)

	blm := s.newBLM()
	blm.Start()
	defer blm.Stop()
	s.Require().NoError(blm.WaitUntilInitialized(context.Background()))

	for range 10 {
		s.Require().Error(blm.SpoolTask(newTestTask(3000 * time.Second)))
	}
	s.Equal(int64(0), totalApproximateBacklogCount(blm),
		"backlog count should not be incremented for rate-limited writes")
}

func (s *BacklogManagerTestSuite) TestSyncState_UnloadsOnOwnershipLoss() {
	s.cfg.UpdateAckInterval = 100 * time.Millisecond

	blm := s.newBLM()
	blm.Start()
	defer blm.Stop()
	s.Require().NoError(blm.WaitUntilInitialized(context.Background()))

	// physical queue should unload soon
	var unloadCalled atomic.Bool
	s.ptqMgr.onUnload = func(c unloadCause) {
		if c == unloadCauseConflict {
			unloadCalled.Store(true)
		}
	}

	// simulate another partition stealing and releasing ownership
	resp, err := s.taskMgr.GetQueue(context.Background(), s.ptqMgr.key)
	s.Require().NoError(err)
	s.Require().NoError(s.taskMgr.UpdateQueue(context.Background(), &store.UpdateQueueRequest{
		RangeID: resp.RangeID + 1, PrevRangeID: resp.RangeID, Info: resp.Info,
	}))

	s.Eventually(unloadCalled.Load, time.Second, 100*time.Millisecond)
}

type taskBlock struct {
	count   int
	expired bool
}

func expiredBlock(n int) taskBlock { return taskBlock{count: n, expired: true} }
func validBlock(n int) taskBlock   { return taskBlock{count: n, expired: false} }

func (s *BacklogManagerTestSuite) TestSkipExpiredTasks_ExpiredThenValid() {
	s.testSkipExpiredTasks(10, expiredBlock(33), validBlock(3))
}

func (s *BacklogManagerTestSuite) TestSkipExpiredTasks_ValidExpiredValid() {
	s.testSkipExpiredTasks(10, validBlock(3), expiredBlock(33), validBlock(3))
}

func (s *BacklogManagerTestSuite) TestSkipExpiredTasks_ValidThenExpired() {
	s.testSkipExpiredTasks(10, validBlock(3), expiredBlock(33))
}

func (s *BacklogManagerTestSuite) TestSkipExpiredTasks_AllExpired() {
	s.testSkipExpiredTasks(10, expiredBlock(33))
}

func (s *BacklogManagerTestSuite) createQueueWithTasks(tasks []store.TaskInfo) {
	ctx := context.Background()
	info := &store.QueueInfo{Key: s.ptqMgr.key} // start with ack level at zero
	s.Require().NoError(s.taskMgr.CreateQueue(ctx, &store.CreateQueueRequest{RangeID: 1, Info: info}))
	if len(tasks) > 0 {
		_, err := s.taskMgr.CreateTasks(ctx, &store.CreateTasksRequest{RangeID: 1, Info: info, Tasks: tasks})
		s.Require().NoError(err)
	}
}

// testSkipExpiredTasks verifies that the task reader correctly skips over expired tasks
// in the DB and advances the ack level past them.
func (s *BacklogManagerTestSuite) testSkipExpiredTasks(batchSize int, blocks ...taskBlock) {
	s.cfg.GetTasksBatchSize = batchSize

	// Pre-populate the DB with tasks before starting the backlog manager.
	var dbTasks []store.TaskInfo
	numValid := 0
	lastID := int64(0)
	now := time.Now()
	for _, block := range blocks {
		for range block.count {
			lastID++
			task := store.TaskInfo{ID: lastID, Pass: lastID * 1000, Data: store.Task{CreateTime: now.Add(-time.Hour)}}
			if block.expired {
				task.Data.ExpiryTime = now.Add(-time.Minute)
			} else {
				task.Data.ExpiryTime = now.Add(time.Hour)
				numValid++
			}
			dbTasks = append(dbTasks, task)
		}
	}
	s.createQueueWithTasks(dbTasks)
	s.setupToCaptureTasks()

	blm := s.newBLM()
	blm.Start()
	defer blm.Stop()
	s.Require().NoError(blm.WaitUntilInitialized(context.Background()))

	// Wait for all valid tasks to be delivered.
	s.Require().Eventually(func() bool {
		return s.capturedTasksLen() >= numValid
	}, 2*time.Second, 10*time.Millisecond, "timed out waiting for valid tasks to be delivered")

	// Complete the delivered tasks.
	for _, t := range s.capturedTasks() {
		t.finish(taskFinishResult{consumedToken: true})
	}

	// Verify the ack level advances past all tasks (expired + valid).
	s.Eventually(func() bool {
		db := blm.getDB()
		db.Lock()
		defer db.Unlock()
		ackLevel := fairLevelFromStore(db.subqueues[subqueueZero].FairAckLevel)
		return !ackLevel.less(fairLevel{pass: lastID * 1000, id: lastID})
	}, 2*time.Second, 10*time.Millisecond, "ack level did not advance past all tasks")
}

// TestExpiredTasksOnRead_EmitTasksDropped verifies tasks_dropped is emitted once per
// task that has already expired when read from persistence, with reason=expired_read.
func (s *BacklogManagerTestSuite) TestExpiredTasksOnRead_EmitTasksDropped() {
	const numExpired = 3
	now := time.Now()
	var dbTasks []store.TaskInfo
	for id := int64(1); id <= numExpired+1; id++ {
		t := store.TaskInfo{ID: id, Pass: id * 1000, Data: store.Task{CreateTime: now.Add(-time.Hour)}}
		if id <= numExpired {
			t.Data.ExpiryTime = now.Add(-time.Minute) // expired
		} else {
			t.Data.ExpiryTime = now.Add(time.Hour) // valid
		}
		dbTasks = append(dbTasks, t)
	}
	s.createQueueWithTasks(dbTasks)
	s.setupToCaptureTasks()

	blm := s.newBLM()
	blm.Start()
	defer blm.Stop()
	s.Require().NoError(blm.WaitUntilInitialized(context.Background()))

	s.Require().Eventually(func() bool {
		return len(s.metricsCap.snapshot(metrics.TasksDropped)) == numExpired
	}, 2*time.Second, 10*time.Millisecond)
	for _, tags := range s.metricsCap.snapshot(metrics.TasksDropped) {
		s.Equal(string(dropReasonExpiredRead), tags["reason"])
	}
}

func (s *BacklogManagerTestSuite) TestBypassReader() {
	s.setupToCaptureTasks()
	// set up initial qkey in db so that we always read one range on load
	s.createQueueWithTasks(nil)

	blm := s.newBLM()
	blm.Start()
	defer blm.Stop()
	s.Require().NoError(blm.WaitUntilInitialized(context.Background()))

	// wait for the initial read to complete so we're at the end
	s.Eventually(func() bool {
		g, _ := s.taskMgr.counts()
		return g == 1
	}, 5*time.Second, 10*time.Millisecond)

	for range 3 {
		_, prevCreateCount := s.taskMgr.counts()
		prevCaptureCount := s.capturedTasksLen()

		// write a task
		s.Require().NoError(blm.SpoolTask(newTestTask(3000 * time.Second)))

		// we have written one batch of tasks
		_, c := s.taskMgr.counts()
		s.Equal(prevCreateCount+1, c)

		// wait for the task to arrive at the matcher bypassing the read path
		s.Eventually(func() bool { return s.capturedTasksLen() == prevCaptureCount+1 }, 5*time.Second, 10*time.Millisecond)

		// we should have passed the task in memory without any more GetTasks calls
		g, _ := s.taskMgr.counts()
		s.Equal(1, g)
	}
}

type standingBacklogParams struct {
	lower, upper   int64         // range of standing backlog
	gap            int64         // add/finish tasks as long as we're within gap of the target
	period         time.Duration // interval between peaks/troughs
	duration       time.Duration // total duration
	keys           int           // unique fairness keys
	zipfS, zipfV   float64       // parameters for fairness key distribution
	batchSize      int
	reloadAt       int
	maxBatch       int
	delayInjection time.Duration
	faultInjection float32
	// hybridCounter (library addition): use upstream's production counter
	// (NewHybridCounter(DefaultCounterParams)) instead of the test's NewMapCounter(1000).
	hybridCounter bool
}

var defaultStandingBacklogParams = standingBacklogParams{
	lower:    20,
	upper:    200,
	gap:      2,
	period:   3 * time.Second,
	duration: 5 * time.Second,
	keys:     30,
	zipfS:    3,
	zipfV:    1,
	// reduce these for better coverage
	batchSize:      100,
	reloadAt:       40,
	maxBatch:       50,
	delayInjection: 1 * time.Millisecond,
	faultInjection: 0.015,
}

func (s *BacklogManagerTestSuite) TestStandingBacklog_Short() {
	s.testStandingBacklog(defaultStandingBacklogParams)
}

func (s *BacklogManagerTestSuite) TestStandingBacklog_ManyKeysUniform() {
	longTest(s)
	p := defaultStandingBacklogParams
	p.zipfS = 1.01 // not exactly uniform but closer
	p.zipfV = 10000
	p.keys = 10000
	p.period = 5 * time.Second
	p.duration = 15 * time.Second
	s.testStandingBacklog(p)
}

// Library addition: same as ManyKeysUniform but through the hybrid (count-min sketch) counter
// that production uses, so the >MapLimit path runs under the standing-backlog invariants.
// Not gated as a long test so it runs by default.
func (s *BacklogManagerTestSuite) TestStandingBacklog_ManyKeysUniformHybrid() {
	p := defaultStandingBacklogParams
	p.zipfS = 1.01
	p.zipfV = 10000
	p.keys = 10000
	p.hybridCounter = true
	s.testStandingBacklog(p)
}

func (s *BacklogManagerTestSuite) TestStandingBacklog_FullyDrain() {
	longTest(s)
	p := defaultStandingBacklogParams
	p.lower = -20
	p.period = 3 * time.Second
	p.duration = 15 * time.Second
	s.testStandingBacklog(p)
}

func (s *BacklogManagerTestSuite) TestStandingBacklog_WideRange() {
	longTest(s)
	p := defaultStandingBacklogParams
	p.lower = 3
	p.upper = 1000
	p.period = 15 * time.Second
	p.duration = 15 * time.Second
	s.testStandingBacklog(p)
}

func (s *BacklogManagerTestSuite) TestStandingBacklog_FiveMin() {
	longTest(s)
	p := defaultStandingBacklogParams
	p.lower = -10
	p.upper = 400
	p.period = time.Minute
	p.duration = 5 * time.Minute
	p.batchSize = 300
	p.reloadAt = 60
	p.delayInjection = 3 * time.Millisecond
	s.testStandingBacklog(p)
}

func (s *BacklogManagerTestSuite) testStandingBacklog(p standingBacklogParams) {
	zipf := rand.NewZipf(rand.New(rand.NewSource(time.Now().UnixNano())), p.zipfS, p.zipfV, uint64(p.keys-1))
	var zipfLock sync.Mutex

	s.cfg.GetTasksBatchSize = p.batchSize
	s.cfg.GetTasksReloadAt = p.reloadAt
	s.cfg.MaxTaskBatchSize = p.maxBatch
	if p.hybridCounter {
		s.counterFactory = func() counter.Counter {
			return counter.NewHybridCounter(counter.DefaultCounterParams(), rand2.NewPCG(rand2.Uint64(), rand2.Uint64()))
		}
	}

	// add delays and fault injection
	s.taskMgr.delay = p.delayInjection
	if p.faultInjection > 0 {
		s.taskMgr.addFault("GetTasks", nil, p.faultInjection)
		s.taskMgr.addFault("CreateTasks", nil, p.faultInjection)
	}

	ctx, cancel := context.WithTimeout(context.Background(), p.duration+15*time.Second)
	defer cancel()

	var wg sync.WaitGroup
	var lock sync.Mutex
	var tasks list.List // this is the in-memory buffer (mock for the matcher)
	var target, inflight, processed, index atomic.Int64
	var tracker sync.Map // tracks tasks so we can find missing ones
	target.Store((p.lower + p.upper) / 2)
	const testIsOver = int64(-1000000)

	s.ptqMgr.addSpooled = func(t *internalTask) error {
		lock.Lock()
		defer lock.Unlock()
		e := tasks.PushBack(t)
		if !t.setRemoveFunc(func() {
			lock.Lock()
			defer lock.Unlock()
			tasks.Remove(e)
		}) {
			tasks.Remove(e)
			return nil
		}
		return nil
	}
	getTask := func() *internalTask {
		lock.Lock()
		defer lock.Unlock()
		e := tasks.Front()
		if e == nil {
			return nil
		}
		return tasks.Remove(e).(*internalTask)
	}
	makeNewTask := func() *store.Task {
		zipfLock.Lock()
		key := fmt.Sprintf("fkey-%02d", zipf.Uint64())
		zipfLock.Unlock()
		// upstream tracks tasks by ScheduledEventId; we use the payload
		id := index.Add(1)
		return &store.Task{
			CreateTime: time.Now(),
			Payload:    []byte(fmt.Sprint(id)),
			Priority:   store.Priority{FairnessKey: key},
		}
	}
	delta := func() int64 {
		return inflight.Load() - target.Load()
	}
	sleep := func() {
		d := time.Millisecond + time.Duration(rand.Float32()*float32(3*time.Millisecond))
		_ = backoff.InterruptibleSleep(ctx, d)
	}
	finished := func() bool { return ctx.Err() != nil || target.Load() == testIsOver && inflight.Load() == 0 }
	sleepUntil := func(cond func() bool) bool {
		for !finished() && !cond() {
			sleep()
		}
		return !finished()
	}

	start := time.Now()
	blm := s.newBLM()
	blm.Start()
	defer blm.Stop()
	s.NoError(blm.WaitUntilInitialized(context.Background()))

	// writer
	wg.Go(func() {
		for sleepUntil(func() bool { return delta() <= p.gap }) {
			info := makeNewTask()
			tracker.Store(string(info.Payload), info.Priority.FairnessKey)
			inflight.Add(1)
			if blm.SpoolTask(info) != nil {
				tracker.Delete(string(info.Payload))
				inflight.Add(-1)
				sleep()
			}
		}
	})

	// poller
	wg.Go(func() {
		for sleepUntil(func() bool { return delta() >= -p.gap }) {
			if t := getTask(); t != nil {
				t.finish(taskFinishResult{consumedToken: true})
				if _, loaded := tracker.LoadAndDelete(string(t.event.Data.Payload)); loaded {
					inflight.Add(-1)
				}
				// else: a duplicate task (as if matching called RecordTaskStarted twice)
				processed.Add(1)
			} else {
				sleep()
			}
		}
	})

	// adjust target over time
	for t := target.Load(); time.Since(start) < p.duration; sleep() {
		factor := (math.Sin(2*math.Pi*time.Since(start).Seconds()/p.period.Seconds()) + 1.0) / 2
		next := p.lower + int64(factor*float64(p.upper-p.lower+1))
		if t != next {
			t = next
			target.Store(t)
		}
	}

	// drain and wait until exited
	target.Store(testIsOver)
	wg.Wait()

	if !s.Zero(inflight.Load(), "did not drain all tasks!") {
		tracker.Range(func(k, v any) bool {
			s.T().Logf("  outstanding task: %v %s", k, v.(string))
			return true
		})
	}
	g, c := s.taskMgr.counts()
	s.T().Logf("reads %d, writes %d; processed %d tasks, %.3f/s", g, c, processed.Load(),
		float64(processed.Load())/time.Since(start).Seconds())
}

// TestFairReaderReMergeOfCompletedWriteKeepsReadLevel reproduces the write/read race that
// triggered a bug in fair reader.
func (s *BacklogManagerTestSuite) TestFairReaderReMergeOfCompletedWriteKeepsReadLevel() {
	s.setupToCaptureTasks()
	s.createQueueWithTasks(nil)
	blm := s.newBLM()
	blm.Start()
	// We advance our reader's ack level past the real (empty) subqueue reader's; skip the final
	// ack-level write on Stop so it isn't flagged as moving backwards.
	defer func() { blm.skipFinalUpdate.Store(true); blm.Stop() }()
	s.Require().NoError(blm.WaitUntilInitialized(context.Background()))

	// Drive our own reader so the async read loop doesn't race with our hand-sequenced merges.
	tr := newFairTaskReader(blm, subqueueZero, fairLevel{})

	mkTask := func(id int64) *store.TaskInfo {
		return &store.TaskInfo{
			Pass: 1,
			ID:   id,
			Data: store.Task{CreateTime: time.Now(), ExpiryTime: time.Now().Add(3000 * time.Second)},
		}
	}
	lvl := func(id int64) fairLevel { return fairLevel{pass: 1, id: id} }
	finish := func(id int64) {
		for _, t := range s.capturedTasks() {
			if t.fairLevel() == lvl(id) {
				t.finish(taskFinishResult{consumedToken: true})
				return
			}
		}
		s.FailNowf("no captured task to finish", "level %v", lvl(id))
	}

	// 1. Writer pins the ack level at the start of its writeBatch.
	tr.getAndPinAckLevel()

	// 2. An in-flight read pulls the just-written task 5 from the DB and delivers it to the matcher.
	tr.mergeTasks([]*store.TaskInfo{mkTask(5)}, mergeReadToEnd)
	s.Require().Equal(1, tr.getLoadedTasks())
	s.Require().Equal(1, s.capturedTasksLen())

	// 3. The matcher completes task 5. It becomes a pre-acked (nil) entry, but the ack level can't
	//    advance because it's pinned by the writer, so the ack "piles up" in memory.
	finish(5)
	s.Require().Equal(0, tr.getLoadedTasks())
	readLevel, _ := tr.getLevels()
	s.Require().Equal(lvl(5), readLevel, "read level should be at the task we read")

	// 4. The writer's wroteNewTasks finally runs and re-merges task 5.
	tr.mergeTasks([]*store.TaskInfo{mkTask(5)}, mergeWrite)
	readLevel, _ = tr.getLevels()
	s.Require().Equal(lvl(5), readLevel, "write-merge of an already-acked task must not move read level")
	s.Require().Equal(1, s.capturedTasksLen(), "already-acked task must not be re-delivered")

	// 5. Writer finishes the batch and unpins; the piled-up ack now advances.
	tr.unpinAckLevel(nil)
	_, ackLevel := tr.getLevels()
	s.Require().Equal(lvl(5), ackLevel, "ack level should advance past the completed task after unpin")

	// 6. The reader is not stuck: a subsequent write is still delivered to the matcher.
	tr.mergeTasks([]*store.TaskInfo{mkTask(6)}, mergeWrite)
	s.Require().Equal(2, s.capturedTasksLen(), "reader should still deliver new tasks")
	s.Require().Equal(1, tr.getLoadedTasks())
}
