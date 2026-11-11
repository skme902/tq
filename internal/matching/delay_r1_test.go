package matching

import (
	"context"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/skme902/tq/clock"
	"github.com/skme902/tq/store"
	"github.com/skme902/tq/store/memstore"
)

// A delay timer that fires while the reader trims the task delivers the row once, not twice:
// once from the stale in-memory task and once from the re-read that loads the row again
// (review backoff r1 P1). The hook trims the task in the window after delayFired drops the
// reader lock, the way mergeTasksLocked's chop does, and starts a re-read from below it.
func TestDelayFiredDuringTrimDeliversOnce(t *testing.T) {
	fired := make(chan struct{})
	var reader *fairTaskReader
	var stale *internalTask
	testHookDelayFiredUnlocked = func(tr *fairTaskReader, task *internalTask) {
		reader, stale = tr, task
		tr.lock.Lock()
		level := task.fairLevel()
		tr.outstandingTasks.Delete(outstandingTask{level: level})
		tr.loadedTasks--
		tr.backlogAge.record(task.event.Data.CreateTime, -1)
		task.setEvicted()
		tr.readLevel = fairLevel{pass: level.pass, id: level.id - 1}
		tr.atEnd = false
		tr.maybeReadTasksLocked()
		tr.lock.Unlock()
		close(fired)
	}
	t.Cleanup(func() { testHookDelayFiredUnlocked = nil })

	e := newR2Engine(t, memstore.New(), nil, nil, nil)
	ctx := context.Background()
	_, err := e.AddTask(ctx, portQueue, store.Task{Payload: []byte("later"), VisibleAt: time.Now().Add(100 * time.Millisecond)})
	require.NoError(t, err)
	<-fired
	// Wait until the re-read has loaded the row again as a new task (and the stale add, if it
	// happened, is done) before polling.
	require.Eventually(t, func() bool {
		reader.lock.Lock()
		defer reader.lock.Unlock()
		o, ok := reader.outstandingTasks.Get(outstandingTask{level: stale.fairLevel()})
		return ok && o.task != nil && o.task != stale
	}, 3*time.Second, time.Millisecond, "re-read loads the row")
	time.Sleep(50 * time.Millisecond)

	delivered := 0
	for {
		d, err := e.PollTask(ctx, portQueue, PollOptions{NoWait: true})
		if err != nil {
			require.ErrorIs(t, err, ErrNoTasks)
			break
		}
		require.Equal(t, "later", string(d.Task.Payload))
		delivered++
		require.NoError(t, d.Ack())
	}
	require.Equal(t, 1, delivered, "the row is delivered exactly once")
}

// plainStore hides every optional interface (DeadLetterStore included), so a Nack delay is
// held in memory only.
type plainStore struct{ store.Store }

// A trimmed lease nacked with a delay keeps that delay when the row is re-read, even when the
// delay could not be written to the row (review backoff r1 P1: completeTask's not-found path
// dropped redeliverAt and the re-read delivered the task at once).
func TestTrimmedLeaseNackDelayKept(t *testing.T) {
	e := newR2Engine(t, plainStore{memstore.New()}, nil, nil, nil)
	ctx := context.Background()
	_, err := e.AddTask(ctx, portQueue, store.Task{Payload: []byte("a")})
	require.NoError(t, err)
	pctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	d, err := e.PollTask(pctx, portQueue, PollOptions{})
	require.NoError(t, err)

	tr := e.partitions[portQueue].defaultQueue.backlogMgr.subqueues[0]
	level := d.task.fairLevel()
	tr.lock.Lock() // trim the leased task, as mergeTasksLocked's chop does
	tr.outstandingTasks.Delete(outstandingTask{level: level})
	tr.backlogAge.record(d.task.event.Data.CreateTime, -1)
	d.task.leased = false
	tr.leasedTasks--
	tr.leasedEvicted[level] = d.task
	tr.readLevel = fairLevel{pass: level.pass, id: level.id - 1}
	tr.atEnd = false
	tr.lock.Unlock()

	require.NoError(t, d.Nack(NackDelay(time.Hour)))
	tr.lock.Lock()
	tr.maybeReadTasksLocked()
	tr.lock.Unlock()
	require.Eventually(t, func() bool {
		tr.lock.Lock()
		defer tr.lock.Unlock()
		_, ok := tr.outstandingTasks.Get(outstandingTask{level: level})
		return ok
	}, 3*time.Second, time.Millisecond, "re-read loads the row")
	_, err = e.PollTask(ctx, portQueue, PollOptions{NoWait: true})
	require.ErrorIs(t, err, ErrNoTasks, "the nacked task stays delayed")
}

// Every engine timer goes through afterFunc/afterFuncAt, whose callbacks never run inline on
// the clock, so the only lock order is engine lock -> clock lock (review backoff r2 P0: an
// inline idle-unload callback deadlocked against a delay timer stopped under tr.lock).
func TestTimerCallbacksNeverInline(t *testing.T) {
	files, err := filepath.Glob("*.go")
	require.NoError(t, err)
	re := regexp.MustCompile(`\.(AfterFunc|AfterFuncAt)\(`)
	for _, f := range files {
		if strings.HasSuffix(f, "_test.go") || f == "timers.go" {
			continue
		}
		b, err := os.ReadFile(f)
		require.NoError(t, err)
		for i, line := range strings.Split(string(b), "\n") {
			if re.MatchString(line) && !strings.Contains(line, "context.AfterFunc(") {
				t.Errorf("%s:%d arms a clock timer directly; use afterFunc: %s", f, i+1, strings.TrimSpace(line))
			}
		}
	}
}

// Idle unload with an armed delay timer on a synchronous fake clock: the idle callback runs
// from Advance and stops the delay timer. It used to run inline under the clock lock and
// deadlock there (review backoff r2 P0).
func TestIdleUnloadWithDelayedTaskDoesNotDeadlock(t *testing.T) {
	ts := clock.NewEventTimeSource().Update(time.Unix(1_700_000_000, 0))
	e := newR2Engine(t, memstore.New(), ts, nil, func(c *Config) { c.MaxTaskQueueIdleTime = time.Minute })
	ctx := context.Background()
	_, err := e.AddTask(ctx, portQueue, store.Task{Payload: []byte("later"), VisibleAt: ts.Now().Add(1000 * time.Hour)})
	require.NoError(t, err)
	tr := e.partitions[portQueue].defaultQueue.backlogMgr.subqueues[0]
	require.Eventually(t, func() bool {
		tr.lock.Lock()
		defer tr.lock.Unlock()
		return tr.delayedTasks == 1
	}, 3*time.Second, time.Millisecond)
	done := make(chan struct{})
	go func() { ts.Advance(2 * time.Minute); close(done) }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Advance deadlocked in the idle-unload callback")
	}
	require.Eventually(t, func() bool {
		tr.lock.Lock()
		defer tr.lock.Unlock()
		return tr.delayStopped
	}, 3*time.Second, time.Millisecond, "the queue unloaded and stopped its delay timers")
}

// A delay callback already scheduled when the queue stopped does not re-add the task
// (review backoff r2 P1: stopDelayTimers did not invalidate it).
func TestDelayFiredAfterStopIgnored(t *testing.T) {
	ts := clock.NewEventTimeSource().Update(time.Unix(1_700_000_000, 0))
	e := newR2Engine(t, memstore.New(), ts, nil, nil)
	_, err := e.AddTask(context.Background(), portQueue, store.Task{Payload: []byte("later"), VisibleAt: ts.Now().Add(time.Hour)})
	require.NoError(t, err)
	tr := e.partitions[portQueue].defaultQueue.backlogMgr.subqueues[0]
	var task *internalTask
	var gen uint64
	require.Eventually(t, func() bool {
		tr.lock.Lock()
		defer tr.lock.Unlock()
		tr.outstandingTasks.Scan(func(o outstandingTask) bool {
			if o.task != nil && o.task.delayed {
				task, gen = o.task, o.task.delayGen
			}
			return true
		})
		return task != nil
	}, 3*time.Second, time.Millisecond)
	tr.stopDelayTimers()
	tr.delayFired(task, gen) // the callback that was already on its way
	tr.lock.Lock()
	defer tr.lock.Unlock()
	require.True(t, task.delayed, "still delayed: the row keeps VisibleAt for the next load")
	require.Equal(t, 0, tr.loadedTasks)
	require.Equal(t, 1, tr.delayedTasks)
}

// The evicted-delay cache is bounded deterministically: it drops the highest other level and
// keeps the one just written (review backoff r2 P1: it deleted random map keys).
func TestEvictedDelaysBoundedDeterministically(t *testing.T) {
	tr := &fairTaskReader{}
	at := time.Unix(1_700_000_000, 0)
	for i := int64(1); i <= evictedAcksCacheSize+50; i++ {
		tr.recordEvictedDelayLocked(fairLevel{id: i}, at)
	}
	require.Len(t, tr.evictedDelays, evictedAcksCacheSize)
	require.Contains(t, tr.evictedDelays, fairLevel{id: evictedAcksCacheSize + 50}, "the level just written")
	for i := int64(1); i < evictedAcksCacheSize; i++ {
		require.Contains(t, tr.evictedDelays, fairLevel{id: i}, "the lowest levels are kept")
	}
}

// A delayed task chopped by a trim keeps its in-memory delay on the re-read (review backoff
// r2 P1). plainStore has no DeadLetterStore, so the delay is only in memory.
func TestTrimmedDelayedTaskStaysDelayed(t *testing.T) {
	e := newR2Engine(t, plainStore{memstore.New()}, nil, nil, nil)
	ctx := context.Background()
	_, err := e.AddTask(ctx, portQueue, store.Task{Payload: []byte("a")})
	require.NoError(t, err)
	pctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	d, err := e.PollTask(pctx, portQueue, PollOptions{})
	require.NoError(t, err)
	require.NoError(t, d.Nack(NackDelay(time.Hour)))

	tr := e.partitions[portQueue].defaultQueue.backlogMgr.subqueues[0]
	level := d.task.fairLevel()
	tr.lock.Lock() // chop the delayed task, as mergeTasksLocked does
	o, ok := tr.outstandingTasks.Get(outstandingTask{level: level})
	require.True(t, ok && o.task != nil && o.task.delayed, "the nacked task is delayed")
	tr.outstandingTasks.Delete(outstandingTask{level: level})
	tr.evictDelayedLocked(o.task)
	tr.readLevel = fairLevel{pass: level.pass, id: level.id - 1}
	tr.atEnd = false
	tr.maybeReadTasksLocked()
	tr.lock.Unlock()
	require.Eventually(t, func() bool {
		tr.lock.Lock()
		defer tr.lock.Unlock()
		o, ok := tr.outstandingTasks.Get(outstandingTask{level: level})
		return ok && o.task != nil && o.task != d.task
	}, 3*time.Second, time.Millisecond, "re-read loads the row")
	_, err = e.PollTask(ctx, portQueue, PollOptions{NoWait: true})
	require.ErrorIs(t, err, ErrNoTasks, "the re-read task stays delayed")
}

// An uncapped backoff saturates at the latest representable time instead of overflowing
// UnixNano into the past (review backoff r2 P1).
func TestRedeliveryTimeCapped(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	huge := RetryBackoff{Initial: time.Second}.delay(80)
	at := redeliveryTime(now, huge)
	require.Equal(t, maxVisibleAt, at)
	require.Positive(t, at.UnixNano())
	require.True(t, at.After(now))
	require.Equal(t, now.Add(time.Minute), redeliveryTime(now, time.Minute))
}

// A delayed AddTask counts toward TasksAddRate like any other add (review backoff r2 P2).
func TestDelayedAddCountsAsAdded(t *testing.T) {
	ts := clock.NewEventTimeSource().Update(time.Unix(1_700_000_000, 0))
	e := newR2Engine(t, memstore.New(), ts, nil, nil)
	ctx := context.Background()
	for range 4 {
		_, err := e.AddTask(ctx, portQueue, store.Task{Payload: []byte("later"), VisibleAt: ts.Now().Add(time.Hour)})
		require.NoError(t, err)
	}
	ts.Advance(4 * time.Second)
	desc, err := e.DescribeQueue(ctx, portQueue)
	require.NoError(t, err)
	require.InDelta(t, 1.0, desc.Stats.TasksAddRate, 1e-6, "4 adds over 4 s")
}
