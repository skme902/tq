package matching

import (
	"context"
	"sync/atomic"
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

// lockProbeClock checks, on each long timer (a delay timer here), that the reader lock is free:
// a fake clock's callback that needs tr.lock would deadlock otherwise.
type lockProbeClock struct {
	*clock.EventTimeSource
	reader atomic.Pointer[fairTaskReader]
	armed  atomic.Int32
	held   atomic.Bool
}

func (c *lockProbeClock) AfterFunc(d time.Duration, f func()) clock.Timer {
	if tr := c.reader.Load(); tr != nil && d >= time.Hour {
		got := make(chan struct{})
		go func() { tr.lock.Lock(); close(got); tr.lock.Unlock() }()
		select {
		case <-got:
		case <-time.After(time.Second):
			c.held.Store(true)
		}
		c.armed.Add(1)
	}
	return c.EventTimeSource.AfterFunc(d, f)
}

// The delay timer is registered without holding the reader lock (review backoff r1 P1: with
// EventTimeSource, arming under tr.lock against a callback taking tr.lock deadlocks).
func TestDelayTimerArmedOutsideReaderLock(t *testing.T) {
	ts := &lockProbeClock{EventTimeSource: clock.NewEventTimeSource().Update(time.Unix(1_700_000_000, 0))}
	ts.UseAsyncTimers(true)
	e := newR2Engine(t, memstore.New(), ts, nil, nil)
	ctx := context.Background()
	_, err := e.AddTask(ctx, portQueue, store.Task{Payload: []byte("warm")})
	require.NoError(t, err)
	ts.reader.Store(e.partitions[portQueue].defaultQueue.backlogMgr.subqueues[0])
	_, err = e.AddTask(ctx, portQueue, store.Task{Payload: []byte("later"), VisibleAt: ts.Now().Add(1000 * time.Hour)})
	require.NoError(t, err)
	require.Eventually(t, func() bool { return ts.armed.Load() == 1 }, 3*time.Second, time.Millisecond)
	require.False(t, ts.held.Load(), "AfterFunc was called with the reader lock held")
}
