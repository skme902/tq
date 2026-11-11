package storetest

// Conformance cases for store.QueueSweeper (new; no upstream counterpart).

import (
	"math"
	"testing"
	"time"

	"github.com/skme902/tq/store"
)

func (s *suite) sweeper() store.QueueSweeper {
	d, ok := s.store.(store.QueueSweeper)
	if !ok {
		s.t.Skip("store does not implement store.QueueSweeper")
	}
	return d
}

func (s *suite) deleteIfIdle(d store.QueueSweeper, rangeID int64) bool {
	ok, err := d.DeleteQueueIfIdle(s.ctx, &store.DeleteQueueIfIdleRequest{Key: s.key, RangeID: rangeID})
	s.NoError(err)
	return ok
}

func (s *suite) queueExists() bool {
	_, err := s.store.GetQueue(s.ctx, s.key)
	if err == nil {
		return true
	}
	var nf *store.NotFoundError
	s.ErrorAs(err, &nf)
	return false
}

func (s *suite) setAckLevel(rangeID int64, info *store.QueueInfo, sq int, l store.Level) {
	info.Subqueues[sq].FairAckLevel = l
	s.NoError(s.store.UpdateQueue(s.ctx, &store.UpdateQueueRequest{RangeID: rangeID, PrevRangeID: rangeID, Info: info}))
}

func testSweepEmptyQueue(_ *testing.T, s *suite) {
	d := s.sweeper()
	rangeID := randRangeID()
	info := s.createQueue(rangeID)
	s.False(s.deleteIfIdle(d, rangeID+1), "rangeID mismatch: someone took the queue over")
	ok, err := d.DeleteQueueIfIdle(s.ctx, &store.DeleteQueueIfIdleRequest{Key: s.key, RangeID: rangeID, IdleBefore: info.LastUpdateTime})
	s.NoError(err)
	s.False(ok, "written at IdleBefore: not idle")
	s.True(s.queueExists())
	ok, err = d.DeleteQueueIfIdle(s.ctx, &store.DeleteQueueIfIdleRequest{Key: s.key, RangeID: rangeID, IdleBefore: info.LastUpdateTime.Add(time.Microsecond)})
	s.NoError(err)
	s.True(ok)
	s.False(s.queueExists())
	s.False(s.deleteIfIdle(d, rangeID), "missing queue")
}

func testSweepLiveTasks(_ *testing.T, s *suite) {
	d := s.sweeper()
	rangeID := randRangeID()
	info := s.createQueue(rangeID)
	s.setAckLevel(rangeID, info, 0, store.Level{Pass: 1, ID: 4})
	acked, live := s.randomTask(3, 1), s.randomTask(5, 1)
	s.NoError(s.createTasks(rangeID, info, []store.TaskInfo{acked, live}, nil))
	s.False(s.deleteIfIdle(d, rangeID), "a task above the ack level is live")
	s.Len(s.getAll(0), 2)

	// Once the ack level covers it, the remaining rows are only waiting for GC.
	s.setAckLevel(rangeID, info, 0, store.Level{Pass: 1, ID: 5})
	s.True(s.deleteIfIdle(d, rangeID))
	s.False(s.queueExists())
	s.createQueue(rangeID)
	s.Empty(s.getAll(0), "task rows go with the queue")
}

func testSweepUntrackedSubqueue(_ *testing.T, s *suite) {
	d := s.sweeper()
	rangeID := randRangeID()
	info := s.createQueue(rangeID)
	s.setAckLevel(rangeID, info, 0, store.Level{Pass: 9, ID: 9})
	// Subqueue 1 has rows but no SubqueueInfo yet: its ack level counts as zero.
	s.NoError(s.createTasks(rangeID, info, []store.TaskInfo{s.randomTask(1, 1)}, []int{1}))
	s.False(s.deleteIfIdle(d, rangeID))
	s.True(s.queueExists())
}

func testSweepDeadTasks(_ *testing.T, s *suite) {
	d := s.sweeper()
	dl := s.dls()
	rangeID := randRangeID()
	s.createQueue(rangeID)
	s.NoError(s.deadLetter(dl, rangeID, s.randomTask(1, 1), "test", s.now()))
	s.False(s.deleteIfIdle(d, rangeID), "dead tasks keep the queue")
	n, err := dl.CountDeadTasks(s.ctx, s.key)
	s.NoError(err)
	s.EqualValues(1, n)
}

// Liveness compares (pass, id) strictly above the ack level: a higher pass with a lower id is
// live, and a row exactly at an ack level of MaxInt64 is not (review retention r1: the memstore
// pivot ack.ID+1 overflowed).
func testSweepLevelOrder(_ *testing.T, s *suite) {
	d := s.sweeper()
	rangeID := randRangeID()
	info := s.createQueue(rangeID)
	s.setAckLevel(rangeID, info, 0, store.Level{Pass: 1, ID: 9})
	s.NoError(s.createTasks(rangeID, info, []store.TaskInfo{s.randomTask(1, 2)}, nil))
	s.False(s.deleteIfIdle(d, rangeID), "(2,1) is above (1,9)")
	s.setAckLevel(rangeID, info, 0, store.Level{Pass: 2, ID: math.MaxInt64})
	s.NoError(s.createTasks(rangeID, info, []store.TaskInfo{s.randomTask(math.MaxInt64, 2)}, nil))
	s.True(s.deleteIfIdle(d, rangeID), "every row is at or below the ack level")
}

// With Now set, tasks above the ack level that have expired do not keep the queue, and they go
// with it; an unexpired or unset expiry does (review retention r1 P1).
func testSweepExpiredTasks(_ *testing.T, s *suite) {
	d := s.sweeper()
	rangeID := randRangeID()
	info := s.createQueue(rangeID)
	s.setAckLevel(rangeID, info, 0, store.Level{Pass: 1, ID: 0})
	expired := s.randomTask(1, 1)
	expired.Data.ExpiryTime = s.now().Add(-time.Hour)
	s.NoError(s.createTasks(rangeID, info, []store.TaskInfo{expired}, nil))
	del := func(now time.Time) bool {
		ok, err := d.DeleteQueueIfIdle(s.ctx, &store.DeleteQueueIfIdleRequest{Key: s.key, RangeID: rangeID, Now: now})
		s.NoError(err)
		return ok
	}
	s.False(del(time.Time{}), "without Now every row above the ack level is live")
	s.True(del(s.now()), "only expired tasks: deleted")
	s.False(s.queueExists())
	info = s.createQueue(rangeID)
	s.Empty(s.getAll(0), "expired rows go with the queue")
	s.setAckLevel(rangeID, info, 0, store.Level{Pass: 1, ID: 0})

	expired = s.randomTask(1, 1)
	expired.Data.ExpiryTime = s.now().Add(-time.Hour)
	noExpiry := s.randomTask(2, 1)
	noExpiry.Data.ExpiryTime = time.Time{}
	s.NoError(s.createTasks(rangeID, info, []store.TaskInfo{expired, noExpiry}, nil))
	s.False(del(s.now()), "a task with no expiry is live")
	unexpired := s.randomTask(3, 2)
	s.NoError(s.createTasks(rangeID, info, []store.TaskInfo{unexpired}, nil))
	s.Len(s.getAll(0), 3)
}
