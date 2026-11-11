package storetest

// Conformance cases for store.DeadLetterStore (new; no upstream counterpart).

import (
	"testing"
	"time"

	"github.com/skme902/tq/store"
)

func (s *suite) dls() store.DeadLetterStore {
	d, ok := s.store.(store.DeadLetterStore)
	if !ok {
		s.t.Skip("store does not implement store.DeadLetterStore")
	}
	return d
}

func testTaskAttemptsRoundTrip(_ *testing.T, s *suite) {
	rangeID := randRangeID()
	info := s.createQueue(rangeID)
	t := s.randomTask(1, 1)
	t.Data.Attempts = 3
	t.Data.VisibleAt = s.now().Add(time.Hour)
	s.NoError(s.createTasks(rangeID, info, []store.TaskInfo{t}, nil))
	s.Equal([]store.TaskInfo{t}, s.getAll(0))
	// UpdateTask rewrites VisibleAt (Nack with a delay).
	if d, ok := s.store.(store.DeadLetterStore); ok {
		t.Data.VisibleAt = t.Data.VisibleAt.Add(time.Minute)
		s.NoError(d.UpdateTask(s.ctx, &store.UpdateTaskRequest{Key: s.key, RangeID: rangeID, Task: t}))
		s.Equal([]store.TaskInfo{t}, s.getAll(0))
	}
}

func testUpdateTaskInPlace(_ *testing.T, s *suite) {
	d := s.dls()
	rangeID := randRangeID()
	info := s.createQueue(rangeID)
	tasks := []store.TaskInfo{s.randomTask(1, 1), s.randomTask(2, 1), s.randomTask(3, 2)}
	s.NoError(s.createTasks(rangeID, info, tasks, []int{0, 0, 1}))
	up := tasks[1]
	up.Data.Attempts = 7
	up.Data.Payload = []byte("rewritten")
	s.NoError(d.UpdateTask(s.ctx, &store.UpdateTaskRequest{Key: s.key, RangeID: rangeID, Subqueue: 0, Task: up}))
	s.Equal([]store.TaskInfo{tasks[0], up}, s.getAll(0))
	s.Equal([]store.TaskInfo{tasks[2]}, s.getAll(1))
	// A task that is not there (wrong subqueue, or deleted) is NotFound.
	err := d.UpdateTask(s.ctx, &store.UpdateTaskRequest{Key: s.key, RangeID: rangeID, Subqueue: 1, Task: up})
	var nf *store.NotFoundError
	s.ErrorAs(err, &nf)
}

func testUpdateTaskFenced(_ *testing.T, s *suite) {
	d := s.dls()
	rangeID := randRangeID()
	info := s.createQueue(rangeID)
	t := s.randomTask(1, 1)
	s.NoError(s.createTasks(rangeID, info, []store.TaskInfo{t}, nil))
	up := t
	up.Data.Attempts = 1
	err := d.UpdateTask(s.ctx, &store.UpdateTaskRequest{Key: s.key, RangeID: rangeID + 1, Task: up})
	var cf *store.ConditionFailedError
	s.ErrorAs(err, &cf)
	s.Equal([]store.TaskInfo{t}, s.getAll(0))
}

func (s *suite) deadAll(d store.DeadLetterStore, pageSize int) (all []store.DeadTask, pages int) {
	var tok []byte
	for {
		resp, err := d.ListDeadTasks(s.ctx, &store.ListDeadTasksRequest{Key: s.key, PageSize: pageSize, PageToken: tok})
		s.NoError(err)
		s.LessOrEqual(len(resp.Tasks), pageSize)
		all = append(all, resp.Tasks...)
		pages++
		if resp.NextPageToken == nil {
			return all, pages
		}
		tok = resp.NextPageToken
	}
}

func (s *suite) deadLetter(d store.DeadLetterStore, rangeID int64, t store.TaskInfo, reason string, at time.Time) error {
	return d.DeadLetterTask(s.ctx, &store.DeadLetterTaskRequest{Key: s.key, RangeID: rangeID, Task: t, Reason: reason, DeadTime: at})
}

func testDeadLetterRoundTrip(_ *testing.T, s *suite) {
	d := s.dls()
	rangeID := randRangeID()
	s.createQueue(rangeID)
	at := s.now()
	t := s.randomTask(42, 3)
	t.Data.Attempts = 5
	s.NoError(s.deadLetter(d, rangeID, t, "lease expired", at))
	got, _ := s.deadAll(d, 10)
	s.Len(got, 1)
	s.Equal(t, got[0].Task)
	s.Equal("lease expired", got[0].Reason)
	s.True(at.Equal(got[0].DeadTime), "dead time %v != %v", got[0].DeadTime, at)
	n, err := d.CountDeadTasks(s.ctx, s.key)
	s.NoError(err)
	s.EqualValues(1, n)

	// Recording the same task again (a retry after a crash) overwrites, it does not duplicate.
	t.Data.Attempts = 6
	s.NoError(s.deadLetter(d, rangeID, t, "nacked", at.Add(time.Second)))
	got, _ = s.deadAll(d, 10)
	s.Len(got, 1)
	s.Equal(int32(6), got[0].Task.Data.Attempts)
	s.Equal("nacked", got[0].Reason)

	// Dead letters do not touch the backlog, and the payload is not aliased.
	s.Empty(s.getAll(0))
	got[0].Task.Data.Payload[0] = 'X'
	again, _ := s.deadAll(d, 10)
	s.Equal(t.Data.Payload, again[0].Task.Data.Payload)
}

func testDeadLetterFenced(_ *testing.T, s *suite) {
	d := s.dls()
	rangeID := randRangeID()
	s.createQueue(rangeID)
	var cf *store.ConditionFailedError
	s.ErrorAs(s.deadLetter(d, rangeID+1, s.randomTask(1, 1), "x", s.now()), &cf)
	got, _ := s.deadAll(d, 10)
	s.Empty(got)
	// No queue row: also a fence failure.
	other := *s
	other.key.Name += "-missing"
	s.ErrorAs(other.deadLetter(d, rangeID, s.randomTask(1, 1), "x", s.now()), &cf)
}

func testDeadLetterPaged(_ *testing.T, s *suite) {
	d := s.dls()
	rangeID := randRangeID()
	s.createQueue(rangeID)
	ids := []int64{30, 10, 20, 50, 40, 60, 70}
	for _, id := range ids {
		s.NoError(s.deadLetter(d, rangeID, s.randomTask(id, 1), "r", s.now()))
	}
	for _, ps := range []int{1, 3, 7, 100} {
		got, pages := s.deadAll(d, ps)
		var gotIDs []int64
		for _, g := range got {
			gotIDs = append(gotIDs, g.Task.ID)
		}
		s.Equal([]int64{10, 20, 30, 40, 50, 60, 70}, gotIDs, "page size %d", ps)
		s.Equal((len(ids)+ps-1)/ps, pages, "page size %d", ps)
	}
}

func testDeadLetterDelete(_ *testing.T, s *suite) {
	d := s.dls()
	rangeID := randRangeID()
	s.createQueue(rangeID)
	base := s.now()
	for i := int64(1); i <= 5; i++ {
		s.NoError(s.deadLetter(d, rangeID, s.randomTask(i, 1), "r", base.Add(time.Duration(i)*time.Minute)))
	}
	n, err := d.DeleteDeadTasks(s.ctx, &store.DeleteDeadTasksRequest{Key: s.key, TaskIDs: []int64{2, 4, 99}})
	s.NoError(err)
	s.Equal(2, n)
	// Strictly before: task 3 (dead at +3m) stays.
	n, err = d.DeleteDeadTasks(s.ctx, &store.DeleteDeadTasksRequest{Key: s.key, DeadBefore: base.Add(3 * time.Minute)})
	s.NoError(err)
	s.Equal(1, n)
	got, _ := s.deadAll(d, 10)
	s.Len(got, 2)
	s.Equal(int64(3), got[0].Task.ID)
	s.Equal(int64(5), got[1].Task.ID)
	n, err = d.DeleteDeadTasks(s.ctx, &store.DeleteDeadTasksRequest{Key: s.key, TaskIDs: []int64{}})
	s.NoError(err)
	s.Zero(n)
	c, err := d.CountDeadTasks(s.ctx, s.key)
	s.NoError(err)
	s.EqualValues(2, c)
}

func testDeadLetterDeleteQueue(_ *testing.T, s *suite) {
	d := s.dls()
	rangeID := randRangeID()
	s.createQueue(rangeID)
	s.NoError(s.deadLetter(d, rangeID, s.randomTask(1, 1), "r", s.now()))
	s.NoError(s.store.DeleteQueue(s.ctx, &store.DeleteQueueRequest{Key: s.key, RangeID: rangeID}))
	got, _ := s.deadAll(d, 10)
	s.Empty(got)
	c, err := d.CountDeadTasks(s.ctx, s.key)
	s.NoError(err)
	s.Zero(c)
}

func testDeadLetterInvalidArgs(_ *testing.T, s *suite) {
	d := s.dls()
	var ia *store.InvalidArgumentError
	_, err := d.ListDeadTasks(s.ctx, &store.ListDeadTasksRequest{Key: s.key, PageSize: 0})
	s.ErrorAs(err, &ia)
	_, err = d.ListDeadTasks(s.ctx, &store.ListDeadTasksRequest{Key: s.key, PageSize: 1, PageToken: []byte("junk")})
	s.ErrorAs(err, &ia)
	_, err = d.DeleteDeadTasks(s.ctx, &store.DeleteDeadTasksRequest{Key: s.key})
	s.ErrorAs(err, &ia)
}

// DeadLetterTask removes the backlog row in the same write, and only that row; a row that is
// already gone (GC'd) is not an error.
func testDeadLetterMovesFromBacklog(_ *testing.T, s *suite) {
	d := s.dls()
	rangeID := randRangeID()
	info := s.createQueue(rangeID)
	t1, t2 := s.randomTask(10, 1), s.randomTask(11, 1)
	s.NoError(s.createTasks(rangeID, info, []store.TaskInfo{t1, t2}, []int{0, 0}))
	t1.Data.Attempts = 3
	s.NoError(d.DeadLetterTask(s.ctx, &store.DeadLetterTaskRequest{Key: s.key, RangeID: rangeID, Subqueue: 0, Task: t1, Reason: "r", DeadTime: s.now()}))
	left := s.getAll(0)
	s.Len(left, 1)
	s.Equal(t2.ID, left[0].ID)
	got, _ := s.deadAll(d, 10)
	s.Len(got, 1)
	s.Equal(t1, got[0].Task)
	// Again (row already gone): still fine, still one dead entry.
	s.NoError(d.DeadLetterTask(s.ctx, &store.DeadLetterTaskRequest{Key: s.key, RangeID: rangeID, Subqueue: 0, Task: t1, Reason: "r2", DeadTime: s.now()}))
	got, _ = s.deadAll(d, 10)
	s.Len(got, 1)
	s.Len(s.getAll(0), 1)
}
