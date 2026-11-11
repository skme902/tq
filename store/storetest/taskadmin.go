package storetest

// Conformance cases for store.TaskAdminStore (new; no upstream counterpart).

import (
	"testing"

	"github.com/skme902/tq/store"
)

func (s *suite) tas() store.TaskAdminStore {
	d, ok := s.store.(store.TaskAdminStore)
	if !ok {
		s.t.Skip("store does not implement store.TaskAdminStore")
	}
	return d
}

func testDeleteTaskOne(_ *testing.T, s *suite) {
	d := s.tas()
	rangeID := randRangeID()
	info := s.createQueue(rangeID)
	info.Subqueues = append(info.Subqueues, store.SubqueueInfo{Key: store.SubqueueKey{Priority: 2}})
	a, b, c := s.randomTask(1, 1), s.randomTask(2, 1), s.randomTask(3, 2)
	s.NoError(s.createTasks(rangeID, info, []store.TaskInfo{a, b, c}, []int{0, 0, 1}))

	ok, err := d.DeleteTask(s.ctx, &store.DeleteTaskRequest{Key: s.key, RangeID: rangeID, Subqueue: 0, Pass: 1, ID: 2})
	s.NoError(err)
	s.True(ok)
	s.Equal([]store.TaskInfo{a}, s.getAll(0))
	s.Equal([]store.TaskInfo{c}, s.getAll(1))

	// Missing: wrong subqueue, wrong pass, or already deleted.
	for _, r := range []store.DeleteTaskRequest{
		{Subqueue: 1, Pass: 1, ID: 1}, {Subqueue: 0, Pass: 2, ID: 1}, {Subqueue: 0, Pass: 1, ID: 2}, {Subqueue: 7, Pass: 1, ID: 1},
	} {
		r.Key, r.RangeID = s.key, rangeID
		ok, err := d.DeleteTask(s.ctx, &r)
		s.NoError(err)
		s.False(ok, "%+v", r)
	}
	s.Equal([]store.TaskInfo{a}, s.getAll(0))
}

func testDeleteTaskFenced(_ *testing.T, s *suite) {
	d := s.tas()
	rangeID := randRangeID()
	info := s.createQueue(rangeID)
	t := s.randomTask(1, 1)
	s.NoError(s.createTasks(rangeID, info, []store.TaskInfo{t}, nil))
	var cf *store.ConditionFailedError
	_, err := d.DeleteTask(s.ctx, &store.DeleteTaskRequest{Key: s.key, RangeID: rangeID + 1, Pass: 1, ID: 1})
	s.ErrorAs(err, &cf)
	s.Equal([]store.TaskInfo{t}, s.getAll(0))
	other := *s
	other.key.Name += "-missing"
	_, err = d.DeleteTask(s.ctx, &store.DeleteTaskRequest{Key: other.key, RangeID: rangeID, Pass: 1, ID: 1})
	s.ErrorAs(err, &cf)
}
