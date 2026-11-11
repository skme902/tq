// Ported from temporalio/temporal v1.32.1 af08a5e5, common/persistence/tests/task_queue_fair_task.go
// (TestCreateGet_Order, TestCreateDelete_Range, TestCreateDeleteWithConditionRangeID), plus the
// fair-applicable cases of common/persistence/tests/task_queue.go and task_queue_task.go, adapted
// to the store.Store interface. Cases marked "(new)" are not upstream.
// Copyright (c) 2025 Temporal Technologies Inc. MIT License; see UPSTREAM_LICENSE and NOTICE.

// Package storetest is a conformance suite for store.Store implementations.
package storetest

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/skme902/tq/store"
)

// Run runs the conformance suite. newStore must return a store; it may be shared across
// subtests because every subtest uses a fresh random queue key.
func Run(t *testing.T, newStore func(t *testing.T) store.Store) {
	tests := []struct {
		name string
		fn   func(t *testing.T, s *suite)
	}{
		// task_queue_fair_task.go
		{"CreateGet_Order", testCreateGetOrder},
		{"CreateDelete_Range", testCreateDeleteRange},
		{"CreateDeleteWithConditionRangeID", testCreateDeleteWithConditionRangeID},
		// task_queue.go
		{"CreateQueue", testCreateQueue},
		{"CreateQueue_Dup", testCreateQueueDup},
		{"UpdateQueue", testUpdateQueue},
		{"UpdateQueue_Conflict", testUpdateQueueConflict},
		{"DeleteQueue", testDeleteQueue},
		{"DeleteQueue_Conflict", testDeleteQueueConflict},
		// task_queue_task.go
		{"CreateGet_Conflict", testCreateGetConflict},
		{"CreateGet_One", testCreateGetOne},
		{"CreateGet_Multiple_Paged", testCreateGetMultiplePaged},
		{"CreateDelete_Multiple", testCreateDeleteMultiple},
		// (new)
		{"GetQueue_NotFound", testGetQueueNotFound},
		{"Subqueue_Isolation", testSubqueueIsolation},
		{"CreateTasks_UpdateMetadata", testCreateTasksUpdateMetadata},
		{"CreateTasks_AllOrNothing_OnConflict", testCreateTasksAllOrNothing},
		{"GetTasks_InclusiveMin", testGetTasksInclusiveMin},
		{"GetTasks_RejectsPassZero", testGetTasksRejectsPassZero},
		{"CompleteTasksLessThan_Limit", testCompleteLimit},
		{"Payload_NotAliased", testPayloadNotAliased},
		{"ListQueues_Paged", testListQueuesPaged},
		{"ListQueues_ReadOnly", testListQueuesReadOnly},
		{"ListQueues_InvalidArgs", testListQueuesInvalidArgs},
		{"UpdateTask_InPlace", testUpdateTaskInPlace},
		{"UpdateTask_Fenced", testUpdateTaskFenced},
		{"CheckHealth", testCheckHealth},
		{"DeleteTask_One", testDeleteTaskOne},
		{"DeleteTask_Fenced", testDeleteTaskFenced},
		{"DeadLetter_RoundTrip", testDeadLetterRoundTrip},
		{"DeadLetter_MovesFromBacklog", testDeadLetterMovesFromBacklog},
		{"DeadLetter_Fenced", testDeadLetterFenced},
		{"DeadLetter_Paged", testDeadLetterPaged},
		{"DeadLetter_Delete", testDeadLetterDelete},
		{"DeadLetter_DeleteQueue", testDeadLetterDeleteQueue},
		{"DeadLetter_InvalidArgs", testDeadLetterInvalidArgs},
		{"Task_AttemptsRoundTrip", testTaskAttemptsRoundTrip},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			s := &suite{
				Assertions: require.New(t),
				t:          t,
				ctx:        ctx,
				store:      newStore(t),
				key: store.QueueKey{
					Namespace: fmt.Sprintf("ns-%d", rand.Int63()),
					Name:      fmt.Sprintf("tq-%d", rand.Int63()),
					Partition: rand.Intn(4),
				},
			}
			tc.fn(t, s)
		})
	}
}

type suite struct {
	*require.Assertions
	t     *testing.T
	ctx   context.Context
	store store.Store
	key   store.QueueKey
}

// positive random rangeID (0 means "unconditional" in CompleteTasksLessThan)
func randRangeID() int64 { return rand.Int63n(math.MaxInt64-2) + 2 }

func (s *suite) now() time.Time { return time.Now().UTC().Truncate(time.Microsecond) }

func (s *suite) randomQueueInfo() *store.QueueInfo {
	now := s.now()
	return &store.QueueInfo{
		Key: s.key,
		Subqueues: []store.SubqueueInfo{{
			Key:                     store.SubqueueKey{Priority: 3},
			FairAckLevel:            store.Level{Pass: rand.Int63n(1000) + 1, ID: rand.Int63n(1000)},
			ApproximateBacklogCount: rand.Int63n(100),
			TopKFairnessCounts:      []store.FairnessKeyCount{{Key: "k", Count: rand.Int63n(100)}},
		}},
		LastUpdateTime: now,
	}
}

func (s *suite) createQueue(rangeID int64) *store.QueueInfo {
	info := s.randomQueueInfo()
	s.NoError(s.store.CreateQueue(s.ctx, &store.CreateQueueRequest{RangeID: rangeID, Info: info}))
	return info
}

func (s *suite) randomTask(id, pass int64) store.TaskInfo {
	now := s.now()
	return store.TaskInfo{
		ID:   id,
		Pass: pass,
		Data: store.Task{
			Payload:    []byte(fmt.Sprintf("payload-%d", rand.Int63())),
			Priority:   store.Priority{PriorityKey: int32(rand.Intn(5) + 1), FairnessKey: "k", FairnessWeight: 1},
			CreateTime: now,
			ExpiryTime: now.Add(16 * time.Second),
		},
	}
}

func (s *suite) createTasks(rangeID int64, info *store.QueueInfo, tasks []store.TaskInfo, subqueues []int) error {
	_, err := s.store.CreateTasks(s.ctx, &store.CreateTasksRequest{
		RangeID: rangeID, Info: info, Tasks: tasks, Subqueues: subqueues,
	})
	return err
}

func (s *suite) getTasks(subqueue int, min store.Level, pageSize int) []store.TaskInfo {
	resp, err := s.store.GetTasks(s.ctx, &store.GetTasksRequest{
		Key: s.key, Subqueue: subqueue, InclusiveMin: min, PageSize: pageSize,
	})
	s.NoError(err)
	return resp.Tasks
}

func (s *suite) getAll(subqueue int) []store.TaskInfo {
	return s.getTasks(subqueue, store.Level{Pass: 1}, 100000)
}

func (s *suite) assertQueue(rangeID int64, info *store.QueueInfo) {
	resp, err := s.store.GetQueue(s.ctx, s.key)
	s.NoError(err)
	s.Equal(rangeID, resp.RangeID)
	s.Equal(info, resp.Info)
}

// --- task_queue_fair_task.go ---

func testCreateGetOrder(_ *testing.T, s *suite) {
	rangeID := randRangeID()
	info := s.createQueue(rangeID)
	tasks := []store.TaskInfo{
		s.randomTask(10, 2), // higher pass
		s.randomTask(20, 1), // lower pass, higher id
		s.randomTask(5, 1),  // same pass, lower id
		s.randomTask(15, 2), // same pass as first but lower id
	}
	s.NoError(s.createTasks(rangeID, info, tasks, nil))
	got := s.getTasks(0, store.Level{Pass: 1, ID: 0}, 10)
	s.Equal([]store.TaskInfo{tasks[2], tasks[1], tasks[0], tasks[3]}, got)
}

func testCreateDeleteRange(_ *testing.T, s *suite) {
	rangeID := randRangeID()
	info := s.createQueue(rangeID)
	var tasks []store.TaskInfo
	for pass := int64(1); pass <= 5; pass++ {
		tasks = append(tasks, s.randomTask(pass, pass))
	}
	s.NoError(s.createTasks(rangeID, info, tasks, nil))
	_, err := s.store.CompleteTasksLessThan(s.ctx, &store.CompleteTasksLessThanRequest{
		Key: s.key, ExclusiveMax: store.Level{Pass: 3, ID: 0}, Limit: 10,
	})
	s.NoError(err)
	s.Equal(tasks[2:], s.getTasks(0, store.Level{Pass: 1}, 10))
}

func testCreateDeleteWithConditionRangeID(_ *testing.T, s *suite) {
	rangeID := randRangeID()
	info := s.createQueue(rangeID)
	var tasks []store.TaskInfo
	for pass := int64(1); pass <= 5; pass++ {
		tasks = append(tasks, s.randomTask(pass, pass))
	}
	s.NoError(s.createTasks(rangeID, info, tasks, nil))
	completeLessThan := func(pass, conditionRangeID int64) error {
		_, err := s.store.CompleteTasksLessThan(s.ctx, &store.CompleteTasksLessThanRequest{
			Key: s.key, ExclusiveMax: store.Level{Pass: pass, ID: 0}, Limit: 10,
			ConditionRangeID: conditionRangeID,
		})
		return err
	}
	// matching range id: tasks are deleted
	s.NoError(completeLessThan(3, rangeID))
	s.Equal(tasks[2:], s.getTasks(0, store.Level{Pass: 1}, 10))
	// wrong range id: condition fails and nothing is deleted
	err := completeLessThan(5, rangeID-1)
	s.True(store.IsConditionFailed(err), "expected ConditionFailedError, got %v", err)
	s.Equal(tasks[2:], s.getTasks(0, store.Level{Pass: 1}, 10))
}

// --- task_queue.go ---

func testCreateQueue(_ *testing.T, s *suite) {
	rangeID := randRangeID()
	info := s.createQueue(rangeID)
	s.assertQueue(rangeID, info)
}

func testCreateQueueDup(_ *testing.T, s *suite) {
	rangeID := randRangeID()
	info := s.createQueue(rangeID)
	err := s.store.CreateQueue(s.ctx, &store.CreateQueueRequest{RangeID: rangeID, Info: s.randomQueueInfo()})
	s.True(store.IsConditionFailed(err), "expected ConditionFailedError, got %v", err)
	s.assertQueue(rangeID, info)
}

func testUpdateQueue(_ *testing.T, s *suite) {
	prev := randRangeID()
	s.createQueue(prev)
	rangeID := randRangeID()
	info := s.randomQueueInfo()
	s.NoError(s.store.UpdateQueue(s.ctx, &store.UpdateQueueRequest{RangeID: rangeID, PrevRangeID: prev, Info: info}))
	s.assertQueue(rangeID, info)
}

func testUpdateQueueConflict(_ *testing.T, s *suite) {
	prev := randRangeID()
	info := s.createQueue(prev)
	err := s.store.UpdateQueue(s.ctx, &store.UpdateQueueRequest{
		RangeID: randRangeID(), PrevRangeID: prev - 1, Info: s.randomQueueInfo(),
	})
	s.True(store.IsConditionFailed(err), "expected ConditionFailedError, got %v", err)
	s.assertQueue(prev, info)
}

func testDeleteQueue(_ *testing.T, s *suite) {
	rangeID := randRangeID()
	s.createQueue(rangeID)
	s.NoError(s.store.DeleteQueue(s.ctx, &store.DeleteQueueRequest{Key: s.key, RangeID: rangeID}))
	_, err := s.store.GetQueue(s.ctx, s.key)
	s.True(store.IsNotFound(err), "expected NotFoundError, got %v", err)
}

func testDeleteQueueConflict(_ *testing.T, s *suite) {
	rangeID := randRangeID()
	info := s.createQueue(rangeID)
	err := s.store.DeleteQueue(s.ctx, &store.DeleteQueueRequest{Key: s.key, RangeID: rangeID - 1})
	s.True(store.IsConditionFailed(err), "expected ConditionFailedError, got %v", err)
	s.assertQueue(rangeID, info)
}

// --- task_queue_task.go ---

func testCreateGetConflict(_ *testing.T, s *suite) {
	rangeID := randRangeID()
	info := s.createQueue(rangeID)
	err := s.createTasks(rangeID-1, info, []store.TaskInfo{s.randomTask(7, 1)}, nil)
	s.True(store.IsConditionFailed(err), "expected ConditionFailedError, got %v", err)
	s.Empty(s.getAll(0))
}

func testCreateGetOne(_ *testing.T, s *suite) {
	rangeID := randRangeID()
	info := s.createQueue(rangeID)
	task := s.randomTask(rand.Int63n(1<<40)+1, rand.Int63n(1<<40)+1)
	s.NoError(s.createTasks(rangeID, info, []store.TaskInfo{task}, nil))
	s.Equal([]store.TaskInfo{task}, s.getTasks(0, store.Level{Pass: task.Pass, ID: task.ID}, 100))
}

func testCreateGetMultiplePaged(_ *testing.T, s *suite) {
	const numBatch, batchSize = 32, 32
	rangeID := randRangeID()
	info := s.createQueue(rangeID)
	minID := rand.Int63n(1 << 40)
	var expected []store.TaskInfo
	for i := range numBatch {
		var tasks []store.TaskInfo
		for j := range batchSize {
			id := minID + int64(i*batchSize+j) + 1
			t := s.randomTask(id, 1)
			tasks = append(tasks, t)
			expected = append(expected, t)
		}
		s.NoError(s.createTasks(rangeID, info, tasks, nil))
	}
	// page through with PageSize 1, resuming after the last level returned
	var got []store.TaskInfo
	from := store.Level{Pass: 1, ID: 0}
	for {
		page := s.getTasks(0, from, 1)
		if len(page) == 0 {
			break
		}
		s.Len(page, 1)
		got = append(got, page...)
		from = store.Level{Pass: page[0].Pass, ID: page[0].ID + 1}
	}
	s.Equal(expected, got)
}

func testCreateDeleteMultiple(_ *testing.T, s *suite) {
	const numBatch, batchSize = 32, 32
	rangeID := randRangeID()
	info := s.createQueue(rangeID)
	minID := rand.Int63n(1 << 40)
	for i := range numBatch {
		var tasks []store.TaskInfo
		for j := range batchSize {
			tasks = append(tasks, s.randomTask(minID+int64(i*batchSize+j)+1, 1))
		}
		s.NoError(s.createTasks(rangeID, info, tasks, nil))
	}
	_, err := s.store.CompleteTasksLessThan(s.ctx, &store.CompleteTasksLessThanRequest{
		Key: s.key, ExclusiveMax: store.Level{Pass: 2, ID: 0}, Limit: numBatch * batchSize,
	})
	s.NoError(err)
	s.Empty(s.getAll(0))
}

// --- (new) ---

func testGetQueueNotFound(_ *testing.T, s *suite) {
	_, err := s.store.GetQueue(s.ctx, s.key)
	s.True(store.IsNotFound(err), "expected NotFoundError, got %v", err)
}

func testSubqueueIsolation(_ *testing.T, s *suite) {
	rangeID := randRangeID()
	info := s.createQueue(rangeID)
	t0 := []store.TaskInfo{s.randomTask(1, 5), s.randomTask(2, 6)}
	t1 := []store.TaskInfo{s.randomTask(3, 1), s.randomTask(4, 2)}
	s.NoError(s.createTasks(rangeID, info, append(append([]store.TaskInfo{}, t0...), t1...), []int{0, 0, 1, 1}))
	s.Equal(t0, s.getAll(0))
	s.Equal(t1, s.getAll(1))
	s.Empty(s.getAll(2))
	// delete in subqueue 1 does not touch subqueue 0
	n, err := s.store.CompleteTasksLessThan(s.ctx, &store.CompleteTasksLessThanRequest{
		Key: s.key, Subqueue: 1, ExclusiveMax: store.Level{Pass: math.MaxInt64, ID: math.MaxInt64}, Limit: 100,
		ConditionRangeID: rangeID,
	})
	s.NoError(err)
	s.True(n == 2 || n == store.UnknownNumRowsAffected, "n=%d", n)
	s.Empty(s.getAll(1))
	s.Equal(t0, s.getAll(0))
}

func testCreateTasksUpdateMetadata(_ *testing.T, s *suite) {
	rangeID := randRangeID()
	info := s.createQueue(rangeID)
	newInfo := s.randomQueueInfo()
	resp, err := s.store.CreateTasks(s.ctx, &store.CreateTasksRequest{
		RangeID: rangeID, Info: newInfo, Tasks: []store.TaskInfo{s.randomTask(1, 1)}, UpdateMetadata: true,
	})
	s.NoError(err)
	if resp.UpdatedMetadata {
		s.assertQueue(rangeID, newInfo)
	} else {
		s.assertQueue(rangeID, info)
	}
	// UpdateMetadata=false never rewrites the row
	resp, err = s.store.CreateTasks(s.ctx, &store.CreateTasksRequest{
		RangeID: rangeID, Info: s.randomQueueInfo(), Tasks: []store.TaskInfo{s.randomTask(2, 1)},
	})
	s.NoError(err)
	s.False(resp.UpdatedMetadata)
}

func testCreateTasksAllOrNothing(_ *testing.T, s *suite) {
	rangeID := randRangeID()
	info := s.createQueue(rangeID)
	s.NoError(s.store.UpdateQueue(s.ctx, &store.UpdateQueueRequest{RangeID: rangeID + 1, PrevRangeID: rangeID, Info: info}))
	err := s.createTasks(rangeID, info, []store.TaskInfo{s.randomTask(1, 1), s.randomTask(2, 1)}, []int{0, 1})
	s.True(store.IsConditionFailed(err), "expected ConditionFailedError, got %v", err)
	s.Empty(s.getAll(0))
	s.Empty(s.getAll(1))
}

func testGetTasksInclusiveMin(_ *testing.T, s *suite) {
	rangeID := randRangeID()
	info := s.createQueue(rangeID)
	tasks := []store.TaskInfo{s.randomTask(1, 1), s.randomTask(2, 1), s.randomTask(3, 2), s.randomTask(1, 3)}
	s.NoError(s.createTasks(rangeID, info, tasks, nil))
	s.Equal(tasks[1:], s.getTasks(0, store.Level{Pass: 1, ID: 2}, 10))
	s.Equal(tasks[2:], s.getTasks(0, store.Level{Pass: 1, ID: 3}, 10))
	s.Equal(tasks[2:3], s.getTasks(0, store.Level{Pass: 2, ID: 0}, 1))
}

func testGetTasksRejectsPassZero(_ *testing.T, s *suite) {
	s.createQueue(randRangeID())
	_, err := s.store.GetTasks(s.ctx, &store.GetTasksRequest{Key: s.key, InclusiveMin: store.Level{}, PageSize: 10})
	s.Error(err)
}

func testCompleteLimit(_ *testing.T, s *suite) {
	rangeID := randRangeID()
	info := s.createQueue(rangeID)
	var tasks []store.TaskInfo
	for i := int64(1); i <= 10; i++ {
		tasks = append(tasks, s.randomTask(i, 1))
	}
	s.NoError(s.createTasks(rangeID, info, tasks, nil))
	n, err := s.store.CompleteTasksLessThan(s.ctx, &store.CompleteTasksLessThanRequest{
		Key: s.key, ExclusiveMax: store.Level{Pass: 1, ID: 9}, Limit: 3, ConditionRangeID: rangeID,
	})
	s.NoError(err)
	left := s.getAll(0)
	switch {
	case n == store.UnknownNumRowsAffected:
		// limit may be ignored; never deletes at or above the bound
		s.GreaterOrEqual(len(left), 2)
	default:
		s.LessOrEqual(n, 8)
		s.Len(left, 10-n)
	}
	for _, t := range left[len(left)-2:] {
		s.GreaterOrEqual(t.ID, int64(9))
	}
}

func testPayloadNotAliased(_ *testing.T, s *suite) {
	rangeID := randRangeID()
	info := s.createQueue(rangeID)
	task := s.randomTask(1, 1)
	want := append([]byte(nil), task.Data.Payload...)
	s.NoError(s.createTasks(rangeID, info, []store.TaskInfo{task}, nil))
	task.Data.Payload[0] ^= 0xff
	got := s.getAll(0)
	s.Equal(want, got[0].Data.Payload)
}

func (s *suite) lister() store.QueueLister {
	l, ok := s.store.(store.QueueLister)
	if !ok {
		s.t.Skip("store does not implement store.QueueLister")
	}
	return l
}

// listAll pages through ListQueues and returns the rows in this suite's namespace (the store
// may be shared with other subtests).
func (s *suite) listAll(pageSize int) (mine []store.ListedQueue, pages int) {
	l := s.lister()
	var tok []byte
	for {
		resp, err := l.ListQueues(s.ctx, &store.ListQueuesRequest{PageSize: pageSize, PageToken: tok})
		s.NoError(err)
		s.LessOrEqual(len(resp.Queues), pageSize)
		pages++
		for _, q := range resp.Queues {
			if q.Info.Key.Namespace == s.key.Namespace {
				mine = append(mine, q)
			}
		}
		if resp.NextPageToken == nil {
			return mine, pages
		}
		s.NotEmpty(resp.Queues, "a page with a next token must not be empty")
		tok = resp.NextPageToken
	}
}

func testListQueuesPaged(_ *testing.T, s *suite) {
	s.lister()
	want := map[store.QueueKey]int64{}
	for i := range 7 {
		s.key.Name = fmt.Sprintf("list-%d", i)
		s.key.Partition = i % 2
		rangeID := randRangeID()
		s.createQueue(rangeID)
		want[s.key] = rangeID
	}
	for _, pageSize := range []int{1, 3, 7, 1000} {
		got, pages := s.listAll(pageSize)
		s.Len(got, len(want), "page size %d", pageSize)
		seen := map[store.QueueKey]bool{}
		for _, q := range got {
			s.False(seen[q.Info.Key], "queue %v listed twice", q.Info.Key)
			seen[q.Info.Key] = true
			s.Equal(want[q.Info.Key], q.RangeID)
			s.Len(q.Info.Subqueues, 1)
		}
		if pageSize == 1 {
			s.GreaterOrEqual(pages, len(want))
		}
	}
}

func testListQueuesReadOnly(_ *testing.T, s *suite) {
	rangeID := randRangeID()
	info := s.createQueue(rangeID)
	got, _ := s.listAll(2)
	s.Len(got, 1)
	s.assertQueue(rangeID, info)
	// The listed copy is not aliased to the store.
	got[0].Info.Subqueues[0].ApproximateBacklogCount = -42
	s.assertQueue(rangeID, info)
}

func testListQueuesInvalidArgs(_ *testing.T, s *suite) {
	l := s.lister()
	_, err := l.ListQueues(s.ctx, &store.ListQueuesRequest{PageSize: 0})
	var ia *store.InvalidArgumentError
	s.ErrorAs(err, &ia)
	_, err = l.ListQueues(s.ctx, &store.ListQueuesRequest{PageSize: 1, PageToken: []byte("not a token")})
	s.ErrorAs(err, &ia)
}
