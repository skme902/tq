// Package memstore is an in-memory store.Store with real subqueue isolation.
//
// It is modelled on upstream Temporal's in-memory testTaskManager
// (service/matching/matching_engine_test.go, v1.32.1 af08a5e5) but, unlike it, keys tasks by
// subqueue and enforces the full store contract (see store/storetest).
package memstore

import (
	"context"
	"fmt"
	"slices"
	"sync"

	"github.com/tidwall/btree"

	"github.com/skme902/tq/store"
)

// Op names for error injection.
const (
	OpCreateQueue           = "CreateQueue"
	OpGetQueue              = "GetQueue"
	OpUpdateQueue           = "UpdateQueue"
	OpDeleteQueue           = "DeleteQueue"
	OpCreateTasks           = "CreateTasks"
	OpGetTasks              = "GetTasks"
	OpCompleteTasksLessThan = "CompleteTasksLessThan"
)

// Store is an in-memory store.Store. The zero value is not usable; call New.
type Store struct {
	mu     sync.Mutex
	queues map[store.QueueKey]*queueRow
	faults map[string][]error
}

type queueRow struct {
	rangeID   int64
	info      *store.QueueInfo
	subqueues map[int]*btree.BTreeG[store.TaskInfo]
}

var _ store.Store = (*Store)(nil)

// New returns an empty store.
func New() *Store {
	return &Store{
		queues: make(map[store.QueueKey]*queueRow),
		faults: make(map[string][]error),
	}
}

func taskLess(a, b store.TaskInfo) bool {
	return store.Level{Pass: a.Pass, ID: a.ID}.Less(store.Level{Pass: b.Pass, ID: b.ID})
}

// InjectError makes the next calls of op return err (one call per queued error), before
// touching any state.
func (s *Store) InjectError(op string, errs ...error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.faults[op] = append(s.faults[op], errs...)
}

func (s *Store) faultLocked(op string) error {
	if q := s.faults[op]; len(q) > 0 {
		s.faults[op] = q[1:]
		return q[0]
	}
	return nil
}

func ctxErr(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return nil
}

func (s *Store) CreateQueue(ctx context.Context, req *store.CreateQueueRequest) error {
	if err := ctxErr(ctx); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.faultLocked(OpCreateQueue); err != nil {
		return err
	}
	key := req.Info.Key
	if _, ok := s.queues[key]; ok {
		return &store.ConditionFailedError{Msg: fmt.Sprintf("queue %v already exists", key)}
	}
	s.queues[key] = &queueRow{
		rangeID:   req.RangeID,
		info:      cloneInfo(req.Info),
		subqueues: make(map[int]*btree.BTreeG[store.TaskInfo]),
	}
	return nil
}

func (s *Store) GetQueue(ctx context.Context, key store.QueueKey) (*store.GetQueueResponse, error) {
	if err := ctxErr(ctx); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.faultLocked(OpGetQueue); err != nil {
		return nil, err
	}
	q, ok := s.queues[key]
	if !ok {
		return nil, &store.NotFoundError{Msg: fmt.Sprintf("queue %v", key)}
	}
	return &store.GetQueueResponse{RangeID: q.rangeID, Info: cloneInfo(q.info)}, nil
}

func (s *Store) UpdateQueue(ctx context.Context, req *store.UpdateQueueRequest) error {
	if err := ctxErr(ctx); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.faultLocked(OpUpdateQueue); err != nil {
		return err
	}
	q, ok := s.queues[req.Info.Key]
	if !ok {
		return &store.ConditionFailedError{Msg: fmt.Sprintf("queue %v does not exist", req.Info.Key)}
	}
	if q.rangeID != req.PrevRangeID {
		return &store.ConditionFailedError{
			Msg: fmt.Sprintf("rangeID mismatch: stored %d, expected %d", q.rangeID, req.PrevRangeID),
		}
	}
	q.rangeID = req.RangeID
	q.info = cloneInfo(req.Info)
	return nil
}

func (s *Store) DeleteQueue(ctx context.Context, req *store.DeleteQueueRequest) error {
	if err := ctxErr(ctx); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.faultLocked(OpDeleteQueue); err != nil {
		return err
	}
	q, ok := s.queues[req.Key]
	if !ok {
		return nil
	}
	if q.rangeID != req.RangeID {
		return &store.ConditionFailedError{
			Msg: fmt.Sprintf("rangeID mismatch: stored %d, expected %d", q.rangeID, req.RangeID),
		}
	}
	delete(s.queues, req.Key)
	return nil
}

func (s *Store) CreateTasks(ctx context.Context, req *store.CreateTasksRequest) (*store.CreateTasksResponse, error) {
	if err := ctxErr(ctx); err != nil {
		return nil, err
	}
	if req.Subqueues != nil && len(req.Subqueues) != len(req.Tasks) {
		return nil, &store.InvalidArgumentError{Msg: "len(Subqueues) != len(Tasks)"}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.faultLocked(OpCreateTasks); err != nil {
		return nil, err
	}
	q, ok := s.queues[req.Info.Key]
	if !ok {
		return nil, &store.ConditionFailedError{Msg: fmt.Sprintf("queue %v does not exist", req.Info.Key)}
	}
	if q.rangeID != req.RangeID {
		return nil, &store.ConditionFailedError{
			Msg: fmt.Sprintf("rangeID mismatch: stored %d, request %d", q.rangeID, req.RangeID),
		}
	}
	for _, t := range req.Tasks {
		if t.Pass < 1 {
			return nil, &store.InvalidArgumentError{Msg: fmt.Sprintf("task %d has pass %d < 1", t.ID, t.Pass)}
		}
	}
	for i, t := range req.Tasks {
		sq := 0
		if req.Subqueues != nil {
			sq = req.Subqueues[i]
		}
		tree := q.subqueues[sq]
		if tree == nil {
			tree = btree.NewBTreeGOptions(taskLess, btree.Options{NoLocks: true})
			q.subqueues[sq] = tree
		}
		tree.Set(cloneTask(t))
	}
	if req.UpdateMetadata {
		q.info = cloneInfo(req.Info)
	}
	return &store.CreateTasksResponse{UpdatedMetadata: req.UpdateMetadata}, nil
}

func (s *Store) GetTasks(ctx context.Context, req *store.GetTasksRequest) (*store.GetTasksResponse, error) {
	if err := ctxErr(ctx); err != nil {
		return nil, err
	}
	if req.InclusiveMin.Pass < 1 {
		return nil, &store.InvalidArgumentError{Msg: "InclusiveMin.Pass must be >= 1"}
	}
	if req.PageSize <= 0 {
		return nil, &store.InvalidArgumentError{Msg: "PageSize must be > 0"}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.faultLocked(OpGetTasks); err != nil {
		return nil, err
	}
	resp := &store.GetTasksResponse{Tasks: []store.TaskInfo{}}
	q, ok := s.queues[req.Key]
	if !ok {
		return resp, nil
	}
	tree := q.subqueues[req.Subqueue]
	if tree == nil {
		return resp, nil
	}
	pivot := store.TaskInfo{Pass: req.InclusiveMin.Pass, ID: req.InclusiveMin.ID}
	tree.Ascend(pivot, func(t store.TaskInfo) bool {
		resp.Tasks = append(resp.Tasks, cloneTask(t))
		return len(resp.Tasks) < req.PageSize
	})
	return resp, nil
}

func (s *Store) CompleteTasksLessThan(ctx context.Context, req *store.CompleteTasksLessThanRequest) (int, error) {
	if err := ctxErr(ctx); err != nil {
		return 0, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.faultLocked(OpCompleteTasksLessThan); err != nil {
		return 0, err
	}
	q, ok := s.queues[req.Key]
	if !ok {
		if req.ConditionRangeID != 0 {
			return 0, &store.ConditionFailedError{Msg: fmt.Sprintf("queue %v does not exist", req.Key)}
		}
		return 0, nil
	}
	if req.ConditionRangeID != 0 && q.rangeID != req.ConditionRangeID {
		return 0, &store.ConditionFailedError{
			Msg: fmt.Sprintf("rangeID mismatch: stored %d, condition %d", q.rangeID, req.ConditionRangeID),
		}
	}
	tree := q.subqueues[req.Subqueue]
	if tree == nil {
		return 0, nil
	}
	var del []store.TaskInfo
	tree.Scan(func(t store.TaskInfo) bool {
		if !(store.Level{Pass: t.Pass, ID: t.ID}).Less(req.ExclusiveMax) {
			return false
		}
		if req.Limit > 0 && len(del) >= req.Limit {
			return false
		}
		del = append(del, t)
		return true
	})
	for _, t := range del {
		tree.Delete(t)
	}
	return len(del), nil
}

// TaskCount returns the number of stored tasks in a subqueue (test helper).
func (s *Store) TaskCount(key store.QueueKey, subqueue int) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	q, ok := s.queues[key]
	if !ok || q.subqueues[subqueue] == nil {
		return 0
	}
	return q.subqueues[subqueue].Len()
}

// TotalTaskCount returns the number of stored tasks across all subqueues (test helper).
func (s *Store) TotalTaskCount(key store.QueueKey) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	q, ok := s.queues[key]
	if !ok {
		return 0
	}
	n := 0
	for _, t := range q.subqueues {
		n += t.Len()
	}
	return n
}

func cloneTask(t store.TaskInfo) store.TaskInfo {
	t.Data.Payload = slices.Clone(t.Data.Payload)
	return t
}

func cloneInfo(in *store.QueueInfo) *store.QueueInfo {
	if in == nil {
		return nil
	}
	out := *in
	out.Subqueues = make([]store.SubqueueInfo, len(in.Subqueues))
	for i, sq := range in.Subqueues {
		sq.TopKFairnessCounts = slices.Clone(sq.TopKFairnessCounts)
		out.Subqueues[i] = sq
	}
	return &out
}
