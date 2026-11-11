// Package memstore is an in-memory store.Store with real subqueue isolation.
//
// It is modelled on upstream Temporal's in-memory testTaskManager
// (service/matching/matching_engine_test.go, v1.32.1 af08a5e5) but, unlike it, keys tasks by
// subqueue and enforces the full store contract (see store/storetest).
package memstore

import (
	"cmp"
	"context"
	"encoding/json"
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
	OpListQueues            = "ListQueues"
	OpUpdateTask            = "UpdateTask"
	OpDeadLetterTask        = "DeadLetterTask"
	OpListDeadTasks         = "ListDeadTasks"
	OpDeleteDeadTasks       = "DeleteDeadTasks"
	OpCountDeadTasks        = "CountDeadTasks"
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
	dead      map[int64]store.DeadTask
}

var (
	_ store.Store           = (*Store)(nil)
	_ store.QueueLister     = (*Store)(nil)
	_ store.DeadLetterStore = (*Store)(nil)
)

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

func (s *Store) fencedLocked(key store.QueueKey, rangeID int64) (*queueRow, error) {
	q, ok := s.queues[key]
	if !ok {
		return nil, &store.ConditionFailedError{Msg: fmt.Sprintf("queue %v does not exist", key)}
	}
	if q.rangeID != rangeID {
		return nil, &store.ConditionFailedError{
			Msg: fmt.Sprintf("rangeID mismatch: stored %d, request %d", q.rangeID, rangeID),
		}
	}
	return q, nil
}

// UpdateTask implements store.DeadLetterStore.
func (s *Store) UpdateTask(ctx context.Context, req *store.UpdateTaskRequest) error {
	if err := ctxErr(ctx); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.faultLocked(OpUpdateTask); err != nil {
		return err
	}
	q, err := s.fencedLocked(req.Key, req.RangeID)
	if err != nil {
		return err
	}
	tree := q.subqueues[req.Subqueue]
	if tree == nil {
		return &store.NotFoundError{Msg: fmt.Sprintf("task %d", req.Task.ID)}
	}
	if _, ok := tree.Get(store.TaskInfo{Pass: req.Task.Pass, ID: req.Task.ID}); !ok {
		return &store.NotFoundError{Msg: fmt.Sprintf("task %d", req.Task.ID)}
	}
	tree.Set(cloneTask(req.Task))
	return nil
}

// DeadLetterTask implements store.DeadLetterStore.
func (s *Store) DeadLetterTask(ctx context.Context, req *store.DeadLetterTaskRequest) error {
	if err := ctxErr(ctx); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.faultLocked(OpDeadLetterTask); err != nil {
		return err
	}
	q, err := s.fencedLocked(req.Key, req.RangeID)
	if err != nil {
		return err
	}
	if q.dead == nil {
		q.dead = make(map[int64]store.DeadTask)
	}
	// The backlog row stays. Ack-level GC removes it, the same as an acked task.
	q.dead[req.Task.ID] = store.DeadTask{Task: cloneTask(req.Task), Reason: req.Reason, DeadTime: req.DeadTime}
	return nil
}

// ListDeadTasks implements store.DeadLetterStore: task ID order; the token is the last ID.
func (s *Store) ListDeadTasks(ctx context.Context, req *store.ListDeadTasksRequest) (*store.ListDeadTasksResponse, error) {
	if err := ctxErr(ctx); err != nil {
		return nil, err
	}
	if req.PageSize <= 0 {
		return nil, &store.InvalidArgumentError{Msg: fmt.Sprintf("ListDeadTasks: PageSize %d must be positive", req.PageSize)}
	}
	var after *int64
	if len(req.PageToken) > 0 {
		after = new(int64)
		if err := json.Unmarshal(req.PageToken, after); err != nil {
			return nil, &store.InvalidArgumentError{Msg: "ListDeadTasks: invalid page token"}
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.faultLocked(OpListDeadTasks); err != nil {
		return nil, err
	}
	resp := &store.ListDeadTasksResponse{}
	q, ok := s.queues[req.Key]
	if !ok {
		return resp, nil
	}
	ids := make([]int64, 0, len(q.dead))
	for id := range q.dead {
		if after == nil || id > *after {
			ids = append(ids, id)
		}
	}
	slices.Sort(ids)
	for i, id := range ids {
		if i == req.PageSize {
			tok, err := json.Marshal(ids[i-1])
			if err != nil {
				return nil, err
			}
			resp.NextPageToken = tok
			break
		}
		d := q.dead[id]
		d.Task = cloneTask(d.Task)
		resp.Tasks = append(resp.Tasks, d)
	}
	return resp, nil
}

// DeleteDeadTasks implements store.DeadLetterStore.
func (s *Store) DeleteDeadTasks(ctx context.Context, req *store.DeleteDeadTasksRequest) (int, error) {
	if err := ctxErr(ctx); err != nil {
		return 0, err
	}
	if req.TaskIDs == nil && req.DeadBefore.IsZero() {
		return 0, &store.InvalidArgumentError{Msg: "DeleteDeadTasks: TaskIDs or DeadBefore is required"}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.faultLocked(OpDeleteDeadTasks); err != nil {
		return 0, err
	}
	q, ok := s.queues[req.Key]
	if !ok {
		return 0, nil
	}
	n := 0
	if req.TaskIDs != nil {
		for _, id := range req.TaskIDs {
			if _, ok := q.dead[id]; ok {
				delete(q.dead, id)
				n++
			}
		}
		return n, nil
	}
	for id, d := range q.dead {
		if d.DeadTime.Before(req.DeadBefore) {
			delete(q.dead, id)
			n++
		}
	}
	return n, nil
}

// CountDeadTasks implements store.DeadLetterStore.
func (s *Store) CountDeadTasks(ctx context.Context, key store.QueueKey) (int64, error) {
	if err := ctxErr(ctx); err != nil {
		return 0, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.faultLocked(OpCountDeadTasks); err != nil {
		return 0, err
	}
	if q, ok := s.queues[key]; ok {
		return int64(len(q.dead)), nil
	}
	return 0, nil
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

func compareKeys(a, b store.QueueKey) int {
	return cmp.Or(cmp.Compare(a.Namespace, b.Namespace), cmp.Compare(a.Name, b.Name), cmp.Compare(a.Partition, b.Partition))
}

// ListQueues pages through queues in (Namespace, Name, Partition) order. The page token is
// the last key returned, so queues created or deleted during a listing do not shift pages.
func (s *Store) ListQueues(ctx context.Context, req *store.ListQueuesRequest) (*store.ListQueuesResponse, error) {
	if err := ctxErr(ctx); err != nil {
		return nil, err
	}
	if req.PageSize <= 0 {
		return nil, &store.InvalidArgumentError{Msg: fmt.Sprintf("ListQueues: PageSize %d must be positive", req.PageSize)}
	}
	var after *store.QueueKey
	if len(req.PageToken) > 0 {
		after = new(store.QueueKey)
		if err := json.Unmarshal(req.PageToken, after); err != nil {
			return nil, &store.InvalidArgumentError{Msg: "ListQueues: invalid page token"}
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.faultLocked(OpListQueues); err != nil {
		return nil, err
	}
	keys := make([]store.QueueKey, 0, len(s.queues))
	for k := range s.queues {
		if after == nil || compareKeys(k, *after) > 0 {
			keys = append(keys, k)
		}
	}
	slices.SortFunc(keys, compareKeys)
	resp := &store.ListQueuesResponse{}
	for i, k := range keys {
		if i == req.PageSize {
			tok, _ := json.Marshal(keys[i-1])
			resp.NextPageToken = tok
			break
		}
		q := s.queues[k]
		resp.Queues = append(resp.Queues, store.ListedQueue{RangeID: q.rangeID, Info: cloneInfo(q.info)})
	}
	return resp, nil
}
