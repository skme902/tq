package matching

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/skme902/tq/store"
	"github.com/skme902/tq/store/memstore"
)

// A purge whose ctx ends while one priority level is pinned (here by an in-flight admin
// delete) still persists the levels it already purged and reports their rows; the pinned
// level is untouched (review admin r2 #3). The pin is set directly on that level's reader.
func TestPurgePartialOnPinnedLevel(t *testing.T) {
	g := memstore.New()
	e := newR2Engine(t, g, nil, nil, nil)
	for _, tk := range []struct {
		p   string
		pri int32
	}{{"hi0", 1}, {"hi1", 1}, {"lo0", 4}, {"lo1", 4}} {
		_, err := e.AddTask(context.Background(), portQueue, store.Task{Payload: []byte(tk.p), Priority: store.Priority{PriorityKey: tk.pri}})
		require.NoError(t, err)
	}
	page, _, err := e.PeekTasks(context.Background(), portQueue, PeekOptions{})
	require.NoError(t, err)
	require.Len(t, page, 4)
	last := page[0]
	for _, p := range page {
		if p.Ref.Subqueue > last.Ref.Subqueue {
			last = p
		}
	}
	other := map[int]int{}
	for _, p := range page {
		other[p.Ref.Subqueue]++
	}
	require.Len(t, other, 2, "two priority levels, two subqueues")
	firstRows := 4 - other[last.Ref.Subqueue]

	pm := e.partitions[portQueue].defaultQueue.backlogMgr
	pm.subqueueLock.Lock()
	pinned := pm.subqueues[last.Ref.Subqueue]
	pm.subqueueLock.Unlock()
	pinned.lock.Lock()
	pinned.ackLevelPinnedByWriter = true
	pinned.lock.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	n, err := e.PurgeQueue(ctx, portQueue)
	cancel()
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Equal(t, firstRows, n, "rows of the level purged before the wait failed")

	pinned.lock.Lock()
	pinned.ackLevelPinnedByWriter = false
	pinned.advanceAckLevelLocked()
	pinned.lock.Unlock()

	e.Stop()
	rows := backlogRows(t, g)
	require.Len(t, rows, 2, "the pinned level is untouched")
	e2 := newR2Engine(t, g, nil, nil, nil)
	got := map[string]bool{}
	for range rows {
		pctx, pcancel := context.WithTimeout(context.Background(), 2*time.Second)
		d, err := e2.PollTask(pctx, portQueue, PollOptions{})
		pcancel()
		require.NoError(t, err)
		got[string(d.Task.Payload)] = true
		require.NoError(t, d.Ack())
	}
	want := map[string]bool{}
	for _, r := range rows {
		want[string(r.Data.Payload)] = true
	}
	require.Equal(t, want, got)
	_, err = e2.PollTask(context.Background(), portQueue, PollOptions{NoWait: true})
	require.ErrorIs(t, err, ErrNoTasks)
}

// countingDeadStore counts ListDeadTasks calls.
type countingDeadStore struct {
	*memstore.Store
	lists int
}

func (c *countingDeadStore) ListDeadTasks(ctx context.Context, req *store.ListDeadTasksRequest) (*store.ListDeadTasksResponse, error) {
	c.lists++
	return c.Store.ListDeadTasks(ctx, req)
}

// One PeekTasks call pages the dead-letter set once, continuing where the previous backlog
// batch stopped, instead of rescanning it from the start for every batch (review admin r2 #4).
func TestPeekDeadSetScannedOncePerCall(t *testing.T) {
	cs := &countingDeadStore{Store: memstore.New()}
	e := newR2Engine(t, cs, nil, nil, nil)
	e.deadLetters = cs
	_, err := e.AddTask(context.Background(), portQueue, store.Task{Payload: []byte("seed")})
	require.NoError(t, err)
	q, err := cs.GetQueue(context.Background(), portQueue)
	require.NoError(t, err)
	const nDead = 2500 // three ListDeadTasks pages of peekMaxPageSize
	for id := int64(1); id <= nDead; id++ {
		require.NoError(t, cs.DeadLetterTask(context.Background(), &store.DeadLetterTaskRequest{
			Key: portQueue, RangeID: q.RangeID, Task: store.TaskInfo{ID: id, Pass: 1000},
			Reason: "test", DeadTime: time.Now(),
		}))
	}
	var ds peekDeadSet
	batch := func(ids ...int64) map[int64]struct{} {
		tasks := make([]store.TaskInfo, len(ids))
		for i, id := range ids {
			tasks[i] = store.TaskInfo{ID: id}
		}
		dead, err := e.peekDeadIDs(context.Background(), portQueue, &ds, tasks)
		require.NoError(t, err)
		return dead
	}
	for i := 0; i < 20; i++ { // many backlog batches with IDs above the whole dead set
		dead := batch(nDead+int64(i)+1, 7)
		require.Contains(t, dead, int64(7))
		require.NotContains(t, dead, nDead+int64(i)+1)
	}
	require.Equal(t, 3, cs.lists, "dead-letter set paged once for the whole call")
}
