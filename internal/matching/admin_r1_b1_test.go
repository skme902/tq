package matching

// Regression tests for feat/ops-admin review round 1 batch 1 (delivery attempt rollback,
// delete/ack backlog count, delete failure re-read, purge vs writer ack pin, purge GC retry).

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/skme902/tq/clock"
	"github.com/skme902/tq/store"
	"github.com/skme902/tq/store/memstore"
)

func closeOnce(ch chan struct{}) {
	select {
	case <-ch:
	default:
		close(ch)
	}
}

func subqueueReader(t *testing.T, e *Engine) *fairTaskReader {
	t.Helper()
	e.partitionsLock.RLock()
	pm := e.partitions[portQueue]
	e.partitionsLock.RUnlock()
	require.NotNil(t, pm, "queue should be loaded")
	bm := pm.defaultQueue.backlogMgr
	bm.subqueueLock.Lock()
	defer bm.subqueueLock.Unlock()
	require.NotEmpty(t, bm.subqueues)
	return bm.subqueues[0]
}

func backlogCount(t *testing.T, e *Engine) int64 {
	t.Helper()
	desc, err := e.DescribeQueue(context.Background(), portQueue)
	require.NoError(t, err)
	return desc.Stats.ApproximateBacklogCount
}

type attemptLog struct {
	*memstore.Store
	mu     sync.Mutex
	writes []int32
}

func (a *attemptLog) UpdateTask(ctx context.Context, req *store.UpdateTaskRequest) error {
	a.mu.Lock()
	a.writes = append(a.writes, req.Task.Data.Attempts)
	a.mu.Unlock()
	return a.Store.UpdateTask(ctx, req)
}

func (a *attemptLog) attemptWrites() []int32 {
	a.mu.Lock()
	defer a.mu.Unlock()
	return append([]int32(nil), a.writes...)
}

// DeleteTask during the attempt write settles the lease with ErrLeaseLost. PollTask must
// report ErrNoTasks (the engine is not stopping) and must not roll the attempt count back.
func TestDeleteDuringAttemptWriteReturnsNoTasks(t *testing.T) {
	st := &attemptLog{Store: memstore.New()}
	e := newR2Engine(t, st, nil, nil, func(c *Config) { c.MaxAttempts = 4 })
	_, err := e.AddTask(context.Background(), portQueue, store.Task{Payload: []byte("gone")})
	require.NoError(t, err)
	page, _, err := e.PeekTasks(context.Background(), portQueue, PeekOptions{})
	require.NoError(t, err)
	require.Len(t, page, 1)
	ref := page[0].Ref

	testHookAfterAttemptRecord = func() {
		ok, delErr := e.DeleteTask(context.Background(), portQueue, ref)
		require.NoError(t, delErr)
		require.True(t, ok)
	}
	t.Cleanup(func() { testHookAfterAttemptRecord = nil })

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	d, err := e.PollTask(ctx, portQueue, PollOptions{})
	require.ErrorIs(t, err, ErrNoTasks)
	require.Nil(t, d)
	require.Equal(t, []int32{1}, st.attemptWrites(), "attempt write stands; no rollback UpdateTask")
}

// An ack that lands while DeleteTask's store delete is in flight owns the backlog delta.
// Applying it again leaves ApproximateBacklogCount one low.
func TestAckDuringDeleteKeepsBacklogCount(t *testing.T) {
	st := memstore.New()
	e1 := newR2Engine(t, st, nil, nil, nil)
	for _, p := range []string{"a", "b"} {
		_, err := e1.AddTask(context.Background(), portQueue, store.Task{Payload: []byte(p)})
		require.NoError(t, err)
	}
	e1.Stop()

	e := newR2Engine(t, st, nil, nil, func(c *Config) {
		c.GetTasksBatchSize = 2
		c.GetTasksReloadAt = 1
	})
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	d, err := e.PollTask(ctx, portQueue, PollOptions{})
	require.NoError(t, err)
	require.Equal(t, "a", string(d.Task.Payload))

	tr := subqueueReader(t, e)
	tr.lock.Lock()
	atEnd := tr.atEnd
	tr.lock.Unlock()
	require.False(t, atEnd, "a full read batch is not the end, so the count is a delta")
	require.Equal(t, int64(2), backlogCount(t, e))

	page, _, err := e.PeekTasks(context.Background(), portQueue, PeekOptions{})
	require.NoError(t, err)
	require.Equal(t, "a", string(page[0].Task.Payload))

	testHookAfterAdminStoreDelete = func() { require.NoError(t, d.Ack()) }
	t.Cleanup(func() { testHookAfterAdminStoreDelete = nil })
	ok, err := e.DeleteTask(context.Background(), portQueue, page[0].Ref)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, int64(1), backlogCount(t, e))
}

type createGate struct {
	*memstore.Store
	mu      sync.Mutex
	arm     bool
	entered chan struct{}
	release chan struct{}
}

func (g *createGate) CreateTasks(ctx context.Context, req *store.CreateTasksRequest) (*store.CreateTasksResponse, error) {
	g.mu.Lock()
	block := g.arm
	if block {
		g.arm = false
	}
	g.mu.Unlock()
	if block {
		close(g.entered)
		select {
		case <-g.release:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return g.Store.CreateTasks(ctx, req)
}

func fairTask(payload, key string) store.Task {
	return store.Task{Payload: []byte(payload), Priority: store.Priority{FairnessKey: key}}
}

// PurgeQueue must not move the ack level across a task whose pass was assigned while the
// writer pin was held. Two fairness keys make that pass fall below the other key's max.
func TestPurgeKeepsTaskPinnedAcrossPurge(t *testing.T) {
	g := &createGate{
		Store:   memstore.New(),
		entered: make(chan struct{}),
		release: make(chan struct{}),
	}
	t.Cleanup(func() { closeOnce(g.release) })
	e := newR2Engine(t, g, nil, nil, nil)
	for _, tk := range []store.Task{
		fairTask("a0", "a"),
		fairTask("b0", "b"),
		fairTask("b1", "b"),
		fairTask("b2", "b"),
	} {
		_, err := e.AddTask(context.Background(), portQueue, tk)
		require.NoError(t, err)
	}
	require.Eventually(t, func() bool {
		tr := subqueueReader(t, e)
		tr.lock.Lock()
		defer tr.lock.Unlock()
		return tr.atEnd && tr.loadedTasks == 4
	}, 3*time.Second, time.Millisecond)

	g.mu.Lock()
	g.arm = true
	g.mu.Unlock()
	addDone := make(chan error, 1)
	go func() {
		_, err := e.AddTask(context.Background(), portQueue, fairTask("pinned", "a"))
		addDone <- err
	}()
	select {
	case <-g.entered:
	case <-time.After(3 * time.Second):
		t.Fatal("pinned AddTask did not reach CreateTasks")
	}

	waiting := make(chan struct{})
	var once sync.Once
	adminPurgePinWaitHook = func() { once.Do(func() { close(waiting) }) }
	t.Cleanup(func() { adminPurgePinWaitHook = nil })

	purgeDone := make(chan error, 1)
	go func() {
		_, err := e.PurgeQueue(context.Background(), portQueue)
		purgeDone <- err
	}()
	select {
	case <-waiting:
	case <-time.After(3 * time.Second):
		t.Fatal("PurgeQueue did not wait for the writer ack pin")
	}
	closeOnce(g.release)
	select {
	case err := <-addDone:
		require.NoError(t, err)
	case <-time.After(3 * time.Second):
		t.Fatal("pinned AddTask did not return")
	}
	select {
	case err := <-purgeDone:
		require.NoError(t, err)
	case <-time.After(3 * time.Second):
		t.Fatal("PurgeQueue did not return")
	}

	require.Contains(t, payloadSet(t, g), "pinned")
	requireDelivered(t, e, "pinned")

	e.Stop()
	e2 := newR2Engine(t, g, nil, nil, nil)
	requireDelivered(t, e2, "pinned")
}

func payloadSet(t *testing.T, st store.Store) map[string]bool {
	t.Helper()
	got := map[string]bool{}
	for _, row := range backlogRows(t, st) {
		got[string(row.Data.Payload)] = true
	}
	return got
}

func requireDelivered(t *testing.T, e *Engine, payload string) {
	t.Helper()
	require.Eventually(t, func() bool {
		d, err := e.PollTask(context.Background(), portQueue, PollOptions{NoWait: true})
		if err != nil {
			return false
		}
		got := string(d.Task.Payload)
		if got != payload {
			require.NoError(t, d.Ack())
			return false
		}
		require.NoError(t, d.Nack())
		return true
	}, 3*time.Second, 5*time.Millisecond)
}

// A failed CompleteFairTasksLessThan in PurgeQueue leaves the rows. Ack-level GC must
// retry the delete because the ack level already moved.
func TestPurgeGCRetriesFailedDelete(t *testing.T) {
	ts := clock.NewEventTimeSource().Update(time.Unix(1_700_000_000, 0))
	ts.UseAsyncTimers(true)
	st := memstore.New()
	e := newR2Engine(t, st, ts, nil, func(c *Config) {
		c.UpdateAckInterval = time.Second
		c.TaskDeleteInterval = time.Millisecond
		c.MaxTaskQueueIdleTime = 24 * time.Hour
		c.BacklogMetricsEmitInterval = 24 * time.Hour
		c.MetadataUpdateOnAppendInterval = 24 * time.Hour
	})
	for _, p := range []string{"a", "b"} {
		_, err := e.AddTask(context.Background(), portQueue, store.Task{Payload: []byte(p)})
		require.NoError(t, err)
	}
	require.Equal(t, 2, st.TotalTaskCount(portQueue))
	st.InjectError(memstore.OpCompleteTasksLessThan, &store.TransientError{Err: errors.New("injected purge delete")})
	_, err := e.PurgeQueue(context.Background(), portQueue)
	require.Error(t, err)
	require.Equal(t, 2, st.TotalTaskCount(portQueue), "failed purge delete leaves the rows behind the new ack level")

	ts.Advance(time.Second)
	require.Eventually(t, func() bool {
		return st.TotalTaskCount(portQueue) == 0
	}, 3*time.Second, 5*time.Millisecond, "ack-level GC retries the purge delete")
}
