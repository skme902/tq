package matching

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/skme902/tq/clock"
	"github.com/skme902/tq/store"
	"github.com/skme902/tq/store/memstore"
)

// blockingSweepStore holds DeleteQueueIfIdle until released.
type blockingSweepStore struct {
	*memstore.Store
	entered chan struct{}
	release chan struct{}
}

func (s *blockingSweepStore) DeleteQueueIfIdle(ctx context.Context, req *store.DeleteQueueIfIdleRequest) (bool, error) {
	s.entered <- struct{}{}
	<-s.release
	return s.Store.DeleteQueueIfIdle(ctx, req)
}

// A sweep's store delete does not block loading other queues, and a load of the swept queue
// waits for the delete (review retention r1 P2: partitionsLock was held across the delete).
func TestSweepDeleteDoesNotBlockOtherLoads(t *testing.T) {
	ts := clock.NewEventTimeSource().Update(time.Unix(1_700_000_000, 0))
	st := &blockingSweepStore{Store: memstore.New(), entered: make(chan struct{}, 1), release: make(chan struct{})}
	var once sync.Once
	release := func() { once.Do(func() { close(st.release) }) }
	t.Cleanup(release)
	cfg := func(c *Config) { c.MaxTaskQueueIdleTime = 1000 * time.Hour }

	e1 := newR2Engine(t, st, ts, nil, cfg)
	ctx := context.Background()
	_, err := e1.AddTask(ctx, portQueue, store.Task{Payload: []byte("a")})
	require.NoError(t, err)
	pctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	d, err := e1.PollTask(pctx, portQueue, PollOptions{})
	require.NoError(t, err)
	require.NoError(t, d.Ack())
	e1.Stop()
	ts.Advance(MinSweepIdle + time.Minute)

	e2 := newR2Engine(t, st, ts, nil, cfg)
	swept := make(chan SweepResult, 1)
	go func() {
		r, err := e2.SweepIdleQueues(ctx, SweepOptions{IdleFor: MinSweepIdle})
		if err != nil {
			t.Error(err)
		}
		swept <- r
	}()
	<-st.entered

	other := store.QueueKey{Namespace: portQueue.Namespace, Name: "other"}
	otherDone := make(chan error, 1)
	go func() {
		_, err := e2.AddTask(ctx, other, store.Task{Payload: []byte("x")})
		otherDone <- err
	}()
	select {
	case err := <-otherDone:
		require.NoError(t, err)
	case <-time.After(2 * time.Second):
		release()
		t.Fatal("loading another queue blocked on the sweep's store delete")
	}

	sameDone := make(chan error, 1)
	go func() {
		_, err := e2.AddTask(ctx, portQueue, store.Task{Payload: []byte("b")})
		sameDone <- err
	}()
	select {
	case <-sameDone:
		t.Fatal("the swept queue loaded while its delete was running")
	case <-time.After(100 * time.Millisecond):
	}
	release()
	require.NoError(t, <-sameDone)
	r := <-swept
	require.Equal(t, []store.QueueKey{portQueue}, r.Deleted)
}

// A second sweep of a key that is already being deleted does not replace the reservation
// (review retention r2 #3).
func TestConcurrentSweepKeepsReservation(t *testing.T) {
	st := &blockingSweepStore{Store: memstore.New(), entered: make(chan struct{}, 2), release: make(chan struct{})}
	var once sync.Once
	release := func() { once.Do(func() { close(st.release) }) }
	t.Cleanup(release)
	e := newR2Engine(t, st, nil, nil, nil)
	ctx := context.Background()
	first := make(chan error, 1)
	go func() {
		_, err := e.deleteIfIdle(ctx, st, portQueue, 1, time.Time{})
		first <- err
	}()
	<-st.entered
	e.partitionsLock.RLock()
	ch := e.sweeping[portQueue]
	e.partitionsLock.RUnlock()
	ok, err := e.deleteIfIdle(ctx, st, portQueue, 1, time.Time{})
	require.NoError(t, err)
	require.False(t, ok, "second sweep of the same key backs off")
	e.partitionsLock.RLock()
	require.Equal(t, ch, e.sweeping[portQueue], "first sweep's reservation is intact")
	e.partitionsLock.RUnlock()
	release()
	require.NoError(t, <-first)
	e.partitionsLock.RLock()
	require.Empty(t, e.sweeping)
	e.partitionsLock.RUnlock()
}
