package matching

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/skme902/tq/store"
	"github.com/skme902/tq/store/memstore"
)

// probeStore is a memstore whose CheckHealth can block until released, and counts calls.
type probeStore struct {
	*memstore.Store
	calls   atomic.Int32
	block   atomic.Bool
	entered chan struct{}
	release chan struct{}
}

func newProbeStore() *probeStore {
	return &probeStore{Store: memstore.New(), entered: make(chan struct{}, 16), release: make(chan struct{})}
}

func (p *probeStore) CheckHealth(ctx context.Context) (store.StoreHealth, error) {
	p.calls.Add(1)
	if p.block.Load() {
		p.entered <- struct{}{}
		select {
		case <-p.release:
		case <-ctx.Done():
			return store.StoreHealth{}, ctx.Err()
		}
	}
	return p.Store.CheckHealth(ctx)
}

// A probe that overlaps the start of Shutdown does not report ready, and a probe that starts
// during the drain does not touch the store (review health r1 #2).
func TestHealthNotReadyOnceDrainStarts(t *testing.T) {
	ps := newProbeStore()
	e := newR2Engine(t, ps, nil, nil, nil)
	_, err := e.AddTask(context.Background(), portQueue, store.Task{Payload: []byte("a")})
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	d, err := e.PollTask(ctx, portQueue, PollOptions{})
	require.NoError(t, err) // a held lease keeps Shutdown draining

	ps.block.Store(true)
	got := make(chan HealthReport, 1)
	go func() { got <- e.Health(context.Background()) }()
	<-ps.entered
	shutdownDone := make(chan error, 1)
	go func() { shutdownDone <- e.Shutdown(context.Background()) }()
	require.Eventually(t, e.Draining, time.Second, time.Millisecond)
	close(ps.release)
	r := <-got
	require.False(t, r.Ready, "probe started before Shutdown must not report ready")
	require.True(t, r.Draining)

	ps.block.Store(false)
	before := ps.calls.Load()
	r = e.Health(context.Background())
	require.False(t, r.Ready)
	require.Equal(t, before, ps.calls.Load(), "no store probe while draining")

	require.NoError(t, d.Ack())
	require.NoError(t, <-shutdownDone)
}

// Health bounds the store probe even when the caller's ctx has no deadline (review r1 #7).
func TestHealthProbeHasOwnDeadline(t *testing.T) {
	old := healthProbeTimeout
	healthProbeTimeout = 100 * time.Millisecond
	t.Cleanup(func() { healthProbeTimeout = old })
	ps := newProbeStore()
	ps.block.Store(true)
	t.Cleanup(func() { close(ps.release) })
	e := newR2Engine(t, ps, nil, nil, nil)
	start := time.Now()
	r := e.Health(context.Background())
	require.Less(t, time.Since(start), time.Second)
	require.ErrorIs(t, r.StoreErr, context.DeadlineExceeded)
	require.False(t, r.Ready)
}

// Only persistence failures mark a queue degraded; a throttle or a closed queue does not, and
// an expired note is dropped (review health r1 #4, #12).
func TestDegradedOnlyForStoreErrors(t *testing.T) {
	e := newR2Engine(t, memstore.New(), nil, nil, nil)
	_, err := e.AddTask(context.Background(), portQueue, store.Task{Payload: []byte("a")})
	require.NoError(t, err)
	bm := e.partitions[portQueue].defaultQueue.backlogMgr

	require.False(t, bm.signalIfFatal(ErrResourceExhausted))
	require.False(t, bm.signalIfFatal(errTaskQueueClosed))
	require.False(t, bm.signalIfFatal(errMatcherStopped))
	require.Empty(t, e.Health(context.Background()).Degraded, "in-process conditions are not store problems")

	require.False(t, bm.signalIfFatal(&store.TransientError{Err: errors.New("db flaky")}))
	r := e.Health(context.Background())
	require.Len(t, r.Degraded, 1)
	require.Contains(t, r.Degraded[0].LastError, "db flaky")

	n := bm.lastStoreErr.Load()
	bm.lastStoreErr.Store(&storeErrNote{at: n.at.Add(-2 * HealthDegradedWindow), err: n.err})
	require.Empty(t, e.Health(context.Background()).Degraded)
	require.Nil(t, bm.lastStoreErr.Load(), "expired note is cleared")
}
