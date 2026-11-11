package matching

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/skme902/tq/store"
	"github.com/skme902/tq/store/memstore"
)

// A caller deadline longer than healthProbeTimeout is honoured (health.Options.Timeout can be
// raised above 2s); the engine default only applies without one (review health r2 #1).
func TestHealthCallerDeadlineExtendsProbe(t *testing.T) {
	old := healthProbeTimeout
	healthProbeTimeout = 50 * time.Millisecond
	t.Cleanup(func() { healthProbeTimeout = old })
	ps := newProbeStore()
	ps.block.Store(true)
	e := newR2Engine(t, ps, nil, nil, nil)
	go func() {
		<-ps.entered
		time.Sleep(200 * time.Millisecond)
		close(ps.release)
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	r := e.Health(ctx)
	require.NoError(t, r.StoreErr)
	require.True(t, r.Ready)
}

// The remaining in-process errors are not store problems, and stopping the queue clears its
// note so an unloaded queue is never listed (review health r2 #6).
func TestDegradedFilterAndStopClear(t *testing.T) {
	e := newR2Engine(t, memstore.New(), nil, nil, nil)
	_, err := e.AddTask(context.Background(), portQueue, store.Task{Payload: []byte("a")})
	require.NoError(t, err)
	bm := e.partitions[portQueue].defaultQueue.backlogMgr
	bm.signalIfFatal(&store.TransientError{Err: context.DeadlineExceeded})
	require.NotNil(t, bm.lastStoreErr.Load())
	require.NoError(t, e.Shutdown(context.Background()))
	require.Nil(t, bm.lastStoreErr.Load(), "Stop clears the note")
	// errNonContiguousBlocks is fatal (it unloads the queue), so check the filter last.
	for _, err := range []error{context.Canceled, errShutdown, errNonContiguousBlocks} {
		bm.signalIfFatal(err)
		require.Nil(t, bm.lastStoreErr.Load(), "%v is not a store problem", err)
	}
}
