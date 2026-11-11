package matching

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/skme902/tq/store"
)

// A4: when the last lease is released in the same beat as ctx ending, Shutdown
// returns nil. The hook readies both signals before the wait select so a version
// that trusts the select's choice fails about half the time; the loop makes that
// miss negligible. The task must not come back on a new engine.
func TestShutdownFinishedDrainBeatsExpiredCtx(t *testing.T) {
	for range 16 {
		ts := newTestStore()
		e := newPortEngine(t, ts, nil)
		matched, err := e.AddTask(context.Background(), portQueue, store.Task{Payload: []byte("acked")})
		require.NoError(t, err)
		require.False(t, matched)
		d, err := e.PollTask(context.Background(), portQueue, PollOptions{})
		require.NoError(t, err)

		ctx, cancel := context.WithCancel(context.Background())
		var once sync.Once
		e.testHookBeforeDrainSelect = func() {
			once.Do(func() {
				require.NoError(t, d.Ack())
				cancel()
			})
		}
		require.NoError(t, e.Shutdown(ctx))
		cancel()

		e2 := newPortEngine(t, ts, nil)
		_, err = e2.PollTask(context.Background(), portQueue, PollOptions{NoWait: true})
		require.ErrorIs(t, err, ErrNoTasks)
		e2.Stop()
	}
}

// Review r2 #1: Stop during a Nack's in-flight respool waits for it instead of unloading
// the queue underneath it, so the next engine delivers the task.
func TestStopWaitsForInFlightRespool(t *testing.T) {
	ts := newTestStore()
	e := newPortEngine(t, ts, nil)
	polled := make(chan *Delivery, 1)
	go func() {
		d, err := e.PollTask(context.Background(), portQueue, PollOptions{})
		require.NoError(t, err)
		polled <- d
	}()
	require.Eventually(t, func() bool { return e.WaitingPollers(portQueue) == 1 }, 2*time.Second, time.Millisecond)
	matched, err := e.AddTask(context.Background(), portQueue, store.Task{Payload: []byte("respool")})
	require.NoError(t, err)
	require.True(t, matched)
	d := <-polled

	entered, proceed := make(chan struct{}), make(chan struct{})
	e.testHookBeforeRespool = func(shuttingDown bool) {
		if !shuttingDown {
			close(entered)
			<-proceed
		}
	}
	nacked := make(chan error, 1)
	go func() { nacked <- d.Nack() }()
	<-entered
	stopped := make(chan struct{})
	go func() { e.Stop(); close(stopped) }()
	select {
	case <-stopped:
		t.Fatal("Stop returned while a respool was in flight")
	case <-time.After(200 * time.Millisecond):
	}
	close(proceed)
	require.NoError(t, <-nacked)
	<-stopped

	e2 := newPortEngine(t, ts, nil)
	pctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	d2, err := e2.PollTask(pctx, portQueue, PollOptions{})
	require.NoError(t, err)
	require.Equal(t, "respool", string(d2.Task.Payload))
	require.NoError(t, d2.Ack())
	e2.Stop()
}
