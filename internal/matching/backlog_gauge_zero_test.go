// Regression tests for upstream main a7eaebfc1 (#12316, after v1.32.2): "Zero logical backlog
// metrics from the emitter when a partition stops". Upstream's tests are versioning-specific;
// these reproduce the race its description names (an emit in flight when the queue unloads must
// not leave a stale value) and, not upstream (review r1), that the exiting emitter does not zero
// the series of a manager that reloaded the same queue meanwhile.
// Copyright (c) 2025 Temporal Technologies Inc. MIT License; see UPSTREAM_LICENSE and NOTICE.

package matching

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/skme902/tq/metrics"
	"github.com/skme902/tq/store"
)

// gaugeRec records the last value of each logical backlog gauge.
type gaugeRec struct {
	mu   sync.Mutex
	last map[string]float64
}

func (g *gaugeRec) Counter(string, int64, ...metrics.Tag)       {}
func (g *gaugeRec) Timer(string, time.Duration, ...metrics.Tag) {}
func (g *gaugeRec) Gauge(name string, v float64, tags ...metrics.Tag) {
	if name != metrics.ApproximateBacklogCount && name != metrics.ApproximateBacklogAgeSeconds {
		return
	}
	for _, t := range tags {
		if t.Key == metrics.TagTaskPriority {
			name += "/" + t.Value
		}
	}
	g.mu.Lock()
	g.last[name] = v
	g.mu.Unlock()
}

func (g *gaugeRec) snapshot() map[string]float64 {
	g.mu.Lock()
	defer g.mu.Unlock()
	out := make(map[string]float64, len(g.last))
	for k, v := range g.last {
		out[k] = v
	}
	return out
}

// blockFirstEmit makes the first logical backlog emit of e's managers wait for release.
// It returns a channel closed once that emit is blocked and the manager that made it.
func blockFirstEmit(t *testing.T, e *Engine, release <-chan struct{}) (<-chan struct{}, func() *taskQueuePartitionManagerImpl) {
	var mu sync.Mutex
	var first *taskQueuePartitionManagerImpl
	blocked := make(chan struct{})
	h := func(pm *taskQueuePartitionManagerImpl) {
		if pm.engine != e {
			return
		}
		mu.Lock()
		block := first == nil
		if block {
			first = pm
		}
		mu.Unlock()
		if block {
			close(blocked)
			<-release
		}
	}
	require.True(t, logicalBacklogEmitHook.CompareAndSwap(nil, &h), "another test holds the emit hook")
	t.Cleanup(func() { logicalBacklogEmitHook.Store(nil) })
	return blocked, func() *taskQueuePartitionManagerImpl { mu.Lock(); defer mu.Unlock(); return first }
}

func newGaugeEngine(t *testing.T, ts *testStore, g *gaugeRec) *Engine {
	cfg := DefaultConfig()
	cfg.BacklogMetricsEmitInterval = 10 * time.Millisecond
	e, err := NewEngine(Options{Config: cfg, Store: ts, Logger: discardLogger(), Metrics: g})
	require.NoError(t, err)
	t.Cleanup(e.Stop)
	return e
}

func waitClosed(t *testing.T, ch <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(5 * time.Second):
		t.Fatal(what)
	}
}

// Both tests share the package-level emit hook, so they run serially (not t.Parallel).

func TestBacklogGaugesZeroedAfterInFlightEmit(t *testing.T) {
	g := &gaugeRec{last: map[string]float64{}}
	e := newGaugeEngine(t, newTestStore(), g)
	release := make(chan struct{})
	blocked, _ := blockFirstEmit(t, e, release)
	_, err := e.AddTask(context.Background(), portQueue, store.Task{Payload: []byte("x")})
	require.NoError(t, err)
	waitClosed(t, blocked, "backlog gauges were not emitted")

	stopped := make(chan struct{})
	go func() { e.Stop(); close(stopped) }()
	select { // let Stop get as far as it can while the emit is in flight
	case <-stopped:
	case <-time.After(100 * time.Millisecond):
	}
	close(release)
	waitClosed(t, stopped, "Stop did not return")

	last := g.snapshot()
	require.NotEmpty(t, last)
	for k, v := range last {
		require.Zero(t, v, "gauge %s after Stop", k)
	}
}

func TestBacklogGaugesNotZeroedForReloadedQueue(t *testing.T) {
	g := &gaugeRec{last: map[string]float64{}}
	e := newGaugeEngine(t, newTestStore(), g)
	release := make(chan struct{})
	blocked, first := blockFirstEmit(t, e, release)
	_, err := e.AddTask(context.Background(), portQueue, store.Task{Payload: []byte("x")})
	require.NoError(t, err)
	waitClosed(t, blocked, "backlog gauges were not emitted")
	old := first()

	// Unload the queue while its emit is in flight (Stop waits for the emitter), then load it
	// again: the new manager emits the same series.
	unloaded := make(chan struct{})
	go func() { e.unloadTaskQueuePartition(old, unloadCauseIdle); close(unloaded) }()
	require.Eventually(t, func() bool {
		e.partitionsLock.RLock()
		defer e.partitionsLock.RUnlock()
		_, ok := e.partitions[portQueue]
		return !ok
	}, 5*time.Second, time.Millisecond)
	_, err = e.getTaskQueuePartitionManager(context.Background(), portQueue, true)
	require.NoError(t, err)
	require.Eventually(t, func() bool { return g.snapshot()[metrics.ApproximateBacklogAgeSeconds] > 0 },
		5*time.Second, time.Millisecond, "the reloaded queue should emit")

	close(release)
	waitClosed(t, unloaded, "unload did not return")
	// The old emitter's exit must not have zeroed the reloaded queue's count.
	require.Equal(t, 1.0, g.snapshot()[metrics.ApproximateBacklogCount+"/3"])
}
