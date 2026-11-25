// Regression test for upstream main a7eaebfc1 (#12316, after v1.32.2): "Zero logical backlog
// metrics from the emitter when a partition stops". Upstream's tests are versioning-specific;
// this one reproduces the race its description names: an emit already in flight when Stop zeroes
// the gauges lands after the zero and leaves a stale backlog.
// Copyright (c) 2025 Temporal Technologies Inc. MIT License; see UPSTREAM_LICENSE and NOTICE.

package matching

import (
	"context"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/skme902/tq/metrics"
	"github.com/skme902/tq/store"
)

// gaugeGate records the last value of each backlog gauge and blocks the first non-zero
// approximate_backlog_count emit until released.
type gaugeGate struct {
	mu       sync.Mutex
	last     map[string]float64
	armed    bool
	blocked  chan struct{}
	recorded chan struct{}
	release  chan struct{}
}

func (g *gaugeGate) Counter(string, int64, ...metrics.Tag)       {}
func (g *gaugeGate) Timer(string, time.Duration, ...metrics.Tag) {}
func (g *gaugeGate) Gauge(name string, v float64, tags ...metrics.Tag) {
	if name != metrics.ApproximateBacklogCount && name != metrics.ApproximateBacklogAgeSeconds {
		return
	}
	g.mu.Lock()
	block := g.armed && name == metrics.ApproximateBacklogCount && v != 0 && fromEmitter()
	if block {
		g.armed = false
	}
	g.mu.Unlock()
	if block {
		close(g.blocked)
		<-g.release
	}
	key := name
	for _, t := range tags {
		if t.Key == metrics.TagTaskPriority {
			key += "/" + t.Value
		}
	}
	g.mu.Lock()
	g.last[key] = v
	g.mu.Unlock()
	if block {
		close(g.recorded)
	}
}

// fromEmitter reports whether the periodic logical-backlog emitter is the caller (the ack-level
// sync also records approximate_backlog_count; leave it alone).
func fromEmitter() bool {
	pc := make([]uintptr, 32)
	frames := runtime.CallersFrames(pc[:runtime.Callers(2, pc)])
	for {
		f, more := frames.Next()
		if strings.Contains(f.Function, "emitLogicalBacklogMetrics") {
			return true
		}
		if !more {
			return false
		}
	}
}

func (g *gaugeGate) snapshot() map[string]float64 {
	g.mu.Lock()
	defer g.mu.Unlock()
	out := make(map[string]float64, len(g.last))
	for k, v := range g.last {
		out[k] = v
	}
	return out
}

func TestBacklogGaugesZeroedAfterInFlightEmit(t *testing.T) {
	t.Parallel()
	g := &gaugeGate{last: map[string]float64{}, armed: true, blocked: make(chan struct{}), recorded: make(chan struct{}), release: make(chan struct{})}
	cfg := DefaultConfig()
	cfg.BacklogMetricsEmitInterval = 10 * time.Millisecond
	e, err := NewEngine(Options{Config: cfg, Store: newTestStore(), Logger: discardLogger(), Metrics: g})
	require.NoError(t, err)
	t.Cleanup(e.Stop)
	_, err = e.AddTask(context.Background(), portQueue, store.Task{Payload: []byte("x")})
	require.NoError(t, err)

	select {
	case <-g.blocked: // an emit of the non-zero count is in flight
	case <-time.After(5 * time.Second):
		t.Fatal("backlog gauges were not emitted")
	}
	stopped := make(chan struct{})
	go func() { e.Stop(); close(stopped) }()
	// Let Stop get as far as it can while the emit is still in flight.
	select {
	case <-stopped:
	case <-time.After(100 * time.Millisecond):
	}
	close(g.release)
	select {
	case <-stopped:
	case <-time.After(5 * time.Second):
		t.Fatal("Stop did not return")
	}

	select {
	case <-g.recorded: // the in-flight emit has landed (with the fix, before Stop returned)
	case <-time.After(5 * time.Second):
		t.Fatal("in-flight emit did not finish")
	}
	last := g.snapshot()
	require.NotEmpty(t, last)
	for k, v := range last {
		require.Zero(t, v, "gauge %s after Stop", k)
	}
}
