package matching

// Engine.Health (library addition; upstream exposes gRPC health from the service wrapper).

import (
	"context"
	"time"

	"github.com/skme902/tq/metrics"
	"github.com/skme902/tq/store"
)

// HealthDegradedWindow is how long a loaded queue is reported degraded after a store error.
const HealthDegradedWindow = time.Minute

type storeErrNote struct {
	at  time.Time
	err string
}

// HealthReport is Engine.Health's result.
type HealthReport struct {
	// Live is false once the engine is stopped.
	Live bool
	// Ready is Live, not draining, the store answered, and its schema is current.
	Ready bool
	// Draining is true while Shutdown drains.
	Draining bool
	// StoreErr is the store probe's error (nil if it answered or cannot be probed).
	StoreErr error
	// SchemaVersion / WantSchemaVersion are the applied and embedded migrations (0 if the
	// store has no schema or does not implement store.HealthChecker).
	SchemaVersion, WantSchemaVersion int
	// Degraded lists loaded queues with a store error in the last HealthDegradedWindow.
	// They do not make the engine unready: the error may be one queue's or transient.
	Degraded []QueueProblem
}

// QueueProblem is one degraded queue.
type QueueProblem struct {
	Queue store.QueueKey
	// LastError is the most recent store error the queue saw, and At when.
	LastError string
	At        time.Time
}

// Live reports whether the engine is running (not stopped). It does not touch the store.
func (e *Engine) Live() bool { return !e.isStopped() }

// Health probes the engine and its store. It does not load or take over any queue; the store
// probe (store.HealthChecker) is bounded by ctx. It emits engine_ready (0/1).
func (e *Engine) Health(ctx context.Context) HealthReport {
	r := HealthReport{Live: !e.isStopped(), Draining: e.Draining()}
	if hc, ok := e.store.(store.HealthChecker); ok {
		start := e.timeSource.Now()
		h, err := hc.CheckHealth(ctx)
		e.metricsHandler.Timer(metrics.StoreHealthLatency, e.timeSource.Since(start))
		r.StoreErr = err
		r.SchemaVersion, r.WantSchemaVersion = h.SchemaVersion, h.WantSchemaVersion
	}
	r.Ready = r.Live && !r.Draining && r.StoreErr == nil && r.SchemaVersion >= r.WantSchemaVersion
	now := e.timeSource.Now()
	e.partitionsLock.RLock()
	pms := make([]*taskQueuePartitionManagerImpl, 0, len(e.partitions))
	for _, pm := range e.partitions {
		pms = append(pms, pm)
	}
	e.partitionsLock.RUnlock()
	for _, pm := range pms {
		if n := pm.defaultQueue.backlogMgr.lastStoreErr.Load(); n != nil && now.Sub(n.at) < HealthDegradedWindow {
			r.Degraded = append(r.Degraded, QueueProblem{Queue: pm.partition, LastError: n.err, At: n.at})
		}
	}
	ready := 0.0
	if r.Ready {
		ready = 1
	}
	e.metricsHandler.Gauge(metrics.EngineReady, ready)
	return r
}
