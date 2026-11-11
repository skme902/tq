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

// healthProbeTimeout bounds the store probe even when ctx has no deadline (a library caller
// without the HTTP handler). A var so tests can shorten it.
var healthProbeTimeout = 2 * time.Second

// Health probes the engine and its store. It does not load or take over any queue; the store
// probe (store.HealthChecker) is bounded by ctx and by healthProbeTimeout. A stopped or
// draining engine is not ready and is not probed, and liveness and draining are read again
// after the probe, so a probe that overlaps the start of Shutdown never reports ready. A
// store schema at or above the build's newest migration counts as current, so old builds stay
// ready while a rolling deploy's new build migrates (migrations are additive). It emits
// engine_ready (0/1).
func (e *Engine) Health(ctx context.Context) HealthReport {
	r := HealthReport{Live: !e.isStopped(), Draining: e.Draining()}
	if hc, ok := e.store.(store.HealthChecker); ok && r.Live && !r.Draining {
		pctx, cancel := context.WithTimeout(ctx, healthProbeTimeout)
		start := e.timeSource.Now()
		h, err := hc.CheckHealth(pctx)
		cancel()
		e.metricsHandler.Timer(metrics.StoreHealthLatency, e.timeSource.Since(start))
		r.StoreErr = err
		r.SchemaVersion, r.WantSchemaVersion = h.SchemaVersion, h.WantSchemaVersion
		r.Live, r.Draining = !e.isStopped(), e.Draining()
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
		bm := pm.defaultQueue.backlogMgr
		n := bm.lastStoreErr.Load()
		if n == nil {
			continue
		}
		if now.Sub(n.at) >= HealthDegradedWindow {
			bm.lastStoreErr.CompareAndSwap(n, nil) // expired: drop the note
			continue
		}
		r.Degraded = append(r.Degraded, QueueProblem{Queue: pm.partition, LastError: n.err, At: n.at})
	}
	ready := 0.0
	if r.Ready {
		ready = 1
	}
	e.metricsHandler.Gauge(metrics.EngineReady, ready)
	return r
}
