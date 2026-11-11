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

// healthProbeTimeout bounds the store probe when ctx has no deadline (a library caller without
// the HTTP handler). A caller's own deadline, such as health.Options.Timeout, is used as is,
// longer or shorter. A var so tests can shorten it.
var healthProbeTimeout = 2 * time.Second

// Health probes the engine and its store. It does not load or take over any queue; the store
// probe (store.HealthChecker) is bounded by ctx's deadline, or by healthProbeTimeout if ctx
// has none. A stopped or
// draining engine is not ready and is not probed, and liveness and draining are read again
// after the probe, so a probe that overlaps the start of Shutdown never reports ready. A
// store schema at or above the build's newest migration counts as current, so old builds stay
// ready while a rolling deploy's new build migrates (migrations are additive). It emits
// engine_ready (0/1).
func (e *Engine) Health(ctx context.Context) HealthReport {
	r := HealthReport{Live: !e.isStopped(), Draining: e.Draining()}
	if hc, ok := e.store.(store.HealthChecker); ok && r.Live && !r.Draining {
		start := e.timeSource.Now()
		h, err := probeHealth(ctx, hc)
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
		// Drop an expired note. If the swap fails, signalIfFatal just stored a newer one:
		// look again rather than skip a fresh error.
		for n != nil && now.Sub(n.at) >= HealthDegradedWindow && !bm.lastStoreErr.CompareAndSwap(n, nil) {
			n = bm.lastStoreErr.Load()
		}
		if n == nil || now.Sub(n.at) >= HealthDegradedWindow {
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

// probeHealth runs CheckHealth under ctx's deadline, or under healthProbeTimeout if ctx has none.
func probeHealth(ctx context.Context, hc store.HealthChecker) (store.StoreHealth, error) {
	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, healthProbeTimeout)
		defer cancel()
	}
	return hc.CheckHealth(ctx)
}
