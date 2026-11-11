// Package health serves an Engine's liveness and readiness over HTTP (library addition).
//
//	mux.Handle("/", health.Handler(engine, health.Options{}))
//
// GET /livez returns 200 while the engine is not stopped, else 503, with {"live":bool} and
// without touching the store. GET /readyz returns 200 when Engine.Health reports Ready, else 503, with the report as
// JSON. A HEAD request gets the status only; other methods get 405.
package health

import (
	"context"
	"encoding/json"
	"net/http"
	"time"

	taskqueue "github.com/skme902/tq"
)

// Checker is what Handler needs (taskqueue.Engine implements it).
type Checker interface {
	Live() bool
	Health(ctx context.Context) taskqueue.HealthReport
}

// Options configures Handler.
type Options struct {
	// Timeout bounds the store probe of /readyz (default 2s). It is passed to Engine.Health as
	// the request deadline, so it may be longer or shorter than the engine's own 2s default.
	Timeout time.Duration
}

type queueProblem struct {
	Namespace string    `json:"namespace"`
	Name      string    `json:"name"`
	Partition int       `json:"partition"`
	LastError string    `json:"last_error"`
	At        time.Time `json:"at"`
}

type report struct {
	Live              bool           `json:"live"`
	Ready             bool           `json:"ready"`
	Draining          bool           `json:"draining"`
	StoreError        string         `json:"store_error,omitempty"`
	SchemaVersion     int            `json:"schema_version"`
	WantSchemaVersion int            `json:"want_schema_version"`
	Degraded          []queueProblem `json:"degraded,omitempty"`
}

// Handler returns an http.Handler serving /livez and /readyz; other paths are 404.
func Handler(c Checker, opts Options) http.Handler {
	if opts.Timeout <= 0 {
		opts.Timeout = 2 * time.Second
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/livez", func(w http.ResponseWriter, r *http.Request) {
		if !allowed(w, r) {
			return
		}
		// Liveness does not touch the store: a store outage makes the engine unready, and
		// restarting it would not help. The body carries liveness only.
		live := c.Live()
		status := http.StatusOK
		if !live {
			status = http.StatusServiceUnavailable
		}
		writeJSON(w, r, status, liveReport{Live: live})
	})
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
		if !allowed(w, r) {
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), opts.Timeout)
		defer cancel()
		h := c.Health(ctx)
		write(w, r, h.Ready, h)
	})
	return mux
}

type liveReport struct {
	Live bool `json:"live"`
}

// allowed admits GET and HEAD; anything else gets 405 without probing the engine.
func allowed(w http.ResponseWriter, r *http.Request) bool {
	if r.Method == http.MethodGet || r.Method == http.MethodHead {
		return true
	}
	w.Header().Set("Allow", "GET, HEAD")
	http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	return false
}

func writeJSON(w http.ResponseWriter, r *http.Request, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	if r.Method == http.MethodHead {
		return
	}
	_ = json.NewEncoder(w).Encode(body)
}

func write(w http.ResponseWriter, r *http.Request, ok bool, h taskqueue.HealthReport) {
	status := http.StatusOK
	if !ok {
		status = http.StatusServiceUnavailable
	}
	rep := report{
		Live: h.Live, Ready: h.Ready, Draining: h.Draining,
		SchemaVersion: h.SchemaVersion, WantSchemaVersion: h.WantSchemaVersion,
	}
	if h.StoreErr != nil {
		rep.StoreError = h.StoreErr.Error()
	}
	for _, p := range h.Degraded {
		rep.Degraded = append(rep.Degraded, queueProblem{
			Namespace: p.Queue.Namespace, Name: p.Queue.Name, Partition: p.Queue.Partition,
			LastError: p.LastError, At: p.At,
		})
	}
	writeJSON(w, r, status, rep)
}
