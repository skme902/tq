package health_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	taskqueue "github.com/skme902/tq"
	"github.com/skme902/tq/health"
	"github.com/skme902/tq/internal/testdb"
	"github.com/skme902/tq/metrics"
	"github.com/skme902/tq/metrics/metricstest"
	"github.com/skme902/tq/store"
	"github.com/skme902/tq/store/memstore"
	"github.com/skme902/tq/store/sqlite"
)

var q = taskqueue.QueueKey{Namespace: "ns", Name: "q"}

func get(t *testing.T, h http.Handler, path string) (int, map[string]any) {
	t.Helper()
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, path, nil))
	var body map[string]any
	require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &body))
	return rr.Code, body
}

func newEngine(t *testing.T, st store.Store, opts ...func(*taskqueue.Options)) *taskqueue.Engine {
	o := taskqueue.Options{Store: st, Config: taskqueue.Config{LongPollExpirationInterval: 2 * time.Second}}
	for _, f := range opts {
		f(&o)
	}
	e, err := taskqueue.New(o)
	require.NoError(t, err)
	t.Cleanup(e.Stop)
	return e
}

func TestLiveReadyLifecycle(t *testing.T) {
	rec := metricstest.New()
	st := memstore.New()
	e := newEngine(t, st, func(o *taskqueue.Options) { o.Metrics = rec })
	h := health.Handler(e, health.Options{})

	code, body := get(t, h, "/livez")
	require.Equal(t, http.StatusOK, code)
	require.Equal(t, true, body["live"])
	code, body = get(t, h, "/readyz")
	require.Equal(t, http.StatusOK, code)
	require.Equal(t, true, body["ready"])
	v, _ := rec.GaugeValue(metrics.EngineReady)
	require.Equal(t, 1.0, v)

	// Store down: unready, still live.
	st.InjectError(memstore.OpCheckHealth, errors.New("db down"))
	code, body = get(t, h, "/readyz")
	require.Equal(t, http.StatusServiceUnavailable, code)
	require.Equal(t, "db down", body["store_error"])
	code, _ = get(t, h, "/livez")
	require.Equal(t, http.StatusOK, code)
	v, _ = rec.GaugeValue(metrics.EngineReady)
	require.Equal(t, 0.0, v)

	// Draining: unready.
	_, err := e.AddTask(context.Background(), q, taskqueue.Task{Payload: []byte("a")})
	require.NoError(t, err)
	d, err := e.PollTask(context.Background(), q, taskqueue.PollOptions{})
	require.NoError(t, err)
	done := make(chan error, 1)
	go func() { done <- e.Shutdown(context.Background()) }()
	require.Eventually(t, e.Draining, time.Second, time.Millisecond)
	code, body = get(t, h, "/readyz")
	require.Equal(t, http.StatusServiceUnavailable, code)
	require.Equal(t, true, body["draining"])
	require.NoError(t, d.Ack())
	require.NoError(t, <-done)

	// Stopped: not live.
	code, body = get(t, h, "/livez")
	require.Equal(t, http.StatusServiceUnavailable, code)
	require.Equal(t, map[string]any{"live": false}, body)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodHead, "/readyz", nil))
	require.Equal(t, http.StatusServiceUnavailable, rr.Code)
	require.Zero(t, rr.Body.Len())
	rr = httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/other", nil))
	require.Equal(t, http.StatusNotFound, rr.Code)
}

// A queue whose store calls fail shows up as degraded without making the engine unready.
func TestDegradedQueue(t *testing.T) {
	st := memstore.New()
	e := newEngine(t, st, func(o *taskqueue.Options) {
		o.Config.UpdateAckInterval = 20 * time.Millisecond
	})
	_, err := e.AddTask(context.Background(), q, taskqueue.Task{Payload: []byte("a")})
	require.NoError(t, err)
	st.InjectError(memstore.OpGetQueue, errors.New("flaky"), errors.New("flaky"), errors.New("flaky"))
	st.InjectError(memstore.OpUpdateQueue, errors.New("flaky"), errors.New("flaky"), errors.New("flaky"))
	require.Eventually(t, func() bool { return len(e.Health(context.Background()).Degraded) == 1 }, 3*time.Second, 5*time.Millisecond)
	r := e.Health(context.Background())
	require.True(t, r.Ready)
	require.Equal(t, q, r.Degraded[0].Queue)
	require.Contains(t, r.Degraded[0].LastError, "flaky")
}

func TestSchemaCurrentIsReady(t *testing.T) {
	for _, k := range testdb.Kinds() {
		if !k.Durable {
			continue
		}
		t.Run(k.Name, func(t *testing.T) {
			st := k.NewDB(t)(t)
			e := newEngine(t, st)
			r := e.Health(context.Background())
			require.NoError(t, r.StoreErr)
			require.True(t, r.Ready)
			require.Positive(t, r.WantSchemaVersion)
			require.Equal(t, r.WantSchemaVersion, r.SchemaVersion)
		})
	}
}

// A store migrated to an older schema than the build embeds is unready (deploy the
// migration first).
func TestSchemaBehindIsUnready(t *testing.T) {
	ctx := context.Background()
	st, err := sqlite.Open(ctx, t.TempDir()+"/h.db", sqlite.Options{})
	require.NoError(t, err)
	t.Cleanup(func() { _ = st.Close() })
	_, err = st.WriteDB().ExecContext(ctx, `CREATE TABLE tq_schema_version (version INTEGER PRIMARY KEY, name TEXT, applied_at TEXT)`)
	require.NoError(t, err)
	_, err = st.WriteDB().ExecContext(ctx, `INSERT INTO tq_schema_version VALUES (1, 'init', '')`)
	require.NoError(t, err)
	e := newEngine(t, st)
	r := e.Health(ctx)
	require.NoError(t, r.StoreErr)
	require.False(t, r.Ready)
	require.Equal(t, 1, r.SchemaVersion)
	require.Greater(t, r.WantSchemaVersion, 1)
}
