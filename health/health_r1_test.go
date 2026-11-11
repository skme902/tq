package health_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	taskqueue "github.com/skme902/tq"
	"github.com/skme902/tq/health"
	"github.com/skme902/tq/store/sqlite"
)

// countingChecker counts Health calls.
type countingChecker struct{ health atomic.Int32 }

func (c *countingChecker) Live() bool { return true }
func (c *countingChecker) Health(context.Context) taskqueue.HealthReport {
	c.health.Add(1)
	return taskqueue.HealthReport{Live: true, Ready: true}
}

// /livez reports liveness only, not readiness fields (review health r1 #5).
func TestLivezBodyIsLiveOnly(t *testing.T) {
	h := health.Handler(&countingChecker{}, health.Options{})
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/livez", nil))
	require.Equal(t, http.StatusOK, rr.Code)
	var body map[string]any
	require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &body))
	require.Equal(t, map[string]any{"live": true}, body)
}

// Only GET and HEAD are served; other methods get 405 without probing (review health r1 #6).
func TestHealthMethodNotAllowed(t *testing.T) {
	c := &countingChecker{}
	h := health.Handler(c, health.Options{})
	for _, path := range []string{"/livez", "/readyz"} {
		for _, m := range []string{http.MethodPost, http.MethodPut, http.MethodDelete} {
			rr := httptest.NewRecorder()
			h.ServeHTTP(rr, httptest.NewRequest(m, path, nil))
			require.Equal(t, http.StatusMethodNotAllowed, rr.Code, "%s %s", m, path)
			require.Equal(t, "GET, HEAD", rr.Header().Get("Allow"))
		}
	}
	require.Zero(t, c.health.Load(), "Health must not run for a rejected method")
}

// A schema newer than the build is accepted, so old builds stay ready while a rolling deploy's
// new build migrates (review health r1 #3: kept as designed, migrations are additive).
func TestSchemaAheadStaysReady(t *testing.T) {
	ctx := context.Background()
	st, err := sqlite.Open(ctx, t.TempDir()+"/h.db", sqlite.Options{})
	require.NoError(t, err)
	t.Cleanup(func() { _ = st.Close() })
	require.NoError(t, st.Migrate(ctx))
	h, err := st.CheckHealth(ctx)
	require.NoError(t, err)
	_, err = st.WriteDB().ExecContext(ctx, `INSERT INTO tq_schema_version VALUES (?, 'from-a-newer-build', '')`, h.WantSchemaVersion+1)
	require.NoError(t, err)
	e := newEngine(t, st)
	r := e.Health(ctx)
	require.NoError(t, r.StoreErr)
	require.True(t, r.Ready)
	require.Equal(t, r.WantSchemaVersion+1, r.SchemaVersion)
}

// The sqlite probe never takes the single writer connection, so a write in progress does not
// make the store look unhealthy (review health r1 #1).
func TestSQLiteCheckHealthDuringWrite(t *testing.T) {
	ctx := context.Background()
	st, err := sqlite.Open(ctx, t.TempDir()+"/h.db", sqlite.Options{})
	require.NoError(t, err)
	t.Cleanup(func() { _ = st.Close() })
	require.NoError(t, st.Migrate(ctx))
	tx, err := st.WriteDB().BeginTx(ctx, nil) // holds the only writer connection
	require.NoError(t, err)
	defer func() { _ = tx.Rollback() }()
	pctx, cancel := context.WithTimeout(ctx, 300*time.Millisecond)
	defer cancel()
	start := time.Now()
	h, err := st.CheckHealth(pctx)
	require.NoError(t, err)
	require.Less(t, time.Since(start), 250*time.Millisecond)
	require.Equal(t, h.WantSchemaVersion, h.SchemaVersion)
}
