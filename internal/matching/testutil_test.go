package matching

import (
	"context"
	"io"
	"log/slog"
	"math/rand"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/skme902/tq/metrics"
	"github.com/skme902/tq/store"
	"github.com/skme902/tq/store/memstore"
)

func discardLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

// testStore wraps memstore with call counters, probabilistic faults and delays. It plays the
// role of upstream's testTaskManager (matching_engine_test.go) fault/counter helpers.
type testStore struct {
	*memstore.Store

	mu          sync.Mutex
	delay       time.Duration
	faults      map[string]float32 // op -> probability of a transient error
	faultErr    map[string]error   // op -> error to return (default TransientError)
	getTasks    int
	createTasks int
}

func newTestStore() *testStore {
	return &testStore{Store: memstore.New(), faults: map[string]float32{}, faultErr: map[string]error{}}
}

func (s *testStore) addFault(op string, err error, prob float32) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.faults[op] = prob
	s.faultErr[op] = err
}

func (s *testStore) maybeFault(op string) error {
	s.mu.Lock()
	d := s.delay
	p := s.faults[op]
	e := s.faultErr[op]
	s.mu.Unlock()
	if d > 0 {
		time.Sleep(d)
	}
	if p > 0 && rand.Float32() < p {
		if e == nil {
			e = &store.TransientError{Err: io.ErrUnexpectedEOF}
		}
		return e
	}
	return nil
}

func (s *testStore) counts() (getTasks, createTasks int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.getTasks, s.createTasks
}

func (s *testStore) CreateTasks(ctx context.Context, req *store.CreateTasksRequest) (*store.CreateTasksResponse, error) {
	if err := s.maybeFault(memstore.OpCreateTasks); err != nil {
		return nil, err
	}
	s.mu.Lock()
	s.createTasks++
	s.mu.Unlock()
	return s.Store.CreateTasks(ctx, req)
}

func (s *testStore) GetTasks(ctx context.Context, req *store.GetTasksRequest) (*store.GetTasksResponse, error) {
	if err := s.maybeFault(memstore.OpGetTasks); err != nil {
		return nil, err
	}
	s.mu.Lock()
	s.getTasks++
	s.mu.Unlock()
	return s.Store.GetTasks(ctx, req)
}

// captureMetrics records counters (replaces metricstest.CaptureHandler).
type captureMetrics struct {
	mu       sync.Mutex
	counters map[string][]map[string]string
}

func newCaptureMetrics() *captureMetrics {
	return &captureMetrics{counters: map[string][]map[string]string{}}
}

func (c *captureMetrics) Counter(name string, _ int64, tags ...metrics.Tag) {
	m := map[string]string{}
	for _, t := range tags {
		m[t.Key] = t.Value
	}
	c.mu.Lock()
	c.counters[name] = append(c.counters[name], m)
	c.mu.Unlock()
}
func (c *captureMetrics) Gauge(string, float64, ...metrics.Tag)       {}
func (c *captureMetrics) Timer(string, time.Duration, ...metrics.Tag) {}

func (c *captureMetrics) snapshot(name string) []map[string]string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]map[string]string(nil), c.counters[name]...)
}

// longTest is upstream common/testing/long.go LongTest: skip unless TEMPORAL_TEST_LONG=1.
func longTest(t any) {
	if long, _ := strconv.ParseBool(os.Getenv("TEMPORAL_TEST_LONG")); long {
		return
	}
	if s, ok := t.(interface{ T() *testing.T }); ok {
		t = s.T()
	}
	if s, ok := t.(interface{ Skip(...any) }); ok {
		s.Skip("skipping long test, use TEMPORAL_TEST_LONG=1 to run")
	}
	panic("skipping long test, use TEMPORAL_TEST_LONG=1 to run")
}
