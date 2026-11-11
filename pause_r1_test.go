package taskqueue_test

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	taskqueue "github.com/skme902/tq"
	"github.com/skme902/tq/metrics"
	"github.com/skme902/tq/metrics/metricstest"
	"github.com/skme902/tq/store"
	"github.com/skme902/tq/store/memstore"
)

// pauseSource is a QueueConfigSource whose pause flag and MaxTaskAge can be changed with or
// without notifying watchers.
type pauseSource struct {
	mu       sync.Mutex
	paused   bool
	age      *time.Duration
	watchers []func()
}

func (s *pauseSource) QueueConfig(store.QueueKey) taskqueue.QueueConfig {
	s.mu.Lock()
	defer s.mu.Unlock()
	return taskqueue.QueueConfig{Paused: s.paused, MaxTaskAge: s.age}
}

func (s *pauseSource) WatchQueueConfig(_ store.QueueKey, fn func()) func() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.watchers = append(s.watchers, fn)
	return func() {}
}

func (s *pauseSource) set(f func(*pauseSource), notify bool) {
	s.mu.Lock()
	f(s)
	ws := append([]func(){}, s.watchers...)
	s.mu.Unlock()
	if notify {
		for _, fn := range ws {
			fn()
		}
	}
}

func pausedGauge(t *testing.T, rec *metricstest.Recorder) float64 {
	t.Helper()
	v, _ := rec.GaugeValue(metrics.QueuePaused, qTags()...)
	return v
}

// queue_paused goes back to 0 when the queue unloads (review pause r1 P1).
func TestPausedGaugeClearedOnUnload(t *testing.T) {
	rec := metricstest.New()
	src := taskqueue.NewMemoryQueueConfigSource(0)
	src.SetPaused(q, true)
	e := newEngine(t, memstore.New(), nil, withConfigSource(src), withMetrics(rec))
	add(t, e, task("a"))
	require.Eventually(t, func() bool { return pausedGauge(t, rec) == 1 }, 3*time.Second, time.Millisecond)
	e.Stop()
	require.Zero(t, pausedGauge(t, rec), "an unloaded queue does not report paused")
}

// A pause still updates queue_paused when the same refresh ignores a negative MaxTaskAge
// (review pause r1 P1: the early return skipped the gauge).
func TestPausedGaugeWithNegativeMaxTaskAge(t *testing.T) {
	rec := metricstest.New()
	neg := -time.Second
	src := &pauseSource{age: &neg}
	e := newEngine(t, memstore.New(), nil, withConfigSource(src), withMetrics(rec))
	add(t, e, task("a"))
	src.set(func(s *pauseSource) { s.paused = true }, true)
	require.Eventually(t, func() bool { return pausedGauge(t, rec) == 1 }, 3*time.Second, time.Millisecond)
}

// DescribeQueue on a loaded queue reports the pause that dispatch uses, not a source value
// the queue has not picked up (review pause r1 P2).
func TestDescribeLoadedReportsDispatchPause(t *testing.T) {
	src := &pauseSource{}
	e := newEngine(t, memstore.New(), nil, withConfigSource(src))
	add(t, e, task("a"))
	src.set(func(s *pauseSource) { s.paused = true }, false) // no notification
	desc, err := e.DescribeQueue(context.Background(), q)
	require.NoError(t, err)
	require.True(t, desc.Loaded)
	require.False(t, desc.Paused, "dispatch has not seen the pause")
	require.Equal(t, "a", string(pollFrom(t, e, q).Task.Payload), "and still dispatches")
}

// SetFairnessKeyPaused validates keys like fairness weight overrides and caps the set
// (review pause r1 P2).
func TestSetFairnessKeyPausedValidation(t *testing.T) {
	src := taskqueue.NewMemoryQueueConfigSource(2)
	require.Error(t, src.SetFairnessKeyPaused(q, "", true), "empty key")
	require.Error(t, src.SetFairnessKeyPaused(q, strings.Repeat("k", 65), true), "too long")
	require.NoError(t, src.SetFairnessKeyPaused(q, "a", true))
	require.NoError(t, src.SetFairnessKeyPaused(q, "a", true), "unchanged is fine")
	require.NoError(t, src.SetFairnessKeyPaused(q, "b", true))
	require.Error(t, src.SetFairnessKeyPaused(q, "c", true), "over the cap")
	require.NoError(t, src.SetFairnessKeyPaused(q, "a", false))
	require.Equal(t, []string{"b"}, src.QueueConfig(q).PausedFairnessKeys)
}

// A paused key with more loaded tasks than GetTasksReloadAt does not stop the reader: tasks of
// other keys behind them are still read and delivered (review pause r1 P0).
func TestPausedKeyDoesNotStallReader(t *testing.T) {
	src := taskqueue.NewMemoryQueueConfigSource(0)
	require.NoError(t, src.SetFairnessKeyPaused(q, "p", true))
	e := newEngine(t, memstore.New(), func(c *taskqueue.Config) {
		c.GetTasksBatchSize = 10
		c.GetTasksReloadAt = 3
	}, withConfigSource(src))
	for range 40 {
		for _, k := range []string{"p", "b"} {
			tk := task(k)
			tk.Priority = store.Priority{FairnessKey: k}
			add(t, e, tk)
		}
	}
	for range 40 {
		d := pollFrom(t, e, q)
		require.Equal(t, "b", string(d.Task.Payload))
		require.NoError(t, d.Ack())
	}
	requireNoTask(t, e, q)
	require.NoError(t, src.SetFairnessKeyPaused(q, "p", false))
	for range 40 {
		d := pollFrom(t, e, q)
		require.Equal(t, "p", string(d.Task.Payload))
		require.NoError(t, d.Ack())
	}
}

// A lease handed out before the pause can still be acked while paused (review pause r1 P1).
func TestLeaseAckWhilePaused(t *testing.T) {
	src := taskqueue.NewMemoryQueueConfigSource(0)
	e := newEngine(t, memstore.New(), nil, withConfigSource(src))
	add(t, e, task("a"))
	d := pollFrom(t, e, q)
	src.SetPaused(q, true)
	require.NoError(t, d.Ack())
	requireNoTask(t, e, q)
}

// Pausing a key whose tasks fill the reader's window makes the reader load past them at once;
// no other event would wake it (review pause r1 P0, the pause-change path).
func TestPauseChangeWakesStoppedReader(t *testing.T) {
	src := taskqueue.NewMemoryQueueConfigSource(0)
	// A heavy key's tasks get small pass increments, so all of them sort before the other key's.
	require.NoError(t, src.UpdateFairnessWeightOverrides(q, map[string]float32{"p": 1000}, nil))
	e := newEngine(t, memstore.New(), func(c *taskqueue.Config) {
		c.GetTasksBatchSize = 10
		c.GetTasksReloadAt = 3
	}, withConfigSource(src))
	for _, k := range []string{"p", "b"} {
		for range 20 {
			tk := task(k)
			tk.Priority = store.Priority{FairnessKey: k}
			add(t, e, tk)
		}
	}
	require.NoError(t, src.SetFairnessKeyPaused(q, "p", true))
	for range 20 {
		d := pollFrom(t, e, q)
		require.Equal(t, "b", string(d.Task.Payload))
		require.NoError(t, d.Ack())
	}
}
