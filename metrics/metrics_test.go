package metrics_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/skme902/tq/metrics"
	"github.com/skme902/tq/metrics/metricstest"
)

type binder struct {
	*metricstest.Recorder
	bound []metrics.Tag
}

func (b *binder) WithTags(tags ...metrics.Tag) metrics.Handler {
	b.bound = append(b.bound, tags...)
	return b.Recorder
}

func TestWithTags(t *testing.T) {
	rec := metricstest.New()
	a := metrics.Tag{Key: "a", Value: "1"}
	b := metrics.Tag{Key: "b", Value: "2"}
	c := metrics.Tag{Key: "c", Value: "3"}

	require.Same(t, rec, metrics.WithTags(rec), "no tags returns the handler")
	require.Equal(t, metrics.Noop, metrics.WithTags(metrics.Noop, a))

	h := metrics.WithTags(metrics.WithTags(rec, a), b) // nested binds flatten
	h.Counter("n", 2, c)
	h.Counter("n", 1, c)
	h.Gauge("g", 1.5)
	h.Timer("t", time.Second)
	require.Equal(t, int64(3), rec.CounterValue("n", a, b, c))
	v, ok := rec.GaugeValue("g", a, b)
	require.True(t, ok)
	require.Equal(t, 1.5, v)
	require.Equal(t, []time.Duration{time.Second}, rec.TimerValues("t", a, b))

	// Siblings bound from one parent do not share tag storage.
	p := metrics.WithTags(rec, a)
	metrics.WithTags(p, b).Counter("s", 1)
	metrics.WithTags(p, c).Counter("s", 1)
	require.Equal(t, int64(1), rec.CounterValue("s", a, b))
	require.Equal(t, int64(1), rec.CounterValue("s", a, c))

	// A handler that binds tags itself is used as is.
	bd := &binder{Recorder: rec}
	metrics.WithTags(bd, a)
	require.Equal(t, []metrics.Tag{a}, bd.bound)
}

func TestTaskPriorityTag(t *testing.T) {
	require.Equal(t, metrics.Tag{Key: "task_priority", Value: ""}, metrics.TaskPriorityTag(0))
	require.Equal(t, metrics.Tag{Key: "task_priority", Value: "3"}, metrics.TaskPriorityTag(3))
}
