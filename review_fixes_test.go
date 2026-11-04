package taskqueue_test

// Regression tests for REVIEW_2026-11-04 Low findings visible through the public API.

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	taskqueue "github.com/skme902/tq"
	"github.com/skme902/tq/store/memstore"
)

// L3: like upstream (common/priorities.Validate + clipPriority), a priority key above
// PriorityLevels is accepted and clamped to the lowest level; only negative keys are rejected.
func TestPriorityKeyAboveLevelsIsClamped(t *testing.T) {
	e := newEngine(t, memstore.New(), nil)
	low := task("low")
	low.Priority.PriorityKey = 99
	high := task("high")
	high.Priority.PriorityKey = 1
	add(t, e, low)
	add(t, e, high)
	require.Eventually(t, func() bool { return e.MatcherTasks(q) == 2 }, 3*time.Second, time.Millisecond)

	d := poll(t, e)
	require.Equal(t, "high", string(d.Task.Payload))
	require.NoError(t, d.Ack())
	d = poll(t, e)
	require.Equal(t, "low", string(d.Task.Payload))
	require.EqualValues(t, 99, d.Task.Priority.PriorityKey, "the stored key is not rewritten")
	require.NoError(t, d.Ack())

	neg := task("neg")
	neg.Priority.PriorityKey = -1
	_, err := e.AddTask(context.Background(), q, neg)
	require.Error(t, err)
}

// L8: the default fairness counter params are returned by value; callers cannot change the
// library's defaults.
func TestDefaultCounterParamsImmutable(t *testing.T) {
	p := taskqueue.DefaultCounterParams()
	p.MapLimit = 1
	require.Equal(t, 100, taskqueue.DefaultCounterParams().MapLimit)
	require.Equal(t, taskqueue.DefaultCounterParams(), taskqueue.DefaultConfig().FairnessCounter)
}
