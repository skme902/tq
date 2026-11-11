package taskqueue_test

// Regression tests for Grok Build review round 2 on feat/ops-retention.

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	taskqueue "github.com/skme902/tq"
	"github.com/skme902/tq/clock"
	"github.com/skme902/tq/store/memstore"
	"github.com/skme902/tq/store/sqlite"
)

// A negative QueueConfig.MaxTaskAge on a queue's first load keeps Config.MaxTaskAge
// (review r2 #1: the cap started at 0, i.e. off, on every load).
func TestNegativeMaxTaskAgeOnFirstLoadKeepsDefault(t *testing.T) {
	ts := clock.NewEventTimeSource().Update(time.Now())
	src := &negAgeSource{age: -time.Second}
	e := newEngine(t, memstore.New(), func(c *taskqueue.Config) {
		c.MaxTaskAge = time.Hour
		c.MaxTaskQueueIdleTime = 1000 * time.Hour
	}, withClock(ts), func(o *taskqueue.Options) { o.QueueConfigSource = src })
	addTo(t, e, q, "a")
	d := pollFrom(t, e, q)
	require.Equal(t, ts.Now().Add(time.Hour), d.Task.ExpiryTime)
	require.NoError(t, d.Ack())
}

// A task row the sweep cannot decode keeps its queue and is reported in SweepResult.Corrupt;
// the sweep goes on to other queues (review r2 #6).
func TestSweepReportsCorruptQueue(t *testing.T) {
	ctx := context.Background()
	st, err := sqlite.Open(ctx, t.TempDir()+"/s.db", sqlite.Options{})
	require.NoError(t, err)
	t.Cleanup(func() { _ = st.Close() })
	require.NoError(t, st.Migrate(ctx))
	ts := clock.NewEventTimeSource().Update(time.Now())
	empty := taskqueue.QueueKey{Namespace: "ns", Name: "zz-empty"}
	e1 := newEngine(t, st, nil, withClock(ts))
	addTo(t, e1, q, "x")
	addTo(t, e1, empty, "y")
	require.NoError(t, pollFrom(t, e1, empty).Ack())
	e1.Stop()
	_, err = st.WriteDB().ExecContext(ctx, `UPDATE tq_tasks SET data = ?`, []byte("{not json"))
	require.NoError(t, err)

	ts.Advance(taskqueue.MinSweepIdle + time.Minute)
	e2 := newEngine(t, st, nil, withClock(ts))
	res, err := e2.SweepIdleQueues(ctx, taskqueue.SweepOptions{IdleFor: taskqueue.MinSweepIdle})
	require.NoError(t, err)
	require.Equal(t, []taskqueue.QueueKey{q}, res.Corrupt)
	require.Equal(t, []taskqueue.QueueKey{empty}, res.Deleted)
	require.True(t, queueExists(t, st, q))
}
