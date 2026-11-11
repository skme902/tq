package taskqueue_test

// Regression tests for Grok Build review round 1 on feat/ops-retention.

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	taskqueue "github.com/skme902/tq"
	"github.com/skme902/tq/clock"
	"github.com/skme902/tq/store"
	"github.com/skme902/tq/store/memstore"
)

// A dead task redriven after MaxTaskAge has passed gets a fresh window and is delivered
// (review r1 P0: it kept its old, past expiry and was dropped unseen).
func TestRedriveAfterMaxTaskAgeGetsFreshWindow(t *testing.T) {
	ts := clock.NewEventTimeSource().Update(time.Now())
	e := newEngine(t, memstore.New(), func(c *taskqueue.Config) {
		c.MaxAttempts = 1
		c.MaxTaskAge = time.Hour
		c.MaxTaskQueueIdleTime = 1000 * time.Hour
	}, withClock(ts))
	addTo(t, e, q, "a")
	require.NoError(t, pollFrom(t, e, q).Nack())
	require.Len(t, deadTasks(t, e), 1)

	ts.Advance(2 * time.Hour)
	n, err := e.RedriveDeadTasks(context.Background(), q, taskqueue.RedriveOptions{})
	require.NoError(t, err)
	require.Equal(t, 1, n)
	d := pollFrom(t, e, q)
	require.Equal(t, "a", string(d.Task.Payload))
	require.Equal(t, ts.Now().Add(time.Hour), d.Task.ExpiryTime)
	require.NoError(t, d.Ack())
}

// MinSweepIdle leaves room for a missed 12h heartbeat (review r1 P1).
func TestMinSweepIdleToleratesMissedHeartbeat(t *testing.T) {
	require.GreaterOrEqual(t, taskqueue.MinSweepIdle, 48*time.Hour)
	e := newEngine(t, memstore.New(), nil)
	_, err := e.SweepIdleQueues(context.Background(), taskqueue.SweepOptions{IdleFor: 24 * time.Hour})
	var ia *store.InvalidArgumentError
	require.ErrorAs(t, err, &ia, "one heartbeat-pair of idleness is not enough")
}

// negAgeSource reports a per-queue MaxTaskAge that can be changed, including to a negative value.
type negAgeSource struct {
	mu       sync.Mutex
	age      time.Duration
	watchers []func()
}

func (s *negAgeSource) QueueConfig(store.QueueKey) taskqueue.QueueConfig {
	s.mu.Lock()
	defer s.mu.Unlock()
	age := s.age
	return taskqueue.QueueConfig{MaxTaskAge: &age}
}

func (s *negAgeSource) WatchQueueConfig(_ store.QueueKey, fn func()) func() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.watchers = append(s.watchers, fn)
	return func() {}
}

func (s *negAgeSource) set(age time.Duration) {
	s.mu.Lock()
	s.age = age
	ws := append([]func(){}, s.watchers...)
	s.mu.Unlock()
	for _, fn := range ws {
		fn()
	}
}

// A negative QueueConfig.MaxTaskAge is ignored and keeps the previous cap, like MaxAttempts
// (review r1 P2: it was stored as 0, turning the cap off).
func TestNegativeMaxTaskAgeOverrideIgnored(t *testing.T) {
	ts := clock.NewEventTimeSource().Update(time.Now())
	src := &negAgeSource{age: time.Hour}
	e := newEngine(t, memstore.New(), func(c *taskqueue.Config) {
		c.MaxTaskQueueIdleTime = 1000 * time.Hour
	}, withClock(ts), func(o *taskqueue.Options) { o.QueueConfigSource = src })
	addTo(t, e, q, "a")
	src.set(-time.Second)
	addTo(t, e, q, "b")
	for _, want := range []string{"a", "b"} {
		d := pollFrom(t, e, q)
		require.Equal(t, want, string(d.Task.Payload))
		require.Equal(t, ts.Now().Add(time.Hour), d.Task.ExpiryTime, "cap kept for %s", want)
		require.NoError(t, d.Ack())
	}
}
