package taskqueue_test

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	taskqueue "github.com/skme902/tq"
	"github.com/skme902/tq/clock"
	"github.com/skme902/tq/store"
	"github.com/skme902/tq/store/memstore"
)

// ageAndNegAttemptsSource returns a negative MaxAttempts (ignored by the engine) together with
// a MaxTaskAge, to check that ignoring the first does not skip refreshing the second.
type ageAndNegAttemptsSource struct {
	mu  sync.Mutex
	age time.Duration
}

func (s *ageAndNegAttemptsSource) QueueConfig(store.QueueKey) taskqueue.QueueConfig {
	s.mu.Lock()
	defer s.mu.Unlock()
	neg, age := int32(-1), s.age
	return taskqueue.QueueConfig{MaxAttempts: &neg, MaxTaskAge: &age}
}

func (s *ageAndNegAttemptsSource) WatchQueueConfig(store.QueueKey, func()) func() { return func() {} }

func TestMaxTaskAgeAppliedWithNegativeMaxAttempts(t *testing.T) {
	ts := clock.NewEventTimeSource().Update(time.Now())
	src := &ageAndNegAttemptsSource{age: time.Hour}
	e := newEngine(t, memstore.New(), func(c *taskqueue.Config) {
		c.MaxTaskQueueIdleTime = 1000 * time.Hour
	}, withClock(ts), func(o *taskqueue.Options) { o.QueueConfigSource = src })
	addTo(t, e, q, "a")
	d := pollFrom(t, e, q)
	require.Equal(t, ts.Now().Add(time.Hour), d.Task.ExpiryTime, "per-queue MaxTaskAge must apply")
	require.NoError(t, d.Ack())
}
