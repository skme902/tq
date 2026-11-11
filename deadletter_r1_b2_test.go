package taskqueue_test

// Regression tests for the feat/ops-dlq review round, batch 2.
//
// Each case is aimed at the pre-fix behaviour:
//   - a DeleteDeadTasks error was dropped or the returned count was the number spooled;
//   - concurrent redrives of one queue could spool the same dead row twice;
//   - a negative QueueConfig.MaxAttempts became unlimited, including over a live override;
//   - Delivery.Nack panicked on a nil option;
//   - RedriveOptions.Max < 0 was treated as a cap or as "all" and moved dead tasks.

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	taskqueue "github.com/skme902/tq"
	"github.com/skme902/tq/store"
	"github.com/skme902/tq/store/memstore"
)

// errDeleteStore deletes only the first requested dead id, then returns err.
// The deleted count is what RedriveDeadTasks must report.
type errDeleteStore struct {
	*memstore.Store
	err error
}

func (s *errDeleteStore) DeleteDeadTasks(ctx context.Context, req *store.DeleteDeadTasksRequest) (int, error) {
	if len(req.TaskIDs) == 0 {
		return s.Store.DeleteDeadTasks(ctx, req)
	}
	one := *req
	one.TaskIDs = req.TaskIDs[:1]
	n, err := s.Store.DeleteDeadTasks(ctx, &one)
	if err != nil {
		return n, err
	}
	return n, s.err
}

func TestRedriveDeleteErrorReportsDeletedCount(t *testing.T) {
	sentinel := errors.New("delete dead")
	st := &errDeleteStore{Store: memstore.New(), err: sentinel}
	e := newEngine(t, st, maxAttempts(1))
	deadN(t, e, "a", "b")
	before := deadTasks(t, e)
	require.Len(t, before, 2)

	n, err := e.RedriveDeadTasks(context.Background(), q, taskqueue.RedriveOptions{})
	require.ErrorIs(t, err, sentinel)
	require.Equal(t, 1, n, "count is rows actually deleted, not rows spooled")

	left := deadTasks(t, e)
	require.Len(t, left, 1)
	require.Equal(t, before[1].Task.ID, left[0].Task.ID)
}

func TestRedriveConcurrentNoDuplicates(t *testing.T) {
	st := memstore.New()
	e := newEngine(t, st, maxAttempts(1))
	const nDead = 32
	payloads := make([]string, nDead)
	for i := range payloads {
		payloads[i] = fmt.Sprintf("t%d", i)
	}
	deadN(t, e, payloads...)
	require.Len(t, deadTasks(t, e), nDead)
	require.Eventually(t, func() bool { return len(storedTasks(t, st, q)) == 0 }, 3*time.Second, 10*time.Millisecond,
		"ack-level GC removes dead-lettered rows before redrive")

	const nG = 8
	var wg sync.WaitGroup
	counts := make([]int, nG)
	errs := make([]error, nG)
	start := make(chan struct{})
	wg.Add(nG)
	for i := range nG {
		go func() {
			defer wg.Done()
			<-start
			counts[i], errs[i] = e.RedriveDeadTasks(context.Background(), q, taskqueue.RedriveOptions{})
		}()
	}
	close(start)
	wg.Wait()

	sum := 0
	for i := range nG {
		require.NoError(t, errs[i])
		sum += counts[i]
	}
	require.Equal(t, nDead, sum, "each dead row is deleted once")
	require.Empty(t, deadTasks(t, e))
	require.Len(t, storedTasks(t, st, q), nDead, "each dead row is spooled once")
}

// maxAttemptsSource is a QueueConfigSource whose MaxAttempts can be negative.
// MemoryQueueConfigSource rejects negatives; this exists to feed the refresh path.
type maxAttemptsSource struct {
	mu       sync.Mutex
	n        *int32
	watchMu  sync.Mutex
	watchers []func()
}

func (s *maxAttemptsSource) QueueConfig(store.QueueKey) taskqueue.QueueConfig {
	s.mu.Lock()
	defer s.mu.Unlock()
	return taskqueue.QueueConfig{MaxAttempts: s.n}
}

func (s *maxAttemptsSource) WatchQueueConfig(_ store.QueueKey, onChange func()) func() {
	s.watchMu.Lock()
	defer s.watchMu.Unlock()
	s.watchers = append(s.watchers, onChange)
	return func() {}
}

func (s *maxAttemptsSource) set(n *int32) {
	s.mu.Lock()
	s.n = n
	s.mu.Unlock()
	s.watchMu.Lock()
	ws := make([]func(), len(s.watchers))
	copy(ws, s.watchers)
	s.watchMu.Unlock()
	for _, fn := range ws {
		fn()
	}
}

func TestNegativeMaxAttemptsOverrideIgnored(t *testing.T) {
	t.Run("keeps engine default", func(t *testing.T) {
		neg := int32(-1)
		src := &maxAttemptsSource{n: &neg}
		e := newEngine(t, memstore.New(), maxAttempts(2), func(o *taskqueue.Options) {
			o.QueueConfigSource = src
		})
		add(t, e, task("a"))
		require.NoError(t, poll(t, e).Nack())
		require.NoError(t, poll(t, e).Nack())
		_, err := pollNoWait(e)
		require.ErrorIs(t, err, taskqueue.ErrNoTasks)
		require.Len(t, deadTasks(t, e), 1)
	})

	t.Run("keeps previous positive override", func(t *testing.T) {
		one := int32(1)
		src := &maxAttemptsSource{n: &one}
		e := newEngine(t, memstore.New(), maxAttempts(2), func(o *taskqueue.Options) {
			o.QueueConfigSource = src
		})
		add(t, e, task("a"))
		require.NoError(t, poll(t, e).Nack())
		_, err := pollNoWait(e)
		require.ErrorIs(t, err, taskqueue.ErrNoTasks)
		require.Len(t, deadTasks(t, e), 1, "override of 1 is in effect")

		neg := int32(-1)
		src.set(&neg)
		add(t, e, task("b"))
		require.NoError(t, poll(t, e).Nack())
		_, err = pollNoWait(e)
		require.ErrorIs(t, err, taskqueue.ErrNoTasks)
		dead := deadTasks(t, e)
		require.Len(t, dead, 2)
		require.Equal(t, []string{"a", "b"}, payloads(dead))
	})
}

func TestNackNilOptionRedelivers(t *testing.T) {
	e := newEngine(t, memstore.New(), nil)
	add(t, e, task("a"))

	d := poll(t, e)
	require.Equal(t, int32(1), d.Task.Attempts)
	require.NotPanics(t, func() {
		require.NoError(t, d.Nack(nil))
	})

	d = poll(t, e)
	require.Equal(t, "a", string(d.Task.Payload))
	require.Equal(t, int32(2), d.Task.Attempts)
	require.NotPanics(t, func() {
		require.NoError(t, d.Nack(nil, taskqueue.NackReason("x")))
	})

	d = poll(t, e)
	require.Equal(t, "a", string(d.Task.Payload))
	require.Equal(t, int32(3), d.Task.Attempts)
	require.Empty(t, deadTasks(t, e))
	require.NoError(t, d.Ack())
}

func TestRedriveNegativeMaxRejected(t *testing.T) {
	e := newEngine(t, memstore.New(), maxAttempts(1))
	deadN(t, e, "a", "b")
	before := deadTasks(t, e)
	require.Len(t, before, 2)

	n, err := e.RedriveDeadTasks(context.Background(), q, taskqueue.RedriveOptions{Max: -1})
	require.Equal(t, 0, n)
	var ia *store.InvalidArgumentError
	require.ErrorAs(t, err, &ia)
	require.Equal(t, before, deadTasks(t, e))
}
