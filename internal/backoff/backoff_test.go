package backoff

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/skme902/tq/store"
)

func TestIntervalCapped(t *testing.T) {
	p := Policy{Initial: 10 * time.Millisecond, Max: 50 * time.Millisecond}
	r := NewRetrier(p)
	for range 10 {
		d := r.NextBackOff()
		require.LessOrEqual(t, d, 50*time.Millisecond)
		require.Greater(t, d, time.Duration(0))
	}
}

func TestThrottleRetryStopsOnNonRetryable(t *testing.T) {
	calls := 0
	bad := errors.New("bad")
	err := ThrottleRetryContext(context.Background(), func(context.Context) error { calls++; return bad },
		Policy{Initial: time.Millisecond}, func(error) bool { return false })
	require.ErrorIs(t, err, bad)
	require.Equal(t, 1, calls)
}

func TestThrottleRetryEventuallySucceeds(t *testing.T) {
	calls := 0
	err := ThrottleRetryContext(context.Background(), func(context.Context) error {
		calls++
		if calls < 3 {
			return errors.New("transient")
		}
		return nil
	}, Policy{Initial: time.Millisecond}, nil)
	require.NoError(t, err)
	require.Equal(t, 3, calls)
}

func TestThrottleRetryExpires(t *testing.T) {
	err := ThrottleRetryContext(context.Background(), func(context.Context) error { return errors.New("x") },
		Policy{Initial: 5 * time.Millisecond, Expiration: 20 * time.Millisecond}, nil)
	require.Error(t, err)
}

// REVIEW L1: an uncapped policy must not overflow into a zero or negative delay.
func TestIntervalSaturatesWithoutMax(t *testing.T) {
	r := NewRetrier(Policy{Initial: 2 * time.Second})
	for i := range 100 {
		require.Greater(t, r.NextBackOff(), time.Duration(0), "attempt %d", i)
	}
}

// REVIEW L2: upstream ThrottleRetryContext semantics.
func TestThrottleRetryContextUpstreamSemantics(t *testing.T) {
	t.Run("done ctx runs nothing", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		calls := 0
		err := ThrottleRetryContext(ctx, func(context.Context) error { calls++; return nil }, Policy{Initial: time.Millisecond}, nil)
		require.ErrorIs(t, err, context.Canceled)
		require.Zero(t, calls)
	})
	t.Run("stops when the next wait passes the deadline", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
		defer cancel()
		x := errors.New("x")
		start := time.Now()
		err := ThrottleRetryContext(ctx, func(context.Context) error { return x }, Policy{Initial: time.Second}, nil)
		require.ErrorIs(t, err, x)
		require.Less(t, time.Since(start), 100*time.Millisecond, "should not sleep into the deadline")
	})
	t.Run("resource exhausted uses the throttle policy", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
		defer cancel()
		calls := 0
		err := ThrottleRetryContext(ctx, func(context.Context) error {
			calls++
			return &store.ResourceExhaustedError{Msg: "busy"}
		}, Policy{Initial: time.Millisecond}, nil)
		require.True(t, store.IsResourceExhausted(err))
		require.Equal(t, 1, calls, "throttle wait (>=0.8s) exceeds the deadline")
	})
	t.Run("ctx error is not retried", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		calls := 0
		err := ThrottleRetryContext(ctx, func(ctx context.Context) error { calls++; cancel(); return ctx.Err() },
			Policy{Initial: time.Millisecond}, nil)
		require.ErrorIs(t, err, context.Canceled)
		require.Equal(t, 1, calls)
	})
}
