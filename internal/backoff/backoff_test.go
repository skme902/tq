package backoff

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
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
