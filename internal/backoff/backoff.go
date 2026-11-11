// Package backoff is a small replacement for upstream go.temporal.io/server/common/backoff,
// covering only what the queue uses: exponential policies, a Retrier, and ThrottleRetryContext.
package backoff

import (
	"context"
	"errors"
	"math"
	"math/rand/v2"
	"time"

	"github.com/skme902/tq/store"
)

// NoInterval means "no expiration".
const NoInterval time.Duration = 0

// Policy is an exponential backoff policy (coefficient 2, 20% jitter, like upstream defaults).
type Policy struct {
	Initial    time.Duration
	Max        time.Duration
	Expiration time.Duration // NoInterval: retry forever
}

func (p Policy) interval(attempt int) time.Duration {
	d := p.Initial
	for range attempt {
		if d > math.MaxInt64/2 {
			break // saturate instead of overflowing (REVIEW L1)
		}
		d *= 2
		if p.Max > 0 && d >= p.Max {
			d = p.Max
			break
		}
	}
	// up to 20% jitter downwards
	return d - time.Duration(rand.Float64()*0.2*float64(d))
}

// Retrier tracks attempts for a policy. Not safe for concurrent use.
type Retrier struct {
	p       Policy
	attempt int
}

func NewRetrier(p Policy) *Retrier { return &Retrier{p: p} }

// NextBackOff returns the next delay and advances the attempt counter.
func (r *Retrier) NextBackOff() time.Duration {
	d := r.p.interval(r.attempt)
	r.attempt++
	return d
}

// Reset resets the attempt counter.
func (r *Retrier) Reset() { r.attempt = 0 }

// throttlePolicy is upstream's throttleRetryPolicy, used on ResourceExhausted errors.
var throttlePolicy = Policy{Initial: time.Second, Max: 10 * time.Second}

// ThrottleRetryContext runs op until it succeeds, returns a non-retryable error, ctx is done, or
// the policy expires. isRetryable nil means every error is retryable. Like upstream
// (common/backoff/retry.go): a done ctx runs nothing, ctx errors are never retried, a
// ResourceExhausted error waits at least the throttle policy's delay, and it gives up when the
// next wait would pass ctx's deadline. It always returns the last error from op, or ctx.Err() if
// op never ran.
func ThrottleRetryContext(ctx context.Context, op func(context.Context) error, p Policy, isRetryable func(error) bool) error {
	start := time.Now()
	deadline, hasDeadline := ctx.Deadline()
	r := NewRetrier(p)
	t := NewRetrier(throttlePolicy)
	var err error
	for ctx.Err() == nil {
		if err = op(ctx); err == nil {
			return nil
		}
		next := r.NextBackOff()
		if p.Expiration != NoInterval && time.Since(start)+next > p.Expiration {
			return err
		}
		if errors.Is(err, ctx.Err()) || (isRetryable != nil && !isRetryable(err)) {
			return err
		}
		if store.IsResourceExhausted(err) {
			next = max(next, t.NextBackOff())
		}
		if hasDeadline && time.Now().Add(next).After(deadline) {
			return err
		}
		_ = InterruptibleSleep(ctx, next)
	}
	if err != nil {
		return err
	}
	return ctx.Err()
}

// InterruptibleSleep sleeps for d or until ctx is done, returning ctx.Err() in the latter case.
func InterruptibleSleep(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
