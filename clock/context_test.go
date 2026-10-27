// Ported from temporalio/temporal v1.32.1 af08a5e5, common/clock/context_test.go
// Copyright (c) 2025 Temporal Technologies Inc. MIT License; see UPSTREAM_LICENSE and NOTICE.

package clock_test

import (
	"context"
	"testing"
	"time"

	"github.com/skme902/tq/clock"
	"github.com/stretchr/testify/assert"
)

func TestContextWithTimeout_Canceled(t *testing.T) {
	t.Parallel()

	timeSource := clock.NewEventTimeSource()
	timeSource.Update(time.Unix(0, 0))
	ctx := context.Background()
	ctx, cancel := clock.ContextWithTimeout(ctx, time.Second, timeSource)
	deadline, ok := ctx.Deadline()
	assert.True(t, ok)
	assert.Equal(t, time.Unix(1, 0), deadline)
	cancel()
	select {
	case <-ctx.Done():
		assert.ErrorIs(t, ctx.Err(), context.Canceled)
	default:
		t.Fatal("expected context to be canceled")
	}
}

func TestContextWithTimeout_Fire(t *testing.T) {
	t.Parallel()

	timeSource := clock.NewEventTimeSource()
	timeSource.Update(time.Unix(0, 0))
	ctx := context.Background()
	ctx, cancel := clock.ContextWithTimeout(ctx, time.Second, timeSource)
	deadline, ok := ctx.Deadline()
	assert.True(t, ok)
	assert.Equal(t, time.Unix(1, 0), deadline)
	timeSource.Advance(time.Second - time.Millisecond)
	select {
	case <-ctx.Done():
		t.Fatal("expected context to not be canceled")
	default:
		assert.NoError(t, ctx.Err())
	}
	timeSource.Advance(time.Millisecond)
	select {
	case <-ctx.Done():
		assert.ErrorIs(t, ctx.Err(), context.DeadlineExceeded)
	default:
		t.Fatal("expected context to be canceled")
	}
	cancel() // should be a no-op
}
