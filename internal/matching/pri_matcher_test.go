// Ported from temporalio/temporal v1.32.1 af08a5e5, service/matching/pri_matcher_test.go
// Kept: TestValidatorWorksOnRoot, TestValidatorDrop_SetsDropReason. Dropped:
// TestForwardPollRetriesOnResourceExhausted (needs a child partition and forwarder; not
// implemented). The gomock taskValidator is replaced by a func-backed Validator.
// Copyright (c) 2025 Temporal Technologies Inc. MIT License; see UPSTREAM_LICENSE and NOTICE.

package matching

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"

	"github.com/skme902/tq/clock"
	"github.com/skme902/tq/metrics"
	"github.com/skme902/tq/store"
)

type validatorFunc func(store.QueueKey, *store.TaskInfo) bool

func (f validatorFunc) MaybeValid(q store.QueueKey, t *store.TaskInfo) bool { return f(q, t) }

type PriMatcherSuite struct {
	suite.Suite
}

func TestPriMatcherSuite(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(PriMatcherSuite))
}

func (s *PriMatcherSuite) newRootMatcher(ctx context.Context, v Validator) *priTaskMatcher {
	cfg := newTaskQueueConfig(DefaultConfig())
	partition := store.QueueKey{Namespace: "nsid", Name: "tq"}
	ts := clock.NewRealTimeSource()
	rateLimitManager := newRateLimitManager(NewMemoryQueueConfigSource(0), partition, cfg, ts)
	rateLimitManager.Start(func() {})
	s.T().Cleanup(rateLimitManager.Stop)
	return newPriTaskMatcher(
		ctx,
		cfg,
		partition,
		nil, // nil forwarder = root partition
		v,
		discardLogger(),
		metrics.Noop,
		ts,
		rateLimitManager,
		func() {}, // onRateLimited
		func() {}, // markAlive
	)
}

// TestValidatorWorksOnRoot tests that the validator goroutine can pick up tasks
// on a root partition (where there is no forwarder).
func (s *PriMatcherSuite) TestValidatorWorksOnRoot() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var validatorValidatedTask atomic.Bool
	tm := s.newRootMatcher(ctx, validatorFunc(func(store.QueueKey, *store.TaskInfo) bool {
		validatorValidatedTask.Store(true)
		return true // task is valid
	}))
	tm.Start()
	defer tm.Stop()

	completionCalled := make(chan taskResponse, 1)
	task := newInternalTaskFromBacklog(&store.TaskInfo{
		ID:   1,
		Data: store.Task{CreateTime: time.Now()},
	}, func(t *internalTask, res taskResponse) {
		completionCalled <- res
	})

	task.resetMatcherState()
	_ = tm.AddTask(task)

	select {
	case res := <-completionCalled:
		s.ErrorIs(res.err(), errReprocessTask) //nolint:testifylint
	case <-time.After(2 * time.Second):
		s.Fail("Timeout waiting for validator to process task")
	}
	s.True(validatorValidatedTask.Load(), "Validator should have called maybeValidate")
}

func (s *PriMatcherSuite) TestValidatorDrop_SetsDropReason() {
	cases := []struct {
		name       string
		expiryTime time.Time
		wantReason dropReason
	}{
		{"ExpiredMemory", time.Now().Add(-time.Hour), dropReasonExpiredMemory},
		{"Invalid", time.Time{}, dropReasonInvalid},
	}

	for _, tc := range cases {
		s.Run(tc.name, func() {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			// Validator rejects every task it sees, forcing the drop path.
			tm := s.newRootMatcher(ctx, validatorFunc(func(store.QueueKey, *store.TaskInfo) bool { return false }))
			tm.Start()
			defer tm.Stop()

			completionCalled := make(chan taskResponse, 1)
			task := newInternalTaskFromBacklog(&store.TaskInfo{
				ID:   1,
				Data: store.Task{CreateTime: time.Now(), ExpiryTime: tc.expiryTime},
			}, func(_ *internalTask, res taskResponse) {
				completionCalled <- res
			})

			task.resetMatcherState()
			_ = tm.AddTask(task)

			select {
			case res := <-completionCalled:
				s.Require().NoError(res.err()) // task is dropped, not reprocessed
				s.Equal(tc.wantReason, res.dropReason)
			case <-time.After(2 * time.Second):
				s.Fail("timed out waiting for validator to drop task")
			}
		})
	}
}
