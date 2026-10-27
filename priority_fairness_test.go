// Adapted from temporalio/temporal v1.32.1 af08a5e5, tests/priority_fairness_test.go
// PrioritySuite.TestActivity_Basic and FairnessSuite.Test_Activity_Basic, with the same
// dynamic config (GetTasksBatchSize 20, GetTasksReloadAt 5, 1 partition), workload shape and
// thresholds (wrongorderness < 0.1, unfairness < 1.0). The workflow/activity plumbing of the
// functional cluster is replaced by direct AddTask/PollTask; tasks are added in "workflow"
// order as the transfer queue would. Not ported: TestSubqueue_Migration, the Migration_* tests
// (classic/pri -> fair migration not implemented), TestStickyInteraction_SinglePartition
// (sticky queues), TestUpdateWorkflowExecutionOptions_InvalidatesPendingTask (history).
// Copyright (c) 2025 Temporal Technologies Inc. MIT License; see UPSTREAM_LICENSE and NOTICE.

package taskqueue_test

import (
	"context"
	"fmt"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"

	taskqueue "github.com/skme902/tq"
	"github.com/skme902/tq/store/memstore"
)

func smallBatchConfig(c *taskqueue.Config) {
	c.GetTasksBatchSize = 20
	c.GetTasksReloadAt = 5
}

func wrongorderness(vs []int) float64 {
	l := len(vs)
	wrong := 0
	for i, v := range vs[:l-1] {
		for _, w := range vs[i+1:] {
			if v > w {
				wrong++
			}
		}
	}
	return float64(wrong) / float64(l*(l-1)/2)
}

func unfairness(vs []int) float64 {
	firsts := make(map[int]int)
	for i, v := range vs {
		if _, ok := firsts[v]; !ok {
			firsts[v] = i
		}
	}
	var totalDelay int
	for _, first := range firsts {
		totalDelay += first
	}
	return float64(totalDelay) / float64(len(firsts)*len(firsts))
}

func addBacklog(t *testing.T, e *taskqueue.Engine, task taskqueue.Task) {
	t.Helper()
	matched, err := e.AddTask(context.Background(), q, task)
	require.NoError(t, err)
	require.False(t, matched, "no pollers yet: task must go to the backlog")
}

func TestUpstreamPriorityActivityBasic(t *testing.T) {
	const N = 20
	const Levels = 5
	e := newEngine(t, memstore.New(), smallBatchConfig)

	for wfidx := range N {
		for _, pri := range rand.Perm(Levels) {
			pri++ // 1-based
			p := taskqueue.Priority{PriorityKey: int32(pri)}
			if pri == (Levels+1)/2 {
				p = taskqueue.Priority{} // zero should be treated as default (3)
			}
			addBacklog(t, e, taskqueue.Task{Payload: []byte(fmt.Sprintf("%d:%d", wfidx, pri)), Priority: p})
		}
	}

	var runs []int
	for range N * Levels {
		d := poll(t, e)
		var wfidx, pri int
		_, err := fmt.Sscanf(string(d.Task.Payload), "%d:%d", &wfidx, &pri)
		require.NoError(t, err)
		runs = append(runs, pri)
		require.NoError(t, d.Ack())
	}
	w := wrongorderness(runs)
	t.Log("wrongorderness:", w)
	require.Less(t, w, 0.1)
}

func TestUpstreamFairnessActivityBasic(t *testing.T) {
	const Workflows = 15
	const Tasks = 15
	const Keys = 10
	e := newEngine(t, memstore.New(), smallBatchConfig)

	zipf := rand.NewZipf(rand.New(rand.NewSource(12345)), 2, 2, Keys-1)
	for wfidx := range Workflows {
		for range Tasks {
			fkey := int(zipf.Uint64())
			addBacklog(t, e, taskqueue.Task{
				Payload:  []byte(fmt.Sprintf("%d:%d", wfidx, fkey)),
				Priority: taskqueue.Priority{FairnessKey: fmt.Sprintf("key%d", fkey)},
			})
		}
	}

	var runs []int
	for range Workflows * Tasks {
		d := poll(t, e)
		var wfidx, fkey int
		_, err := fmt.Sscanf(string(d.Task.Payload), "%d:%d", &wfidx, &fkey)
		require.NoError(t, err)
		runs = append(runs, fkey)
		require.NoError(t, d.Ack())
	}
	u := unfairness(runs)
	t.Log("unfairness:", u)
	require.Less(t, u, 1.0)
}
