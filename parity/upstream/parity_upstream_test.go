//go:build parity_upstream

// Parity harness adapter: drives upstream temporalio/temporal v1.32.1 af08a5e5 matching
// (service/matching, in-process matchingEngineImpl, fairness on) with the shared parity driver.
// This file is compiled only through `go test -overlay` into a copy-free overlay of the
// pristine upstream clone (see parity/run.sh); it is not part of the library build.
// Construction mirrors matching_engine_test.go SetupTest/newMatchingEngine.
// Copyright (c) 2025 Temporal Technologies Inc. MIT License; see UPSTREAM_LICENSE and NOTICE.

package matching

import (
	"context"
	"math/rand"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	commandpb "go.temporal.io/api/command/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/api/historyservicemock/v1"
	"go.temporal.io/server/api/matchingservice/v1"
	"go.temporal.io/server/api/matchingservicemock/v1"
	"go.temporal.io/server/common/collection"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/membership"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/common/persistence/sql"
	_ "go.temporal.io/server/common/persistence/sql/sqlplugin/sqlite"
	"go.temporal.io/server/common/persistence/visibility/manager"
	"go.temporal.io/server/common/resolver"
	"go.temporal.io/server/common/stream_batcher"
	"go.temporal.io/server/common/tqid"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type parityUpstream struct {
	e     *matchingEngineImpl
	ns    *namespace.Namespace
	parts int
	rng   *rand.Rand
	rngMu sync.Mutex
	runID string
}

func (p *parityUpstream) tq(q string) *taskqueuepb.TaskQueue {
	name := q
	if p.parts > 1 {
		p.rngMu.Lock()
		n := p.rng.Intn(p.parts)
		p.rngMu.Unlock()
		f, _ := tqid.NewTaskQueueFamily(p.ns.ID().String(), q)
		name = f.TaskQueue(enumspb.TASK_QUEUE_TYPE_ACTIVITY).NormalPartition(n).RpcName()
	}
	return &taskqueuepb.TaskQueue{Name: name, Kind: enumspb.TASK_QUEUE_KIND_NORMAL}
}

func (p *parityUpstream) Add(ctx context.Context, q string, id int64, pri int32, key string, w float32) (bool, error) {
	_, sync, err := p.e.AddActivityTask(ctx, &matchingservice.AddActivityTaskRequest{
		NamespaceId:            p.ns.ID().String(),
		Execution:              &commonpb.WorkflowExecution{WorkflowId: q, RunId: p.runID},
		ScheduledEventId:       id,
		TaskQueue:              p.tq(q),
		ScheduleToStartTimeout: durationpb.New(time.Hour),
		Priority:               &commonpb.Priority{PriorityKey: pri, FairnessKey: key, FairnessWeight: w},
	})
	return sync, err
}

func (p *parityUpstream) Poll(ctx context.Context, q, poller string) error {
	_, err := p.e.PollActivityTaskQueue(ctx, &matchingservice.PollActivityTaskQueueRequest{
		NamespaceId: p.ns.ID().String(),
		PollRequest: &workflowservice.PollActivityTaskQueueRequest{TaskQueue: p.tq(q), Identity: poller},
	}, metrics.NoopMetricsHandler)
	return err // deliveries are recorded in RecordActivityTaskStarted
}

func (p *parityUpstream) updateConfig(ctx context.Context, q string, f func(*workflowservice.UpdateTaskQueueConfigRequest)) error {
	req := &workflowservice.UpdateTaskQueueConfigRequest{
		Namespace: p.ns.Name().String(), Identity: "parity", TaskQueue: q,
		TaskQueueType: enumspb.TASK_QUEUE_TYPE_ACTIVITY,
	}
	f(req)
	_, err := p.e.UpdateTaskQueueConfig(ctx, &matchingservice.UpdateTaskQueueConfigRequest{
		NamespaceId: p.ns.ID().String(), UpdateTaskqueueConfig: req,
	})
	return err
}

func rlUpdate(rps *float32) *workflowservice.UpdateTaskQueueConfigRequest_RateLimitUpdate {
	u := &workflowservice.UpdateTaskQueueConfigRequest_RateLimitUpdate{Reason: "parity"}
	if rps != nil {
		u.RateLimit = &taskqueuepb.RateLimit{RequestsPerSecond: *rps}
	}
	return u
}

func (p *parityUpstream) SetQueueRPS(ctx context.Context, q string, rps *float32) error {
	return p.updateConfig(ctx, q, func(r *workflowservice.UpdateTaskQueueConfigRequest) { r.UpdateQueueRateLimit = rlUpdate(rps) })
}
func (p *parityUpstream) SetKeyRPS(ctx context.Context, q string, rps *float32) error {
	return p.updateConfig(ctx, q, func(r *workflowservice.UpdateTaskQueueConfigRequest) {
		r.UpdateFairnessKeyRateLimitDefault = rlUpdate(rps)
	})
}
func (p *parityUpstream) SetWeights(ctx context.Context, q string, w map[string]float32) error {
	return p.updateConfig(ctx, q, func(r *workflowservice.UpdateTaskQueueConfigRequest) { r.SetFairnessWeightOverrides = w })
}

func falseTaskQueueSub(_, _ string, _ enumspb.TaskQueueType, _ func(bool)) (bool, func()) {
	return false, func() {}
}

func TestParityHarness(t *testing.T) {
	scriptPath := os.Getenv("PARITY_SCRIPT")
	if scriptPath == "" {
		t.Skip("PARITY_SCRIPT not set")
	}
	s, err := LoadScript(scriptPath)
	if err != nil {
		t.Fatal(err)
	}
	storeName := os.Getenv("PARITY_STORE")
	if storeName == "" {
		storeName = "sqlite"
	}
	parts, _ := strconv.Atoi(os.Getenv("PARITY_PARTITIONS"))
	parts = max(parts, 1)
	run, _ := strconv.Atoi(os.Getenv("PARITY_RUN"))

	logger := log.NewNoopLogger()
	controller := gomock.NewController(t)

	config := defaultTestConfig()
	useFairness(config)
	config.AutoEnableV2Sub = falseTaskQueueSub
	config.MetadataUpdateOnAppendInterval = dynamicconfig.GetDurationPropertyFnFilteredByTaskQueue(time.Minute) // upstream default (test config sets 0)
	config.MaxTaskDeleteBatchSize = dynamicconfig.GetIntPropertyFnFilteredByTaskQueue(100)                      // upstream default (test config sets 1)
	config.LongPollExpirationInterval = dynamicconfig.GetDurationPropertyFnFilteredByTaskQueue(time.Duration(s.LongPollMS) * time.Millisecond)
	config.NumTaskqueueReadPartitions = dynamicconfig.GetIntPropertyFnFilteredByTaskQueue(parts)
	config.NumTaskqueueWritePartitions = dynamicconfig.GetIntPropertyFnFilteredByTaskQueue(parts)
	config.NumTaskqueueReadPartitionsSub = func(_, _ string, _ enumspb.TaskQueueType, _ func(int)) (int, func()) {
		return parts, func() {}
	}

	var taskMgr, fairMgr persistence.TaskManager
	switch storeName {
	case "sqlite":
		cfg := config_SQLiteMemory()
		factory := sql.NewFactory(*cfg, resolver.NewNoopResolver(), "active", logger, metrics.NoopMetricsHandler, serialization.NewSerializer())
		defer factory.Close()
		classic, err := factory.NewTaskStore()
		if err != nil {
			t.Fatal(err)
		}
		fair, err := factory.NewFairTaskStore()
		if err != nil {
			t.Fatal(err)
		}
		taskMgr = persistence.NewTaskManager(classic, serialization.NewSerializer())
		fairMgr = persistence.NewTaskManager(fair, serialization.NewSerializer())
	case "mem":
		taskMgr = newTestTaskManager(logger)
		fairMgr = newTestFairTaskManager(logger)
	default:
		t.Fatalf("unknown PARITY_STORE %q", storeName)
	}

	rec := NewRecorder("upstream", storeName, parts, s.Name, run)
	var eng *matchingEngineImpl

	historyClient := historyservicemock.NewMockHistoryServiceClient(controller)
	historyClient.EXPECT().RecordActivityTaskStarted(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, r *historyservice.RecordActivityTaskStartedRequest, _ ...any) (*historyservice.RecordActivityTaskStartedResponse, error) {
			rec.Delivered(r.GetWorkflowExecution().GetWorkflowId(), r.GetScheduledEventId(), r.GetPollRequest().GetIdentity())
			return &historyservice.RecordActivityTaskStartedResponse{
				Attempt: 1,
				ScheduledEvent: newActivityTaskScheduledEvent(r.ScheduledEventId, 0, &commandpb.ScheduleActivityTaskCommandAttributes{
					ActivityId:             strconv.FormatInt(r.ScheduledEventId, 10),
					TaskQueue:              &taskqueuepb.TaskQueue{Name: r.GetWorkflowExecution().GetWorkflowId()},
					ActivityType:           &commonpb.ActivityType{Name: "parity"},
					ScheduleToCloseTimeout: durationpb.New(time.Hour),
					ScheduleToStartTimeout: durationpb.New(time.Hour),
					StartToCloseTimeout:    durationpb.New(time.Hour),
				}),
				StartedTime: timestamppb.Now(),
			}, nil
		}).AnyTimes()

	matchingClient := matchingservicemock.NewMockMatchingServiceClient(controller)
	// Route user-data and forwarding RPCs back into the same engine (single-host cluster).
	matchingClient.EXPECT().GetTaskQueueUserData(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(ctx context.Context, r *matchingservice.GetTaskQueueUserDataRequest, _ ...any) (*matchingservice.GetTaskQueueUserDataResponse, error) {
			return eng.GetTaskQueueUserData(ctx, r)
		}).AnyTimes()
	matchingClient.EXPECT().UpdateTaskQueueUserData(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(ctx context.Context, r *matchingservice.UpdateTaskQueueUserDataRequest, _ ...any) (*matchingservice.UpdateTaskQueueUserDataResponse, error) {
			return eng.UpdateTaskQueueUserData(ctx, r)
		}).AnyTimes()
	matchingClient.EXPECT().ReplicateTaskQueueUserData(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(&matchingservice.ReplicateTaskQueueUserDataResponse{}, nil).AnyTimes()
	matchingClient.EXPECT().ForceLoadTaskQueuePartition(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(&matchingservice.ForceLoadTaskQueuePartitionResponse{WasUnloaded: true}, nil).AnyTimes()
	matchingClient.EXPECT().AddActivityTask(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(ctx context.Context, r *matchingservice.AddActivityTaskRequest, _ ...any) (*matchingservice.AddActivityTaskResponse, error) {
			_, _, err := eng.AddActivityTask(ctx, r)
			return &matchingservice.AddActivityTaskResponse{}, err
		}).AnyTimes()
	matchingClient.EXPECT().PollActivityTaskQueue(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(ctx context.Context, r *matchingservice.PollActivityTaskQueueRequest, _ ...any) (*matchingservice.PollActivityTaskQueueResponse, error) {
			return eng.PollActivityTaskQueue(ctx, r, metrics.NoopMetricsHandler)
		}).AnyTimes()

	ns, nsCache := createMockNamespaceCache(controller, namespace.Name(s.Namespace))
	vis := manager.NewMockVisibilityManager(controller)
	vis.EXPECT().Close().AnyTimes()
	hostInfoProvider := membership.NewMockHostInfoProvider(controller)
	hostInfo := membership.NewHostInfoFromAddress("self")
	hostInfoProvider.EXPECT().HostInfo().Return(hostInfo).AnyTimes()
	sr := membership.NewMockServiceResolver(controller)
	sr.EXPECT().Lookup(gomock.Any()).Return(hostInfo, nil).AnyTimes()
	sr.EXPECT().AddListener(gomock.Any(), gomock.Any()).AnyTimes()
	sr.EXPECT().RemoveListener(gomock.Any()).AnyTimes()
	nexus := persistence.NewMockNexusEndpointManager(controller)
	nexus.EXPECT().ListNexusEndpoints(gomock.Any(), gomock.Any()).Return(&persistence.ListNexusEndpointsResponse{}, nil).AnyTimes()

	eng = newMatchingEngine(config, taskMgr, fairMgr, historyClient, logger, nsCache, matchingClient, vis, hostInfoProvider, sr, nexus)
	// newMatchingEngine (test constructor) leaves this nil; NewEngine (matching_engine.go:328)
	// initializes it. Needed because UpdateTaskQueueConfig goes through UpdateTaskQueueUserData.
	eng.userDataUpdateBatchers = collection.NewSyncMap[namespace.ID, *stream_batcher.Batcher[*userDataUpdate, error]]()
	eng.outstandingPollers = collection.NewSyncMap[string, context.CancelFunc]()
	eng.Start()

	impl := &parityUpstream{e: eng, ns: ns, parts: parts, rng: rand.New(rand.NewSource(s.Seed)), runID: uuid.NewString()}
	tr := Run(s, impl, rec)
	eng.Stop()
	if err := WriteTrace(os.Getenv("PARITY_TRACE"), tr); err != nil {
		t.Fatal(err)
	}
}

func config_SQLiteMemory() *config.SQL {
	return &config.SQL{
		PluginName:        "sqlite",
		DatabaseName:      uuid.NewString(),
		ConnectAttributes: map[string]string{"mode": "memory", "cache": "private"},
	}
}
