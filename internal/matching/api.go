// Interfaces that replace Temporal-only dependencies of service/matching (temporalio/temporal
// v1.32.1 af08a5e5). See docs/UPSTREAM_MAP.md §9.

package matching

import (
	"context"
	"errors"
	"time"

	"github.com/skme902/tq/store"
)

type (
	// StartRequest describes a matched task about to be handed to a poller.
	StartRequest struct {
		Queue store.QueueKey
		// TaskID is 0 for a sync-matched task (it was never persisted).
		TaskID   int64
		Task     store.Task
		Identity string
	}

	// Starter replaces history's Record{Workflow,Activity}TaskStarted: it is called after a
	// task is matched and before it is returned from Poll. Errors are classified like the
	// upstream poll loop (matching_engine.go pollActivityTaskQueue):
	//   - errors wrapping ErrDropTask: the task is dropped (acked without delivery).
	//   - Retryable errors: the task is put back in the matcher (backlog) or spooled (sync).
	//   - any other error: the task is re-spooled with a new ID ("respoolTaskAfterError").
	// In all error cases the poll continues waiting for another task.
	Starter interface {
		Start(ctx context.Context, req *StartRequest) error
	}

	// Validator replaces task_validation.go taskValidator. It is consulted periodically for
	// the head of the backlog when no poller is waiting; returning false drops the task.
	Validator interface {
		MaybeValid(queue store.QueueKey, task *store.TaskInfo) bool
	}

	// QueueConfig is per-queue runtime config (replaces the parts of userData that matching
	// reads, plus the per-queue admin dispatch-rate dynamic config).
	QueueConfig struct {
		FairnessWeightOverrides map[string]float32
		// RateLimit is the API-set whole-queue rate in tasks/s (upstream
		// TaskQueueConfig.QueueRateLimit, float32 like the proto). nil means unset; 0 pauses.
		// It takes precedence over pollers' MaxTasksPerSecond.
		RateLimit *float32
		// FairnessKeyRateLimitDefault is the API-set per-fairness-key rate in tasks/s, scaled
		// by each key's weight (upstream TaskQueueConfig.FairnessKeysRateLimitDefault). nil
		// means no per-key limit.
		FairnessKeyRateLimitDefault *float32
		// AdminNamespaceDispatchRate and AdminQueueDispatchRate override
		// Config.AdminNamespaceToPartitionDispatchRate and
		// Config.AdminNamespaceTaskQueueToPartitionDispatchRate for this queue (upstream
		// dynamic config). nil means the Config value.
		AdminNamespaceDispatchRate *float64
		AdminQueueDispatchRate     *float64
		// MaxAttempts overrides Config.MaxAttempts for this queue (library addition). nil means
		// the Config value; 0 means unlimited.
		MaxAttempts *int32
		// MaxTaskAge overrides Config.MaxTaskAge for this queue (library addition). nil means
		// the Config value; 0 means no cap.
		MaxTaskAge *time.Duration
		// Paused stops dispatch from this queue (library addition): AddTask still accepts
		// tasks (they are spooled, never sync-matched) and polls wait as if it were empty.
		Paused bool
		// PausedFairnessKeys stops dispatch of tasks with these fairness keys only; other
		// keys are dispatched as usual (library addition).
		PausedFairnessKeys []string
	}

	// QueueConfigSource replaces userDataManager for per-queue config. A loaded queue reads
	// QueueConfig once when it loads and again only when notified through QueueConfigWatcher:
	// every field (rate limits, admin dispatch rates, fairness weight overrides, Paused, PausedFairnessKeys) is cached in
	// the queue's rate limit manager. A source that changes values without implementing
	// QueueConfigWatcher takes effect only after the queue unloads and reloads.
	QueueConfigSource interface {
		QueueConfig(queue store.QueueKey) QueueConfig
	}

	// QueueConfigWatcher is optionally implemented by a QueueConfigSource to push changes
	// (replaces userDataManager's onUserDataChanged and dynamic config Subscribe). A loaded
	// queue re-reads its QueueConfig and re-applies rate limits, fairness weight overrides and
	// pause on every onChange call (upstream's onUserDataChanged).
	QueueConfigWatcher interface {
		WatchQueueConfig(queue store.QueueKey, onChange func()) (cancel func())
	}

	// PartitionRouter replaces the matching RPC client used for task/poll forwarding between
	// partitions. Slice 2 is single-partition, so it is accepted but never called.
	PartitionRouter interface {
		ForwardTask(ctx context.Context, from store.QueueKey, toPartition int, task store.Task) error
		ForwardPoll(ctx context.Context, from store.QueueKey, toPartition int, identity string) (*store.TaskInfo, error)
	}
)

// ErrDropTask can be returned (or wrapped) by Starter to drop a task.
var ErrDropTask = errors.New("drop task")

// retryableError marks an error as transient for the backlog reader.
type retryableError struct{ err error }

func (e *retryableError) Error() string { return e.err.Error() }
func (e *retryableError) Unwrap() error { return e.err }

// Retryable wraps err so the queue treats it as transient (re-deliver, don't re-spool).
func Retryable(err error) error {
	if err == nil {
		return nil
	}
	return &retryableError{err: err}
}

// IsRetryable reports whether err was wrapped with Retryable.
func IsRetryable(err error) bool {
	var r *retryableError
	return errors.As(err, &r)
}

type noopRouter struct{}

func (noopRouter) ForwardTask(context.Context, store.QueueKey, int, store.Task) error {
	return errors.New("forwarding not supported")
}

func (noopRouter) ForwardPoll(context.Context, store.QueueKey, int, string) (*store.TaskInfo, error) {
	return nil, errors.New("forwarding not supported")
}

// NoopRouter is a PartitionRouter that never forwards.
var NoopRouter PartitionRouter = noopRouter{}
