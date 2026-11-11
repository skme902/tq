package store

import (
	"context"
	"time"
)

// QueueConfigRecord is one version of a queue's persisted runtime config (library addition,
// see QueueConfigStore). Config is opaque to the store (the engine writes JSON).
type QueueConfigRecord struct {
	Key QueueKey
	// Version counts writes to this queue's config, starting at 1.
	Version int64
	// Seq is the global change sequence of this write: strictly increasing across all
	// queues, in commit order.
	Seq        int64
	Config     []byte
	UpdatedBy  string
	Reason     string
	UpdateTime time.Time
}

// PutQueueConfigRequest writes a new version of a queue's config.
type PutQueueConfigRequest struct {
	Key QueueKey
	// ExpectedVersion must equal the stored version (0: no config stored yet), else
	// *ConditionFailedError.
	ExpectedVersion int64
	Config          []byte
	UpdatedBy       string
	Reason          string
	UpdateTime      time.Time
}

// QueueConfigStore persists per-queue runtime config with an append-only history
// (memstore, sqlite and postgres implement it). Config rows are independent of queue rows:
// DeleteQueue and the idle-queue sweep leave them.
type QueueConfigStore interface {
	// GetQueueConfig returns the current version, or *NotFoundError.
	GetQueueConfig(ctx context.Context, key QueueKey) (*QueueConfigRecord, error)
	// PutQueueConfig writes version ExpectedVersion+1 and appends it to the history.
	PutQueueConfig(ctx context.Context, req *PutQueueConfigRequest) (*QueueConfigRecord, error)
	// ListQueueConfigChanges returns up to limit history entries with Seq > afterSeq in Seq
	// order (all queues). A reader that remembers the last Seq it saw misses no write.
	ListQueueConfigChanges(ctx context.Context, afterSeq int64, limit int) ([]QueueConfigRecord, error)
	// ListCurrentQueueConfigs returns the current version of every queue's config, each with
	// the Seq of that version. Writes are serialised in Seq order, so the highest Seq returned
	// is a feed position that covers every write these rows reflect.
	ListCurrentQueueConfigs(ctx context.Context) ([]QueueConfigRecord, error)
	// QueueConfigHistory returns up to limit versions of one queue's config, newest first.
	QueueConfigHistory(ctx context.Context, key QueueKey, limit int) ([]QueueConfigRecord, error)
}
