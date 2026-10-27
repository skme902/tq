// Adapted from temporalio/temporal v1.32.1 af08a5e5, common/persistence/sql/task_util.go
// (taskQueueIdAndHash / taskQueueId).
// Copyright (c) 2025 Temporal Technologies Inc. MIT License; see UPSTREAM_LICENSE and NOTICE.

package sqlcore

import (
	"encoding/binary"

	"github.com/dgryski/go-farm"

	"github.com/skme902/tq/store"
)

// queueIDAndHash returns the persistence id of a queue and its uint32 farm fingerprint,
// which the schema uses as range_hash (the leading primary-key column, so rows of one queue
// cluster together and hash-partitioning stays possible).
//
// Upstream builds the id from a fixed 16-byte namespace UUID + name + task type (+ a
// subqueue marker). Our namespace is a free-form string, so every variable-length part is
// length-prefixed to keep ids collision-free, and the partition is encoded explicitly
// instead of being mangled into the name. Subqueues are a column (see schema), so the id
// identifies the whole queue.
func queueIDAndHash(k store.QueueKey) ([]byte, uint32) {
	id := make([]byte, 0, 3*binary.MaxVarintLen64+len(k.Namespace)+len(k.Name))
	id = binary.AppendUvarint(id, uint64(len(k.Namespace)))
	id = append(id, k.Namespace...)
	id = binary.AppendUvarint(id, uint64(len(k.Name)))
	id = append(id, k.Name...)
	id = binary.AppendUvarint(id, uint64(k.Partition))
	return id, farm.Fingerprint32(id)
}
