package sqlcore

// Persisted queue config (store.QueueConfigStore, library addition).

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/skme902/tq/store"
)

var _ store.QueueConfigStore = (*Store)(nil)

type configQueries struct {
	get, lock, insertHistory, upsert, changes, history string
}

const configCols = `namespace, name, partition, version, config, updated_by, reason, update_time`

func buildConfigQueries(d Dialect) configQueries {
	key := `namespace = ? AND name = ? AND partition = ?`
	q := configQueries{
		get:  `SELECT ` + configCols + `, seq FROM tq_queue_config WHERE ` + key,
		lock: `SELECT version FROM tq_queue_config WHERE ` + key + d.LockSuffix,
		insertHistory: `INSERT INTO tq_queue_config_history (` + configCols + `) ` +
			`VALUES (?, ?, ?, ?, ?, ?, ?, ?) RETURNING seq`,
		upsert: `INSERT INTO tq_queue_config (` + configCols + `, seq) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) ` +
			`ON CONFLICT (namespace, name, partition) DO UPDATE SET version = excluded.version, ` +
			`config = excluded.config, updated_by = excluded.updated_by, reason = excluded.reason, ` +
			`update_time = excluded.update_time, seq = excluded.seq`,
		changes: `SELECT ` + configCols + `, seq FROM tq_queue_config_history WHERE seq > ? ORDER BY seq LIMIT ?`,
		history: `SELECT ` + configCols + `, seq FROM tq_queue_config_history WHERE ` + key +
			` ORDER BY version DESC LIMIT ?`,
	}
	if d.NumberedPlaceholders {
		q.get, q.lock, q.insertHistory = rebind(q.get, 0), rebind(q.lock, 0), rebind(q.insertHistory, 0)
		q.upsert, q.changes, q.history = rebind(q.upsert, 0), rebind(q.changes, 0), rebind(q.history, 0)
	}
	return q
}

type scanner interface{ Scan(...any) error }

func scanConfig(r scanner) (store.QueueConfigRecord, error) {
	var (
		rec store.QueueConfigRecord
		ts  int64
	)
	err := r.Scan(&rec.Key.Namespace, &rec.Key.Name, &rec.Key.Partition, &rec.Version, &rec.Config,
		&rec.UpdatedBy, &rec.Reason, &ts, &rec.Seq)
	rec.UpdateTime = time.Unix(0, ts).UTC()
	return rec, err
}

// GetQueueConfig implements store.QueueConfigStore.
func (s *Store) GetQueueConfig(ctx context.Context, key store.QueueKey) (*store.QueueConfigRecord, error) {
	rec, err := scanConfig(s.r.QueryRowContext(ctx, s.cq.get, key.Namespace, key.Name, key.Partition))
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return nil, &store.NotFoundError{Msg: fmt.Sprintf("queue config %v", key)}
	case err != nil:
		return nil, s.wrap("GetQueueConfig", err)
	}
	return &rec, nil
}

// PutQueueConfig implements store.QueueConfigStore. Writers serialise on Dialect.ConfigLockSQL
// (or SQLite's single writer) so history seq values commit in order.
func (s *Store) PutQueueConfig(ctx context.Context, req *store.PutQueueConfigRequest) (*store.QueueConfigRecord, error) {
	if req.ExpectedVersion < 0 || req.Config == nil {
		return nil, &store.InvalidArgumentError{Msg: "PutQueueConfig: ExpectedVersion must be >= 0 and Config non-nil"}
	}
	k := req.Key
	rec := store.QueueConfigRecord{Key: k, Version: req.ExpectedVersion + 1, Config: slices.Clone(req.Config), // not aliased to the caller's buffer, like memstore
		UpdatedBy: req.UpdatedBy, Reason: req.Reason, UpdateTime: req.UpdateTime.UTC()}
	err := s.txExecute(ctx, "PutQueueConfig", func(tx *sql.Tx) error {
		if s.d.ConfigLockSQL != "" {
			if _, err := tx.ExecContext(ctx, s.d.ConfigLockSQL); err != nil {
				return err
			}
		}
		var cur int64
		switch err := tx.QueryRowContext(ctx, s.cq.lock, k.Namespace, k.Name, k.Partition).Scan(&cur); {
		case errors.Is(err, sql.ErrNoRows):
			cur = 0
		case err != nil:
			return err
		}
		if cur != req.ExpectedVersion {
			return &store.ConditionFailedError{Msg: fmt.Sprintf("queue config version is %d, expected %d", cur, req.ExpectedVersion)}
		}
		ts := rec.UpdateTime.UnixNano()
		if err := tx.QueryRowContext(ctx, s.cq.insertHistory, k.Namespace, k.Name, k.Partition, rec.Version,
			rec.Config, rec.UpdatedBy, rec.Reason, ts).Scan(&rec.Seq); err != nil {
			return err
		}
		_, err := tx.ExecContext(ctx, s.cq.upsert, k.Namespace, k.Name, k.Partition, rec.Version,
			rec.Config, rec.UpdatedBy, rec.Reason, ts, rec.Seq)
		return err
	})
	if err != nil {
		return nil, err
	}
	return &rec, nil
}

func (s *Store) queryConfigs(ctx context.Context, op, query string, args ...any) ([]store.QueueConfigRecord, error) {
	rows, err := s.r.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, s.wrap(op, err)
	}
	defer rows.Close()
	var out []store.QueueConfigRecord
	for rows.Next() {
		rec, err := scanConfig(rows)
		if err != nil {
			return nil, s.wrap(op+": scan", err)
		}
		out = append(out, rec)
	}
	if err := rows.Err(); err != nil {
		return nil, s.wrap(op, err)
	}
	return out, nil
}

// ListQueueConfigChanges implements store.QueueConfigStore.
func (s *Store) ListQueueConfigChanges(ctx context.Context, afterSeq int64, limit int) ([]store.QueueConfigRecord, error) {
	if limit <= 0 {
		return nil, &store.InvalidArgumentError{Msg: "ListQueueConfigChanges: limit must be positive"}
	}
	return s.queryConfigs(ctx, "ListQueueConfigChanges", s.cq.changes, afterSeq, limit)
}

// QueueConfigHistory implements store.QueueConfigStore.
func (s *Store) QueueConfigHistory(ctx context.Context, key store.QueueKey, limit int) ([]store.QueueConfigRecord, error) {
	if limit <= 0 {
		return nil, &store.InvalidArgumentError{Msg: "QueueConfigHistory: limit must be positive"}
	}
	return s.queryConfigs(ctx, "QueueConfigHistory", s.cq.history, key.Namespace, key.Name, key.Partition, limit)
}
