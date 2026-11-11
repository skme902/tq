// Adapted from temporalio/temporal v1.32.1 af08a5e5, common/persistence/sql/task_v2.go,
// common/persistence/sql/task_queues.go, common/persistence/sql/task_v1.go (lockTaskQueue),
// common/persistence/sql/common.go (txExecute) and the tasks_v2 / task_queues_v2 queries in
// common/persistence/sql/sqlplugin/{postgresql,sqlite}/{task_v2,task_queues}.go.
// Copyright (c) 2025 Temporal Technologies Inc. MIT License; see UPSTREAM_LICENSE and NOTICE.

// Package sqlcore is the dialect-neutral SQL implementation of store.Store shared by
// store/postgres and store/sqlite, in the same way upstream's sqlTaskManagerV2 and
// taskQueueStore sit on top of per-database sqlplugin query sets.
package sqlcore

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/skme902/tq/store"
)

// DataEncoding is the value written to the data_encoding columns. Upstream stores proto3
// blobs; this library has no proto dependency and stores JSON instead.
const DataEncoding = "json"

// Dialect captures the differences between the supported databases.
type Dialect struct {
	// Name is used in error messages ("postgres", "sqlite").
	Name string
	// NumberedPlaceholders rewrites "?" to "$1", "$2", ... (Postgres).
	NumberedPlaceholders bool
	// LockSuffix is appended to the row-lock query. Postgres uses " FOR UPDATE" (as
	// upstream); SQLite has no row locks and instead relies on BEGIN IMMEDIATE taking the
	// database write lock at transaction start (upstream's sqlite plugin serialises through
	// a single connection instead).
	LockSuffix string
	// MigrationLockSQL, if set, runs first inside the migration transaction to serialise
	// concurrent migrators (Postgres advisory lock).
	MigrationLockSQL string
	// ConfigLockSQL, if set, runs first in PutQueueConfig so config writes commit in history
	// seq order (Postgres advisory lock; SQLite has a single writer).
	ConfigLockSQL string
	// MaxRowsPerInsert bounds multi-row INSERTs (bind-parameter limits).
	MaxRowsPerInsert int
	// RowRef, if set, is the row identity used by the limited range delete instead of the
	// (pass, task_id) key. Postgres uses "ctid": with pgx's cached prepared statements the
	// generic plan for "(pass, task_id) IN (subquery)" is a hash semi-join over a seq scan
	// of the queue's rows (measured ~30 ms per GC at 20k rows), while "ctid = ANY(ARRAY(...))"
	// is always a TID scan.
	RowRef string
	// Classify may map a driver error to a store error. Returning nil falls back to the
	// default mapping (store.TransientError, like upstream's serviceerror.Unavailable).
	Classify func(err error) error
}

// Store implements store.Store on database/sql.
type Store struct {
	w  *sql.DB // used for every write and for transactions
	r  *sql.DB // used for plain reads; may equal w
	d  Dialect
	q  queries
	cq configQueries
}

var (
	_ store.Store           = (*Store)(nil)
	_ store.QueueLister     = (*Store)(nil)
	_ store.DeadLetterStore = (*Store)(nil)
	_ store.QueueSweeper    = (*Store)(nil)
)

type queries struct {
	insertQueue, getQueue, lockQueue, updateQueue, updateQueueData, deleteQueue        string
	deleteAllTasks, insertTasksPrefix, insertTasksSuffix, getTasks                     string
	rangeDelete, rangeDeleteNoLimit                                                    string
	listQueuesFirst, listQueuesAfter                                                   string
	updateTask, deleteTask, insertDead, listDeadFirst, listDeadAfter, deleteDeadBefore string
	countDead, deleteAllDead                                                           string
	taskSubqueues, liveTaskExists, liveTaskRows, deadExists                            string
}

// New returns a Store that writes through w and reads through r (r may be nil to use w).
func New(w, r *sql.DB, d Dialect) *Store {
	if r == nil {
		r = w
	}
	if d.MaxRowsPerInsert <= 0 {
		d.MaxRowsPerInsert = 500
	}
	return &Store{w: w, r: r, d: d, q: buildQueries(d), cq: buildConfigQueries(d)}
}

const (
	queueCols = `range_hash, task_queue_id`
	queueKey  = `range_hash = ? AND task_queue_id = ?`
	taskKey   = `range_hash = ? AND task_queue_id = ? AND subqueue = ?`
)

func buildQueries(d Dialect) queries {
	q := queries{
		insertQueue: `INSERT INTO tq_task_queues (` + queueCols + `, range_id, data, data_encoding) ` +
			`VALUES (?, ?, ?, ?, ?) ON CONFLICT (` + queueCols + `) DO NOTHING`,
		getQueue:        `SELECT range_id, data, data_encoding FROM tq_task_queues WHERE ` + queueKey,
		lockQueue:       `SELECT range_id FROM tq_task_queues WHERE ` + queueKey + d.LockSuffix,
		updateQueue:     `UPDATE tq_task_queues SET range_id = ?, data = ?, data_encoding = ? WHERE ` + queueKey,
		updateQueueData: `UPDATE tq_task_queues SET data = ?, data_encoding = ? WHERE ` + queueKey,
		deleteQueue:     `DELETE FROM tq_task_queues WHERE ` + queueKey + ` AND range_id = ?`,
		deleteAllTasks:  `DELETE FROM tq_tasks WHERE ` + queueKey,
		insertTasksPrefix: `INSERT INTO tq_tasks ` +
			`(range_hash, task_queue_id, subqueue, pass, task_id, data, data_encoding) VALUES `,
		// Upstream errors on a duplicate (pass, task_id); we overwrite instead, matching
		// memstore, so that retrying a CreateTasks whose commit outcome was unknown is safe.
		insertTasksSuffix: ` ON CONFLICT (range_hash, task_queue_id, subqueue, pass, task_id) ` +
			`DO UPDATE SET data = excluded.data, data_encoding = excluded.data_encoding`,
		getTasks: `SELECT pass, task_id, data, data_encoding FROM tq_tasks WHERE ` + taskKey +
			` AND (pass, task_id) >= (?, ?) ORDER BY pass, task_id LIMIT ?`,
		rangeDelete: `DELETE FROM tq_tasks WHERE ` + taskKey + ` AND (pass, task_id) IN (` +
			`SELECT pass, task_id FROM tq_tasks WHERE ` + taskKey +
			` AND (pass, task_id) < (?, ?) ORDER BY pass, task_id LIMIT ?)`,
		rangeDeleteNoLimit: `DELETE FROM tq_tasks WHERE ` + taskKey + ` AND (pass, task_id) < (?, ?)`,
		// Upstream ListTaskQueue pages by (range_hash, task_queue_id) the same way.
		listQueuesFirst: `SELECT range_hash, task_queue_id, range_id, data, data_encoding FROM tq_task_queues ` +
			`ORDER BY range_hash, task_queue_id LIMIT ?`,
		listQueuesAfter: `SELECT range_hash, task_queue_id, range_id, data, data_encoding FROM tq_task_queues ` +
			`WHERE (range_hash, task_queue_id) > (?, ?) ORDER BY range_hash, task_queue_id LIMIT ?`,
		deleteTask: `DELETE FROM tq_tasks WHERE ` + taskKey + ` AND pass = ? AND task_id = ?`,
		updateTask: `UPDATE tq_tasks SET data = ?, data_encoding = ? WHERE ` + taskKey + ` AND pass = ? AND task_id = ?`,
		insertDead: `INSERT INTO tq_dead_tasks (range_hash, task_queue_id, task_id, pass, dead_time, reason, data, data_encoding) ` +
			`VALUES (?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT (range_hash, task_queue_id, task_id) DO UPDATE SET ` +
			`pass = excluded.pass, dead_time = excluded.dead_time, reason = excluded.reason, data = excluded.data, data_encoding = excluded.data_encoding`,
		listDeadFirst: `SELECT task_id, pass, dead_time, reason, data, data_encoding FROM tq_dead_tasks WHERE ` + queueKey +
			` ORDER BY task_id LIMIT ?`,
		listDeadAfter: `SELECT task_id, pass, dead_time, reason, data, data_encoding FROM tq_dead_tasks WHERE ` + queueKey +
			` AND task_id > ? ORDER BY task_id LIMIT ?`,
		deleteDeadBefore: `DELETE FROM tq_dead_tasks WHERE ` + queueKey + ` AND dead_time < ?`,
		countDead:        `SELECT COUNT(*) FROM tq_dead_tasks WHERE ` + queueKey,
		deleteAllDead:    `DELETE FROM tq_dead_tasks WHERE ` + queueKey,
		taskSubqueues:    `SELECT DISTINCT subqueue FROM tq_tasks WHERE ` + queueKey,
		liveTaskExists:   `SELECT 1 FROM tq_tasks WHERE ` + taskKey + ` AND (pass, task_id) > (?, ?) LIMIT 1`,
		liveTaskRows:     `SELECT pass, task_id, data, data_encoding FROM tq_tasks WHERE ` + taskKey + ` AND (pass, task_id) > (?, ?) ORDER BY pass, task_id`,
		deadExists:       `SELECT 1 FROM tq_dead_tasks WHERE ` + queueKey + ` LIMIT 1`,
	}
	if d.RowRef != "" {
		q.rangeDelete = `DELETE FROM tq_tasks WHERE ` + taskKey + ` AND ` + d.RowRef + ` = ANY(ARRAY(` +
			`SELECT ` + d.RowRef + ` FROM tq_tasks WHERE ` + taskKey +
			` AND (pass, task_id) < (?, ?) ORDER BY pass, task_id LIMIT ?))`
	}
	if d.NumberedPlaceholders {
		// TestNumberedQueriesHaveNoQuestionMarks fails if a query is missing here.
		q.insertQueue = rebind(q.insertQueue, 0)
		q.getQueue = rebind(q.getQueue, 0)
		q.lockQueue = rebind(q.lockQueue, 0)
		q.updateQueue = rebind(q.updateQueue, 0)
		q.updateQueueData = rebind(q.updateQueueData, 0)
		q.deleteQueue = rebind(q.deleteQueue, 0)
		q.deleteAllTasks = rebind(q.deleteAllTasks, 0)
		q.getTasks = rebind(q.getTasks, 0)
		q.rangeDelete = rebind(q.rangeDelete, 0)
		q.rangeDeleteNoLimit = rebind(q.rangeDeleteNoLimit, 0)
		q.listQueuesFirst = rebind(q.listQueuesFirst, 0)
		q.listQueuesAfter = rebind(q.listQueuesAfter, 0)
		q.updateTask = rebind(q.updateTask, 0)
		q.deleteTask = rebind(q.deleteTask, 0)
		q.insertDead = rebind(q.insertDead, 0)
		q.listDeadFirst = rebind(q.listDeadFirst, 0)
		q.listDeadAfter = rebind(q.listDeadAfter, 0)
		q.deleteDeadBefore = rebind(q.deleteDeadBefore, 0)
		q.countDead = rebind(q.countDead, 0)
		q.deleteAllDead = rebind(q.deleteAllDead, 0)
		q.taskSubqueues = rebind(q.taskSubqueues, 0)
		q.liveTaskExists = rebind(q.liveTaskExists, 0)
		q.liveTaskRows = rebind(q.liveTaskRows, 0)
		q.deadExists = rebind(q.deadExists, 0)
	}
	return q
}

// rebind replaces each "?" with "$n", numbering from start+1. Queries here never contain
// "?" inside literals.
func rebind(q string, start int) string {
	var b strings.Builder
	n := start
	for _, c := range q {
		if c == '?' {
			n++
			b.WriteByte('$')
			b.WriteString(strconv.Itoa(n))
			continue
		}
		b.WriteRune(c)
	}
	return b.String()
}

// ---- errors ----

func (s *Store) wrap(op string, err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	var (
		cf *store.ConditionFailedError
		nf *store.NotFoundError
		ia *store.InvalidArgumentError
		te *store.TransientError
		re *store.ResourceExhaustedError
	)
	if errors.As(err, &cf) || errors.As(err, &nf) || errors.As(err, &ia) || errors.As(err, &te) || errors.As(err, &re) {
		return err
	}
	if s.d.Classify != nil {
		if mapped := s.d.Classify(err); mapped != nil {
			return mapped
		}
	}
	return &store.TransientError{Err: fmt.Errorf("%s %s: %w", s.d.Name, op, err)}
}

// txExecute mirrors upstream SqlStore.txExecute: run f in a transaction, roll back on any
// error, pass typed store errors through and map everything else.
func (s *Store) txExecute(ctx context.Context, op string, f func(tx *sql.Tx) error) error {
	tx, err := s.w.BeginTx(ctx, nil)
	if err != nil {
		return s.wrap(op+": begin", err)
	}
	if err := f(tx); err != nil {
		_ = tx.Rollback()
		return s.wrap(op, err)
	}
	if err := tx.Commit(); err != nil {
		return s.wrap(op+": commit", err)
	}
	return nil
}

// lockTaskQueue mirrors upstream lockTaskQueue: read (and on Postgres row-lock) the queue
// row and compare its range id.
func (s *Store) lockTaskQueue(ctx context.Context, tx *sql.Tx, id []byte, hash uint32, oldRangeID int64) error {
	var rangeID int64
	err := tx.QueryRowContext(ctx, s.q.lockQueue, int64(hash), id).Scan(&rangeID)
	switch {
	case err == nil:
		if rangeID != oldRangeID {
			return &store.ConditionFailedError{
				Msg: fmt.Sprintf("task queue range ID was %v when it should have been %v", rangeID, oldRangeID),
			}
		}
		return nil
	case errors.Is(err, sql.ErrNoRows):
		return &store.ConditionFailedError{Msg: "task queue does not exist"}
	default:
		return err
	}
}

// ---- codec ----

func encodeInfo(info *store.QueueInfo) ([]byte, error) {
	if info == nil {
		return nil, &store.InvalidArgumentError{Msg: "nil QueueInfo"}
	}
	return json.Marshal(info)
}

func decodeInfo(data []byte, enc string) (*store.QueueInfo, error) {
	if enc != DataEncoding {
		return nil, fmt.Errorf("unsupported data_encoding %q", enc)
	}
	var info store.QueueInfo
	if err := json.Unmarshal(data, &info); err != nil {
		return nil, fmt.Errorf("decode queue info: %w", err)
	}
	return &info, nil
}

// ---- task queues (upstream taskQueueStore) ----

func (s *Store) CreateQueue(ctx context.Context, req *store.CreateQueueRequest) error {
	data, err := encodeInfo(req.Info)
	if err != nil {
		return err
	}
	id, hash := queueIDAndHash(req.Info.Key)
	res, err := s.w.ExecContext(ctx, s.q.insertQueue, int64(hash), id, req.RangeID, data, DataEncoding)
	if err != nil {
		return s.wrap("CreateQueue", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return s.wrap("CreateQueue: rows affected", err)
	}
	if n != 1 {
		// Upstream detects this via IsDupEntryError; ON CONFLICT DO NOTHING is portable.
		return &store.ConditionFailedError{Msg: fmt.Sprintf("queue %v already exists", req.Info.Key)}
	}
	return nil
}

func (s *Store) GetQueue(ctx context.Context, key store.QueueKey) (*store.GetQueueResponse, error) {
	id, hash := queueIDAndHash(key)
	var (
		rangeID int64
		data    []byte
		enc     string
	)
	err := s.r.QueryRowContext(ctx, s.q.getQueue, int64(hash), id).Scan(&rangeID, &data, &enc)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return nil, &store.NotFoundError{Msg: fmt.Sprintf("queue %v", key)}
	case err != nil:
		return nil, s.wrap("GetQueue", err)
	}
	info, err := decodeInfo(data, enc)
	if err != nil {
		return nil, err
	}
	return &store.GetQueueResponse{RangeID: rangeID, Info: info}, nil
}

// listToken is the ListQueues page token: the primary key of the last row returned.
type listToken struct {
	Hash int64  `json:"h"`
	ID   []byte `json:"id"`
}

// ListQueues pages through tq_task_queues in primary-key order. It only reads; no row is
// locked and no rangeID changes.
func (s *Store) ListQueues(ctx context.Context, req *store.ListQueuesRequest) (*store.ListQueuesResponse, error) {
	if req.PageSize <= 0 {
		return nil, &store.InvalidArgumentError{Msg: fmt.Sprintf("ListQueues: PageSize %d must be positive", req.PageSize)}
	}
	var (
		rows *sql.Rows
		err  error
	)
	// Fetch one extra row to know whether another page exists.
	if len(req.PageToken) == 0 {
		rows, err = s.r.QueryContext(ctx, s.q.listQueuesFirst, req.PageSize+1)
	} else {
		var tok listToken
		if jerr := json.Unmarshal(req.PageToken, &tok); jerr != nil || tok.ID == nil {
			return nil, &store.InvalidArgumentError{Msg: "ListQueues: invalid page token"}
		}
		rows, err = s.r.QueryContext(ctx, s.q.listQueuesAfter, tok.Hash, tok.ID, req.PageSize+1)
	}
	if err != nil {
		return nil, s.wrap("ListQueues", err)
	}
	defer rows.Close()
	resp := &store.ListQueuesResponse{}
	var last listToken
	for rows.Next() {
		var (
			hash    int64
			id      []byte
			rangeID int64
			data    []byte
			enc     string
		)
		if err := rows.Scan(&hash, &id, &rangeID, &data, &enc); err != nil {
			return nil, s.wrap("ListQueues", err)
		}
		if len(resp.Queues) == req.PageSize {
			tok, _ := json.Marshal(last)
			resp.NextPageToken = tok
			break
		}
		info, err := decodeInfo(data, enc)
		if err != nil {
			return nil, &store.InvalidArgumentError{Msg: fmt.Sprintf("ListQueues: queue row: %v", err)}
		}
		resp.Queues = append(resp.Queues, store.ListedQueue{RangeID: rangeID, Info: info})
		last = listToken{Hash: hash, ID: id}
	}
	if err := rows.Err(); err != nil {
		return nil, s.wrap("ListQueues", err)
	}
	return resp, nil
}

func (s *Store) UpdateQueue(ctx context.Context, req *store.UpdateQueueRequest) error {
	data, err := encodeInfo(req.Info)
	if err != nil {
		return err
	}
	id, hash := queueIDAndHash(req.Info.Key)
	return s.txExecute(ctx, "UpdateQueue", func(tx *sql.Tx) error {
		if err := s.lockTaskQueue(ctx, tx, id, hash, req.PrevRangeID); err != nil {
			return err
		}
		res, err := tx.ExecContext(ctx, s.q.updateQueue, req.RangeID, data, DataEncoding, int64(hash), id)
		if err != nil {
			return err
		}
		n, err := res.RowsAffected()
		if err != nil {
			return err
		}
		if n != 1 {
			return fmt.Errorf("%v rows were affected instead of 1", n)
		}
		return nil
	})
}

// DeleteQueue deletes the queue row and, unlike upstream (which leaves task rows to TTL /
// the scavenger), all of its tasks, matching the store contract.
// DeleteQueueIfIdle implements store.QueueSweeper in one transaction that holds the queue row
// lock, so a concurrent fenced write (which takes the same lock) either commits first and is
// seen as live work or fails its rangeID check afterwards.
func (s *Store) DeleteQueueIfIdle(ctx context.Context, req *store.DeleteQueueIfIdleRequest) (bool, error) {
	id, hash := queueIDAndHash(req.Key)
	deleted := false
	err := s.txExecute(ctx, "DeleteQueueIfIdle", func(tx *sql.Tx) error {
		var rangeID int64
		err := tx.QueryRowContext(ctx, s.q.lockQueue, int64(hash), id).Scan(&rangeID)
		switch {
		case errors.Is(err, sql.ErrNoRows):
			return nil
		case err != nil:
			return err
		case rangeID != req.RangeID:
			return nil
		}
		var one int
		switch err := tx.QueryRowContext(ctx, s.q.deadExists, int64(hash), id).Scan(&one); {
		case err == nil:
			return nil
		case !errors.Is(err, sql.ErrNoRows):
			return err
		}
		var (
			data []byte
			enc  string
		)
		if err := tx.QueryRowContext(ctx, s.q.getQueue, int64(hash), id).Scan(&rangeID, &data, &enc); err != nil {
			return err
		}
		info, err := decodeInfo(data, enc)
		if err != nil {
			return err
		}
		if !req.IdleBefore.IsZero() && !info.LastUpdateTime.Before(req.IdleBefore) {
			return nil
		}
		rows, err := tx.QueryContext(ctx, s.q.taskSubqueues, int64(hash), id)
		if err != nil {
			return err
		}
		var subqueues []int
		for rows.Next() {
			var sq int
			if err := rows.Scan(&sq); err != nil {
				_ = rows.Close()
				return err
			}
			subqueues = append(subqueues, sq)
		}
		if err := rows.Close(); err != nil {
			return err
		}
		if err := rows.Err(); err != nil {
			return err
		}
		for _, sq := range subqueues {
			var ack store.Level
			if sq < len(info.Subqueues) {
				ack = info.Subqueues[sq].FairAckLevel
			}
			if !req.Now.IsZero() {
				live, err := s.unexpiredTaskAbove(ctx, tx, int64(hash), id, sq, ack, req.Now)
				if err != nil {
					return err
				}
				if live {
					return nil
				}
				continue
			}
			switch err := tx.QueryRowContext(ctx, s.q.liveTaskExists, int64(hash), id, sq, ack.Pass, ack.ID).Scan(&one); {
			case err == nil:
				return nil
			case !errors.Is(err, sql.ErrNoRows):
				return err
			}
		}
		if _, err := tx.ExecContext(ctx, s.q.deleteAllTasks, int64(hash), id); err != nil {
			return err
		}
		res, err := tx.ExecContext(ctx, s.q.deleteQueue, int64(hash), id, req.RangeID)
		if err != nil {
			return err
		}
		n, err := res.RowsAffected()
		if err != nil {
			return err
		}
		deleted = n > 0
		return nil
	})
	if err != nil {
		return false, err
	}
	return deleted, nil
}

// unexpiredTaskAbove reports whether subqueue sq has a task above ack that has not expired at
// now, scanning in level order and stopping at the first unexpired row. A row that cannot be
// decoded is an InvalidArgumentError: the queue is kept and the sweep reports why.
func (s *Store) unexpiredTaskAbove(ctx context.Context, tx *sql.Tx, hash int64, id []byte, sq int, ack store.Level, now time.Time) (bool, error) {
	rows, err := tx.QueryContext(ctx, s.q.liveTaskRows, hash, id, sq, ack.Pass, ack.ID)
	if err != nil {
		return false, err
	}
	defer rows.Close()
	for rows.Next() {
		var (
			pass, taskID int64
			data         []byte
			enc          string
			t            store.Task
		)
		if err := rows.Scan(&pass, &taskID, &data, &enc); err != nil {
			return false, err
		}
		// A corrupt row keeps the queue, and says so instead of leaving it silently.
		if enc != DataEncoding {
			return false, &store.InvalidArgumentError{Msg: fmt.Sprintf("DeleteQueueIfIdle: task %d (pass %d): unsupported data_encoding %q", taskID, pass, enc)}
		}
		if err := json.Unmarshal(data, &t); err != nil {
			return false, &store.InvalidArgumentError{Msg: fmt.Sprintf("DeleteQueueIfIdle: task %d (pass %d): decode: %v", taskID, pass, err)}
		}
		if !(t.ExpiryTime.Unix() > 0 && t.ExpiryTime.Before(now)) {
			return true, nil
		}
	}
	return false, rows.Err()
}

func (s *Store) DeleteQueue(ctx context.Context, req *store.DeleteQueueRequest) error {
	id, hash := queueIDAndHash(req.Key)
	return s.txExecute(ctx, "DeleteQueue", func(tx *sql.Tx) error {
		var rangeID int64
		err := tx.QueryRowContext(ctx, s.q.lockQueue, int64(hash), id).Scan(&rangeID)
		switch {
		case errors.Is(err, sql.ErrNoRows):
			return nil
		case err != nil:
			return err
		case rangeID != req.RangeID:
			return &store.ConditionFailedError{
				Msg: fmt.Sprintf("rangeID mismatch: stored %d, expected %d", rangeID, req.RangeID),
			}
		}
		if _, err := tx.ExecContext(ctx, s.q.deleteAllTasks, int64(hash), id); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, s.q.deleteAllDead, int64(hash), id); err != nil {
			return err
		}
		res, err := tx.ExecContext(ctx, s.q.deleteQueue, int64(hash), id, req.RangeID)
		if err != nil {
			return err
		}
		if n, err := res.RowsAffected(); err != nil {
			return err
		} else if n != 1 {
			return &store.ConditionFailedError{Msg: fmt.Sprintf("delete failed: %v rows affected instead of 1", n)}
		}
		return nil
	})
}

// ---- tasks (upstream sqlTaskManagerV2) ----

func (s *Store) CreateTasks(ctx context.Context, req *store.CreateTasksRequest) (*store.CreateTasksResponse, error) {
	if req.Subqueues != nil && len(req.Subqueues) != len(req.Tasks) {
		return nil, &store.InvalidArgumentError{Msg: "len(Subqueues) != len(Tasks)"}
	}
	var infoData []byte
	if req.UpdateMetadata {
		var err error
		if infoData, err = encodeInfo(req.Info); err != nil {
			return nil, err
		}
	}
	blobs := make([][]byte, len(req.Tasks))
	for i := range req.Tasks {
		b, err := json.Marshal(&req.Tasks[i].Data)
		if err != nil {
			return nil, &store.InvalidArgumentError{Msg: "encode task: " + err.Error()}
		}
		blobs[i] = b
	}
	id, hash := queueIDAndHash(req.Info.Key)
	err := s.txExecute(ctx, "CreateTasks", func(tx *sql.Tx) error {
		// Upstream inserts first and locks before commit; we lock first so a fenced writer
		// does no insert work. Either way the range-id check and the inserts commit atomically.
		if err := s.lockTaskQueue(ctx, tx, id, hash, req.RangeID); err != nil {
			return err
		}
		for _, t := range req.Tasks {
			if t.Pass < 1 {
				return &store.InvalidArgumentError{Msg: fmt.Sprintf("task %d has pass %d < 1", t.ID, t.Pass)}
			}
		}
		for start := 0; start < len(req.Tasks); start += s.d.MaxRowsPerInsert {
			end := min(start+s.d.MaxRowsPerInsert, len(req.Tasks))
			if err := s.insertTasks(ctx, tx, id, hash, req, blobs, start, end); err != nil {
				return err
			}
		}
		if req.UpdateMetadata {
			if _, err := tx.ExecContext(ctx, s.q.updateQueueData, infoData, DataEncoding, int64(hash), id); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return &store.CreateTasksResponse{UpdatedMetadata: req.UpdateMetadata}, nil
}

func (s *Store) insertTasks(ctx context.Context, tx *sql.Tx, id []byte, hash uint32,
	req *store.CreateTasksRequest, blobs [][]byte, start, end int,
) error {
	const cols = 7
	var b strings.Builder
	b.WriteString(s.q.insertTasksPrefix)
	args := make([]any, 0, (end-start)*cols)
	for i := start; i < end; i++ {
		if i > start {
			b.WriteString(", ")
		}
		if s.d.NumberedPlaceholders {
			n := len(args)
			fmt.Fprintf(&b, "($%d, $%d, $%d, $%d, $%d, $%d, $%d)", n+1, n+2, n+3, n+4, n+5, n+6, n+7)
		} else {
			b.WriteString("(?, ?, ?, ?, ?, ?, ?)")
		}
		sq := 0
		if req.Subqueues != nil {
			sq = req.Subqueues[i]
		}
		t := req.Tasks[i]
		args = append(args, int64(hash), id, sq, t.Pass, t.ID, blobs[i], DataEncoding)
	}
	b.WriteString(s.q.insertTasksSuffix)
	_, err := tx.ExecContext(ctx, b.String(), args...)
	return err
}

func (s *Store) GetTasks(ctx context.Context, req *store.GetTasksRequest) (*store.GetTasksResponse, error) {
	if req.InclusiveMin.Pass < 1 {
		return nil, &store.InvalidArgumentError{Msg: "InclusiveMin.Pass must be >= 1"}
	}
	if req.PageSize <= 0 {
		return nil, &store.InvalidArgumentError{Msg: "PageSize must be > 0"}
	}
	id, hash := queueIDAndHash(req.Key)
	rows, err := s.r.QueryContext(ctx, s.q.getTasks, int64(hash), id, req.Subqueue,
		req.InclusiveMin.Pass, req.InclusiveMin.ID, req.PageSize)
	if err != nil {
		return nil, s.wrap("GetTasks", err)
	}
	defer rows.Close()
	resp := &store.GetTasksResponse{Tasks: make([]store.TaskInfo, 0, min(req.PageSize, 64))}
	for rows.Next() {
		var (
			t    store.TaskInfo
			data []byte
			enc  string
		)
		if err := rows.Scan(&t.Pass, &t.ID, &data, &enc); err != nil {
			return nil, s.wrap("GetTasks: scan", err)
		}
		// A row that cannot be decoded is corrupt, not a store outage: report it as
		// InvalidArgument naming the task (REVIEW L10). The reader keeps retrying the batch,
		// as upstream does for a deserialization error, so the row is never skipped silently.
		if enc != DataEncoding {
			return nil, &store.InvalidArgumentError{Msg: fmt.Sprintf("GetTasks: task %d (pass %d): unsupported data_encoding %q", t.ID, t.Pass, enc)}
		}
		if err := json.Unmarshal(data, &t.Data); err != nil {
			return nil, &store.InvalidArgumentError{Msg: fmt.Sprintf("GetTasks: task %d (pass %d): decode: %v", t.ID, t.Pass, err)}
		}
		resp.Tasks = append(resp.Tasks, t)
	}
	if err := rows.Err(); err != nil {
		return nil, s.wrap("GetTasks: rows", err)
	}
	return resp, nil
}

func (s *Store) CompleteTasksLessThan(ctx context.Context, req *store.CompleteTasksLessThanRequest) (int, error) {
	id, hash := queueIDAndHash(req.Key)
	del := func(ctx context.Context, ex interface {
		ExecContext(context.Context, string, ...any) (sql.Result, error)
	}) (int64, error) {
		var res sql.Result
		var err error
		if req.Limit > 0 {
			res, err = ex.ExecContext(ctx, s.q.rangeDelete,
				int64(hash), id, req.Subqueue,
				int64(hash), id, req.Subqueue, req.ExclusiveMax.Pass, req.ExclusiveMax.ID, req.Limit)
		} else {
			res, err = ex.ExecContext(ctx, s.q.rangeDeleteNoLimit,
				int64(hash), id, req.Subqueue, req.ExclusiveMax.Pass, req.ExclusiveMax.ID)
		}
		if err != nil {
			return 0, err
		}
		return res.RowsAffected()
	}

	if req.ConditionRangeID == 0 {
		n, err := del(ctx, s.w)
		if err != nil {
			return 0, s.wrap("CompleteTasksLessThan", err)
		}
		return int(n), nil
	}
	var n int64
	err := s.txExecute(ctx, "CompleteTasksLessThan", func(tx *sql.Tx) error {
		if err := s.lockTaskQueue(ctx, tx, id, hash, req.ConditionRangeID); err != nil {
			return err
		}
		var err error
		n, err = del(ctx, tx)
		return err
	})
	if err != nil {
		return 0, err
	}
	return int(n), nil
}

// ---- dead letters (library addition; store.DeadLetterStore) ----

// UpdateTask rewrites one task's data in place under the rangeID fence.
func (s *Store) UpdateTask(ctx context.Context, req *store.UpdateTaskRequest) error {
	blob, err := json.Marshal(&req.Task.Data)
	if err != nil {
		return &store.InvalidArgumentError{Msg: "encode task: " + err.Error()}
	}
	id, hash := queueIDAndHash(req.Key)
	return s.txExecute(ctx, "UpdateTask", func(tx *sql.Tx) error {
		if err := s.lockTaskQueue(ctx, tx, id, hash, req.RangeID); err != nil {
			return err
		}
		res, err := tx.ExecContext(ctx, s.q.updateTask, blob, DataEncoding, int64(hash), id, req.Subqueue, req.Task.Pass, req.Task.ID)
		if err != nil {
			return err
		}
		if n, err := res.RowsAffected(); err != nil {
			return err
		} else if n == 0 {
			return &store.NotFoundError{Msg: fmt.Sprintf("task %d", req.Task.ID)}
		}
		return nil
	})
}

// DeadLetterTask upserts the dead-letter row under the rangeID fence and leaves the backlog
// row for ack-level GC.
func (s *Store) DeadLetterTask(ctx context.Context, req *store.DeadLetterTaskRequest) error {
	blob, err := json.Marshal(&req.Task.Data)
	if err != nil {
		return &store.InvalidArgumentError{Msg: "encode task: " + err.Error()}
	}
	id, hash := queueIDAndHash(req.Key)
	return s.txExecute(ctx, "DeadLetterTask", func(tx *sql.Tx) error {
		if err := s.lockTaskQueue(ctx, tx, id, hash, req.RangeID); err != nil {
			return err
		}
		_, err := tx.ExecContext(ctx, s.q.insertDead, int64(hash), id, req.Task.ID, req.Task.Pass,
			req.DeadTime.UnixNano(), req.Reason, blob, DataEncoding)
		return err
	})
}

// ListDeadTasks pages a queue's dead tasks in task ID order; the token is the last ID.
func (s *Store) ListDeadTasks(ctx context.Context, req *store.ListDeadTasksRequest) (*store.ListDeadTasksResponse, error) {
	if req.PageSize <= 0 {
		return nil, &store.InvalidArgumentError{Msg: fmt.Sprintf("ListDeadTasks: PageSize %d must be positive", req.PageSize)}
	}
	id, hash := queueIDAndHash(req.Key)
	var (
		rows *sql.Rows
		err  error
	)
	if len(req.PageToken) == 0 {
		rows, err = s.r.QueryContext(ctx, s.q.listDeadFirst, int64(hash), id, req.PageSize+1)
	} else {
		var after int64
		if jerr := json.Unmarshal(req.PageToken, &after); jerr != nil {
			return nil, &store.InvalidArgumentError{Msg: "ListDeadTasks: invalid page token"}
		}
		rows, err = s.r.QueryContext(ctx, s.q.listDeadAfter, int64(hash), id, after, req.PageSize+1)
	}
	if err != nil {
		return nil, s.wrap("ListDeadTasks", err)
	}
	defer rows.Close()
	resp := &store.ListDeadTasksResponse{}
	for rows.Next() {
		var (
			d    store.DeadTask
			dead int64
			data []byte
			enc  string
		)
		if err := rows.Scan(&d.Task.ID, &d.Task.Pass, &dead, &d.Reason, &data, &enc); err != nil {
			return nil, s.wrap("ListDeadTasks: scan", err)
		}
		if len(resp.Tasks) == req.PageSize {
			tok, err := json.Marshal(resp.Tasks[len(resp.Tasks)-1].Task.ID)
			if err != nil {
				return nil, err
			}
			resp.NextPageToken = tok
			break
		}
		if enc != DataEncoding {
			return nil, &store.InvalidArgumentError{Msg: fmt.Sprintf("ListDeadTasks: task %d: unsupported data_encoding %q", d.Task.ID, enc)}
		}
		if err := json.Unmarshal(data, &d.Task.Data); err != nil {
			return nil, &store.InvalidArgumentError{Msg: fmt.Sprintf("ListDeadTasks: task %d: decode: %v", d.Task.ID, err)}
		}
		d.DeadTime = time.Unix(0, dead).UTC()
		resp.Tasks = append(resp.Tasks, d)
	}
	if err := rows.Err(); err != nil {
		return nil, s.wrap("ListDeadTasks: rows", err)
	}
	return resp, nil
}

// DeleteDeadTasks deletes by ID (TaskIDs non-nil) or by DeadBefore.
// Every ID batch runs in one transaction. A later batch error rolls the earlier
// batches back, and the deleted count is returned only after commit.
func (s *Store) DeleteDeadTasks(ctx context.Context, req *store.DeleteDeadTasksRequest) (int, error) {
	if req.TaskIDs == nil && req.DeadBefore.IsZero() {
		return 0, &store.InvalidArgumentError{Msg: "DeleteDeadTasks: TaskIDs or DeadBefore is required"}
	}
	id, hash := queueIDAndHash(req.Key)
	if req.TaskIDs == nil {
		res, err := s.w.ExecContext(ctx, s.q.deleteDeadBefore, int64(hash), id, req.DeadBefore.UnixNano())
		if err != nil {
			return 0, s.wrap("DeleteDeadTasks", err)
		}
		n, err := res.RowsAffected()
		return int(n), s.wrap("DeleteDeadTasks", err)
	}
	if len(req.TaskIDs) == 0 {
		return 0, nil
	}
	var total int
	err := s.txExecute(ctx, "DeleteDeadTasks", func(tx *sql.Tx) error {
		for start := 0; start < len(req.TaskIDs); start += s.d.MaxRowsPerInsert {
			ids := req.TaskIDs[start:min(start+s.d.MaxRowsPerInsert, len(req.TaskIDs))]
			q := `DELETE FROM tq_dead_tasks WHERE ` + queueKey + ` AND task_id IN (?` + strings.Repeat(", ?", len(ids)-1) + `)`
			if s.d.NumberedPlaceholders {
				q = rebind(q, 0)
			}
			args := make([]any, 0, 2+len(ids))
			args = append(args, int64(hash), id)
			for _, t := range ids {
				args = append(args, t)
			}
			res, err := tx.ExecContext(ctx, q, args...)
			if err != nil {
				return err
			}
			n, err := res.RowsAffected()
			if err != nil {
				return err
			}
			total += int(n)
		}
		return nil
	})
	if err != nil {
		return 0, err
	}
	return total, nil
}

// CountDeadTasks returns the number of dead tasks of a queue.
func (s *Store) CountDeadTasks(ctx context.Context, key store.QueueKey) (int64, error) {
	id, hash := queueIDAndHash(key)
	var n int64
	if err := s.r.QueryRowContext(ctx, s.q.countDead, int64(hash), id).Scan(&n); err != nil {
		return 0, s.wrap("CountDeadTasks", err)
	}
	return n, nil
}

// DeleteTask deletes one backlog task under the rangeID fence.
func (s *Store) DeleteTask(ctx context.Context, req *store.DeleteTaskRequest) (bool, error) {
	id, hash := queueIDAndHash(req.Key)
	var n int64
	err := s.txExecute(ctx, "DeleteTask", func(tx *sql.Tx) error {
		if err := s.lockTaskQueue(ctx, tx, id, hash, req.RangeID); err != nil {
			return err
		}
		res, err := tx.ExecContext(ctx, s.q.deleteTask, int64(hash), id, req.Subqueue, req.Pass, req.ID)
		if err != nil {
			return err
		}
		n, err = res.RowsAffected()
		return err
	})
	return n > 0, err
}

// CheckHealth pings the read pool and reads the applied schema version; want is the highest
// embedded migration. It deliberately never checks out the write pool: on sqlite that pool is
// a single connection that every write holds until commit (up to the busy timeout), so a
// probe on it would report unready while writes succeed, and would block writes while it ran.
// A failing writer shows up as degraded queues in Engine.Health instead. On postgres the read
// and write pools are the same pool.
func (s *Store) CheckHealth(ctx context.Context, want int) (store.StoreHealth, error) {
	h := store.StoreHealth{WantSchemaVersion: want}
	if err := s.r.PingContext(ctx); err != nil {
		return h, s.wrap("Ping", err)
	}
	var v sql.NullInt64
	if err := s.r.QueryRowContext(ctx, `SELECT MAX(version) FROM tq_schema_version`).Scan(&v); err != nil {
		return h, s.wrap("SchemaVersion", err)
	}
	h.SchemaVersion = int(v.Int64)
	return h, nil
}

// LatestMigration returns the highest version in ms (0 if empty).
func LatestMigration(ms []Migration) int {
	n := 0
	for _, m := range ms {
		n = max(n, m.Version)
	}
	return n
}
