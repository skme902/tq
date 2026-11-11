package sqlcore_test

// DeleteDeadTasks used to commit each MaxRowsPerInsert id batch on its own. A later batch
// error then left the earlier batches deleted, and the returned count included them. All id
// batches now run in one transaction, and the count is returned only after that commit.
//
// The rollback case fails on the old code: the first batch has already committed, so the
// count is non-zero and the surviving rows are only the ones the failed batch did not delete.

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/stretchr/testify/require"

	"github.com/skme902/tq/internal/testdb"
	"github.com/skme902/tq/store"
	"github.com/skme902/tq/store/internal/sqlcore"
	"github.com/skme902/tq/store/memstore"
	"github.com/skme902/tq/store/postgres"
	"github.com/skme902/tq/store/sqlite"
)

const dlqBatch = 2 // MaxRowsPerInsert in this test; production dialects use 1000

func TestDeleteDeadTasksR2(t *testing.T) {
	t.Run("sqlite", func(t *testing.T) {
		testDeleteDeadBatches(t, openSQLiteDLQ(t))
	})
	t.Run("postgres", func(t *testing.T) {
		testDeleteDeadBatches(t, openPostgresDLQ(t))
	})
	t.Run("memstore", func(t *testing.T) {
		testDeleteDeadMemstore(t)
	})
}

// dlqFix is a DeadLetterStore whose id deletes are split into batches of dlqBatch, plus a
// hook that makes a later batch fail.
type dlqFix struct {
	dls  store.DeadLetterStore
	fail func(t *testing.T)
}

func testDeleteDeadBatches(t *testing.T, h dlqFix) {
	t.Helper()
	ctx := context.Background()
	const n = dlqBatch*2 + 1 // three batches: full, full, tail
	ids := make([]int64, n)
	for i := range ids {
		ids[i] = int64(i + 1)
	}

	seedDead(t, h.dls, ids)
	got, err := h.dls.DeleteDeadTasks(ctx, &store.DeleteDeadTasksRequest{Key: dlqKey, TaskIDs: ids})
	require.NoError(t, err)
	require.Equal(t, n, got, "delete of more than MaxRowsPerInsert ids returns the rows removed")
	require.Equal(t, int64(0), deadCount(t, h.dls))

	seedDead(t, h.dls, ids)
	h.fail(t)
	got, err = h.dls.DeleteDeadTasks(ctx, &store.DeleteDeadTasksRequest{Key: dlqKey, TaskIDs: ids})
	require.Error(t, err)
	require.Equal(t, 0, got, "a failed delete reports no rows; the count is returned only on commit")
	require.Equal(t, int64(n), deadCount(t, h.dls), "an error in a later id batch rolls back the earlier batches")
	left := listDeadIDs(t, h.dls)
	require.Equal(t, ids, left)
}

func testDeleteDeadMemstore(t *testing.T) {
	st := memstore.New()
	// More ids than a production SQL batch (MaxRowsPerInsert is 1000). memstore deletes in
	// one step; this fix does not change that path.
	const n = 1001
	ids := make([]int64, n)
	for i := range ids {
		ids[i] = int64(i + 1)
	}
	seedDead(t, st, ids)
	got, err := st.DeleteDeadTasks(context.Background(), &store.DeleteDeadTasksRequest{Key: dlqKey, TaskIDs: ids})
	require.NoError(t, err)
	require.Equal(t, n, got)
	require.Equal(t, int64(0), deadCount(t, st))
}

var dlqKey = store.QueueKey{Namespace: "ns", Name: "dlq-r2"}

func seedDead(t *testing.T, dls store.DeadLetterStore, ids []int64) {
	t.Helper()
	ctx := context.Background()
	info := &store.QueueInfo{Key: dlqKey, Subqueues: []store.SubqueueInfo{{}}, LastUpdateTime: time.Unix(0, 0).UTC()}
	err := dls.(store.Store).CreateQueue(ctx, &store.CreateQueueRequest{RangeID: 1, Info: info})
	if err != nil && !store.IsConditionFailed(err) {
		require.NoError(t, err)
	}
	for _, id := range ids {
		require.NoError(t, dls.DeadLetterTask(ctx, &store.DeadLetterTaskRequest{
			Key: dlqKey, RangeID: 1,
			Task:     store.TaskInfo{ID: id, Pass: 1, Data: store.Task{Payload: []byte("p")}},
			Reason:   "r",
			DeadTime: time.Unix(0, id).UTC(),
		}))
	}
}

func deadCount(t *testing.T, dls store.DeadLetterStore) int64 {
	t.Helper()
	n, err := dls.CountDeadTasks(context.Background(), dlqKey)
	require.NoError(t, err)
	return n
}

func listDeadIDs(t *testing.T, dls store.DeadLetterStore) []int64 {
	t.Helper()
	resp, err := dls.ListDeadTasks(context.Background(), &store.ListDeadTasksRequest{Key: dlqKey, PageSize: 100})
	require.NoError(t, err)
	require.Nil(t, resp.NextPageToken)
	out := make([]int64, len(resp.Tasks))
	for i, d := range resp.Tasks {
		out[i] = d.Task.ID
	}
	return out
}

func openSQLiteDLQ(t *testing.T) dlqFix {
	t.Helper()
	path := filepath.Join(t.TempDir(), "q.db")
	db, err := sql.Open("sqlite", "file:"+path+"?_txlock=immediate")
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	db.SetMaxOpenConns(1)
	return dlqFix{dls: migrateDLQ(t, db, sqlcore.Dialect{Name: "sqlite", MaxRowsPerInsert: dlqBatch}), fail: func(t *testing.T) {
		t.Helper()
		_, err := db.Exec(fmt.Sprintf(`CREATE TRIGGER tq_dlq_r2_fail BEFORE DELETE ON tq_dead_tasks
WHEN OLD.task_id > %d
BEGIN
SELECT RAISE(ABORT, 'injected');
END;`, dlqBatch))
		require.NoError(t, err)
	}}
}

func openPostgresDLQ(t *testing.T) dlqFix {
	t.Helper()
	dsn := testdb.PostgresSchemaDSN(t)
	db, err := sql.Open("pgx", dsn)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	d := sqlcore.Dialect{
		Name:                 "postgres",
		NumberedPlaceholders: true,
		LockSuffix:           " FOR UPDATE",
		MaxRowsPerInsert:     dlqBatch,
	}
	ms, err := sqlcore.LoadMigrations(postgres.Migrations(), ".")
	require.NoError(t, err)
	_, err = sqlcore.Migrate(context.Background(), db, d, ms)
	require.NoError(t, err)
	st := sqlcore.New(db, db, d)
	return dlqFix{dls: st, fail: func(t *testing.T) {
		t.Helper()
		_, err := db.Exec(fmt.Sprintf(`
CREATE FUNCTION tq_dlq_r2_fail() RETURNS trigger
LANGUAGE plpgsql AS $fn$
BEGIN
  IF OLD.task_id > %d THEN
    RAISE EXCEPTION 'injected';
  END IF;
  RETURN OLD;
END
$fn$;
CREATE TRIGGER tq_dlq_r2_fail
BEFORE DELETE ON tq_dead_tasks
FOR EACH ROW EXECUTE PROCEDURE tq_dlq_r2_fail();`, dlqBatch))
		require.NoError(t, err)
	}}
}

func migrateDLQ(t *testing.T, db *sql.DB, d sqlcore.Dialect) store.DeadLetterStore {
	t.Helper()
	ms, err := sqlcore.LoadMigrations(sqlite.Migrations(), ".")
	require.NoError(t, err)
	_, err = sqlcore.Migrate(context.Background(), db, d, ms)
	require.NoError(t, err)
	return sqlcore.New(db, db, d)
}
