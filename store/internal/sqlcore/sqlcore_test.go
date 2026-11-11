package sqlcore_test

// SQL-store tests beyond storetest: behaviour that needs two handles on one database (two
// processes), concurrent migrators, or batches larger than one multi-row INSERT.

import (
	"context"
	"database/sql"
	"fmt"
	"io/fs"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/skme902/tq/internal/testdb"
	"github.com/skme902/tq/store"
	"github.com/skme902/tq/store/internal/sqlcore"
	"github.com/skme902/tq/store/postgres"
	"github.com/skme902/tq/store/sqlite"
)

var key = store.QueueKey{Namespace: "ns", Name: "q"}

func sqlKinds() []testdb.Kind {
	var out []testdb.Kind
	for _, k := range testdb.Kinds() {
		if k.Durable {
			out = append(out, k)
		}
	}
	return out
}

func info() *store.QueueInfo {
	return &store.QueueInfo{Key: key, Subqueues: []store.SubqueueInfo{{}}, LastUpdateTime: time.Now()}
}

func tasks(from, n int) []store.TaskInfo {
	out := make([]store.TaskInfo, n)
	for i := range out {
		out[i] = store.TaskInfo{ID: int64(from + i), Pass: 1, Data: store.Task{Payload: []byte(fmt.Sprint(from + i))}}
	}
	return out
}

func count(t *testing.T, st store.Store) int {
	t.Helper()
	resp, err := st.GetTasks(context.Background(), &store.GetTasksRequest{Key: key, InclusiveMin: store.Level{Pass: 1}, PageSize: 1 << 20})
	require.NoError(t, err)
	return len(resp.Tasks)
}

func TestCrossHandleFencing(t *testing.T) {
	for _, k := range sqlKinds() {
		t.Run(k.Name, func(t *testing.T) {
			ctx := context.Background()
			open := k.NewDB(t)
			h1, h2 := open(t), open(t)
			require.NoError(t, h1.CreateQueue(ctx, &store.CreateQueueRequest{RangeID: 1, Info: info()}))
			_, err := h1.CreateTasks(ctx, &store.CreateTasksRequest{RangeID: 1, Info: info(), Tasks: tasks(1, 10), Subqueues: make([]int, 10)})
			require.NoError(t, err)
			require.True(t, store.IsConditionFailed(h2.CreateQueue(ctx, &store.CreateQueueRequest{RangeID: 1, Info: info()})))
			require.NoError(t, h2.UpdateQueue(ctx, &store.UpdateQueueRequest{RangeID: 2, PrevRangeID: 1, Info: info()}))

			_, err = h1.CreateTasks(ctx, &store.CreateTasksRequest{RangeID: 1, Info: info(), Tasks: tasks(11, 10), Subqueues: make([]int, 10)})
			require.True(t, store.IsConditionFailed(err), "stale owner write fenced: %v", err)
			_, err = h1.CompleteTasksLessThan(ctx, &store.CompleteTasksLessThanRequest{Key: key, ExclusiveMax: store.Level{Pass: 2}, ConditionRangeID: 1})
			require.True(t, store.IsConditionFailed(err), "stale owner GC fenced: %v", err)
			require.True(t, store.IsConditionFailed(h1.UpdateQueue(ctx, &store.UpdateQueueRequest{RangeID: 2, PrevRangeID: 1, Info: info()})))
			require.Equal(t, 10, count(t, h2), "fenced calls wrote and deleted nothing")
		})
	}
}

// A stale owner racing a takeover: every CreateTasks either fully lands (nil) or not at all
// (ConditionFailed), including batches spanning several multi-row INSERT statements.
func TestCreateTasksAllOrNothingUnderTakeover(t *testing.T) {
	for _, k := range sqlKinds() {
		t.Run(k.Name, func(t *testing.T) {
			ctx := context.Background()
			open := k.NewDB(t)
			owner, thief := open(t), open(t)
			require.NoError(t, owner.CreateQueue(ctx, &store.CreateQueueRequest{RangeID: 1, Info: info()}))

			const batch = 2500 // > MaxRowsPerInsert on both dialects
			type result struct {
				from int
				err  error
			}
			var results []result
			var wg sync.WaitGroup
			twoDone := make(chan struct{})
			wg.Go(func() {
				for b := range 8 {
					if b == 2 {
						close(twoDone) // batch 2 races the takeover
					}
					from := 1 + b*batch
					_, err := owner.CreateTasks(ctx, &store.CreateTasksRequest{RangeID: 1, Info: info(), Tasks: tasks(from, batch), Subqueues: make([]int, batch)})
					results = append(results, result{from, err})
				}
			})
			<-twoDone
			require.NoError(t, thief.UpdateQueue(ctx, &store.UpdateQueueRequest{RangeID: 2, PrevRangeID: 1, Info: info()}))
			wg.Wait()

			resp, err := thief.GetTasks(ctx, &store.GetTasksRequest{Key: key, InclusiveMin: store.Level{Pass: 1}, PageSize: 1 << 20})
			require.NoError(t, err)
			have := map[int64]bool{}
			for _, ti := range resp.Tasks {
				have[ti.ID] = true
			}
			ok, fenced := 0, 0
			for _, r := range results {
				n := 0
				for i := range batch {
					if have[int64(r.from+i)] {
						n++
					}
				}
				switch {
				case r.err == nil:
					ok++
					require.Equal(t, batch, n, "acknowledged batch at %d fully present", r.from)
				case store.IsConditionFailed(r.err):
					fenced++
					require.Zero(t, n, "fenced batch at %d left no rows", r.from)
				default:
					t.Fatalf("batch %d: %v", r.from, r.err)
				}
			}
			require.Equal(t, ok*batch, len(resp.Tasks))
			require.GreaterOrEqual(t, ok, 2, "batches before the takeover land")
			require.GreaterOrEqual(t, fenced, 5, "batches after the takeover are fenced")
			t.Logf("batches ok=%d fenced=%d", ok, fenced)
		})
	}
}

func TestMigrateIdempotentAndConcurrent(t *testing.T) {
	t.Run("sqlite", func(t *testing.T) {
		ctx := context.Background()
		path := t.TempDir() + "/m.db"
		var wg sync.WaitGroup
		errs := make(chan error, 4)
		for range 4 {
			wg.Go(func() {
				st, err := sqlite.Open(ctx, path, sqlite.Options{})
				if err != nil {
					errs <- err
					return
				}
				defer st.Close()
				errs <- st.Migrate(ctx)
			})
		}
		wg.Wait()
		close(errs)
		for err := range errs {
			require.NoError(t, err)
		}
		st, err := sqlite.Open(ctx, path, sqlite.Options{})
		require.NoError(t, err)
		defer st.Close()
		var n int
		require.NoError(t, st.ReadDB().QueryRowContext(ctx, "SELECT count(*) FROM tq_schema_version").Scan(&n))
		require.Equal(t, numMigrations(t, sqlite.Migrations()), n)
		require.NoError(t, st.Migrate(ctx))
	})
	t.Run("postgres", func(t *testing.T) {
		ctx := context.Background()
		dsn := testdb.PostgresSchemaDSN(t)
		var wg sync.WaitGroup
		applied := make(chan []int, 4)
		errs := make(chan error, 4)
		for range 4 {
			wg.Go(func() {
				st, err := postgres.Open(ctx, dsn, postgres.Options{})
				if err != nil {
					errs <- err
					return
				}
				defer st.Close()
				did, err := postgres.Migrate(ctx, st.DB())
				errs <- err
				applied <- did
			})
		}
		wg.Wait()
		close(errs)
		close(applied)
		for err := range errs {
			require.NoError(t, err)
		}
		total := 0
		for did := range applied {
			total += len(did)
		}
		require.Equal(t, numMigrations(t, postgres.Migrations()), total, "each migration applied by exactly one migrator")
	})
}

// L10: a task row that cannot be decoded is reported as an InvalidArgumentError naming the
// task, so callers can tell corruption from a transient store failure.
func TestCorruptTaskRowIsInvalidArgument(t *testing.T) {
	ctx := context.Background()
	path := t.TempDir() + "/corrupt.db"
	st, err := sqlite.Open(ctx, path, sqlite.Options{})
	require.NoError(t, err)
	t.Cleanup(func() { _ = st.Close() })
	require.NoError(t, st.Migrate(ctx))
	require.NoError(t, st.CreateQueue(ctx, &store.CreateQueueRequest{RangeID: 1, Info: info()}))
	_, err = st.CreateTasks(ctx, &store.CreateTasksRequest{RangeID: 1, Info: info(), Tasks: tasks(1, 3), Subqueues: make([]int, 3)})
	require.NoError(t, err)

	raw, err := sql.Open("sqlite", path)
	require.NoError(t, err)
	defer raw.Close()
	_, err = raw.ExecContext(ctx, `UPDATE tq_tasks SET data = ? WHERE task_id = 2`, []byte("{not json"))
	require.NoError(t, err)

	_, err = st.GetTasks(ctx, &store.GetTasksRequest{Key: key, InclusiveMin: store.Level{Pass: 1}, PageSize: 10})
	var inv *store.InvalidArgumentError
	require.ErrorAs(t, err, &inv)
	require.Contains(t, err.Error(), "task 2")
	require.False(t, store.IsTransient(err))
}

func numMigrations(t *testing.T, fsys fs.FS) int {
	ms, err := sqlcore.LoadMigrations(fsys, ".")
	require.NoError(t, err)
	return len(ms)
}

// Upgrading a database created at 0001 keeps its queues and tasks readable and adds the
// later tables (forward migrations only).
func TestMigrateUpgradeKeepsData(t *testing.T) {
	ctx := context.Background()
	run := func(t *testing.T, db *sql.DB, d sqlcore.Dialect, fsys fs.FS, st store.Store) {
		ms, err := sqlcore.LoadMigrations(fsys, ".")
		require.NoError(t, err)
		require.Greater(t, len(ms), 1)
		_, err = sqlcore.Migrate(ctx, db, d, ms[:1])
		require.NoError(t, err)
		require.NoError(t, st.CreateQueue(ctx, &store.CreateQueueRequest{RangeID: 1, Info: info()}))
		_, err = st.CreateTasks(ctx, &store.CreateTasksRequest{RangeID: 1, Info: info(), Tasks: tasks(1, 3)})
		require.NoError(t, err)

		did, err := sqlcore.Migrate(ctx, db, d, ms)
		require.NoError(t, err)
		require.Len(t, did, len(ms)-1)
		got, err := st.GetTasks(ctx, &store.GetTasksRequest{Key: key, InclusiveMin: store.Level{Pass: 1}, PageSize: 10})
		require.NoError(t, err)
		require.Len(t, got.Tasks, 3)
		require.Zero(t, got.Tasks[0].Data.Attempts)
		dls := st.(store.DeadLetterStore)
		require.NoError(t, dls.DeadLetterTask(ctx, &store.DeadLetterTaskRequest{Key: key, RangeID: 1, Task: got.Tasks[0], Reason: "r", DeadTime: time.Now()}))
		n, err := dls.CountDeadTasks(ctx, key)
		require.NoError(t, err)
		require.EqualValues(t, 1, n)
	}
	t.Run("sqlite", func(t *testing.T) {
		st, err := sqlite.Open(ctx, t.TempDir()+"/q.db", sqlite.Options{})
		require.NoError(t, err)
		defer st.Close()
		run(t, st.WriteDB(), sqlcore.Dialect{Name: "sqlite"}, sqlite.Migrations(), st)
	})
	t.Run("postgres", func(t *testing.T) {
		st, err := postgres.Open(ctx, testdb.PostgresSchemaDSN(t), postgres.Options{})
		require.NoError(t, err)
		defer st.Close()
		run(t, st.DB(), sqlcore.Dialect{Name: "postgres", NumberedPlaceholders: true}, postgres.Migrations(), st)
	})
}

// CheckHealth reports the applied schema against the embedded one: behind before Migrate,
// current after, and an error once the pool is closed.
func TestCheckHealthSchemaVersion(t *testing.T) {
	ctx := context.Background()
	st, err := sqlite.Open(ctx, t.TempDir()+"/h.db", sqlite.Options{})
	require.NoError(t, err)
	ms, err := sqlcore.LoadMigrations(sqlite.Migrations(), ".")
	require.NoError(t, err)
	_, err = sqlcore.Migrate(ctx, st.WriteDB(), sqlcore.Dialect{Name: "sqlite"}, ms[:1])
	require.NoError(t, err)
	h, err := st.CheckHealth(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, h.SchemaVersion)
	require.Equal(t, sqlcore.LatestMigration(ms), h.WantSchemaVersion)
	require.NoError(t, st.Migrate(ctx))
	h, err = st.CheckHealth(ctx)
	require.NoError(t, err)
	require.Equal(t, h.WantSchemaVersion, h.SchemaVersion)
	require.NoError(t, st.Close())
	_, err = st.CheckHealth(ctx)
	require.Error(t, err)
}
