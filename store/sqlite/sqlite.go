// Adapted from temporalio/temporal v1.32.1 af08a5e5, common/persistence/sql/sqlplugin/sqlite
// (plugin.go connection setup, task_v2.go, task_queues.go) and common/persistence/sql/task_v2.go.
// Copyright (c) 2025 Temporal Technologies Inc. MIT License; see UPSTREAM_LICENSE and NOTICE.

// Package sqlite is a durable store.Store on a SQLite file, using the pure-Go (cgo-free)
// modernc.org/sqlite driver, the same driver upstream Temporal uses.
//
//	st, err := sqlite.Open(ctx, "/var/lib/myapp/queue.db", sqlite.Options{})
//	if err != nil { ... }
//	defer st.Close()
//	if err := st.Migrate(ctx); err != nil { ... }
//
// SQLite allows one writer at a time. The store is tuned for that: WAL journal (readers
// never block the writer), a dedicated single-connection writer pool so in-process writers
// queue on database/sql instead of spinning on SQLITE_BUSY, a separate read-only pool for
// GetQueue/GetTasks, BEGIN IMMEDIATE transactions (the range-id check and the write happen
// under the database write lock, which is what makes fencing correct across processes),
// and a busy timeout for the cross-process case.
package sqlite

import (
	"context"
	"database/sql"
	"embed"
	"errors"
	"fmt"
	"io/fs"
	"net/url"
	"strings"
	"sync"
	"time"

	msqlite "modernc.org/sqlite" // registers driver "sqlite"

	"github.com/skme902/tq/store"
	"github.com/skme902/tq/store/internal/sqlcore"
)

//go:embed migrations/*.sql
var migrationsFS embed.FS

// Migrations returns the embedded migration files (for use with external tooling).
func Migrations() fs.FS { sub, _ := fs.Sub(migrationsFS, "migrations"); return sub }

// Synchronous values for Options.Synchronous.
const (
	// SyncFull fsyncs the WAL on every commit: a committed write survives power loss.
	SyncFull = "FULL"
	// SyncNormal fsyncs only at checkpoints: committed writes survive a process crash but
	// the last transactions may be lost on power loss / OS crash. Faster.
	SyncNormal = "NORMAL"
)

// Options configures Open. Zero values pick the defaults noted.
type Options struct {
	// BusyTimeout is how long a connection waits for another process's write lock before
	// failing with SQLITE_BUSY (mapped to store.TransientError, which the engine retries).
	// Default 5s.
	BusyTimeout time.Duration
	// Synchronous is SyncFull (default) or SyncNormal.
	Synchronous string
	// ReadConns is the size of the read-only pool (default 4). Writes always use one
	// connection.
	ReadConns int
}

var dialect = sqlcore.Dialect{
	Name: "sqlite",
	// No row locks in SQLite: BEGIN IMMEDIATE (set via _txlock on the writer pool) takes
	// the database write lock before the range-id read.
	LockSuffix: "",
	// 7 bind params per row; SQLite's default limit is 32766.
	MaxRowsPerInsert: 1000,
}

// Store is a store.Store backed by a SQLite database file.
type Store struct {
	*sqlcore.Store
	w, r *sql.DB
}

var _ store.Store = (*Store)(nil)

// Open opens (creating if needed) the database file at path. In-memory databases are not
// supported (the writer and reader pools must see the same database); use store/memstore.
// Open does not create tables; call Migrate.
func Open(ctx context.Context, path string, opts Options) (*Store, error) {
	if path == "" || path == ":memory:" || strings.Contains(path, "mode=memory") {
		return nil, errors.New("sqlite: a database file path is required (use memstore for in-memory)")
	}
	if strings.ContainsAny(path, "?#") {
		return nil, fmt.Errorf("sqlite: path %q must not contain '?' or '#'", path)
	}
	busy := opts.BusyTimeout
	if busy <= 0 {
		busy = 5 * time.Second
	}
	syncMode := strings.ToUpper(opts.Synchronous)
	switch syncMode {
	case "":
		syncMode = SyncFull
	case SyncFull, SyncNormal:
	default:
		return nil, fmt.Errorf("sqlite: unknown Synchronous %q", opts.Synchronous)
	}
	readConns := opts.ReadConns
	if readConns <= 0 {
		readConns = 4
	}

	common := func(extra ...string) string {
		v := url.Values{}
		v.Add("_pragma", fmt.Sprintf("busy_timeout(%d)", busy.Milliseconds()))
		v.Add("_pragma", "synchronous("+syncMode+")")
		for _, p := range extra {
			v.Add("_pragma", p)
		}
		return v.Encode()
	}
	// journal_mode=WAL is persistent in the file; it is set once below rather than as a
	// connect-time pragma, because the switch can return SQLITE_BUSY without consulting the
	// busy handler when several processes open a fresh file at once.
	wDSN := "file:" + path + "?_txlock=immediate&" + common()
	rDSN := "file:" + path + "?" + common("query_only(1)")

	w, err := sql.Open("sqlite", wDSN)
	if err != nil {
		return nil, err
	}
	w.SetMaxOpenConns(1)
	w.SetMaxIdleConns(1)
	w.SetConnMaxIdleTime(0)
	w.SetConnMaxLifetime(0)
	deadline := time.Now().Add(busy)
	var mode string
	if err := retryBusy(ctx, deadline, func() error {
		return w.QueryRowContext(ctx, "PRAGMA journal_mode=WAL").Scan(&mode)
	}); err != nil {
		_ = w.Close()
		return nil, fmt.Errorf("sqlite: set journal_mode=WAL: %w", err)
	}
	if !strings.EqualFold(mode, "wal") {
		_ = w.Close()
		return nil, fmt.Errorf("sqlite: journal_mode is %q, want wal", mode)
	}
	r, err := sql.Open("sqlite", rDSN)
	if err != nil {
		_ = w.Close()
		return nil, err
	}
	r.SetMaxOpenConns(readConns)
	r.SetMaxIdleConns(readConns)
	if err := retryBusy(ctx, deadline, func() error { return r.PingContext(ctx) }); err != nil {
		_ = w.Close()
		_ = r.Close()
		return nil, err
	}
	return &Store{Store: sqlcore.New(w, r, dialect), w: w, r: r}, nil
}

// retryBusy runs f until it returns something other than SQLITE_BUSY/SQLITE_LOCKED, or the
// deadline or ctx ends. Only used at Open, where those errors can bypass busy_timeout.
func retryBusy(ctx context.Context, deadline time.Time, f func() error) error {
	wait := 5 * time.Millisecond
	for {
		err := f()
		if !isBusy(err) || time.Now().After(deadline) {
			return err
		}
		select {
		case <-ctx.Done():
			return err
		case <-time.After(wait):
		}
		wait = min(2*wait, 100*time.Millisecond)
	}
}

func isBusy(err error) bool {
	var se *msqlite.Error
	if !errors.As(err, &se) {
		return false
	}
	c := se.Code() & 0xff   // primary result code
	return c == 5 || c == 6 // SQLITE_BUSY, SQLITE_LOCKED
}

// latestMigration is the newest embedded migration, computed once (the embedded files never
// change at run time).
var latestMigration = sync.OnceValues(func() (int, error) {
	ms, err := sqlcore.LoadMigrations(migrationsFS, "migrations")
	if err != nil {
		return 0, err
	}
	return sqlcore.LatestMigration(ms), nil
})

// CheckHealth implements store.HealthChecker: a ping of the read pool and the applied schema
// version against the newest embedded migration.
func (s *Store) CheckHealth(ctx context.Context) (store.StoreHealth, error) {
	want, err := latestMigration()
	if err != nil {
		return store.StoreHealth{}, err
	}
	return s.Store.CheckHealth(ctx, want)
}

// Migrate applies pending embedded migrations in one BEGIN IMMEDIATE transaction (which
// serialises concurrent migrators). Safe to call on every start-up.
func (s *Store) Migrate(ctx context.Context) error {
	ms, err := sqlcore.LoadMigrations(migrationsFS, "migrations")
	if err != nil {
		return err
	}
	_, err = sqlcore.Migrate(ctx, s.w, dialect, ms)
	return err
}

// WriteDB returns the single-connection writer pool; ReadDB the read-only pool.
func (s *Store) WriteDB() *sql.DB { return s.w }
func (s *Store) ReadDB() *sql.DB  { return s.r }

// Close closes both pools.
func (s *Store) Close() error {
	return errors.Join(s.r.Close(), s.w.Close())
}
