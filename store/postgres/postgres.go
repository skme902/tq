// Adapted from temporalio/temporal v1.32.1 af08a5e5, common/persistence/sql/sqlplugin/postgresql
// (plugin.go, task_v2.go, task_queues.go) and common/persistence/sql/task_v2.go.
// Copyright (c) 2025 Temporal Technologies Inc. MIT License; see UPSTREAM_LICENSE and NOTICE.

// Package postgres is a durable store.Store on PostgreSQL (12+), using pgx v5 through
// database/sql.
//
//	st, err := postgres.Open(ctx, "postgres://user@host:5432/db", postgres.Options{})
//	if err != nil { ... }
//	defer st.Close()
//	if err := st.Migrate(ctx); err != nil { ... } // explicit; creates/updates tq_* tables
//
// Ownership fencing works like upstream: every write that must be fenced runs in a
// transaction that first takes "SELECT range_id ... FOR UPDATE" on the queue row and
// compares the range id.
package postgres

import (
	"context"
	"database/sql"
	"embed"
	"errors"
	"io/fs"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/stdlib"

	"github.com/skme902/tq/store"
	"github.com/skme902/tq/store/internal/sqlcore"
)

//go:embed migrations/*.sql
var migrationsFS embed.FS

// Migrations returns the embedded migration files (for use with external tooling).
func Migrations() fs.FS { sub, _ := fs.Sub(migrationsFS, "migrations"); return sub }

// Options configures Open. Zero values pick the defaults noted.
type Options struct {
	// MaxConns caps open connections (default 16). The engine runs one writer goroutine
	// per queue plus readers and ack-level updaters, so a few connections per active
	// queue partition is plenty.
	MaxConns int
	// MaxIdleConns (default MaxConns).
	MaxIdleConns int
	// ConnMaxLifetime (default 30m).
	ConnMaxLifetime time.Duration
}

var dialect = sqlcore.Dialect{
	Name:                 "postgres",
	NumberedPlaceholders: true,
	LockSuffix:           " FOR UPDATE",
	// Arbitrary constant key: 0x74716d696772 ("tqmigr").
	MigrationLockSQL: "SELECT pg_advisory_xact_lock(128030515750770)",
	// 7 bind params per row; Postgres allows 65535.
	MaxRowsPerInsert: 1000,
	RowRef:           "ctid",
	Classify:         classify,
}

// Store is a store.Store backed by PostgreSQL.
type Store struct {
	*sqlcore.Store
	db     *sql.DB
	closer bool
}

var _ store.Store = (*Store)(nil)

// Open connects using a pgx DSN/URL. It does not create tables; call Migrate.
func Open(ctx context.Context, dsn string, opts Options) (*Store, error) {
	cfg, err := pgx.ParseConfig(dsn)
	if err != nil {
		return nil, err
	}
	db := stdlib.OpenDB(*cfg)
	maxConns := opts.MaxConns
	if maxConns <= 0 {
		maxConns = 16
	}
	db.SetMaxOpenConns(maxConns)
	if opts.MaxIdleConns > 0 {
		db.SetMaxIdleConns(opts.MaxIdleConns)
	} else {
		db.SetMaxIdleConns(maxConns)
	}
	if opts.ConnMaxLifetime > 0 {
		db.SetConnMaxLifetime(opts.ConnMaxLifetime)
	} else {
		db.SetConnMaxLifetime(30 * time.Minute)
	}
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, err
	}
	s := New(db)
	s.closer = true
	return s, nil
}

// New wraps an existing *sql.DB opened with the pgx driver ("pgx" / stdlib). The caller
// keeps ownership of db; Close on the returned Store does not close it.
func New(db *sql.DB) *Store {
	return &Store{Store: sqlcore.New(db, db, dialect), db: db}
}

// Migrate applies pending embedded migrations in one transaction, serialised across
// processes with a Postgres advisory lock. Safe to call on every start-up.
func (s *Store) Migrate(ctx context.Context) error {
	_, err := Migrate(ctx, s.db)
	return err
}

// Migrate applies pending embedded migrations to db and returns the versions applied.
func Migrate(ctx context.Context, db *sql.DB) ([]int, error) {
	ms, err := sqlcore.LoadMigrations(migrationsFS, "migrations")
	if err != nil {
		return nil, err
	}
	return sqlcore.Migrate(ctx, db, dialect, ms)
}

// DB returns the underlying pool.
func (s *Store) DB() *sql.DB { return s.db }

// Close closes the pool if Open created it.
func (s *Store) Close() error {
	if s.closer {
		return s.db.Close()
	}
	return nil
}

// classify maps Postgres "insufficient resources" errors (class 53, e.g. too_many_connections)
// to ResourceExhausted so readers back off longer; everything else falls back to Transient.
func classify(err error) error {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && len(pgErr.Code) == 5 && pgErr.Code[:2] == "53" {
		return &store.ResourceExhaustedError{Msg: pgErr.Error()}
	}
	return nil
}
