-- Persisted per-queue runtime config (library addition, no upstream table; upstream keeps
-- TaskQueueConfig in task queue user data). tq_queue_config holds the current version per
-- queue key; tq_queue_config_history is append-only and its seq is the global change
-- sequence that config sources poll (SQLite has one writer, so seq order is commit order).
-- update_time is Unix nanoseconds.
CREATE TABLE tq_queue_config (
  namespace TEXT NOT NULL,
  name TEXT NOT NULL,
  partition INT NOT NULL,
  version BIGINT NOT NULL,
  seq BIGINT NOT NULL,
  config BLOB NOT NULL,
  updated_by TEXT NOT NULL,
  reason TEXT NOT NULL,
  update_time BIGINT NOT NULL,
  PRIMARY KEY (namespace, name, partition)
) WITHOUT ROWID;

CREATE TABLE tq_queue_config_history (
  seq INTEGER PRIMARY KEY AUTOINCREMENT,
  namespace TEXT NOT NULL,
  name TEXT NOT NULL,
  partition INT NOT NULL,
  version BIGINT NOT NULL,
  config BLOB NOT NULL,
  updated_by TEXT NOT NULL,
  reason TEXT NOT NULL,
  update_time BIGINT NOT NULL
);

CREATE UNIQUE INDEX tq_queue_config_history_key ON tq_queue_config_history (namespace, name, partition, version);
