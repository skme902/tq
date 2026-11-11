-- Dead-lettered tasks (library addition, no upstream table): tasks whose deliveries reached
-- the queue's max attempts. Keyed by the original task ID; DeleteQueue removes them with the
-- queue. dead_time is Unix nanoseconds.
CREATE TABLE tq_dead_tasks (
  range_hash BIGINT NOT NULL,
  task_queue_id BYTEA NOT NULL,
  task_id BIGINT NOT NULL,
  pass BIGINT NOT NULL,
  dead_time BIGINT NOT NULL,
  reason TEXT NOT NULL,
  data BYTEA NOT NULL,
  data_encoding VARCHAR(16) NOT NULL,
  PRIMARY KEY (range_hash, task_queue_id, task_id)
);
