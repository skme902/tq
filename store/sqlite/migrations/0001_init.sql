-- Adapted from temporalio/temporal v1.32.1 af08a5e5,
-- schema/sqlite/v3/temporal/versioned/v0.10/tasks_v2.sql (tasks_v2, task_queues_v2).
-- Copyright (c) 2025 Temporal Technologies Inc. MIT License; see UPSTREAM_LICENSE and NOTICE.
--
-- Differences from upstream: tables are prefixed tq_, the subqueue is an explicit column
-- instead of being folded into task_queue_id, and tq_tasks is WITHOUT ROWID so rows are
-- stored clustered by the primary key (one b-tree instead of table + index).

CREATE TABLE tq_tasks (
  range_hash INT NOT NULL,
  task_queue_id BLOB NOT NULL,
  subqueue INT NOT NULL,
  pass BIGINT NOT NULL,
  task_id BIGINT NOT NULL,
  data BLOB NOT NULL,
  data_encoding VARCHAR(16) NOT NULL,
  PRIMARY KEY (range_hash, task_queue_id, subqueue, pass, task_id)
) WITHOUT ROWID;

CREATE TABLE tq_task_queues (
  range_hash INT NOT NULL,
  task_queue_id BLOB NOT NULL,
  range_id BIGINT NOT NULL,
  data BLOB NOT NULL,
  data_encoding VARCHAR(16) NOT NULL,
  PRIMARY KEY (range_hash, task_queue_id)
) WITHOUT ROWID;
