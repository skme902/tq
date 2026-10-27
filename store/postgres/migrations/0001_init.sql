-- Adapted from temporalio/temporal v1.32.1 af08a5e5,
-- schema/postgresql/v12/temporal/versioned/v1.18/tasks_v2.sql (tasks_v2, task_queues_v2).
-- Copyright (c) 2025 Temporal Technologies Inc. MIT License; see UPSTREAM_LICENSE and NOTICE.
--
-- Differences from upstream: tables are prefixed tq_ so they cannot collide with a Temporal
-- schema in the same database, and the subqueue is an explicit column instead of being
-- folded into task_queue_id (so DeleteQueue can remove every subqueue's tasks).

-- Fair (stride-scheduled) tasks. (pass, task_id) is monotonically increasing per subqueue.
CREATE TABLE tq_tasks (
  range_hash BIGINT NOT NULL,
  task_queue_id BYTEA NOT NULL,
  subqueue INTEGER NOT NULL,
  pass BIGINT NOT NULL,
  task_id BIGINT NOT NULL,
  data BYTEA NOT NULL,
  data_encoding VARCHAR(16) NOT NULL,
  PRIMARY KEY (range_hash, task_queue_id, subqueue, pass, task_id)
);

-- Queue metadata: range id (ownership fence), ack levels, backlog counts, TopK counts.
CREATE TABLE tq_task_queues (
  range_hash BIGINT NOT NULL,
  task_queue_id BYTEA NOT NULL,
  range_id BIGINT NOT NULL,
  data BYTEA NOT NULL,
  data_encoding VARCHAR(16) NOT NULL,
  PRIMARY KEY (range_hash, task_queue_id)
);
