# Operations gaps: what tq needs for running queues in production

Status as of `feat/observability` (metrics with queue tags, `Engine.DescribeQueue`, `Engine.ListQueues`). This is a
gap analysis only; nothing here is implemented. Each section says what exists in the code today, what is missing,
a priority, an effort estimate and a suggested design.

- **Priority.** P0: an operator cannot run the queue safely without it. P1: needed for routine operations at scale.
  P2: quality of life, or needed only by some deployments.
- **Effort.** S: up to 2 days. M: about a week. L: more than a week, or needs a schema change and a migration.

Reference points:

- **Upstream Temporal (v1.32.1).** `DescribeTaskQueue` (enhanced stats, pollers, per-priority stats). The admin APIs
  `DescribeTaskQueuePartition`, `ForceUnloadTaskQueuePartition` and `ListTaskQueuePartitions`. `tdbg taskqueue
  list-tasks`. `UpdateTaskQueueConfig` (queue and per-fairness-key rate limits with `reason`, `update_identity` and
  `update_time` stored in task-queue user data, which is the audit trail). History DLQ v2 (`tdbg dlq read/purge/merge`).
  gRPC health checks. Versioned SQL schema with `temporal-sql-tool`. Shipped Grafana dashboards. OpenTelemetry
  tracing interceptors.
- **SQS.** Visibility timeout, `maxReceiveCount` redrive to a DLQ and DLQ redrive back to the source, `PurgeQueue`,
  `ApproximateAgeOfOldestMessage` in CloudWatch, delay queues and per-message `DelaySeconds`, retention (1 min–14
  days), IAM and SSE.
- **RabbitMQ management.** HTTP API, UI and `rabbitmqctl`/`rabbitmqadmin`: list and browse queues, get messages
  (requeue or not), purge, delete, move with shovels, policies. Quorum-queue `delivery-limit` with dead-letter
  exchanges. Prometheus plugin plus official Grafana dashboards. Health-check endpoints. Graceful drain via
  maintenance mode.
- **River (Postgres).** `max_attempts`, which ends in the `discarded` state. `JobCancel`, `JobRetry`, `JobDelete`.
  `QueuePause`/`QueueResume`. Job cleaner retention (completed 24 h, cancelled and discarded 7 d by default). `river
  migrate-up/down` CLI with versioned migrations. Leader election table. River UI. OpenTelemetry middleware.
- **Asynq (Redis).** `Inspector` API with list, delete and run of pending, scheduled, retry and archived tasks.
  `PauseQueue`/`UnpauseQueue`. `MaxRetry`, which ends in `archived` (the DLQ). Retention. Asynqmon UI. Prometheus
  exporter. `Shutdown` with a `ShutdownTimeout`.
- **DBOS (Postgres).** Queues with concurrency and rate limits. Workflow management: list, cancel, resume, fork
  from step. Conductor UI. Built-in OpenTelemetry tracing. Versioned system-database migrations.

## Summary

| # | Gap | Priority | Effort |
|---|---|---|---|
| 1 | Max attempts, poison tasks and a DLQ (with redrive) — **done** (feat/ops-dlq) | P0 | M–L |
| 2 | Task admin: peek and browse, delete one task, purge a queue | P0 | M |
| 3 | Graceful drain on shutdown (finish in-flight leases before Stop) — **done** (feat/ops-drain) | P0 | S |
| 4 | Retry backoff and delayed visibility on Nack | P1 | M |
| 5 | Pause and resume of a queue or a fairness key | P1 | M |
| 6 | Shared, persisted runtime config with an audit trail | P1 | M–L |
| 7 | Health and readiness checks | P1 | S |
| 8 | Retention and GC of idle queue rows and expired tasks | P1 | S–M |
| 9 | Dashboards and alert rules (Grafana JSON, Prometheus rules) | P1 | S |
| 10 | Admin HTTP endpoint and CLI | P1 | M |
| 11 | Multi-instance ownership visibility and force-unload | P1 | S–M |
| 12 | OpenTelemetry tracing and task headers | P2 | M |
| 13 | Structured logging conventions | P2 | S |
| 14 | Schema versioning checks and rollback story | P2 | S |
| 15 | Backup and restore runbook | P2 | S |
| 16 | Move or requeue tasks between queues, and drain a queue | P2 | M |
| 17 | Security (least-privilege roles, payload codec, admin auth) | P2 | S–M |
| 18 | Capacity planning guide | P2 | S |

The top eight are 1–8. Items 2, 5, 10 and 16 share one admin API surface, so build them as one design.

## 1. Max attempts, poison tasks and a DLQ — P0, M–L — done

**Status: implemented in feat/ops-dlq** (README §Dead letters). Refinements to the design below:
`Attempts` lives in `store.Task` (the JSON blob, no column) and counts deliveries started, persisted with a
fenced in-place `UpdateTask` on each backlog delivery when a limit is set, rather than a rewrite with a new ID
(which would move the task in fair order). `Nack(NackReason(s))` records a string, not an error. `PurgeDeadTasks`
takes a dead-before time; `DeleteDeadTasks` deletes by ID. No `dlq_backlog_count` gauge:
`DescribeQueue().DeadTasks` reads the count.


- **Today.**
  - `Delivery.Nack`, lease expiry, and a Starter that returns a retryable error all redeliver the task. This is
    `redeliver` → `task.finish(err)` for backlog tasks, or a re-spool for sync tasks.
  - Nothing counts attempts: `store.Task` has `Payload`, `Priority`, `CreateTime` and `ExpiryTime` only.
  - A task that always fails, or that crashes its consumer, is delivered forever and blocks its fairness key's share.
  - The only exits are `ExpiryTime` (dropped with `tasks_dropped{reason=expired_read|expired_memory}`) and `Starter` returning
    `ErrDropTask`, which drops the task (`tasks_dropped{reason=starter}`) without keeping it anywhere.
  - Upstream has no task-level attempt count either, because history owns retries. That is why it is missing here.
- **Missing.**
  - An attempt counter.
  - A per-queue `MaxAttempts`.
  - A dead-letter destination that keeps the payload and the last error.
  - Listing, redriving and purging dead tasks.
  - Metrics: `tasks_dead_lettered`, and a `dlq_backlog_count` gauge.
- **Design.**
  - Add `Attempt int32` to `store.TaskInfo`, persisted.
  - Increment it on redelivery. For a backlog task, the reader rewrites the row with attempt+1, the same path as
    `task_rewrites`. Sync tasks start at 1 when spooled.
  - Expose `Delivery.Attempt`.
  - Add `QueueConfig.MaxAttempts` (0 = unlimited, today's behavior). When it is exceeded, move the task in one
    transaction to the subqueue-equivalent table `tq_dead_tasks` (key, task id, payload, attempt, last error,
    dead_at), and delete it from the backlog under the same rangeID fence.
  - Add `Nack(WithError(err))` so the DLQ row records why.
  - Add `Engine.ListDeadTasks(queue, page)`, `RedriveDeadTasks(queue, filter, max)` (re-add with attempt reset, like
    SQS redrive) and `PurgeDeadTasks(queue, olderThan)`.
  - Migration 0002.
  - Upstream reference: the history DLQ v2 API shape (read, purge, merge) for the admin verbs.

## 2. Task admin: peek and browse, delete, purge — P0, M

- **Today.**
  - `ListQueues` and `DescribeQueue` give counts and ages.
  - `store.GetTasks` can read a range of one subqueue, but nothing exposes it at the Engine level.
  - `store.DeleteQueue` deletes a queue and all its tasks, but calling it under a loaded queue races the owner. No
    Engine method fences it.
  - There is no way to delete one bad task or purge a backlog without SQL.
- **Missing.** `PeekTasks` (read-only, paged, per subqueue/priority, optionally filtered by fairness key),
  `DeleteTask(queue, id)`, and `PurgeQueue(queue)`. Upstream has `tdbg taskqueue list-tasks`, SQS `PurgeQueue`,
  RabbitMQ get-messages and purge, and Asynq `Inspector.DeleteTask`.
- **Design.**
  - **Peek.** It follows the same read-only path as `describeUnloaded`: `GetTasks` from the ack level with a page
    token of (subqueue, level). It never takes ownership, works on loaded and unloaded queues, and returns
    `TaskInfo` plus attempt and lease state when the queue is loaded here.
  - **Mutations.** They go through the owner so the fencing stays correct. On the owning engine, delete/purge loads
    the queue and then:
    - Delete: removes the task from the reader's loaded set if present, deletes the row conditioned on rangeID,
      and adjusts the backlog count.
    - Purge: `CompleteTasksLessThan(max level)` on every subqueue, sets the ack level to the max read level, and
      resets the counts and TopK.
  - **Store.** Add `DeleteTask(queue, subqueue, level, rangeID)` to the store as a new optional interface with
    conformance tests.
  - **Leases.** In-flight leases on purged tasks become lost (`ErrLeaseLost` on Ack, harmless).

## 3. Graceful drain on shutdown — P0, S — done

**Status: implemented in feat/ops-drain** as `Engine.Shutdown(ctx)` / `Engine.Draining()` / `ErrEngineDraining`
(README §Graceful shutdown). Refinement: there is no separate `Drain` that leaves the engine running; Shutdown
drains then stops. Readiness is wired in with item 7.


- **Today.**
  - `Engine.Stop` ends every lease immediately with `ErrEngineStopped`. It re-spools sync-matched tasks and leaves
    backlog tasks durable, so nothing is lost.
  - But every in-flight task is redelivered after restart, so a deploy causes a duplicate wave of up to
    (consumers × in-flight) tasks.
  - Stop does not wait for consumers, and polls in progress return errors.
- **Missing.** `Drain(ctx)`/`Shutdown(ctx)`:
  - stop accepting new polls (they return `ErrEngineDraining`)
  - stop sync matching, so AddTask spools
  - let leases finish until a deadline, then Stop
  - expose readiness = false while draining (see 7)
  - Asynq `Shutdown` with `ShutdownTimeout`, River `StopAndCancel`/`Stop(ctx)` and RabbitMQ maintenance mode are the
    models.
- **Design.**
  - Add a `draining` flag checked in `PollTask` and the matcher's sync path.
  - Wait on `len(e.leases)==0` or `ctx.Done()`, then call the existing Stop.
  - Add a metric `engine_draining` gauge and a log line with the counts that remain.

## 4. Retry backoff and delayed visibility — P1, M

- **Today.**
  - A Nacked or expired backlog task is matchable again immediately.
  - There is no `DelaySeconds`/scheduled-at.
  - The only delay is the consumer holding the lease longer and then Nacking, which ties up a lease slot.
- **Missing.** `Nack(WithDelay(d))`, a per-queue retry policy (exponential with a cap), and `Task.VisibleAt` for
  delayed enqueue (SQS delay queues, Asynq `ProcessIn`/`ProcessAt`, River `ScheduledAt`).
- **Design.**
  - Add `VisibleAt` to the task row.
  - The reader skips (and does not count as loaded) tasks whose `VisibleAt` is in the future, and parks them in a
    timer wheel keyed by `VisibleAt`. That is the same structure the lease timers already use, on the injected
    clock.
  - For a Nack with delay, rewrite the row with the new `VisibleAt` and attempt+1 (shares the rewrite from 1).
  - Interaction with fairness: a delayed task keeps its pass. Document that it can come back "early" relative to
    newer tasks of the same key.

## 5. Pause and resume a queue or a fairness key — P1, M

- **Today.**
  - A queue rate limit ≤ 1e-9 never dispatches, so `SetQueueRateLimit(q, 0)` would pause a queue. But 0 is defined
    as unlimited in `QueueConfig` (PARITY_REPORT §3 item 9), so the working value is a tiny positive rate. That is a
    hack and it is invisible in DescribeQueue.
  - There is no per-key pause.
- **Missing.** Explicit `PauseQueue`/`ResumeQueue` (River, Asynq, Temporal activity pause) and `PauseFairnessKey`.
- **Design.**
  - Add `QueueConfig.Paused bool` and `PausedKeys` to the config source (persisted per 6).
  - A paused queue still accepts AddTask (it spools, with no sync match) and blocks dispatch in the matcher, the same
    hook as the rate limiter, so waiting pollers keep waiting.
  - A paused key is a per-key limiter that never admits. This reuses `FairnessKeyRateLimit` plumbing with an infinite
    interval, so other keys are not blocked.
  - Surface `Paused` in DescribeQueue and add a `queue_paused` gauge.

## 6. Shared, persisted runtime config with an audit trail — P1, M–L

- **Today.**
  - `QueueConfigSource` is an interface.
  - The bundled `MemoryQueueConfigSource` is per process: two engines sharing a database can disagree, changes are
    lost on restart, and nothing records who changed what.
  - Live changes are pushed through `QueueConfigWatcher`.
- **Missing.** A store-backed config source with change history. Upstream `UpdateTaskQueueConfig` stores the rate
  limits in task-queue user data with `reason`, `update_identity` and `update_time`.
- **Design.**
  - Add tables `tq_queue_config (key, version, config JSON, updated_by, reason, updated_at)` and an append-only
    `tq_queue_config_history`.
  - Add `StoreQueueConfigSource` implementing `QueueConfigWatcher`. It polls `version` every N seconds, or uses
    `LISTEN/NOTIFY` on Postgres, and pushes changes to loaded queues.
  - Every setter takes `(identity, reason)`.
  - Expose the history through the admin API (10).
  - Keep `MemoryQueueConfigSource` for tests.

## 7. Health and readiness — P1, S

- **Today.** There is no health API. Signals are spread over `LoadedQueues`, `fair_reader_stuck_detected`,
  `lease_failures` and the store's own errors.
- **Missing.**
  - liveness: the engine is not stopped
  - readiness: store reachable within a timeout, schema version current, not draining
  - a degraded flag (queues whose reader is stuck or whose lease renewal is failing)
  - Upstream has gRPC health. RabbitMQ has `rabbitmq-diagnostics check_*`.
- **Design.**
  - Add `Engine.Health(ctx) HealthReport{Ready bool, Draining bool, Store error, SchemaVersion int, Degraded
    []QueueProblem}`.
  - `Store` gains an optional `Pinger` (Postgres/SQLite `PingContext` plus a `tq_schema_version` read).
  - `QueueProblem` comes from flags the managers already compute (reader stuck, last lease failure, a
    `condition_failed` storm).
  - Add an `http.Handler` with `/livez` and `/readyz` in an `admin` subpackage (10).

## 8. Retention and GC — P1, S–M

- **Today.**
  - Acked tasks are deleted by ack-level GC (`maybeGC`, batch deletes).
  - Expired tasks are dropped when read, and their rows go with the ack level.
  - Queue rows (`tq_task_queues`) are never deleted: `persistenceTTL` is 0, so `QueueInfo.ExpiryTime` is always
    zero, and nothing sweeps.
  - A system with many short-lived queue names grows the table forever and `ListQueues` gets slower. Expired tasks
    behind a stuck ack level also stay.
- **Missing.**
  - A sweeper for idle, empty queue rows: no tasks, not loaded, `LastUpdateTime` older than a TTL. Upstream relies on
    Cassandra TTL and a scavenger.
  - Optional max backlog age, after which tasks are dropped or dead-lettered (SQS retention).
  - DLQ retention (1).
- **Design.**
  - Add `Engine.SweepIdleQueues(ctx, olderThan, max)`. It pages `ListQueues`, and for each candidate calls
    `DeleteQueue` conditioned on the listed rangeID and an all-zero backlog. That needs a conditional delete in the
    store, a new optional method `DeleteQueueIfIdle(key, rangeID)`.
  - Run it from a routine, or one elected engine (see 11).
  - Per-queue `MaxTaskAge` checked by the reader on load, the same as `ExpiryTime`.

## 9. Dashboards and alerts — P1, S

- **Today.** Upstream-named metrics with `namespace`/`taskqueue`/`partition` tags, backlog count and age per queue
  and priority, dispatch, ack and write latencies, rate limiting, and lease failures (README §Observability).
  There are no dashboards or alert rules.
- **Missing.** A Grafana dashboard JSON and Prometheus alert rules. Upstream ships
  `develop/docker-compose/grafana/provisioning/temporalio-dashboards`. RabbitMQ ships official dashboards.
- **Design.**
  - Add `docs/grafana/tq.json` with these rows:
    - backlog count and age by queue, plus a top-10 table
    - add vs dispatch rate
    - sync-match ratio (`sync_match / tasks_added`)
    - dispatch latency p50/p99 by source
    - ack latency
    - lease expired/nack rate
    - rate limited
    - write latency and throttle
    - `condition_failed_errors`, `lease_failures`, `task_queue_stopped` by cause
  - Add alert rules in `docs/alerts.yaml`:
    - `approximate_backlog_age_seconds > X for 10m`
    - backlog growing with dispatch ≈ 0 (stuck consumers)
    - `fair_reader_stuck_detected > 0`
    - `lease_failures` rate > 0 (ownership fight)
    - `delivery_lease_expired / delivery_acked > 5%`
    - DLQ non-empty, once 1 exists
  - Test that every metric named in the JSON exists in `metrics.go`, so the JSON cannot drift.

## 10. Admin HTTP endpoint and CLI — P1, M

- **Today.** Library calls only (`DescribeQueue`, `ListQueues`, `RateLimit`, `QueueStatus`, the config source
  setters).
- **Missing.** A mountable admin surface. RabbitMQ management API, Asynqmon/River UI, `tdbg`/`temporal
  task-queue`.
- **Design.**
  - `tq/admin` returns an `http.Handler` for the user to mount behind their own auth, with JSON endpoints:
    - `GET /queues`, `GET /queues/{ns}/{name}` (describe)
    - `GET …/tasks` (peek), `DELETE …/tasks/{id}`, `POST …/purge`
    - `POST …/pause|resume`
    - `GET|PUT …/config`
    - `GET …/dlq`, `POST …/dlq/redrive`
    - `/livez`, `/readyz`
  - A small `cmd/tqctl` that either talks to that endpoint or opens the store directly for read-only commands
    (`list`, `describe`, `peek`), which work without a running engine thanks to the unloaded paths.
  - Mutating commands require the endpoint, so they go through the owning engine's fencing.

## 11. Multi-instance ownership visibility — P1, S–M

- **Today.**
  - Ownership is the rangeID fence: a loading engine bumps it, and the previous owner's next write fails and it
    unloads (`task_queue_stopped{cause=...}`, `lease_failures`).
  - There is no record of *which* engine owns a queue, and no routing. If two engines both serve one queue, they
    ping-pong ownership with every load. That shows up only as `condition_failed_errors` and `lease_failures`.
  - `ListQueues` reports `Loaded` only for the calling engine.
- **Missing.**
  - an owner identity and heartbeat per queue row
  - an "owned by" column in list and describe
  - a `ForceUnload` admin action (upstream `ForceUnloadTaskQueuePartition`)
  - a guide on routing queues to engines
- **Design.**
  - Add `Options.Identity` (default hostname:pid) and write it plus `owner_since` with every `UpdateQueue`. That is
    two nullable columns, so the migration is cheap.
  - `DescribeQueue`/`ListQueues` return `Owner` and `OwnerSince`. A queue whose `LastUpdateTime` is older than 2×
    `UpdateAckInterval` while `Owner` is set is shown as "stale owner".
  - Add `Engine.ForceUnload(queue)` locally. A remote force-unload is just "load it here", since the fence evicts
    the other owner.
  - Add a `queue_ownership_changes` counter for alerting on ping-pong.

## 12. OpenTelemetry tracing — P2, M

- **Today.** No tracing. `store.Task` has no headers field, so trace context can travel only inside `Payload`.
- **Missing.** Spans for AddTask (sync vs spool), the backlog write, PollTask wait, and delivery → ack, with
  propagation from producer to consumer. River, DBOS and Temporal interceptors all do this.
- **Design.**
  - Add `Task.Headers map[string]string`, persisted (JSON rows make this cheap).
  - Add an `Options.Tracer` hook interface so the root module takes no OTel dependency. A `tq/otel` subpackage
    implements it.
  - It injects on AddTask, extracts on delivery, and links the consumer span to the producer span.
  - Span attributes are the same tags as the metrics. Never the fairness key unless the user opts in.

## 13. Structured logging — P2, S

- **Today.** `Options.Logger` (`*slog.Logger`, default discard). Upstream-ported log sites (reader errors, corrupt
  rows at Error, unload causes).
- **Missing.**
  - consistent attributes (`namespace`, `taskqueue`, `partition`, `task_id`, `range_id`) on every queue-scoped line
  - documented levels, plus rate limiting for hot error loops (for example a reader retrying a corrupt row forever,
    PARITY_REPORT §3 item 21)
- **Design.**
  - Each partition manager holds `logger.With(queue attrs)`, mirroring the metrics handler.
  - Add an `internal/logthrottle` wrapper (once per N seconds per key) for retry loops.
  - Document the level policy in the README.

## 14. Schema versioning — P2, S

- **Today.**
  - Embedded forward-only migrations (`0001_init.sql`) applied by an explicit `Migrate`. It is serialized and
    transactional, and records `tq_schema_version`.
  - The engine does not check the schema version at start.
- **Missing.**
  - a startup check (refuse to run against a schema older or newer than the binary understands)
  - a way for DBAs to get the SQL without running Go (`tqctl migrate print`)
  - a documented expand/contract policy so that rolling deploys can run two binary versions at once
  - River and DBOS also ship down migrations; forward-only plus expand/contract is the safer default.
- **Design.**
  - Add `MinSchemaVersion`/`MaxSchemaVersion` constants. `Open` reads `tq_schema_version` and fails with a typed
    error.
  - Every future migration is additive first, with a cleanup migration in a later release.

## 15. Backup and restore — P2, S (docs)

- **Today.** All state is in `tq_tasks` and `tq_task_queues` (Postgres or SQLite WAL), so a normal database
  backup captures it.
- **Missing.** A runbook, and the restore semantics:
  - restoring an older snapshot brings back tasks that were acked after it, so they are redelivered (at-least-once
    holds, but consumers must be idempotent)
  - rangeIDs go backwards, which is safe because every engine bumps on load, but engines must be restarted, not left
    running across a restore
  - SQLite needs `VACUUM INTO` or the backup API, not a file copy while running
- **Design.** Document it, plus an `ExportQueue`/`ImportQueue` pair (JSON lines of `TaskInfo`) built on peek (2)
  for moving one queue between databases.

## 16. Move or requeue, and drain a queue — P2, M

- **Today.** Nothing moves tasks between queues.
- **Missing.**
  - moving tasks from queue A to B, for example after a rename or from the DLQ (RabbitMQ shovel, SQS redrive)
  - draining a queue: stop adds, wait for empty, then delete
- **Design.**
  - `MoveTasks(src, dst, filter, max)`: read from src through the owner, `CreateTasks` on dst, then delete from src.
    It is at-least-once across two fences (a crash in between duplicates rather than loses), which matches the
    library's guarantee.
  - Queue drain = pause adds (a `RejectAdds` flag next to `Paused` in 5) plus a wait on `DescribeQueue` backlog = 0.

## 17. Security — P2, S–M

- **Today.**
  - A library, so authentication is the host application's.
  - SQL is parameterized throughout.
  - Credentials come from the DSN or `*sql.DB` the user passes.
  - Payloads are stored as given, in plaintext JSON rows.
- **Missing.**
  - a documented least-privilege Postgres role (DML on `tq_*`; DDL only for the migration role)
  - an optional payload codec hook for encryption at rest (Temporal DataConverter/codec server)
  - an auth hook on the admin handler (10)
  - guidance that fairness keys and queue names show up in metrics and logs (queue names do; fairness keys do not)
- **Design.**
  - `Options.PayloadCodec{Encode, Decode}` applied at the store boundary.
  - The admin handler takes an `Authorize(r *http.Request, action string, queue QueueKey) error` callback.
  - Document the two DB roles.

## 18. Capacity planning — P2, S (docs)

- **Today.** `BenchmarkEngine`/`BenchmarkStoreCommit` report tasks/s, rows written and read per task, rows per
  commit and sync % per store (README §Testing). The parity probes measure rate-limit behavior.
- **Missing.** A sizing guide:
  - bytes per backlog row
  - expected write amplification (backlog path: 1 insert + 1 range delete per task; sync path: none)
  - Postgres connection-pool sizing against consumers
  - the effect of `GetTasksBatchSize`, `UpdateAckInterval` and `MaxTaskDeleteBatchSize`
  - how many loaded queues one engine handles (memory per loaded queue: reader buffer up to `GetTasksBatchSize`
    tasks, matcher, TopK counter)
  - when to shard queues across engines
- **Design.** A `docs/CAPACITY.md` built from benchmark runs, plus a `BenchmarkManyQueues` (N loaded queues, idle
  and active) to measure per-queue memory and goroutines.
