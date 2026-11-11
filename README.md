# tq

A standalone, embeddable task queue library ported from the matching service of
[temporalio/temporal](https://github.com/temporalio/temporal) v1.32.1 (`af08a5e5`), fair/priority
path. It is MIT licensed (`LICENSE`, copyright skme902); the ported Temporal code keeps its MIT attribution in `UPSTREAM_LICENSE` and `NOTICE`. Every ported file carries an attribution header.

Status (slice 6, final): a single partition; three stores (in-memory, PostgreSQL, SQLite); priority levels,
weighted fairness (the upstream fair-path backlog), rate limiting (the upstream simple-limiter path), long polling,
and delivery leases. Differential parity against upstream matching (in-process engine, SQLite persistence, 1
partition) is measured in `docs/PARITY_REPORT.md`: identical dispatch order where upstream is deterministic,
identical rate-limited throughput, no loss or duplication. Not implemented: partitions/forwarding, versioning,
queries/Nexus, sticky queues, hooks/poller scaling, describe; see the report's gap list.

## Usage

The package is named `taskqueue` (the module path ends in `tq`), so import it as
`taskqueue "github.com/skme902/tq"`.

```go
eng, _ := taskqueue.New(taskqueue.Options{Store: memstore.New()}) // Config zero values = upstream defaults
defer eng.Stop()

key := taskqueue.QueueKey{Namespace: "acme", Name: "emails"}
syncMatched, err := eng.AddTask(ctx, key, taskqueue.Task{
	Payload:  []byte("hello"),
	Priority: taskqueue.Priority{PriorityKey: 1}, // 1 (highest) .. 5; 0 = default (3); >5 runs as 5
})

d, err := eng.PollTask(ctx, key, taskqueue.PollOptions{Identity: "w1"}) // long-polls up to ~1m
if errors.Is(err, taskqueue.ErrNoTasks) { /* poll again */ }
// ... process d.Task.Payload ...
d.Ack()                       // done; the backlog ack level advances and rows are GCed
// d.Nack()                   // redeliver now
// d.ExtendLease(time.Minute) // still working (default lease: Config.DeliveryLeaseDuration = 1m)
```

Semantics:

- **Lazy queues.** A queue is loaded, and its lease taken (rangeID+1), on the first add or poll for its key. It is
  unloaded after `MaxTaskQueueIdleTime` (5m) with no adds or polls. Unloading is deferred while deliveries are leased.
- **Sync match vs. backlog.** `AddTask` hands the task directly to a waiting poller unless the in-memory
  backlog has a task older than `BacklogNegligibleAge` (5s). Otherwise it writes the task to the store, and
  `syncMatched=false`.
- **Ordering.** Tasks are served by effective priority first, then by fairness pass within a priority (see
  Fairness). With no fairness keys, that is FIFO within a priority.
- **At-least-once delivery.** If a lease expires or the task is nacked, it is redelivered. A sync-matched
  task whose lease expires is written to the backlog. While a lease is live, its task is not handed to another
  poller. A task is delivered again after its lease ends without an Ack: on expiry, on Nack, or when the engine
  stops or crashes, or the queue unloads, while the lease is held. So a consumer that is slower than its lease, or
  that is cut off by a stop or crash, can see the task twice.
- **Fencing.** Every write and delete is conditioned on the queue's rangeID. When the engine loses ownership,
  the queue unloads and the next call reloads it.
- **Optional hooks** (`Options`):
  - `Starter`: runs on each match. Returning `ErrDropTask` drops the task, `Retryable(err)` redelivers it,
    and any other error re-spools it.
  - `Validator`
  - `QueueConfigSource`
  - `Router` (a `PartitionRouter`; no-op for now)
  - `TimeSource` (clock)
  - `Metrics`
  - `Logger` (slog)

## Stores

The library ships exactly three `store.Store` implementations. All three pass `store/storetest`.

| Store | Use | Driver |
|---|---|---|
| `store/memstore` | tests, ephemeral queues | none |
| `store/postgres` | multi-process / multi-host | `github.com/jackc/pgx/v5` (stdlib adapter), same as upstream |
| `store/sqlite` | single host, embedded | `modernc.org/sqlite` (pure Go, no cgo), same as upstream |

```go
st, err := postgres.Open(ctx, "postgres://user@host/db?sslmode=disable", postgres.Options{}) // or postgres.New(*sql.DB)
// st, err := sqlite.Open(ctx, "/var/lib/app/queue.db", sqlite.Options{}) // Synchronous: sqlite.SyncFull (default) | SyncNormal
if err != nil { ... }
defer st.Close()
if err := st.Migrate(ctx); err != nil { ... } // explicit; idempotent; safe to run concurrently from many processes
eng, _ := taskqueue.New(taskqueue.Options{Store: st})
```

- `Open` never creates tables. Call `Migrate`. The embedded forward-only migrations live in
  `store/{postgres,sqlite}/migrations/NNNN_*.sql`, and `Migrations()` exposes them for external tools.
  Applied versions are recorded in `tq_schema_version`. Postgres serialises concurrent migrators with an advisory
  lock, and SQLite does it with `BEGIN IMMEDIATE`.
- The tables are `tq_tasks` and `tq_task_queues`, the upstream `tasks_v2`/`task_queues_v2` layout with a `tq_` prefix
  and an explicit `subqueue` column.
- Fencing works across processes: every write and delete runs in a transaction that locks the queue row
  (`SELECT … FOR UPDATE` on Postgres, the database write lock on SQLite) and checks `range_id`. Two engines on one
  database cannot both own a queue. The loser gets `ConditionFailed` and unloads the queue.
- SQLite runs in WAL mode with one writer connection (in-process writers queue instead of spinning on
  SQLITE_BUSY), a read-only reader pool, and `busy_timeout` (default 5s) for other processes. `SyncNormal` trades
  power-loss durability of the last commits for about 2.5× lower commit latency. In-memory SQLite is rejected; use memstore.


Fairness is always available; there is no `enableFairness` switch. Upstream has one because it migrates
queues between the classic and the fair backlog. This library only has the fair backlog. A task with no
`FairnessKey` uses the key `""` at weight 1, so a queue that never sets keys behaves exactly like priority-only
(FIFO per priority level).

```go
eng.AddTask(ctx, key, taskqueue.Task{
	Payload:  p,
	Priority: taskqueue.Priority{PriorityKey: 2, FairnessKey: "tenant-42", FairnessWeight: 5},
})
```

- **Weights.** Within one priority level, keys share dispatches in proportion to their weights. Each task gets a
  pass of `prev + inc` with `inc = max(1, int64(1000/weight))` (upstream `pickPasses`). Weight 0 or unset means 1,
  and weights below 0.001 are clamped to 0.001. A negative weight or a key longer than 64 bytes is rejected.
  Priority always dominates: a lower-priority task is never served before a higher-priority one.
- **New and idle keys** start at the queue's current ack level, so they don't jump ahead of the backlog with a
  stale low pass, and they don't wait behind a heavy key's long tail. With `Config.FairnessPassDither` (default
  false, as upstream) a new key starts at a random point in `[ackLevel, ackLevel+inc)`.
- **Overrides.** `QueueConfig.FairnessWeightOverrides` (from `Options.QueueConfigSource`) replaces the task's weight
  for a key. `NewMemoryQueueConfigSource(max)` offers an `UpdateFairnessWeightOverrides(queue, set, unset)` call
  that applies the upstream merge and validation rules (default max is 1000 keys, past which the update is
  rejected with `ErrFairnessOverridesUpdateRejected`). The source is read on every write batch, and the
  override is applied when a task's pass is computed. A change affects tasks written afterwards; tasks
  already in the backlog keep their pass (as upstream).
- **Counter.** Pass state is tracked per priority subqueue by upstream's hybrid counter: an exact map up
  to 100 keys, then a count-min sketch (upstream parameters, random seed per counter). You can tune it with
  `Config.FairnessCounter` (`DefaultCounterParams()`). The top 100 keys are persisted as TopK in queue metadata
  at most once per `UpdateAckInterval` and restored on load, so a heavy key keeps its place after unload or
  reload. A key outside the TopK, or one whose counts weren't persisted since the last sync, restarts from
  the ack level. Upstream behaves the same way.
- `Engine.WaitingPollers(queue)` and `Engine.MatcherTasks(queue)` report the pollers and tasks queued in the matcher.
  They're for tests and diagnostics.

## Rate limiting

Dispatch from the matcher is paced by upstream's GCRA-style limiter. A limit of rate `R` has an interval of
`1s/R` and a burst of 1s, so an idle queue can dispatch `R+1` tasks at once and then one every `1/R`. A rate of
`<= 1e-9` (including 0) never dispatches. There is no limit unless one is configured.

- **Queue rate.** The effective rate is `min(system, user)`.
  - `user` is the API rate (`QueueConfig.RateLimit`, set it with `MemoryQueueConfigSource.SetQueueRateLimit`).
    When no API rate is set, `user` is the last poller's `PollOptions.MaxTasksPerSecond`, then unlimited.
    `user` is multiplied by `Config.RateLimitFraction(queue)` (default 1) and divided by `Config.NumReadPartitions`.
  - `system` is `min(Config.AdminNamespaceToPartitionDispatchRate (10000), Config.AdminNamespaceTaskQueueToPartitionDispatchRate (1000))`, per partition.
    "Namespace" is `QueueKey.Namespace`. You can override these with `SetAdminNamespaceDispatchRate(ns, …)` or
    `SetAdminQueueDispatchRate(queue, …)`.
- **Per-key rate.** `QueueConfig.FairnessKeyRateLimitDefault` (set it with `SetFairnessKeyRateLimitDefault`) limits each
  fairness key separately. The interval is divided by the key's effective weight (with overrides), so weight 2
  gets twice the rate. Up to `Config.FairnessKeyRateLimitCacheSize` keys (default 2000, LRU) are tracked. A throttled
  key is skipped and doesn't block other keys. Without a per-key limit, a throttled queue stalls as a whole.
- **Live changes.** A `QueueConfigSource` that also implements `QueueConfigWatcher` (`MemoryQueueConfigSource` does)
  applies rate and weight-override changes to loaded queues immediately and re-evaluates waiting tasks. Other sources
  are read only when a queue loads (like upstream, writers use the queue's cached overrides).
- **Tokens.** A matched task that isn't used (the Starter returned `ErrDropTask` or another error) gives its token
  back. Forwarded tasks are not counted twice.
- **Upstream quirk (kept by default).** A new limit is installed only when the effective rate *changes*. A queue with
  no API or worker rate, or whose API rate is above an admin cap at load, therefore runs unlimited until something
  changes the effective rate. `Engine.RateLimit` still reports the system rate in this case. Set
  `Config.ApplySystemRateLimitOnLoad = true` to install the limit when the queue loads.
- `Engine.RateLimit(queue)` reports the effective rate (× read partitions) and its source (API, Worker, System).
  Throttled matches increment the `tasks_rate_limited` metric.

## Graceful shutdown

`Engine.Stop` ends every lease at once: backlog tasks are redelivered after the next load and
sync-matched tasks are spooled, but every in-flight task runs twice. `Stop` waits for an `Ack`,
`Nack` or lease expiry already in progress to finish its write before it unloads; a concurrent
`Stop` or `Shutdown` returns only once the first `Stop` has finished.
`Engine.Shutdown(ctx)` drains first:

```go
ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
defer cancel()
if err := e.Shutdown(ctx); err != nil {
	log.Printf("stopped with leases outstanding: %v", err) // ctx's error
}
```

- From the moment it is called, new `PollTask` calls return `ErrEngineDraining`, and so do polls
  that were waiting with no match. A poll that had already matched a task (for example inside
  `Starter`) can still return a `Delivery`; the drain waits for it. With no waiting pollers, `AddTask` cannot sync-match, so new tasks go to the
  backlog.
- `AddTask`, `Ack`, `Nack` and `ExtendLease` keep working. A nacked or expired backlog lease goes
  back to the backlog. A nacked or expired sync-matched lease is spooled to the backlog.
- It waits until every outstanding lease is settled and every poll that already passed the
  draining check has returned, then calls `Stop` and returns nil. If `ctx` ends first it returns
  that error, and `Stop` ends whatever is still outstanding. If `ctx` is done but the drain has
  finished (no leases left), it returns nil. Concurrent callers share one result: the caller that
  invokes `Stop` records nil or its own `ctx` error, and every other waiter returns that same
  error once the engine has stopped. A caller whose own `ctx` ends first is the one that stops.
  `Shutdown` after a direct `Stop` returns nil. The result is sticky: a later `Shutdown`, even
  with a fresh `ctx`, returns the first result. `Engine.Draining()` reports whether a drain has
  started, and stays true after it has stopped the engine.
- What a drain guarantees: no acknowledged task is lost. A clean drain hands out no lease it
  then abandons. When `ctx` ends first, a poll still inside `Starter` gets `ErrEngineStopped`
  instead of a lease and its sync-matched task goes back to `AddTask`, which spools it or, if the
  engine has already unloaded, returns an error to the producer. The same holds for an `AddTask`
  whose sync match failed in `Starter` during a clean drain. Producers must treat an `AddTask`
  error as "not enqueued" and retry (on another engine during shutdown).
- Metrics (no queue tags): `engine_draining` (1 while draining, 0 after),
  `engine_drain_leases_remaining` (outstanding leases sampled under the lease lock when Shutdown
  decides; 0 when the drain finishes), `engine_drain_duration`. The lease gauge and the duration
  are emitted once, by the call that stops the engine.

## Health

```go
http.Handle("/", health.Handler(engine, health.Options{Timeout: 2 * time.Second}))
// GET /livez  -> 200 while the engine is not stopped (never touches the store)
// GET /readyz -> 200 when Engine.Health(ctx).Ready, else 503; JSON body
```

- `Engine.Live()` is false once the engine is stopped.
- `Engine.Health(ctx)` reports `Ready` = live, not draining, the store answered a ping, and its
  schema is current (`SchemaVersion >= WantSchemaVersion`: the newest migration the build embeds).
  Stores opt in with `store.HealthChecker` (memstore, sqlite and postgres do); for another store
  only liveness and draining count.
- `Degraded` lists loaded queues that saw a store error in the last minute
  (`HealthDegradedWindow`), with the error. It does not make the engine unready.
- `Health` never loads or takes over a queue. Metrics: `engine_ready` (0/1, set on each call),
  `store_health_latency`.
- During `Shutdown` readiness turns false first, so a load balancer stops routing before leases
  drain.

## Dead letters

By default a task that is nacked or whose lease expires is redelivered forever. Set a limit to
move poison tasks aside instead:

```go
cfg := taskqueue.Config{MaxAttempts: 5}           // engine default; 0 = unlimited (the default)
three := int32(3)
src.SetMaxAttempts(queue, &three)                  // per queue (MemoryQueueConfigSource)

d, _ := e.PollTask(ctx, queue, taskqueue.PollOptions{})
log.Print(d.Task.Attempts)                         // 1 on the first delivery
d.Nack(taskqueue.NackReason("bad payload"))        // dead-lettered if this was attempt 5
```

- `Task.Attempts` counts deliveries started (`PollTask` returning the task). A delivery that ends
  without `Ack` (Nack, lease expiry, `Stop`, a crash) used one. A `Starter` error does not count.
  `AddTask` ignores the field.
- With a limit, each backlog delivery writes its new count to the store before `PollTask`
  returns (one extra fenced write per delivery), so a task that crashes its consumer is counted.
- A Nack or expiry on the last attempt moves the task from the backlog to the dead-letter set in
  one fenced write. A task matched with no attempts left (read back after a crash, or the limit
  was lowered) is dead-lettered instead of delivered. A sync-matched task has no row to move: it
  is spooled with its count and dead-lettered when next matched.
- If the dead-letter write fails the task is redelivered, never dropped
  (`dead_letter_failures`).
- `DeadTask.Reason` is `nack` (or `nack: <NackReason>`), `lease_expired` or
  `attempts_exhausted`.
- Admin calls. `ListDeadTasks`, `DeleteDeadTasks`, `PurgeDeadTasks(queue, deadBefore)` (zero time =
  all) and `DescribeQueue().DeadTasks` read or write the store only: they do not load the queue,
  take it over, or bump its rangeID. `RedriveDeadTasks(queue, RedriveOptions{TaskIDs, Max})`
  re-adds tasks as new ones (new ID, `Attempts` 0) through `AddTask`, so it loads the queue on the
  calling engine, taking it over from another owner like any `AddTask`. It is at-least-once: a
  dead entry is deleted after its task was added, so a failure in between can redrive it twice.
- Needs a store that implements `store.DeadLetterStore` (memstore, sqlite and postgres do;
  migration `0002_dead_tasks`). With another store the limit is ignored (logged at start) and the
  admin calls fail with `*store.InvalidArgumentError`.
- Metrics, queue-tagged: `tasks_dead_lettered{reason}`, `dead_letter_failures`,
  `tasks_redriven`, `task_attempt_record_failures`. There is no dead-letter gauge: poll
  `DescribeQueue().DeadTasks`, which is a `COUNT` query.

## Pause and resume

```go
src := taskqueue.NewMemoryQueueConfigSource(0) // Options.QueueConfigSource
src.SetPaused(q, true)                    // nothing is dispatched from q
src.SetFairnessKeyPaused(q, "tenant-7", true) // only this key is held back
src.SetPaused(q, false)                   // resume: waiting pollers are matched at once
```

- A paused queue still accepts `AddTask`: tasks are spooled (never sync-matched) and polls wait
  as if the queue were empty. Leases already handed out are unaffected.
- A paused fairness key is skipped by the matcher; other keys keep their fairness order.
- Pause is part of `QueueConfig` (`Paused`, `PausedFairnessKeys`), so any `QueueConfigSource`
  can provide it. It applies per partition key, like the other `QueueConfig` settings.
- `DescribeQueue` reports `Paused` and `PausedFairnessKeys`; gauge `queue_paused` (per queue).

## Persisted queue config

```go
src, err := taskqueue.NewStoreQueueConfigSource(ctx, st, taskqueue.StoreQueueConfigSourceOptions{}) // st: memstore, sqlite or postgres
defer src.Close()
opts.QueueConfigSource = src

// Read-modify-write with an audit record; retried on a concurrent write by another process.
src.Update(ctx, q, taskqueue.ConfigChange{Identity: "alice", Reason: "INC-42"}, func(c *taskqueue.QueueConfig) error {
	c.Paused = true
	return nil
})
hist, _ := src.History(ctx, q, 20) // newest first: Version, Config, UpdatedBy, Reason, UpdateTime
```

- Opt-in: `MemoryQueueConfigSource` remains the in-process source (tests, single process).
- Every process on the same database sees the same config. A change applies to this process before
  `Update` returns and to other processes on their next poll (`PollInterval`, default 5s); loaded queues
  are notified through `QueueConfigWatcher` as with the memory source.
- Each write is a new version (compare-and-set on the version, up to 10 retries) and a history row.
  The whole `QueueConfig` is validated with the memory source's rules before it is written.
- Stored as JSON of `QueueConfig` (Go field names) in `tq_queue_config` / `tq_queue_config_history`
  (migration `0003_queue_config`). Writes are serialised (a transaction-scoped advisory lock on
  Postgres) so the change feed's sequence numbers are in commit order and a poll cannot skip one.
- Metrics: `queue_config_updates`, `queue_config_refresh_failures` (no queue tags).
- The history is never trimmed by the library.

## Delays and retry backoff

```go
// Delayed enqueue: not delivered before VisibleAt (never sync-matched).
engine.AddTask(ctx, q, taskqueue.Task{Payload: p, VisibleAt: time.Now().Add(time.Hour)})

// Redeliver a failed task after a delay instead of immediately.
d.Nack(taskqueue.NackDelay(30 * time.Second))

// Or for every Nack and lease expiry: min(Initial * Multiplier^(attempt-1), Max).
cfg.RetryBackoff = taskqueue.RetryBackoff{Initial: time.Second, Max: 5 * time.Minute} // Multiplier 0 = 2
```

- Defaults are unchanged: no `VisibleAt`, no `RetryBackoff`, so redelivery is immediate.
  `NackDelay(0)` forces an immediate redelivery when a backoff is set.
- A backlog task's delay is written to its row (`Task.VisibleAt`, part of the task data, so no
  migration) with a fenced update before the task is released, so it survives a restart. That
  needs a store implementing `store.DeadLetterStore` (memstore, sqlite, postgres); otherwise the
  delay applies in memory only. A failed write is counted in `delay_persist_failures`.
- A sync-matched task that is nacked or expires with a delay is spooled with `VisibleAt`.
- The delay is skipped when the Nack or expiry dead-letters the task (`MaxAttempts`).
- A delayed task keeps its place in fairness order (its pass): when it becomes visible it can
  be delivered ahead of newer tasks of the same key.
- Delayed tasks are held in memory by the queue's reader, each on a timer (the injected clock).
  Up to `Config.MaxDelayedInMemory` (default 10000) per subqueue do not count toward
  `GetTasksBatchSize`, so they do not hold back ready tasks behind them. Past that they count as
  loaded, and a longer run of delayed tasks delays the ready tasks behind it until the delayed
  ones become visible. Use a separate queue for large numbers of scheduled tasks.
- Metric: `tasks_delayed` (per queue).

## Retention

Queue rows are never deleted on their own (upstream relies on a persistence TTL that normal
partitions do not have). Sweep idle ones from a routine or one elected process:

```go
res, err := engine.SweepIdleQueues(ctx, taskqueue.SweepOptions{
	IdleFor:           7 * 24 * time.Hour, // at least taskqueue.MinSweepIdle (24h)
	MaxDeletes:        1000,               // 0 = no cap
	DeadTaskRetention: 14 * 24 * time.Hour, // optional: also delete older dead tasks
})
```

- A queue is deleted only when it is not loaded on this Engine, its row was not written for
  `IdleFor`, it has no task above its ack level, and it has no dead tasks. The check and the
  delete are one store transaction conditioned on the listed rangeID and the idle time
  (`store.QueueSweeper.DeleteQueueIfIdle`), so a queue another process loaded or wrote since
  the listing is kept. The sweep never loads or takes over a queue.
- `MinSweepIdle` is 24h because a loaded queue rewrites its row at least every 12h even when
  idle. A shorter window could delete a row under a quiet owner elsewhere.
- A swept queue is recreated empty by its next AddTask or PollTask. A process that was loading
  it at that instant may see one transient condition-failed error.
- `Config.MaxTaskAge` (per queue: `QueueConfig.MaxTaskAge`, `MemoryQueueConfigSource.SetMaxTaskAge`)
  caps each new task's `ExpiryTime` at AddTask time + the age; expired tasks are dropped like any
  task past its expiry. It applies to tasks added after it is set. Default 0: no cap.
- Metrics (engine-level): `queues_swept`, `dead_tasks_expired`.

## Task admin

```go
page, next, _ := e.PeekTasks(ctx, queue, taskqueue.PeekOptions{PageSize: 50, FairnessKey: "tenant-7"})
for _, t := range page {
	log.Print(t.Ref, t.Priority, t.Task.Attempts, t.Leased)
}
ok, _ := e.DeleteTask(ctx, queue, page[0].Ref) // one poison task
n, _ := e.PurgeQueue(ctx, queue)               // the whole backlog
```

- `PeekTasks` pages the unacked backlog in dispatch order within each priority (subqueue by
  subqueue). Filters: `Priority`, `FairnessKey` (applied while scanning, so a page can be short
  with a `NextPageToken`). It reads the store only: it never loads the queue, takes it over or
  bumps its rangeID. On the owning engine it also hides tasks acked but not yet deleted and sets
  `Leased`.
- `DeleteTask` and `PurgeQueue` change the backlog, so they run on the owner: they load the queue
  on the calling engine, taking it over from another owner like `AddTask` does (the old owner's
  next fenced write fails and it unloads). A queue that was never persisted gives
  `*store.NotFoundError` and is not created.
- `DeleteTask` takes the task out of memory and the matcher and deletes its row under the rangeID
  fence. A lease on it ends: `Ack`/`Nack` return `ErrLeaseLost` and nothing is redelivered. A poll
  that matched the task an instant before the delete still delivers it once. Needs a store that
  implements `store.TaskAdminStore` (memstore, sqlite and postgres do).
- `PurgeQueue` moves every subqueue's ack level to its highest written task, persists it, then
  deletes the rows, and returns the count. Leases on purged tasks end like above. Tasks added
  while it runs may or may not be purged; tasks added after it returns are kept. Dead tasks
  (`PurgeDeadTasks`) and sync-matched leases are not touched.
- Metrics, queue-tagged: `tasks_admin_deleted`, `queue_purges`.

## Observability

### Metrics

Pass a `metrics.Handler` in `Options.Metrics` (default: no-op). Handlers that implement `metrics.TagBinder`
can pre-bind each queue's tags once; `metrics/metricstest.Recorder` records everything for tests.

Every per-queue metric carries upstream's `GetPerTaskQueuePartitionIDScope` tags: `namespace`
(`QueueKey.Namespace`, `_unknown_` if empty), `taskqueue` and `partition` (the partition ID).
**Fairness keys are never tags.** Cardinality is capped by `Config.MetricsQueueTagLimit` (default 1000, per
engine, first come first served): queues beyond the cap are tagged `taskqueue=__omitted__` and emit no
per-queue gauges, which is what upstream does with `metrics.breakdownByTaskQueue=false`. A negative limit
omits every queue.

| Metric | Type | Extra tags | When |
|---|---|---|---|
| `tasks_added` | counter | | `AddTask` reached the partition |
| `sync_match` / `tasks_spooled` | counter | | task handed to a waiting poller / written to the backlog |
| `syncmatch_latency` | timer | | `AddTask` duration, sync-matched tasks only |
| `task_dispatch_latency` | timer | `source` (`History` = sync, `DbBacklog`), `forwarded`, `task_priority` | task create time → delivery to a poller |
| `asyncmatch_latency` | timer | | as above, backlog tasks only |
| `poll_success`, `poll_success_sync`, `poll_timeouts` | counter | | poll outcome |
| `pending_polls` | gauge | | pollers currently waiting |
| `delivery_acked`, `delivery_nacked`, `delivery_lease_extended`, `delivery_lease_expired` | counter | | lease outcomes (library-only names) |
| `delivery_ack_latency` | timer | | delivery → `Ack` |
| `approximate_backlog_count` | gauge | `task_priority` | every `Config.BacklogMetricsEmitInterval` (1m, ±5% jitter) |
| `approximate_backlog_age_seconds` | gauge | | age of the oldest backlog task, same cadence; both zeroed on unload |
| `physical_approximate_backlog_count`, `physical_approximate_backlog_age_seconds` | gauge | `task_priority` (count) | at each ack-level sync; unprefixed names if the interval is negative |
| `task_write_latency`, `task_write_throttle` | timer, counter | | backlog writes |
| `tasks_rate_limited` | counter | | match throttled by the rate limiter |
| `tasks_dropped` | counter | `reason` (`expired_read`, `expired_memory`, `invalid`, `starter`) | task dropped |
| `task_rewrites`, `task_retry_transient`, `task_completed_missing`, `condition_failed_errors`, `lease_requests`, `lease_failures`, `fair_reader_stuck_detected` | counter | | backlog internals |
| `task_queue_started`, `task_queue_stopped` | counter | `cause` (stopped) | queue load / unload |

`task_priority` is `""` for the default priority, as upstream. Differences from upstream are listed in
`docs/PARITY_REPORT.md` §3.

### DescribeQueue and ListQueues

`Engine.DescribeQueue` is modeled on upstream `DescribeTaskQueue` with enhanced stats. A loaded queue reports
live values; an unloaded queue is read from the store without loading it (no ownership takeover, no rangeID
bump): `Loaded=false`, stats and the effective rate limit come from the stored rows and config, and the poller, task and lease fields are zero.

```go
d, err := e.DescribeQueue(ctx, store.QueueKey{Namespace: "ns", Name: "emails"})
// d.Stats.ApproximateBacklogCount, d.Stats.ApproximateBacklogAge
// d.Stats.TasksAddRate, d.Stats.TasksDispatchRate (tasks/s over the last 30s), d.Stats.RateLimitingActive
// d.StatsByPriority[3]                       // the same, per priority
// d.EffectiveRateLimit, d.RateLimitSource    // as Engine.RateLimit
// d.WaitingPollers, d.LoadedTasks, d.MatcherTasks, d.OutstandingLeases
// d.RangeID, d.LastUpdateTime, d.Loaded

page, err := e.ListQueues(ctx, 100, nil) // store must implement store.QueueLister (all bundled stores do)
for _, q := range page.Queues { fmt.Println(q.Queue, q.ApproximateBacklogCount, q.Loaded) }
// pass page.NextPageToken for the next page; nil means done
```

Unloaded-queue backlog age is approximate: the oldest create time among the first 100 tasks of each subqueue.

## Layout

| Package | Contents |
|---|---|
| `taskqueue` (root) | Public API: `New`, `Engine`, `Options`, `Config`, `Task`, `Delivery`, errors |
| `store` | `Store` interface, row types, typed errors |
| `store/memstore` | In-memory `Store` with subqueues and fault injection |
| `store/postgres`, `store/sqlite` | Durable stores (dialect, driver setup, embedded migrations) |
| `store/internal/sqlcore` | Dialect-neutral SQL `Store` shared by both (port of upstream `sqlTaskManagerV2`) |
| `internal/testdb` | Test helper that opens each store kind on a fresh database/schema |
| `store/storetest` | Store conformance suite (run it against any `Store`) |
| `internal/matching` | Engine, partition/physical queue managers, pri matcher, fair backlog writer/reader, leases |
| `internal/counter`, `internal/backoff`, `internal/cache` | Fairness counter (hybrid map + count-min sketch), retry helpers, LRU for per-key limiters |
| `clock`, `metrics` | Injected time source (with a fake one for tests) and a metrics interface |
| `parity/` (own module) | Differential harness: script driver, our runner, upstream adapter (go `-overlay` on the upstream tree), generator, comparer |

Upstream file mapping: `docs/UPSTREAM_MAP.md`.

## Testing

```
go vet ./... && go test -race ./...
TEMPORAL_TEST_LONG=1 go test -race ./internal/matching/   # also run upstream's long standing-backlog tests
TEMPORAL_TEST_RATE_REALCLOCK=1 go test -run RealClock -v .  # 10s real-clock 200 rps throughput check
```

SQLite tests always run (temp files). Postgres tests run only when `TQ_TEST_POSTGRES_DSN` is set, and skip
otherwise. Each test creates its own schema and drops it on cleanup, so the role needs `CREATE` on the database.
A throwaway local server:

```
initdb -D /tmp/pg -U tq --auth=trust
pg_ctl -D /tmp/pg -l /tmp/pg.log -o "-p 55432 -k /tmp -c listen_addresses=127.0.0.1" start
createdb -h 127.0.0.1 -p 55432 -U tq tqtest
export TQ_TEST_POSTGRES_DSN="postgres://tq@127.0.0.1:55432/tqtest?sslmode=disable"
go test -race ./...
```

Parity harness (separate module `parity/`; the library does not depend on temporal). It needs a separate
clone of upstream Temporal at tag v1.32.1 (commit af08a5e5), which is not included in this repo. Put it at
`upstream/temporal` or set `UPSTREAM_DIR`; see [`parity/README.md`](parity/README.md):

```
cd parity && ./run_all.sh      # 4 scenarios x 3 runs: upstream sqlite p1/p4, ours mem/sqlite -> results/compare.md
./run_probes.sh                # ablations + system-limit probes -> results/probes.md
```

Benchmarks (enqueue + dequeue + ack, backlog path and sync-match path, per store):

```
go test -run '^$' -bench 'BenchmarkEngine|BenchmarkStoreCommit' -benchtime 20000x .
```
