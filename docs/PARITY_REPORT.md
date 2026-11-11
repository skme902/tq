# Parity report: tq vs upstream Temporal matching (v1.32.1 af08a5e5)

Status: slice 6 (final). All numbers below come from runs on 2026-10-27 (IST). Results:
`parity/results/compare.md` (full matrix, finished 15:20:42 IST) and `parity/results/probes.md`
(ablations, system-limit probes and config-lag probes, finished 15:25:05 IST). Raw traces are in `parity/traces/`
(gitignored). Running the harness again regenerates every number in this report.

Summary: with one upstream partition, our library gives the same delivery results as upstream
matching on every scenario tested. That means zero loss and zero duplicates. Where upstream itself is deterministic, the dispatch
order matches exactly. Where upstream is not deterministic, our results fall inside upstream's own run-to-run
variation. Rate-limited throughput matches to within 0.1–1 task/s. Sync-match rate and latency
distributions also match. The harness found one bug in our port (pri_matcher drop reason), which is fixed. The two
remaining measured differences are intentional or environmental (§4.6). Upstream's default of
4 partitions behaves differently, and we do not implement partitions (§5).

## 1. Method

### 1.1 Harness design

```
parity/                      separate Go module (github.com/skme902/tq/parity);
                             the library module does not import temporal
  cmd/gen/                   seeded workload generator -> scripts/*.json
  driver/driver.go           stdlib-only script replayer + trace recorder (shared byte-for-byte)
  cmd/ours/                  driver + our library (memstore or SQLite WAL temp file)
  upstream/parity_upstream_test.go   //go:build parity_upstream; adapter compiled INTO upstream
                             service/matching via `go test -overlay` (pristine clone untouched)
  run_upstream.sh            builds the overlay (driver.go copied with package clause rewritten) and
                             runs the adapter: store {sqlite|mem}, partitions {1|4}, run index
  cmd/compare/               trace diff -> markdown tables
  run_all.sh                 full matrix (RUNS=3)      run_probes.sh   ablations + probes
  probes.py                  ablation/probe script generator
  FINDINGS.log               every anomaly found, with cause and outcome
```

**Upstream side (a).** The real `matchingEngine` from `newMatchingEngine` runs in-process. It is built
with `go test -tags parity_upstream -c -overlay` inside the pristine upstream tree, so it is the real
upstream code at af08a5e5. The adapter overrides nothing in the matching package.

The adapter is set up as follows:
- **Persistence.** Upstream's own SQL persistence on SQLite in memory mode, through `sql.NewFactory` with MaxOpenConns 1. The `mem` variant uses the
  `testTaskManager` fake from the upstream tests.
- **History client.** A gomock history client. `RecordActivityTaskStarted` records the delivery, and the time of that call is
  our "deliver" timestamp.
- **Matching client.** A gomock matching client routes GetTaskQueueUserData, UpdateTaskQueueUserData and forwarding back into
  the same engine, which makes it a single-host cluster.
- **Limits and weights.** Set through the public `UpdateTaskQueueConfig` RPC.
- **Config.** Starts from `defaultTestConfig()` with fairness on (`useFairness`). The following settings differ from that test config:
  - AutoEnableV2Sub is off. The fair backlog is configured directly, not through migration.
  - MetadataUpdateOnAppendInterval is 1 min (the test config uses 0).
  - MaxTaskDeleteBatchSize is 100, the server default. The test config uses 1, which made upstream drain about 2x slower; see FINDINGS.
  - The long-poll expiration comes from the script.
  - The partition count comes from env (1 for the core comparison, 4 as a gap probe).
- **Fake time.** Upstream does not expose fake time for the engine as a whole, so both sides run on wall clock.

**Our side (b).** `taskqueue.New` with `DefaultConfig`, a `MemoryQueueConfigSource` for limits and weights, and
either `memstore` or `sqlitestore` (a temp file in WAL mode).

**Same scheduling on both sides.** One `driver.go` source replays the same seeded script on both sides. The source covers
adds, poller start/stop, limit and weight changes, barriers and marks, and it collects the same trace format:
`{id, q, poller, at}` per delivery, plus marks and elapsed time. Each poller loops on Poll and Acks each task as soon as it receives it.

### 1.2 Workloads (`parity/cmd/gen`, seed 1)

| scenario | queues | adds | keys | weights | priorities | pollers | what it stresses |
|---|---|---|---|---|---|---|---|
| det-fair | 3 (det-taskw, det-override, det-pri) | 4500 | 71 | task weights 1/2/5; per-key weight overrides (det-override) | 1–5 (det-pri) | 1, started after all adds | deterministic stride order |
| many-keys | 1 | 6000 | 300 (top key 15%) | 1/2/5/unset | – | 1, after all adds | >100 keys → count-min sketch path |
| churn | 30, created on the fly | 5686 | 167 (top key 30%) | 1/2/5/unset | 1–5 | 380 starts / 350 stops | mixed sync match and backlog, consumer churn |
| ratelimit | 4 | 6000 | 130 | 1/2/5/unset | 1–5 | 16 | queue RPS 50/–/100/100, per-key RPS 10 (rl-key10, rl-both), 10 s limited window, then unset |

Matrix per scenario: upstream/sqlite/p1 ×3, ours/mem ×3, ours/sqlite ×3, upstream/sqlite/p4 ×3,
upstream/mem/p1 ×1. The reference is upstream/sqlite/p1 run 0. Each other run is compared with that reference,
including upstream runs 1–2, so upstream's own nondeterminism is measured.

### 1.3 Metrics

- **Loss and duplicates.** An add is acknowledged but never delivered, or a task ID is delivered more than once.
- **exact.** The fraction of positions where the per-queue delivery sequence equals the reference.
- **tau.** The Kendall rank correlation of delivery order against the reference.
- **L1.** The mean per-key share distance over windows of 100 deliveries: 0 means identical, 2 means disjoint. This is the fairness-share curve.
- **wrong.** Priority wrongorderness: the fraction of pairs delivered with the lower priority first.
- **FIFO baseline.** The same metrics for plain add order, which shows what "no fairness" scores.
- **Throughput.** Deliveries per queue (and max per key) per second inside the 10 s limited window.
- **Latency.** From add to deliver, overall and split into sync-matched vs backlog.

## 2. Results

### 2.1 Loss and duplication (every run, every scenario)

| impl/store | add errors | lost | duplicates | complete |
|---|---|---|---|---|
| upstream/sqlite/p1, p4 | 0 | 0 | 0 | all |
| ours/mem, ours/sqlite | 0 | 0 | 0 | all |
| upstream/mem/p1 | 0 | 0 | churn 445, det-fair 41–47, ratelimit 17–20, others 0 | all |

The upstream/mem duplicates come from the test fixture, not from matching. `testTaskManager.GetTasks` ignores
`GetTasksRequest.Subqueue`, so every subqueue reader reads every subqueue. This happens only in queues with more than one
priority, and upstream on real SQL persistence has 0 duplicates. See FINDINGS.log and UPSTREAM_MAP §13.12.

### 2.2 Dispatch order: deterministic scenario (det-fair, single poller)

| comparison | det-taskw (2000) exact / tau / L1 | det-override (1500) | det-pri (1000) |
|---|---|---|---|
| ours/mem vs upstream (3 runs) | 1.000 / 1.000 / 0 | 1.000 / 1.000 / 0 | 1.000 / 1.000 / 0 |
| ours/sqlite vs upstream (3 runs) | 1.000 / 1.000 / 0 | 1.000 / 1.000 / 0 | 1.000 / 1.000 / 0 |
| upstream run 1–2 vs run 0 | 1.000 / 1.000 / 0 | 1.000 / 1.000 / 0 | 1.000 / 1.000 / 0 |
| FIFO baseline | 0.000–0.001 exact, L1 0.854 / 0.892 | | |
| upstream/sqlite/p4 vs p1 (mean) | exact 0.006, tau 0.845, L1 0.303 | | |

**Checking that the metric can fail.** Each ablation below is a run on ours/mem with one feature removed from the script. Each is compared
with the full upstream reference:

| ablation | queue it affects | exact | tau | L1 / wrong |
|---|---|---|---|---|
| no weight overrides | det-override | 0.065 | 0.755 | L1 0.612 |
| no task weights | det-taskw | 0.009 | 0.918 | L1 0.701 |
| no priorities | det-pri | 0.000 | 0.278 | wrongorderness 0.497 |

So the 1.000 result depends on the weight overrides, task weights and priorities actually taking effect. It is not an artifact of a near-FIFO workload.

### 2.3 Dispatch order: nondeterministic scenarios (means over queues and runs)

| scenario | ours/mem | ours/sqlite | upstream self (runs 1–2 vs 0) | upstream/mem | upstream p4 | FIFO L1 |
|---|---|---|---|---|---|---|
| many-keys (300 keys) exact / tau / L1 | 0.393 / 0.946 / 0.365 | 0.389 / 0.946 / 0.373 | 0.407 / 0.947 / 0.356 | 0.398 / 0.943 / 0.377 | 0.001 / 0.829 / 0.803 | ≈1.37 |
| churn (30 queues) | 0.997 / 1.000 / 0.000 | 0.995 / 1.000 / 0.000 | 0.998 / 1.000 / 0.000 | 0.994 / 1.000 / 0.000 | 0.334 / 0.972 / 0.018 | |
| ratelimit | 0.517 / ≈0.99 / 0.063 | 0.593 / ≈0.99 / 0.043 | 0.529 / ≈0.99 / 0.067 | 0.529 / ≈0.99 / 0.069 | 0.008 / 0.796 / 0.227 | |

- **many-keys.** Upstream is not deterministic even against itself here. Above 100 keys, the fairness counter is a count-min
  sketch seeded with `rand.NewPCG(rand.Uint64(), rand.Uint64())` and `maphash.MakeSeed()`. Ours uses the same
  construction, so parity here can only be statistical. Our distance from upstream is the same as upstream's distance from itself:
  exact 0.39 vs 0.41, L1 0.365 vs 0.356. Both are far from FIFO (L1 ≈ 1.37).
- **churn and ratelimit.** Here the order also depends on poller timing on the wall clock. On every metric, ours scores the same as upstream
  run against itself.

### 2.4 Sync match and latency (churn)

| impl | sync % | latency p50 / p99 ms | backlog p50 / p99 ms | sync p50 / p99 ms |
|---|---|---|---|---|
| upstream/sqlite/p1 | 84.8 | 0.1 / 1243.8–1244.5 | 378.6–379.4 / 1619.5–1620.4 | 0.1 / 0.2 |
| ours/mem | 84.9 | 0.0 / 1243.4–1244.7 | 382.2–384.1 / 1619.3–1619.5 | 0.0 / 0.0–0.1 |
| ours/sqlite | 84.9 | 0.0 / 1244.1–1244.2 | 382.7–384.3 / 1618.5–1619.9 | 0.0 / 0.0–0.1 |
| upstream/sqlite/p4 | 36.1–37.5 | 39.1–45.7 / 1323–1398 | 133–145 / 1465–1527 | 0.1–0.3 / 0.2–0.3 |

The p99 is dominated by backlog waiting for churned pollers, and it is the same on both sides. The sync-match path is about 0.1 ms faster in ours
because we have no RecordTaskStarted RPC hop.

**Drain latency in the backlog-only scenarios.** These numbers measure drain speed, not semantics: one poller drains a backlog loaded in advance.

| scenario | upstream/sqlite p50 | ours/mem p50 | ours/sqlite p50 |
|---|---|---|---|
| det-fair | 925–990 ms | 837–935 ms | 964–1076 ms |
| many-keys | 1096–1119 ms | 915–928 ms | 1249–1254 ms |

The stores are not set up the same way: upstream uses SQLite in memory with one connection, ours uses a WAL file. Read these as "same order of magnitude", not as parity.

### 2.5 Throughput under limits (ratelimit, 10 s limited window, tasks/s; 3 runs each)

| queue [configured] | upstream/sqlite/p1 | ours (mem + sqlite) | upstream p4 |
|---|---|---|---|
| rl-queue50 [queue 50] | 54.9–55.1 | 55.0–55.1 | 48.5–51.0 |
| rl-key10 [per-key 10; 8 keys → 80] | 87.7–88.5, max key 11.0–11.1 | 88.0–88.8, max key 11.0–11.1 | 74.3–77.8 |
| rl-both [queue 100, key 10] | 100.0, max key 11.3 | 100.0, max key 11.3 | 91.4–94.1 |
| rl-fair [queue 100, weights] | 109.9–110.0 | 110.0–110.1 | 90.4–93.9 |

The roughly 10% excess over the configured limit is real and appears on both sides. It is the limiter's 1 s burst, about 50 tasks in the first 200 ms,
spread over the 10 s window. After that, rl-queue50 runs at 49.9997/s upstream and 49.9988/s ours.

**System rate limit probes.** 4000 tasks on 8 keys are loaded first, the limit config is changed, there is a 1.5 s settle, then 4 pollers start:

| probe | upstream drain | ours/mem | ours/sqlite | notes |
|---|---|---|---|---|
| never set | 0.40 s | 0.32–0.36 s | 0.36–0.40 s | no limit |
| set 5000 → unset | 0.41–0.42 s | 0.32 s | 0.37 s | no limit |
| set 50 → unset | 3.00 s | 3.00 s | 3.00 s | 2000 in first 1 s, then exactly 1000/s on all three |

These confirm the upstream quirk we reproduced on purpose. After an explicit RPS is removed, the 1000 rps system limit applies only if the
effective rate changes because of the removal.

**Config-change lag.** 4 pollers run at 50 rps. At +2 s the limit is unset or raised to 5000. The lag is the time until 10 deliveries land within 50 ms.

| | upstream | ours/mem | ours/sqlite |
|---|---|---|---|
| unset | 63–80 ms | 0–20 ms | 0 ms |
| raise to 5000 | 62–80 ms | 0–20 ms | 0 ms |

This explains the one end-to-end elapsed difference: ratelimit takes 14.1 s upstream vs 12.7–13.2 s ours. The script issues 8 config updates in sequence
at unset time. Each upstream update is a persisted user-data write followed by long-poll propagation to the partition, so the last queue's
unset lands 0.5–0.75 s late. Per-250 ms buckets show upstream rl-fair still at 100/s during that time, then running at exactly
1000/s. Our config source is in-process. This is an intentional architectural difference (§3, item 9), not a matching difference.

### 2.6 Held leases (ours only) and the review-fixes rerun

Upstream acks at RecordTaskStarted, so it never holds a task. `parity/cmd/ours -hold-ms N` acks each lease N ms after
delivery instead, and `run_probes.sh` runs it (results/probes.md, "Held leases"):

| scenario | ours/mem | ours/sqlite | notes |
|---|---|---|---|
| det-fair, hold 2000 ms (3 × 1500 tasks, batch 1000) | order exact 1.000, tau 1.000; 0 lost, 0 dup; 1.3 s | exact 1.000; 0 lost, 0 dup; 2.0 s | order is the same as upstream acking at once |
| churn, hold 500 ms | 5686/5686, 0 lost, 0 dup, sync 84.9% | 5686/5686, 0 lost, 0 dup, sync 85.0% | |

Before the H1/H2 fix (`6e4b017`, same harness), det-fair with 2000 ms holds still finished in order, but backlog p99
latency was 2822 ms and the run took 3.2 s, compared with 1306 ms and 1.3 s after the fix. More than
`GetTasksBatchSize` held leases stopped reads until the first acks arrived.

The full `run_all.sh` rerun on `feat/review-fixes` matched the earlier results:
- det-fair exact order 1.000 (mem and sqlite).
- 0 lost and 0 duplicates everywhere except the known upstream/mem fixture.
- churn sync 84.9%.
- Limited throughput within 1% of upstream, for example rl-queue50 55.0–55.1 vs 55.0.

One exception was churn exact order: ours/mem 0.983 and ours/sqlite 0.994, compared with 0.997 and 0.995 before. Run 2
was slowed by load on the shared host, and upstream's own run 2 dropped to 0.870 (elapsed 20.6 s instead of 15.0 s).
Reruns of run 2 and a run 3 gave 0.995/0.996 for mem and 0.994/0.993 for sqlite. Upstream's own run 2 rerun gave 0.998.

## 3. Intentional differences (all slices)

1. **Leases and ack model.** Upstream treats "delivery" as the RecordTaskStarted RPC to history, and history dedupes.
   We hand out a lease. The consumer then calls Ack, Nack or ExtendLease, and the lease returns to the backlog when it expires.
   Acks for evicted or unloaded tasks are skipped (the `evictedAcks` path), whereas upstream relies on history dedupe.
2. **Sync-match start failure.** If a start error follows a sync match, the task is spooled to the backlog. Upstream returns the start error to history,
   which retries, and spools only on BUSY_WORKFLOW. We have no history behind us to do that retry.
3. **GC.** `maybeGC` also runs on the periodic tick, not only on ack. There is a floor on `numToGC`. Deletes use a batch size like
   upstream MaxTaskDeleteBatchSize.
4. **Priority-levels clamp.** Upstream config.go:403–405 computes `max(p, min(p, maxPriorityLevels), 1)`, which reduces to `max(p, 1)`,
   so its 60-level cap has no effect. We apply the intended clamp to [1, 60]. Per-task `clipPriority` to [1, levels] is unchanged.
5. **System rate limit on load.** `ApplySystemRateLimitOnLoad` is an opt-in flag and is false by default. False reproduces upstream: the
   1000 rps system limit is enforced only after a live effective-RPS change, never on load. The probes in §2.5 confirm this on both sides.
6. **Namespace mapping.** `QueueKey.Namespace` is a plain string. Namespaces have no registry, no IDs and no per-namespace dynamic config.
7. **NumReadPartitions.** Defaults to 1. Partitions are not implemented (§5).
8. **Store schema.**
   - Rows hold JSON instead of protobuf blobs.
   - Tables use a `tq_` prefix, and subqueue is an explicit column.
   - A duplicate insert is an upsert.
   - `DeleteQueue` also deletes the queue's tasks.
   - Postgres range deletes go through `ctid`.
   - SQLite uses WAL mode and retries when busy.
   - Metadata carries a `changeSeq`.
9. **Config source.** Limits and weights come from an in-process `QueueConfigSource` instead of persisted task-queue user data, so changes apply
   immediately (§2.5 lag). A limit of 0 means unlimited.
10. **TopK persistence.** The TopK fairness counts are persisted before AddTask callers get their reply. The UpdateAckInterval check uses elapsed time on the injected clock
    instead of upstream's ticker.
11. **Idle unload.** `onIdle` defers unloading while leases are outstanding.
12. **Validator.** The validator loop runs only if a Validator is configured. Upstream always validates against history.
13. **AddTask retry.** AddTask retries internally when the queue unloads or a range conflict occurs. Upstream returns the error and the
    frontend or history retries.
14. **Clock.** The long-poll child timeout uses the injected clock. LRU `Entries` returns a snapshot.
15. **Backlog types.** Only the fair-path backlog exists. There is no classic or pri backlog and no classic-to-fair migration.
16. **Leased-task accounting (review H1/H2).** Upstream drops a task from the reader at RecordTaskStarted, so it never has
    "held" tasks. Our leased backlog tasks stay in the reader until Ack/Nack/expiry. They count in `leasedTasks`, not
    `loadedTasks`, so held leases never block reads (`loadedTasks <= GetTasksReloadAt` is unchanged). Trims skip them, and a
    leased task chopped by an eviction is remembered in `leasedEvicted` so a re-read reattaches the entry instead of matching
    it again. The read, trim, ack-level and GC algorithms are otherwise upstream's. §2.6 checks this with held leases.
17. **Leases at Stop (review M1/L6).** `Engine.Stop` ends every live lease and stops its timer. A sync-matched lease is
    re-spooled; a backlog lease stays in the store. Ack, Nack and ExtendLease on those leases return `ErrEngineStopped`,
    and so does any lease created after Stop starts (a sync match then spools the task). Upstream has no leases.
18. **Lease expiry (review M2).** The lease deadline decides expiry: an expiry callback that loses a race with
    ExtendLease does nothing. `ExtendLease(d <= 0)` is an InvalidArgumentError.
19. **Partition lifecycle (review M3).** A physical queue stopped before `Start` stays stopped; `Start` after Stop starts
    nothing, and `WaitUntilInitialized` returns once the queue stops. Upstream has the same publish-then-start window but
    runs inside fx lifecycle code that does not hit it.
20. **Config validation (review L7).** `NewEngine` rejects `GetTasksBatchSize < 1` and `GetTasksReloadAt` outside
    `[0, GetTasksBatchSize)`. Upstream takes dynamic config unchecked.
21. **Corrupt rows (review L10).** The SQL stores return an `InvalidArgumentError` naming the task ID when a row cannot be
    decoded, and the reader logs it at Error. It still retries the batch forever, as upstream does, so the subqueue stays
    blocked until the row is repaired.
22. **Backoff jitter.** `internal/backoff` applies up to 20% downward jitter; upstream's default jitter in v1.32.1 is 0. The
    maximum interval (10 s for add-error retries, review L1) and ThrottleRetryContext rules (review L2) match upstream.
23. **API (review L8).** `DefaultCounterParams` is a function that returns a copy instead of a mutable package variable.

24. **Observability (feat/observability).** Metric names, the `namespace`/`taskqueue`/`partition` tags, `task_priority` on
    `approximate_backlog_count` and `task_dispatch_latency`, `source`/`forwarded` on dispatch latency, the periodic logical
    backlog emitter (1m ± 5%) with `physical_*` gauges at ack-level sync, zeroing on unload, and the task_tracker (5 s buckets,
    30 s window) follow upstream. Differences:
    - No `task_type`, `operation`, `worker_version`/build-ID or `service_name` tags; there is one task type and no versioning.
    - `partition` is always the partition ID (we have no `breakdownByPartition` switch; there is only partition 0 today).
    - Upstream's boolean `breakdownByTaskQueue` becomes `Config.MetricsQueueTagLimit`: the first N (namespace, name) pairs
      per engine get a real tag, the rest `__omitted__` and no per-queue gauges. Negative means all omitted.
    - `Config.BacklogMetricsEmitInterval` < 0 disables the periodic emitter (upstream uses 0); `SyncState` then emits the
      unprefixed names, as upstream does at 0.
    - `delivery_acked`, `delivery_nacked`, `delivery_lease_extended`, `delivery_ack_latency` and `delivery_lease_expired` are
      library-only (upstream has no leases). `delivery_lease_expired` and `tasks_rate_limited` now carry the queue tags.
    - `task_dispatch_latency`/`asyncmatch_latency` are emitted when the delivery is created, which is upstream's
      `emitTaskDispatchLatencyAtPoll=true` default point.
    - `syncmatch_latency` times `Engine.AddTask`, including our internal unload/range-conflict retries (item 13).
    - `pending_polls` is emitted whatever the breakdown setting, as upstream.
    - `Engine.DescribeQueue` returns upstream's enhanced stats for one physical queue (no partition fan-out). For a queue that
      is not loaded it reads the store without taking ownership; upstream always loads the partition. The unloaded backlog
      age is the oldest of the first 100 tasks per subqueue, so it is approximate.
    - `Engine.ListQueues` pages `store.QueueLister` (optional interface). memstore orders by (namespace, name, partition);
      the SQL stores order by (range_hash, task_queue_id) like upstream `ListTaskQueue`.

25. **Graceful shutdown (feat/ops-drain).** `Engine.Shutdown(ctx)` is library-only: upstream matching stops via fx
    lifecycle and relies on history to retry anything in flight. During a drain, polls that are already waiting
    end through the poll context and return `ErrEngineDraining`; the matcher counts those as `poll_timeouts`.
    Polls refused by the fast-path draining check return before the matcher and are not counted as
    `poll_timeouts`. With no waiting poller, `AddTask` spools instead of sync-matching. Shutdown also waits for
    in-flight polls (calls that already passed the fast path, including a match still inside `Starter`) and
    refuses further leases at the drain decision, so no sync match commits after that decision. A sync-matched
    task refused at that point (or one whose `Starter` failed) returns to its `AddTask` caller, which spools it or
    reports an error once the engine has unloaded; the task is never silently dropped. `Stop` waits for settling
    leases (Ack/Nack/expiry whose write is in flight) before unloading. Matching,
    fairness, the limiter and fencing are unchanged.

26. **Max attempts and dead letters (feat/ops-dlq).** Library-only: upstream matching acks a task once history
    starts it, and history owns retries. `store.Task.Attempts` (in the task blob, no column) counts a delivery
    once `PollTask` has returned a `Delivery`. A `PollTask` that returns an error does not count. `Stop` during
    the attempt write rolls that write back, best effort. Only a committed attempt write counts across restarts:
    a non-fence write error still delivers the task, with the new count only in memory. `Starter.Start` runs
    before the attempt write and is at-least-once if that write is fenced (`PollTask` then returns `ErrNoTasks`
    and no delivery). With `MaxAttempts > 0` each backlog delivery does one extra fenced `UpdateTask` on the poll
    path, before the lease timer is armed and before `PollTask` returns. A lost fence on that write is reported
    as `ErrNoTasks` and does not deliver; the backlog row is unchanged. A last failed attempt inserts a row in
    `tq_dead_tasks` and finishes the task like an ack (dead-lettering is an ack plus a dead insert). Ack-level GC
    deletes the backlog row, the same as any other ack. A crash after the insert and before that ack level is
    persisted leaves the task both live and dead (at-least-once duplicate). A sync-matched last attempt is spooled
    and later dead-lettered as `attempts_exhausted`. A redrive during the window before that GC removes the
    original backlog row can produce an at-least-once duplicate after a reload. `RedriveDeadTasks` is serialized
    only within one engine process; redrives from several processes on one store are at-least-once. With
    `MaxAttempts == 0` there are no extra writes and upstream's redelivery path is unchanged: fair order, the
    limiter, sync match vs backlog and fencing match upstream. With `MaxAttempts > 0` the differences are an
    extra fenced write on the poll path, a sync-matched last attempt spooled and later dead-lettered as
    `attempts_exhausted`, and fence loss on the attempt write reported as `ErrNoTasks`. The ack level still
    advances past a dead-lettered task as usual.

27. **Task admin (feat/ops-admin).** `PeekTasks`, `DeleteTask` and `PurgeQueue` are library-only (upstream has
    `tdbg taskqueue list-tasks` but no delete or purge). The reader gains `adminDeleted` levels that a racing read
    skips, and a deleted task is flagged so `addTaskToMatcher` does not re-add it. Purge waits for every ack-level
    pin on a subqueue to drop (a writer between picking passes and committing, tasks held for a read, an admin
    delete in flight), then, without releasing the reader lock, moves the ack level to the max written level.
    `CreateFairTasks` raises that level before it commits, so a task written during the wait is purged too (as
    with SQS PurgeQueue) and no row is left below the ack level. Later writes stay above it because the writer's
    pass base is the ack level, read under the same lock, and task IDs only grow. One
    upstream race is fixed on the way: a task evicted between `priTaskMatcher.AddTask`'s
    `setRemoveFunc` and `EnqueueTaskNoWait` was still queued (its remove ran while it was not queued yet), so a
    trimmed task could be delivered twice; `EnqueueTaskNoWait` now skips evicted tasks
    (`TestEvictedBeforeEnqueueIsNotQueued`). `setEvicted` tolerates a task that was never added.

28. **Health (feat/ops-health).** `Engine.Live`, `Engine.Health` and the `health` HTTP package are library-only
    (upstream registers gRPC health in the service wrapper and flips it to NOT_SERVING before its drain sleep, which
    `Shutdown` + `/readyz` mirror). tq has no NOT_SERVING sleep: `Shutdown` marks the engine draining and new polls
    are refused in the same step readiness flips, and `Health` re-reads draining/stopped after its store probe so a
    drain that starts mid-probe is never reported ready. The only engine change is that `signalIfFatal` records the
    last persistence error per queue (not back-pressure, cancellation or shutdown errors); it still decides fatality
    as upstream does.

29. **Retention (feat/ops-retention).** `SweepIdleQueues` and `store.QueueSweeper.DeleteQueueIfIdle` are new (upstream
    deletes idle task queue rows only through Cassandra/SQL TTL and its scavenger). `Config.MaxTaskAge` /
    `QueueConfig.MaxTaskAge` (default 0, off) caps a task's `ExpiryTime` in `AddTask` and, from the redrive time, in
    `spoolTask` (redrive); the reader's expiry handling is
    unchanged. sqlcore now has a test that every Postgres query is rebound (`TestNumberedQueriesHaveNoQuestionMarks`).

30. **Delays and retry backoff (feat/ops-backoff).** `Task.VisibleAt`, `NackDelay`, `Config.RetryBackoff` and
    `Config.MaxDelayedInMemory` are library additions (upstream history schedules activity retries itself and adds a
    new matching task when due). Changes in ported code, all inert when no task has a future `VisibleAt`:
    `partitionManager.AddTask` spools such a task without trying a sync match; the fair reader holds it as
    "delayed" (`delayedTasks`, not counted in `loadedTasks`; up to `MaxDelayedInMemory` are excluded from the trim
    and read-batch counts) and adds it to the matcher from a timer; `completeTask` delays a nacked/expired task
    carrying a redelivery time instead of re-matching it; the "fair reader stuck" check also requires no delayed
    tasks. Upstream's fairness order, limiter, sync match and fencing are unchanged.

31. **Pause (feat/ops-pause).** `QueueConfig.Paused` / `PausedFairnessKeys` (library additions; upstream has no
    task-queue pause in v1.32.1 matching). Changes in ported code: `matcherData.findMatch` returns no match for a
    paused queue at the top, and skips tasks of a paused key inside the scan (poll forwarders are not skipped);
    `rateLimitManager` caches the pause settings as one immutable snapshot, refreshed with the other QueueConfig
    fields; `taskQueuePartitionManager` emits `queue_paused` after each refresh and zeroes it on unload; the fair
    reader does not count loaded tasks of paused keys toward its batch (up to `MaxDelayedInMemory`, shared with
    delayed tasks) and re-checks its read on a pause change. With nothing paused the scan body and the reader's
    counting are upstream's. Sync-match gating is unchanged: upstream's `isBacklogNegligible` uses the age of every
    DB-backlog task in the matcher, dispatchable or not (a rate-limited key's tasks count), so a paused key's tasks
    count too, and a paused key with a backlog older than `BacklogNegligibleAge` turns sync match off for the queue
    (TestPausedKeyBacklogAgeGatesSyncMatchLikeUpstream).

Review items that now match upstream and so need no entry: priority keys above `PriorityLevels` are clamped by
`clipPriority` and only negative keys are rejected (L3); the writer reads fairness weight overrides through the rate
limit manager (L4).

UPSTREAM_MAP.md §§2–16 has the source-level detail for each item.

## 4. Parity failures and harness findings

| # | finding | kind | outcome |
|---|---|---|---|
| 4.1 | pri_matcher `validateTasksOnRoot` always set dropReasonInvalid; upstream uses `getDroppedTaskExpiryReason` | **port bug** (found by ported TestValidatorDrop_SetsDropReason) | fixed (`droppedTaskExpiryReason`), test passes |
| 4.2 | upstream/mem duplicates | upstream test-fixture artifact (Subqueue ignored) | SQLite used as the reference |
| 4.3 | upstream drained about 2x slower in an early run | harness config (test MaxTaskDeleteBatchSize=1) | set to the server default of 100; full matrix rerun |
| 4.4 | ~10% over the limit in the limited window | both sides; 1 s burst | documented (§2.5) |
| 4.5 | ablation at first changed nothing | harness bug (wrong JSON keys) | fixed; ablations now show the expected drops |
| 4.6 | ratelimit elapsed 14.1 s vs 12.7–13.2 s | config-propagation latency (§2.5); my first explanation, contention, was wrong and has been corrected in FINDINGS | intentional difference §3, item 9 |

No other mismatch was found. Every scenario was run 3 times on 2 of our stores.

## 5. Gaps (not implemented)

- **Partitions and forwarder.** Upstream with its default of 4 partitions is measurably different:
  - sync match drops from 85% to 36–37%
  - sync latency p50 rises to 39–46 ms
  - limited-window throughput is 6–18% lower
  - the many-keys share L1 is 0.80
  - det-fair exact is 0.006
  Our library behaves like upstream with 1 partition.
- **Not implemented:**
  - worker versioning, build IDs and deployments
  - queries, Nexus tasks
  - sticky queues
  - poller-scaling hints, hooks
  - the DescribeTaskQueue RPC surface (reachability, versioning info, partition fan-out); `Engine.DescribeQueue` covers the enhanced stats (§3 item 24)
  - multi-cluster (standby) and replication of user data
  - history integration (RecordTaskStarted, task validation against mutable state)
  - classic→fair backlog migration and draining
  - ConcurrentRequestLimiter
  - `task_lag_per_tl` and the per-build-ID/version metric breakdowns

## 6. Ported tests (slice 6 and earlier)

| upstream file | ported | not portable and why |
|---|---|---|
| matcher_data_test.go | 23/31 | 8 need forwarding or queries |
| backlog_manager_test.go | 20/32 | classic-only or pri-only backlog, ConcurrentRequestLimiter |
| pri_matcher_test.go | 2/3 (TestValidatorWorksOnRoot, TestValidatorDrop_SetsDropReason) | TestForwardPollRetriesOnResourceExhausted needs the forwarder |
| physical_task_queue_manager_test.go | 4 | TestReaderSignaling, TestReaderBacklogAge, TestLegacyDescribeTaskQueue (classic or describe); TestAddTaskStandby (multi-cluster); TestPollScaling* (not implemented); TestDrainCompletionNoReloadDraining (migration) |
| matching_engine_test.go | 3 → `engine_port_test.go` TestPort* (from TestAddConsumeWorkflowTasksNoDBErrors, …DBErrors, TestConcurrentAddWorkflowTasksNoDBErrors) | the rest need versioning, user data, Nexus, history, deployment, sticky queues or queries. TestAddThenConsumeActivities is skipped upstream under the new matcher |
| tests/priority_fairness_test.go | 2 adapted (PrioritySuite.TestActivity_Basic, FairnessSuite.Test_Activity_Basic) | TestSubqueue_Migration and the Migration_* tests (migration), TestStickyInteraction_SinglePartition (sticky), TestUpdateWorkflowExecutionOptions_InvalidatesPendingTask (history) |
| task_tracker_test.go TestAddTasksRate | 1/1 (`internal/matching/task_tracker_test.go`, fake clock) | – |
| liveness, fair_level, fairness_util, ratelimit_manager, counter, cache | ported in slices 2–4 | – |

## 7. Remaining risks

- **No fake time on the upstream side.** Order parity under concurrency is statistical (tau and L1 inside the upstream self band),
  not exact.
- **Above 100 keys the counter is a randomly seeded CMS on both sides.** Only share-curve parity is possible there. Any bias that is smaller than
  run-to-run noise (L1 ≈ 0.36) would not be detected.
- **Latency comparisons.** Store setups differ (in-memory SQLite with one connection vs a WAL file; Postgres was not in the matrix, only in
  the library tests), so absolute drain latencies are not parity claims.
- **Partitions are untested** because they are not implemented. Users who expect upstream's multi-partition throughput or scaling will not get it.
- **Harness coupling.** The upstream adapter uses unexported internals (`newMatchingEngine`, `defaultTestConfig`, test
  fixtures) at af08a5e5. It will need updating for any other upstream tag.
- **Workload coverage.** The harness covers activity task queues only, on one host. It does not inject failures (store errors or crashes); those are
  covered by the ported DBErrors tests and the store test suite, not by differential runs.

## 8. Reproduce

```
cd parity
RUNS=3 ./run_all.sh      # about 15 min; writes results/compare.md
./run_probes.sh          # about 75 s;  writes results/probes.md
```

The upstream clone `upstream/temporal` is never modified. The adapter is injected with `go -overlay`, and
`git -C upstream/temporal status` stays clean.
