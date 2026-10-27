# UPSTREAM_MAP — Temporal matching / task-queue internals

| | |
|---|---|
| Upstream | github.com/temporalio/temporal |
| Tag | **v1.32.1** (latest stable GitHub release; `v1.33.0-*` tags are pre-releases, not used) |
| Commit | **af08a5e5f2dd40f545f2afac7780daaa6987001e** — "Make fair task gc conditional on rangeid (#12283) (#12411)", 2026-10-07 23:48 IST |
| Go | `go 1.26.8` in go.mod (no toolchain line); verified building/testing with go1.27.3 |
| License | MIT (© Temporal Technologies 2025, © Uber 2020) → copied to `../UPSTREAM_LICENSE` |
| Clone | `upstream/temporal` (fetched separately at tag v1.32.1, commit af08a5e5; pristine, `git status` clean; see parity/README.md) |

All paths are relative to `service/matching/` unless prefixed. Line numbers are for the commit above.
Only facts read in the source are stated; anything inferred is marked *(inference)*.

---

## 1. Layering (top → bottom)

```
matchingEngineImpl (matching_engine.go)            map[PartitionKey]taskQueuePartitionManager, lazy load
 └─ taskQueuePartitionManagerImpl (task_queue_partition_manager.go)  one per (ns, tq, type, partition)
     ├─ rateLimitManager (ratelimit_manager.go)    shared by all physical queues of the partition
     ├─ userDataManager (user_data_manager.go)     versioning / per-TQ config (Temporal-only)
     └─ physicalTaskQueueManagerImpl (physical_task_queue_manager.go)  default + versioned queues
         ├─ matcher: priTaskMatcher (pri_matcher.go) → matcherData (matcher_data.go)
         │      (classic TaskMatcher in matcher.go when useNewMatcher=false)
         ├─ forwarder: priForwarder (pri_forwarder.go), child partitions only
         ├─ backlog: fairBacklogManagerImpl | priBacklogManagerImpl | backlogManagerImpl
         │     ├─ fairTaskWriter / priTaskWriter / taskWriter     (ID allocation, batching)
         │     ├─ fairTaskReader / priTaskReader / taskReader     (per subqueue; ack levels, GC)
         │     └─ taskQueueDB (db.go)                              (lease, metadata, subqueues)
         └─ liveness (liveness.go)                 idle-unload timer
```

Three implementations coexist, chosen per partition in `computeEffectiveConfig`
(task_queue_partition_manager.go:211) and the pqm constructor (physical_task_queue_manager.go:129–293):

| mode | dyncfg | backlog | matcher | store ordering |
|---|---|---|---|---|
| classic | useNewMatcher=false | backlogManagerImpl | TaskMatcher (matcher.go) | task_id |
| pri | useNewMatcher=true (default, constants.go:1570) | priBacklogManagerImpl | priTaskMatcher | task_id per subqueue |
| fair | enableFairness=true (default **false**, constants.go:1576) | fairBacklogManagerImpl | priTaskMatcher | (pass, task_id) per subqueue |

`fairness ⇒ newMatcher` (computeEffectiveConfig: `newMatcher = newMatcher || fairness`; fairness also requires
`partition.SupportsFairness`). With `matching.autoEnableV2` (default false, constants.go:1634) a persisted
`fairnessState` V0/V1/V2 overrides both flags; `autoEnableIfNeeded` (:525) issues an UpdateFairnessState RPC.
A config change unloads the partition (`unloadCauseConfigChange`) so it reloads in the new mode.
**The library should port only the fair path (and treat "pri" as fair with all weights equal / no keys — see §4.6).**

---

## 2. Engine: lazy load, idle unload, long poll

- `getTaskQueuePartitionManager(ctx, partition, create, loadCause)` matching_engine.go:462. RLock lookup →
  if absent and `create`, build `newTaskQueueConfig`, `newUserDataManager`, `newTaskQueuePartitionManager`;
  double-checked insert under write lock (loser is dropped unstarted); `newPM.Start()`. Deferred
  `WaitUntilInitialized(ctx)`; on error → `unloadTaskQueuePartition(pm, unloadCauseInitError)`.
- Unload: `unloadTaskQueuePartitionByKey` :3166 — delete from map only if the same instance, then `Stop(cause)`.
- Partition Start (task_queue_partition_manager.go:297): rateLimitManager.Start → userDataManager.Start → hooks →
  `go initialize()` (:236, waits for user data, creates default physical queue). Root then force-loads children.
- Physical-queue unload (`unloadPhysicalQueue` :1946): unloading the **default** queue unloads the whole
  partition; versioned queues are removed alone and `ReprocessRedirectedTasksAfterStop`.
- Idle: `liveness` (liveness.go:23–50) = clock `AfterFunc(ttl)`; `markAlive` resets the timer. pqm wires
  `ttl = MaxTaskQueueIdleTime` (default 5m, constants.go:1320), onIdle → `UnloadFromPartitionManager(unloadCauseIdle)`
  (physical_task_queue_manager.go:172). markAlive is called on PollTask and on non-forwarded TrySyncMatch (:718).
- Long poll (`pollTask` matching_engine.go:3029):
  - `longPollInterval = LongPollExpirationInterval` (default 1m, constants.go:1278).
  - Non-forwarded polls subtract `FullJitter(interval/6)` (`forwardedPollJitterRatio=1/6` :78), capped so the
    interval stays ≥ `forwardedPollMinInterval = CriticalLongPollTimeout = 10s` (:82, common/constants.go:45).
  - `ctx = WithDeadlineBuffer(ctx, longPollInterval, returnEmptyTaskTimeBudget=1s)`
    (physical_task_queue_manager.go:41) → poll returns *empty* ~1s before the caller deadline instead of a
    DeadlineExceeded. Timeout surfaces as `errNoTasks` (pri_matcher.go poll :601).
  - Pollers from a recently shut-down worker instance get `errNoTasks` immediately.
- Partition PollTask (task_queue_partition_manager.go:729): checkPartitionCounts → pick physical queue
  (versioning) → `rateLimitManager.InjectWorkerRPS(pollMetadata)` (:846) → `dbq.PollTask` (:848).
- pqm PollTask (physical_task_queue_manager.go:482–536): markAlive; inactive namespace → query-only poll;
  loop `matcher.Poll`, dropping expired tasks (`dropReasonExpiredMemory`).

---

## 3. AddTask: sync match vs backlog

Partition `AddTask` (task_queue_partition_manager.go:556–688):
1. `checkPartitionCounts`; `getPhysicalQueuesForAdd` (versioning: picks spool queue + sync-match queue).
2. `newInternalTaskForSyncMatch` (task.go:135) + `setDefaultPriority`.
3. If the namespace is active: `syncMatchQueue.TrySyncMatch` (pqm :718–740 → `priMatcher.Offer`).
   Matched → return (except BUSY_WORKFLOW error; `errReprocessTask` → re-resolve).
4. Forwarded task (spoolQueue nil, i.e. this is a parent receiving from a child): return
   `errRemoteSyncMatchFailed` — parents never persist forwarded tasks; the child keeps it in its backlog.
5. Else `spoolQueue.SpoolTask(taskInfo)` → backlog writer.

`priTaskMatcher.Offer` (pri_matcher.go:390–443):
- `data.MatchTaskImmediately(task)` (matcher_data.go:361) — non-blocking; outcomes
  `Success | BacklogPresent | NoPoller | RateLimited`. **Returns BacklogPresent (no sync match) whenever
  there are in-memory backlog tasks unless the backlog is negligible** (`isBacklogNegligible` :598 = oldest
  backlog age < `BacklogNegligibleAge`, default 5s, constants.go:1442). This preserves backlog ordering.
- Only blocks when this is the **root** and the task is a forwarded DB_BACKLOG task (waits for a poller).
- Old-matcher path uses `SyncMatchWaitDuration` (200ms, constants.go:1284); priMatcher does not wait.
- TODO in code: a `startErr` after match is still counted as sync-match success.
- Query/Nexus use `syncOfferTask` (:445): waits until `deadline − returnEmptyTaskTimeBudget`; fails fast with
  `errNoRecentPoller` if no poll within `QueryPollerUnavailableWindow`.

Backlog dispatch: reader loads tasks → `addTaskToMatcher` → pqm `AddSpooledTaskToMatcher` (:593) →
`priMatcher.AddTask` (:515: `setRemoveFunc` + `EnqueueTaskNoWait`). Spooled-task validation (`ProcessSpooledTask`
:573 → taskValidator) runs on root/sticky only in the new matcher (`validateTasksOnRoot`).

---

## 4. Fairness (stride scheduling)

### 4.1 Ordering key
- `fairLevel{pass int64, id int64}` (fair_level.go). `less` (:29) is lexicographic (pass, then id);
  `inc()` (:54) = `{pass, id+1}` (used for exclusive bounds); `max` (:46). Tree map comparator :33.
- Persistence order inside a subqueue is `(pass, task_id)`. SQL PK `(range_hash, task_queue_id, pass, task_id)`
  with the subqueue folded into `task_queue_id` (common/persistence/sql/task_v2.go). Cassandra:
  common/persistence/cassandra/matching_task_store_v2.go.

### 4.2 Pass assignment (writer)
`fairTaskWriter.pickPasses` (fair_task_writer.go:118–141), per task in a write batch:
```
w      = getEffectiveWeight(overrides, priority)      // fairness_util.go:18
inc    = max(1, int64(1000 / w))                      // strideFactor=1000, fairness_util.go:12
base   = ackLevel[subqueue].pass                      // pinned ack level of that subqueue
if FairnessPassDither: base = base + maphash(seed,key) % inc   // ditherPass :39, default off (constants.go:1602)
pass   = counter[subqueue].GetPass(key, base, inc)
```
- `getEffectiveWeight`: override map (from user data, `FairnessWeightOverrides`) else `Priority.FairnessWeight`;
  `w ≤ 0 → 1.0`, else `max(w, 0.001)` (minWeight). So inc ∈ [1, 1_000_000].
- Empty fairness key `""` is just another key *(inference from code: no special-casing in pickPasses)*.
- Counter semantics (`counter.Counter`): returns `max(base, prev(key)+inc)`; for an unseen key
  `max(base, inc)` (counter/map.go:32–45). The returned pass is stored per key. Net effect: each key advances
  its virtual time by `1000/weight` per task; a key that fell behind the ack level restarts at the ack level.
- Counter implementation (`fairnessCounter`, default `DefaultCounterParams`, constants.go:1597):
  hybrid (counter/hybrid.go:52) = exact map with top-K heap until `MapLimit=100` keys, then migrates to a
  count-min sketch (counter/cmsketch.go:77: D=5 rows + 1 shadow, W=100, `pass = max(base, min_row+inc)`, written
  to all rows via `ensureByIndexes`; growth when skip rate > 0.35 over 10000 incs, ×1.5 up to W=10000; reseed
  every 250000 ops; uint32 offsets with `slideBase`). CMS over-estimates ⇒ a colliding key may get a pass that
  is too high (never too low). **Not concurrency-safe** — guarded by the writer loop being single-goroutine.
- Counter restore on load: `newCounterForSubqueue` (fair_backlog_manager.go:405) seeds from persisted TopK via
  `GetPass(key, count, 0)`. TopK persisted every `UpdateAckInterval` (fair_task_writer.go:191–198; db.go:432).

### 4.3 Write path
`fairTaskWriter.taskWriterLoop` (:155): requests from `appendCh` (cap `OutstandingTaskAppendsThreshold`=250,
constants.go:1326; full → `ResourceExhausted SYSTEM_OVERLOADED`, appendTask :62), batched up to
`MaxTaskBatchSize`=100 (constants.go:1331). `writeBatch` (:214–227): `allocTaskIDs` (:102–116) →
`getAndPinAckLevels` (fair_backlog_manager.go:244) → `pickPasses` → `db.CreateFairTasks` (db.go:600) →
`wroteNewTasks` (readers merge the new tasks directly into memory) → unpin ack levels.
Pinning prevents the ack level from advancing past a base that in-flight tasks were assigned relative to.

### 4.4 Read path
`fairTaskReader` (fair_task_reader.go), one per subqueue:
- In-memory `outstandingTasks` ordered by fairLevel; `readLevel` (max level loaded) / `ackLevel`.
- `shouldReadMoreLocked` (:231) — load when loaded < `GetTasksReloadAt` (100, constants.go:1273) and the DB may
  hold more. `readTaskBatch` (:282) → `GetFairTasks(InclusiveMin=readLevel.max({pass:1,id:0}).inc() (:284), PageSize=GetTasksBatchSize=1000)`
  (db.go:718); backoff 50ms→10s, ResourceExhausted 2s→30s.
- `mergeTasks`/`mergeTasksLocked` (:396, :432–563): merges DB reads and freshly written tasks; keeps at most
  `batchSize` in memory (cut at batchSize+1), **evicts** the highest-level tasks beyond that (`setEvicted`,
  task.go:356, removes from matcher) and lowers readLevel to below the eviction point so they're re-read later.
  Acks for evicted-then-completed tasks are remembered in `evictedAcks` (≤256, :101).
- `completeTask` (:150): transient/deadline/cancel errors → re-add to matcher; other errors →
  `respoolTaskAfterError` (fair_backlog_manager.go:367: re-write as new task; failure unloads); success → ack.
- `advanceAckLevelLocked` (:611): ackLevel = highest level such that all outstanding ≤ it are acked; not while
  pinned (:605).
- GC (`maybeGCLocked` :678 / `doGCAt` :717): when gap ≥ `MaxTaskDeleteBatchSize`(100) or `TaskDeleteInterval`
  (15s) elapsed → `CompleteFairTasksLessThan(ackLevel.inc())` with `ConditionRangeID` (db.go:769) — the commit
  at this tag makes fair GC conditional on rangeID.

### 4.5 Matcher ordering
`taskBTreeLess` (matcher_data.go:137): `(effectivePriority, fairLevel, pointer)` ascending. Tasks from all
subqueues/priorities of a physical queue live in one B-tree, so **cross-key fairness is enforced by persisted
pass order + in-memory merge**; there is no separate in-memory round-robin.

### 4.6 Subqueues / priority
- Priority key range `[1, PriorityLevels]`, default `PriorityLevels=5` (constants.go:1582), default key
  `(levels+1)/2 = 3` (config.go:403–405). `clipPriority` (config.go:583): 0 → default; else clamp to [1, levels].
- **Bug:** config.go:403–405 computes `max(p, min(p, maxPriorityLevels), 1)` = `max(p,1)`; the 60 cap
  (`maxPriorityLevels`, matcher_data.go) is ineffective.
- `effectivePriority = 10 × priorityKey` (`effectivePriorityFactor=10`; `setDefaultPriority` config.go:592);
  lower = dispatched first. Draining (migration) queues get `−10×60 = −600` (fair_backlog_manager.go:446).
  Remote priority-backlog forwarders use `10p+1`; poll-forwarder pseudo-tasks use 1e6 (`pollForwarderPriority`).
- One subqueue per priority key: `getSubqueueForPriority` (fair_backlog_manager.go:183) → `db.AllocateSubqueue`
  (db.go:806, persisted in TaskQueueInfo.Subqueues); defaults ensured at db.go:921.

---

## 5. Rate limiting

### 5.1 Limiter primitive — `simpleLimiter` (matcher_data.go:679–748)
GCRA storing `ready` (unix nanos when the next token is available):
```
params(rate, burstDur): rate ≤ 1e-9 → interval = −1 ("never"); else interval = 1s/rate, burst = min(burstDur, 1m)
interval == 0  → unlimited (params zero value)
delay(now)     = ready − now                         // ≤ 0 ⇒ may dispatch
consume(now,n) = max(now, ready+burst) − burst + n·interval     ; never ⇒ 7<<60
clip(now,k)    = min(ready, now + k·interval + burst)           ; applied on param change, k = maxTokens = 1
```
`defaultBurstDuration = 1s` (ratelimit_manager.go:58–66).

### 5.2 Effective queue rate (`rateLimitManager`, ratelimit_manager.go)
- Inputs: `workerRPS` (poller's `TaskQueueMetadata.MaxTasksPerSecond`, `InjectWorkerRPS` :186 on every poll),
  `apiConfigRPS` (user data `QueueRateLimit`, `trySetRPSFromUserDataLocked` :225), `adminNsRate`
  (`admin.matchingNamespaceToPartitionDispatchRate`=10000, constants.go:32), `adminTqRate`
  (`admin.matchingNamespaceTaskqueueToPartitionDispatchRate`=1000, constants.go:37), `numReadPartitions`.
- `computeEffectiveRPSAndSourceLocked` (:139–168):
  `systemRPS = min(adminNsRate, adminTqRate)` (per partition; the comment suggests ×partitions but code does not);
  `userRPS = (apiConfigRPS ?? workerRPS) × fraction / numReadPartitions` (API wins over worker);
  `effectiveRPS = min(userRPS, systemRPS)`, source API/WORKER/SYSTEM. `fraction` from
  rate_limit_fraction_provider.go, clamped [0,1], default 1.
- `computeAndApplyRateLimitLocked` (:170): **only when effectiveRPS changed** → `updateRatelimitLocked` (old
  matcher token bucket; burst `max(ceil(rps), MinTaskThrottlingBurstSize=1)`) and
  `updateSimpleRateLimitWithBurstLocked` (sets `wholeQueueLimit`, clips `wholeQueueReady`). Always →
  `updatePerKeySimpleRateLimitWithBurstLocked` (:286).
- **Observed (probe test, since deleted):** with defaults, after Start/InjectWorkerRPS(nil)/UserDataChanged,
  `effectiveRPS=systemRPS=1000` but `wholeQueueLimit` stays the zero value (**unlimited**); `Start` (:92) only
  computes. So the default 1000/partition system cap is not enforced until the effective RPS changes once.
  Parity tests must reproduce or explicitly diverge from this.
- `GetEffectiveRPSAndSource` (:203) reports `effectiveRPS × numReadPartitions` (whole-queue view).

### 5.3 Per-fairness-key limit
- Single default rate `FairnessKeysRateLimitDefault` (user data) × fraction / numReadPartitions, per partition.
- Per-key state: LRU `perKeyReady` (size `FairnessKeyRateLimitCacheSize`=2000, constants.go:1610).
- `readyTimeForTask` (:336) = `max(wholeQueueReady, perKeyReady[key])`.
- `consumeTokens(now, task, n)` (:356): skip forwarded tasks; consume whole-queue; if per-key enabled consume the
  key's limiter with `interval / weight` (float32 weight from §4.2) — weight scales the per-key rate.
  Called with n=1 on match and n=−1 via `recycleToken` (matcher_data.go:571) when a matched task is not used.
- Weight overrides capped to `MaxFairnessKeyWeightOverrides`=1000 (`mergeFairnessWeightOverrides`, fairness_util.go:43).

### 5.4 Interaction with matching — `findMatch` (matcher_data.go:425–495)
```
(wqReady, perKey) = rateLimitState()
if !perKey && tasks>0 && pollers>0 && wqReady.delay(now) > 0: return delay   // whole queue blocked
for task in taskTree (ascending):
    if perKey: d = readyTimeForTask(task).delay(now); if d>0 { minDelay=min(minDelay,d); continue }
    if task is parent poll-forwarder and !allowForwarding: continue
    for poller in pollers (FIFO):
        skip if poller.queryOnly && !(task.isQuery || task.isPollForwarder)
        skip if task.isPollForwarder && poller.forwardCtx == nil
        skip if poller is parentTaskForwarder && !allowForwarding
        skip if poller is validatorTaskForwarder && task.forwardCtx != nil
        skip if poller.minPriority>0 && task.effectivePriority > 10·minPriority
        → match (task, poller)
```
`findAndWakeMatches` (:531–569) loops: remove both, `consumeTokens(now, task, 1)`, set `task.recycleToken`, wake.
No match but `minDelay>0` → arm `rateLimitTimer` and record rate-limited. With per-key limiting a throttled key
does not block other keys (head-of-line skipping); without it the whole queue waits.
`allowForwarding` (:498–528): always if backlog negligible; otherwise only after `MaxWaitForPollerBeforeFwd`
(200ms, constants.go:1450) since the last local poller.

---

## 6. Task IDs, lease, ack levels (`taskQueueDB`, db.go)

- Ownership = `rangeID` on the TaskQueueInfo row, advanced with CAS (`UpdateTaskQueue{RangeID, PrevRangeID}`,
  data_interfaces.go:508–512). `RenewLease` (db.go:152) / `takeOverTaskQueueLocked` (:176) increment rangeID.
- ID block: `rangeIDToTaskIDBlock(r, size)` (backlog_manager.go:274) = `[(r−1)·size+1, r·size]`,
  `RangeSize = 100000` (config.go:296). Exhausted block → `allocTaskIDBlock` (fair_task_writer.go:247–260) renews
  lease; `errNonContiguousBlocks` if another owner took over in between (→ unload).
- All task writes carry the rangeID; store returns `ConditionFailedError` on mismatch → `unloadCauseConflict`.
- `SyncState` (:298, periodic): writes ack levels/backlog stats if dirty; also re-verifies ownership when
  more than ttl/2 since last write (`verifyOwnershipLocked` :318). `updateFairAckLevel` (:370).
  `MetadataUpdateOnAppendInterval`=5s piggybacks metadata on CreateTasks (`UpdateMetadata`).
- pri/classic ack: `priTaskReader.ackTaskLocked` (pri_task_reader.go:451) — treemap TaskID→acked; ack level =
  longest acked prefix; if fully drained ackLevel = readLevel. `setReadLevelAfterGap` (:483) advances ack over
  empty ranges. GC `doGCAt` (:558) uses `CompleteTasksLessThan(ackLevel+1)`. Classic `ackManager`
  (ack_manager.go:14–130) is the same idea.

---

## 7. Forwarding / partition tree

- Partitions: `NumWritePartitions`/`NumReadPartitions` default 4 (`GlobalDefaultNumTaskQueuePartitions`,
  shared_constants.go:9; 1 for system & per-NS worker TQs). Parent of partition p:
  `(p + degree − 1)/degree − 1` (common/tqid/task_queue_id.go:430), degree = `ForwarderMaxChildrenPerNode`=20.
  With 4 partitions, 1–3 all forward to root 0.
- `priTaskMatcher.Start` (pri_matcher.go:123–152): child → `ForwarderMaxOutstandingTasks`(1) task-forward
  goroutines (rate `ForwarderMaxRatePerSecond`=10, retry 1s→`BacklogTaskForwardTimeout`=60s) and
  `ForwarderMaxOutstandingPolls`(1) poll-forward goroutines; root/sticky → `validateTasksOnRoot`.
  TODO (:135): >1 outstanding task forwarder unsupported.
- Task forwarding = a forwarder goroutine acts as a *poller* on the child's matcher, takes a task and calls the
  parent's AddTask via matching RPC (`priForwarder.ForwardTask` pri_forwarder.go:62). Parent sync-matches or
  rejects (`errRemoteSyncMatchFailed`); child re-adds on failure.
- Poll forwarding = a pseudo-task (`newPollForwarderTask`, task.go:230, priority 1e6) in the child's matcher;
  when it matches a local poller, the poll is forwarded to the parent (`ForwardPoll` :206). `errNoTasks` →
  re-enqueue + sleep 4×EphemeralDataUpdateInterval; ResourceExhausted → backoff (`forwardPolls` pri_matcher.go:287).
- Priority-backlog forwarding (`PriorityBacklogForwarding`=true): poll forwarders carry
  `PollConditions{MinPriority, NoWait}` so the parent only returns tasks better than the child's backlog.

---

## 8. Persistence interface (common/persistence/data_interfaces.go)

`TaskManager` (:1183): `CreateTaskQueue, UpdateTaskQueue, GetTaskQueue, ListTaskQueue, DeleteTaskQueue,
CreateTasks, GetTasks, CompleteTasksLessThan, Get/UpdateTaskQueueUserData, ListTaskQueueUserDataEntries,
GetTaskQueuesByBuildId, CountTaskQueuesByBuildId`. `FairTaskManager TaskManager` (:1220) is a **distinct named
type with the same method set** (not an alias) so DI can bind a different store (v2 tables).

Request semantics used by matching:
- `CreateTasksRequest` (:610): `TaskQueueInfo{RangeID}` condition; `Tasks []*AllocatedTaskInfo` (TaskId, TaskPass);
  `Subqueues []int` parallel to Tasks; `UpdateMetadata` → also write the queue row. All-or-nothing; rangeID
  mismatch → `ConditionFailedError`.
- `GetTasksRequest` (:633): classic `[InclusiveMinTaskID, ExclusiveMaxTaskID)`; fair `≥(InclusiveMinPass≥1,
  InclusiveMinTaskID)` no upper bound; `Subqueue`, `PageSize`, `UseLimit`. Results in key order.
- `CompleteTasksLessThanRequest` (:656): delete `< ExclusiveMaxTaskID` or `< (ExclusiveMaxPass, ExclusiveMaxTaskID)`
  in `Subqueue`, `Limit` (may be ignored), optional `ConditionRangeID`. Returns rows deleted or
  `UnknownNumRowsAffected=−1` ("all").
- Conformance suite: common/persistence/tests/task_queue_fair_task.go (Order, Range delete, ConditionRangeID);
  runs on SQLite in-memory/file, MySQL, Postgres, Cassandra (sqlite_test.go:333, :575).

---

## 9. Temporal-only dependencies → proposed library interfaces

| Upstream dependency | Used for | Proposed interface |
|---|---|---|
| `persistence.TaskManager`/`FairTaskManager` | queue row + tasks | `Store{ LoadQueue/CreateQueue(ctx,id) (Meta,rangeID); UpdateQueue(ctx, Meta, prevRangeID) ; CreateTasks(ctx, rangeID, []Task, updateMeta) ; ReadTasks(ctx, q, subq, from Level, limit) ; DeleteTasksBelow(ctx, q, subq, Level, limit, rangeID) (n int, err)}` |
| historyClient RecordWorkflow/ActivityTaskStarted (matching_engine.go:3465, :3546) | "start" a task after match; errors drive re-add/drop | `Starter{ Start(ctx, Task, PollerInfo) (Result, error) }` with classified errors (Retryable / Drop / Busy) |
| task_validation.go taskValidator (:24) | expire stale backlog tasks on root | `Validator{ Valid(ctx, Task) (bool, error) }`, optional |
| matchingRawClient (forwarder, user data) | partition-tree RPCs | `PartitionRouter{ AddTask(ctx, partition, Task); Poll(ctx, partition, PollReq) (Task, error) }`; in-process impl first |
| userDataManager + versioning (worker_versioning) | per-queue config (QueueRateLimit, FairnessKeysRateLimitDefault, FairnessWeightOverrides) | `QueueConfigSource{ Get(q) QueueConfig; Subscribe(q, func(QueueConfig)) }`; drop versioning |
| dynamicconfig (constants.go) | ~40 knobs | plain `Config` struct with the defaults in §10 + per-queue override func |
| namespace.Registry, cluster metadata | active/passive, names, rate fraction | drop; `fraction` → config |
| membership / ownership | who owns a partition | out of scope; rangeID fencing is the safety mechanism |
| clock.TimeSource, metrics.Handler, log.Logger, testhooks | infra | `Clock` (Now, AfterFunc), `Metrics`, `*slog.Logger`; remove testhooks |
| protos (persistencespb.TaskInfo, commonpb.Priority) | task payload | library-owned `Task{ID, Pass, Priority{Key, FairnessKey, Weight}, ExpiresAt, Payload []byte}` |

---

## 10. Defaults to carry over (common/dynamicconfig/constants.go)

syncMatchWaitDuration 200ms (1284) · getTasksBatchSize 1000 (1268) · getTasksReloadAt 100 (1273) ·
longPollExpirationInterval 1m (1278) · updateAckInterval 1m (1294) · metadataUpdateOnAppendInterval 5s (1311) ·
maxTaskQueueIdleTime 5m (1320) · outstandingTaskAppendsThreshold 250 (1326) · maxTaskBatchSize 100 (1331) ·
maxTaskDeleteBatchSize 100 (1336) · taskDeleteInterval 15s (1341) · numTaskqueueWrite/ReadPartitions 4
(1351/1356) · forwarderMaxOutstandingPolls 1 (1385) · forwarderMaxOutstandingTasks 1 (1390) ·
forwarderMaxRatePerSecond 10 (1395) · forwarderMaxChildrenPerNode 20 (1400) · shutdownDrainDuration 0 (1411) ·
priorityBacklogForwarding true (1437) · backlogNegligibleAge 5s (1442) · maxWaitForPollerBeforeFwd 200ms (1450) ·
membershipUnloadDelay 500ms (1482) · useNewMatcher true (1570) · enableFairness false (1576) · priorityLevels 5
(1582) · backlogTaskForwardTimeout 60s (1587) · forwardPollRetryMaxInterval 10s (1592) · fairnessCounter
DefaultCounterParams (1597) · fairnessPassDither false (1602) · fairnessKeyRateLimitCacheSize 2000 (1610) ·
maxFairnessKeyWeightOverrides 1000 (1615) · autoEnableV2 false (1634) · minTaskThrottlingBurstSize 1 (1263) ·
admin ns→partition rate 10000 (32) · admin ns+tq→partition rate 1000 (37). Code constants: RangeSize 100000,
returnEmptyTaskTimeBudget 1s, defaultBurstDuration 1s, maxBurst 1m, strideFactor 1000, minWeight 0.001,
effectivePriorityFactor 10, maxPriorityLevels 60, evictedAcksCacheSize 256, defaultTaskDispatchRPS 100000
(old matcher, task_queue_partition_manager.go:59).

---

## 11. Portable upstream tests

All run with plain `go test` (unit) — verified: fair backlog, matcher data, limiter, ratelimit manager,
fair pqm and counter suites pass in ~13s.

| File | Lines | Covers | Deps beyond testify |
|---|---|---|---|
| fair_level_test.go | 31 | fairLevel ordering | none → copy verbatim |
| fairness_util_test.go | 131 | weights, dither, override merge | counter |
| counter/{map,hybrid,cmsketch}_test.go | 130/126/274 | counter semantics, growth, reseed | none → copy |
| matcher_data_test.go | 1266 | match rules, priority, rate limits, forwarding gates; `TestSimpleLimiter*` (:1001–1126) | clock, tqid, protos |
| ratelimit_manager_test.go | 233 | effective RPS precedence/sources | dynamicconfig, namespace, tqid |
| liveness_test.go, task_tracker_test.go | 46/66 | idle timer, rate tracker | clock |
| ack_manager_test.go | 235 | classic ack levels | dynamicconfig, tqid |
| backlog_manager_test.go | 1334 | Classic/Pri/**Fair** suites (:54–66): spool, read, ack, GC, faults | gomock, persistence, counter |
| pri_matcher_test.go | 273 | forwarding, validation | matchingservicemock, gomock |
| physical_task_queue_manager_test.go | 792 | pqm per mode; reader signaling; drain | mocks, namespace |
| task_queue_partition_manager_test.go | 2277 | partition flows incl. versioning | heavy mocks |
| matching_engine_test.go | 7474 | end-to-end engine with in-memory `testTaskManager` (:5496–5980), Classic/Pri/Fair suites (:152–161) | history/matching mocks, everything |
| common/persistence/tests/task_queue_fair_task.go | 252 | store conformance (fair) | SQL/Cassandra plugins |
| tests/priority_fairness_test.go | 1030 | functional: priority order, fairness `unfairness()<1.0` metric (:602), migrations | full cluster (testcore) |

Caveat: `testTaskManager` keys tasks only by fairLevel per queue and **ignores `Subqueue`** in
CreateTasks/GetTasks/CompleteTasksLessThan, and validates fair requests (`InclusiveMinPass≥1`,
`ExclusiveMaxTaskID==MaxInt64`). Tests using multiple priorities against it are not exercising real subqueue
isolation; port it with Subqueue support.

---

## 12. Differential-harness options

1. **Functional test cluster (tests/testcore) — recommended.** In-process onebox (frontend+history+matching)
   on SQLite by default (tests/testcore/flag.go:21). Verified: `go test -tags test_dep ./tests/ -run
   'TestFairnessSuite/Test_Activity_Basic'` passes (~34s incl. compile, ~1s run; logged unfairness 0.45).
   **Requires `-tags test_dep`** (otherwise testhooks panic). Dynamic config per test via
   `testcore.WithDynamicConfig`. Drive with FrontendClient Start/Schedule + `TaskPoller`; record dispatch order
   and timestamps, then replay the same script against the library. Cost: whole server dependency tree, but it is
   the only option exercising real persistence + history start semantics.
2. **In-process engine with `testTaskManager` + mocked history** (matching_engine_test.go). Fast and
   deterministic, exposes internal order directly, but needs gomock history client and is in package
   `matching` (internal types) — a harness would have to live inside the upstream module (test file added to a
   scratch copy). Good for fine-grained matcher/limiter parity.
3. **`temporal server start-dev`.** Not installed on the box; the CLI embeds its own server version (may not
   equal v1.32.1) and fairness needs dynamic config flags. Black-box only; use for smoke checks, not parity.

Plan *(proposal)*: (2) for unit-level parity of pass assignment / matcher order / limiter timing using a
fake clock; (1) for end-to-end order and throughput parity.

---

## 13. Surprises & risks

1. Default whole-queue system limit (1000 rps/partition) is computed but **not applied** until effectiveRPS
   changes (ratelimit_manager.go:92, :170) — probe-confirmed.
2. `priorityLevels` 60-cap ineffective (config.go:403–405).
3. `systemRPS` is per partition; comment implies otherwise (ratelimit_manager.go:139–168).
4. Per-key RPS is one default for all keys (`FairnessKeysRateLimitDefault`), scaled per key by its effective weight (ratelimit_manager.go:369–371); the only per-key knob is the weight override map.
5. Fairness is default-off upstream (enableFairness=false); parity tests must enable it explicitly.
6. Dither defaults off; with base=0 (fresh queue) new keys all start at pass=inc, so ties break by task ID.
7. CMS counter is approximate after 100 distinct keys/subqueue → pass order is not exact stride beyond that;
   parity tests with >100 keys must use identical counter params and seeds (`rand.Source` passed at
   counterFactory, physical_task_queue_manager.go:871).
8. Counters are not thread-safe; correctness relies on the single writer goroutine.
9. Eviction + `evictedAcks` (≤256) means memory holds ≤ batchSize tasks; acks beyond the cache rely on GC by
   ack level — porting must keep the pin/evict interplay exact or tasks may be redelivered.
10. Fair GC is rangeID-conditional only as of this tag (#12283) — older behaviour differs.
11. `ForwarderMaxOutstandingTasks>1` unsupported (pri_matcher.go:135); `Offer` counts startErr as success.
12. `testTaskManager` ignores subqueues (§11).
13. Functional tests need `-tags test_dep`.

---

## 14. Library files for slices 3–4 (upstream → this repo)

Paths on the left are upstream (`service/matching/` unless prefixed); on the right, this repo.

| Upstream | Library | Notes |
|---|---|---|
| `fair_backlog_manager.go`, `fair_task_reader.go`, `fair_task_writer.go`, `fair_level.go`, `fairness_util.go` | `internal/matching/` same names | Slice 3. The fair path is the only backlog; there is no `enableFairness` switch. |
| `counter/{counter,map,cmsketch,hybrid}.go` (+ tests) | `internal/counter/` | Slice 3. Fairness pass counter. |
| `fair_level_test.go`, `fairness_util_test.go` | `internal/matching/` same names | Slice 3. Ported tests. |
| user-data fairness weight overrides (`UpdateTaskQueueConfig` validation in `matching_engine.go`) | `internal/matching/queue_config.go` (`MemoryQueueConfigSource`) | Slice 3. Merge and validation rules; slice 4 adds the rate setters and `QueueConfigWatcher`. |
| `ratelimit_manager.go` | `internal/matching/ratelimit_manager.go` | Slice 4. Simple-limiter path only. `dynamicRateLimiter` is dropped because it was for the old matcher. The time source is injected. |
| `ratelimit_manager_test.go` | `internal/matching/ratelimit_manager_test.go` | Slice 4. `TestUpdatePerKey…WhenFairnessKeyRateLimitDefaultIsNil`, the 4 `TestFractionScaling*` tests, plus library tests. |
| `matcher_data.go` §5.1 limiter primitive | `internal/matching/matcher_data.go` | Primitive identical since slice 2. Slice 4 replaces the stub interface with `*rateLimitManager`. |
| `matcher_data_test.go` rate tests | `internal/matching/matcher_data_test.go` | Slice 4. 7 tests: `TestMatchTaskImmediatelyRateLimited`, `TestSyncMatchRateLimitedIncrementsStats`, `TestBacklogRateLimitedIncrementsStats`, `TestRateLimitedBacklog`, `TestPerKeyRateLimit`, `TestPerKeyRateLimitDoesNotBlockOtherKeys`, `TestPerKeyRateLimitRecycleWakesBlockedMatch`. |
| `common/cache/lru.go` (+ test) | `internal/cache/lru.go` | Slice 4. Count-limited generic LRU for the per-key limiter (default 2000 keys). There are no TTL, pin or metrics features. `Entries()` returns a snapshot in place of the locked iterator. |
| `task_queue_partition_manager.go` `userDataChanged` / `InjectWorkerRPS` | `internal/matching/task_queue_partition_manager.go` | Slice 4. Config-source notification → `rlm.UserDataChanged` → `ReprocessAllTasks`. |
| dynamic config `AdminNamespaceToPartitionDispatchRate`, `AdminNamespaceTaskQueueToPartitionDispatchRate`, `MatchingNumTaskqueueReadPartitions`, `FairnessKeyRateLimitCacheSize`, rate-limit fraction | `matching.Config` fields of the same meaning | Slice 4. The default `NumReadPartitions` is 1, not 4, because the library has a single partition (partitions are not implemented; see `docs/PARITY_REPORT.md` §5). |

Upstream rate tests that were not ported:
- `TestSyncMatchActivities` and `TestConcurrentPublishConsume*` cover the old matcher or are skipped under the pri matcher.
- `TestRateLimiterAcrossVersionedQueues` needs versioned queues.
- `TestTaskAddHooks_RateLimited` and the PollScaling tests need hooks and poll scaling, which are not ported.

---

## 15. Library files for slice 5: durable stores (upstream → this repo)

| Upstream | Library | Notes |
|---|---|---|
| `common/persistence/sql/task_v2.go` (`sqlTaskManagerV2`: `CreateFairTasks`, `GetFairTasks`, `CompleteFairTasksLessThan`), `task_queues.go` (create/get/update/delete queue, rangeID CAS), `task_v1.go` (`lockTaskQueue`), `common.go` (`txExecute`) | `store/internal/sqlcore/sqlcore.go` | Dialect-neutral `store.Store`. CreateTasks locks the queue row, checks range_id, optionally updates metadata, and inserts in one tx (all-or-nothing). CompleteTasksLessThan is rangeID-conditional (#12283 semantics) with a LIMIT. GetTasks uses an inclusive `(pass,task_id) >=` lower bound and page limit. |
| `common/persistence/sql/task_util.go` (range hash / queue id) | `store/internal/sqlcore/ids.go` | Length-prefixed queue id (namespace, name, type, partition) and a range_hash. |
| `sqlplugin/postgresql/{task_v2,task_queues}.go` queries | `store/postgres/postgres.go` (dialect) | `$n` placeholders, `FOR UPDATE`, ctid range delete. Error class 53 (insufficient resources) → ResourceExhausted, other driver errors → Transient. A duplicate CreateQueue uses `ON CONFLICT DO NOTHING` → ConditionFailed (upstream uses IsDupEntryError). |
| `sqlplugin/sqlite/{plugin,task_v2,task_queues}.go` | `store/sqlite/sqlite.go` | modernc driver, WAL, busy_timeout, single writer conn + read-only pool, `_txlock=immediate`. Driver errors, including SQLITE_BUSY/LOCKED, → Transient. |
| `schema/postgresql/v12/temporal/versioned/v1.18/tasks_v2.sql` | `store/postgres/migrations/0001_init.sql` | `tq_` prefix, explicit `subqueue` column in PK. |
| `schema/sqlite/v3/temporal/versioned/v0.10/tasks_v2.sql` | `store/sqlite/migrations/0001_init.sql` | Same changes; `WITHOUT ROWID`. |
| `tools/sql` (temporal-sql-tool) | `store/internal/sqlcore/migrate.go` | Replaced by an embedded, explicit `Migrate` (one tx, `tq_schema_version`, advisory lock on PG). |
| `common/persistence/tests/task_queue_fair_task.go` | `store/storetest/suite.go` | Slice 2 suite. It now runs against all three stores (SQLite twice: FULL and NORMAL). |

Deviations in slice 5:
- **Encoding.** Rows are JSON in the `data` column with `data_encoding='json'`, where upstream stores protobuf blobs. The library has no proto types.
- **Duplicate (pass, task_id) insert overwrites (upsert).** This matches memstore and upstream's Cassandra semantics, and makes CreateTasks retries after an ambiguous commit idempotent.
- **DeleteQueue also deletes the queue's tasks** (upstream leaves them to scavengers). Limit 0 means unlimited on reads and deletes.
- **Range delete via `ctid = ANY(ARRAY(SELECT ctid … LIMIT n))` on Postgres.** pgx's cached statements get generic plans, and the upstream `IN (subquery)` form seq-scanned the table under a generic plan (drain fell to ~1.3k tasks/s).
- **SQLite WAL switch with retry.** `journal_mode=WAL` is set once after open, retrying SQLITE_BUSY until the busy timeout. The switch bypasses the busy handler when several processes open a fresh file.
- **Engine fixes found by the durable-store stress runs** (each carries a deviation comment):
  - `fair_task_reader.go` `doGC` keeps `numToGC ≥ 1` after a partial or limit-bounded delete. Otherwise acked rows from before a reload could be stranded at a subqueue ack level.
  - `db.go` uses a change counter (`changeSeq`/`writtenSeq`) instead of comparing `lastChange`/`lastWrite` timestamps, which could tie under an injected or coarse clock and skip a metadata write.
  - `fair_task_writer.go` persists TopK fairness counts before replying to AddTask callers.

---

## 16. Slice 6: ported tests and parity harness (upstream → this repo)

| Upstream | This repo | Notes |
|---|---|---|
| `matching_engine_test.go` `TestAddConsumeWorkflowTasksNoDBErrors`, `…DBErrors`, `TestConcurrentAddWorkflowTasksNoDBErrors` | `internal/matching/engine_port_test.go` (`TestPort*`) | memstore fault injection replaces `testTaskManager.addFault`. |
| `pri_matcher_test.go` `TestValidatorWorksOnRoot`, `TestValidatorDrop_SetsDropReason` | `internal/matching/pri_matcher_test.go` | The second found the drop-reason bug (parity/FINDINGS.log). |
| `physical_task_queue_manager_test.go` (4 fair-suite tests) | `internal/matching/physical_task_queue_manager_test.go` | Dropped tests and the reasons are listed in the file header. |
| `tests/priority_fairness_test.go` `PrioritySuite.TestActivity_Basic`, `FairnessSuite.Test_Activity_Basic` | `priority_fairness_test.go` (root) | Adapted from the functional cluster to the public API; same wrongorderness/unfairness metrics. |
| `matching_engine_test.go` `SetupTest` (engine wiring) | `parity/upstream/parity_upstream_test.go` (build tag `parity_upstream`) | Compiled into the pristine upstream tree with `go test -overlay`; never part of the library. |

The harness design, results, intentional differences and gaps are in `docs/PARITY_REPORT.md`.
