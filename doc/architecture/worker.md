# Worker internals (`src/w*`)

*Current as of 2026-08, derived from code inspection. Trust the code over this doc if
they diverge; update this doc when you change the code.*

The worker is one process, `qserv-worker-http` (`src/wmain/`), colocated with a MariaDB
holding chunk tables and with the `qserv-replica-worker` agent. It exposes **two** HTTP
servers:

1. The REST API (`wcomms::HttpSvc`) on the configured `replication.http_port` (25010 in
   deployments) — used by *both* czars (`/queryjob`, `/querystatus`) and the
   replication system (`/replica*`, `/inventory`) plus monitoring endpoints.
2. A **separate result-file server** owned by `wcontrol::Foreman`
   (`src/wcontrol/Foreman.cc` — `qhttp::Server::create(_io_service, 0)`, i.e. an
   **OS-assigned port**) that serves the results directory as static content and
   accepts `DELETE /:file`. The worker advertises both ports to the registry
   (`management-port` vs `data-port`).

## Startup (`src/wmain/WorkerMain.cc`)

Order: config (`wconfig::WorkerConfig`) → schedulers (GroupScheduler + Scan
fast/med/slow + snail, blended by `wsched::BlendScheduler`) → `wpublish::QueriesAndChunks`
global stats/booting threads → `wcontrol::SqlConnMgr` → `ChunkInventory` (retries
`SELECT db FROM qservw_worker.Dbs` forever, 1 s loop — this doubles as the MariaDB
readiness gate) → `Foreman` → results-dir GC (deletes *all* result files on restart;
`results.clean_up_on_start`, default true) →
`wcomms::HttpSvc` → a registry heartbeat thread POSTing worker name (the UUID from
`qservw_worker.Id`, not the config name), FQDN, and both ports every ~1 s.

Scan schedulers partition tasks by scan rating (`protojson::ScanInfo::Rating`:
FASTEST=0..SLOWEST=100): fast [0,10], med [11,20], slow [21,30], snail [31,100].

## From UberJob to Tasks (`wcomms/`, `wbase/`)

`POST /queryjob` (`wcomms/HttpWorkerCzarModule.cc`) parses `protojson::UberJobMsg`,
registers the query with `QueriesAndChunks` (rejecting UberJobs for already-cancelled
queries or already-dead UberJob ids), and creates `wbase::UberJobData` — which owns the
single `FileChannelShared` output channel, the czar contact info, row limit, and
`maxTableSizeBytes` (arrives **in the message**, not from worker config). Actual task
building is deferred to a scheduler command; the HTTP handler returns immediately, and
later failures are reported asynchronously to the czar.

Task fan-out (`wbase/Task.cc`, `createTasksFromUberJobMsg`): UberJob → one job per
chunk → fragments → sub-query templates → subchunks. One `Task` per (template ×
subchunk); chunk-only fragments get a single task with `subchunkId = -1`. Query
templates are interned per-query in `wbase::UserQueryInfo` and expanded at run time by
substituting the `%CC%`/`%SS%` placeholder tags with chunk/subchunk numbers. All tasks
of an UberJob share one result file. Task states:
`CREATED → QUEUED → STARTED → EXECUTING_QUERY → READING_DATA → FINISHED`.

## Scheduling (`wsched/`)

`BlendScheduler::queCmd` routes a whole UberJob's tasks by the **first** task:
interactive or no scan tables → GroupScheduler (effectively a FIFO for point queries);
otherwise the ScanScheduler whose rating window matches; queries already "booted" go to
the snail scheduler. Key mechanics:

- **Shared scans:** each ScanScheduler wraps a `ChunkTasksQueue` — a map keyed by
  chunkId with a rotating "active chunk" cursor. Tasks arriving for the active chunk
  wait in a pending set so a chunk can't be monopolized; new chunks only open while
  the scheduler is under its `maxactivechunks` limit; within a chunk, tasks for the
  slowest tables run first. UberJobs are built chunk-ordered czar-side so workers'
  scans stay aligned.
- **Thread reservation:** each sub-scheduler reserves `inFlight + 1` threads up to its
  configured reserve, so no scheduler is starved; pool floor is 11 threads.
- **Booting** (`wpublish/QueriesAndChunks::examineAll`, every ~2 min): tasks running
  longer than their scheduler's `scanmaxminutes` allowance are "booted" — the thread
  *leaves the pool* (task keeps running as untracked load, pool spawns a replacement)
  — and queries exceeding `maxtasksbootedperuserquery` are moved wholesale to the
  snail scheduler. There is currently **no escalation past snail** (TODO in
  `BlendScheduler.cc`: should ask the czar to cancel). The per-chunk timing model that
  was meant to drive booting is currently bypassed (`useTimeStatisticsForBoot` is
  hard-coded false).

## Execution (`wdb/`)

`wdb::QueryRunner::runQuery` per task: admission through `wcontrol::SqlConnMgr`
(separate budgets for interactive vs scan connections — **note** the config names
`maxsqlconn`/`reservedinteractivesqlconn` map inversely to the internal scan/shared
limits and the defaults appear to violate the constructor's own validation; check the
deployed config, there's a TODO to fix this) → fresh MySQL connection **always as user
`qsmaster`** (`Task::defaultUser`; the configured `mysql.username` is only used for
inventory/subchunk management connections) → acquire `ChunkResource` → run the
(unbuffered) query → stream rows into the shared result file.

**Subchunks:** `wdb::ChunkResource`/`SQLBackend` refcount and materialize subchunk and
overlap tables on demand as `ENGINE = MEMORY` tables:
`Subchunks_<db>_<chunk>.<tbl>_<chunk>_<sub> AS SELECT ... WHERE subChunkId = <sub>`
(`src/wbase/Base.cc`), dropped when the refcount hits zero — currently per *task*, not
per chunk, so near-neighbor queries can rebuild the same subchunk tables repeatedly
(TODO in `QueryRunner.cc`). A global "memory lock" table
(`q_memoryLockDb.memoryLockTbl`) is truncated and claimed at startup: the newest
worker steals it and a worker that loses it `exit(EXIT_FAILURE)`s on next touch — two
workers must never share one MariaDB.

## Result files (`wbase/FileChannelShared`)

- Named `<czarId>-<queryId>-<uberJobId>.csv` in `results.dirname`
  (`util::ResultFileName`), but the content is **quoted TSV**: tab-separated,
  single-quote-enclosed fields, `\N` for NULL — matching the czar's
  `LOAD DATA ... FIELDS ENCLOSED BY '\''` loader.
- `maxTableSizeBytes` is enforced after each task's rows are appended (an oversized
  task overshoots on disk before failing) → `WORKER_RESULT_TOO_LARGE` error to czar.
- LIMIT queries: once the file holds enough rows (`rowLimitComplete`), the first task
  to notice latches it, remaining tasks self-cancel, and the file ships early.
- When the last task finishes, the worker POSTs `UberJobReadyMsg` to the czar's
  `/queryjob-ready` (`{fileUrl, rowCount, fileSize, ...}`); errors go to
  `/queryjob-error`. Delivery failures are parked in `WorkerCzarComIssue` for retry if
  the czar still looks alive.
- Deletion paths: czar DELETEs after successful merge; worker deletes on error/cancel,
  on czar-restart notice, on `/querystatus` done-lists, and deletes *everything* on
  its own restart. All GC serializes on one static mutex also taken by the `/status`
  monitoring endpoints (a monitoring poll briefly blocks new result channels).

## Worker↔czar status protocol (`protojson/`, `wcontrol/WCzarInfoMap`, czar-side `czar/ActiveWorker`)

Czars POST `WorkerQueryStatusData` to every worker's `/querystatus` on a steady cycle
(even when empty — it doubles as a czar heartbeat): lists of finished queries
(keep-files vs delete-files), dead UberJobs, and czar-restart markers. The worker
applies them (cancels tasks — including for query ids it has never seen, which is how
future UberJobs of cancelled queries get rejected — deletes files) and echoes the
lists back so the czar can prune its maps, plus its `w-startup-time` so the czar
detects worker restarts (→ kills incomplete UberJobs and reassigns). Conversely a
worker that hears nothing from a czar for `czar.DeadTimeSec` (180 s) kills all that
czar's queries; a czar that the registry says is stale transitions
ALIVE→QUESTIONABLE→DEAD czar-side and its workers' incomplete UberJobs are reassigned.

## Chunk inventory (`wpublish/ChunkInventory`)

Tables `Dbs`, `Chunks`, `Id` (UUID) in `qservw_worker`. Maintained push-style by the
replication system through `/replica*` / `PUT /inventory` (rebuild regenerates
`Chunks` from `information_schema`). Note: the query path never checks the inventory —
a task for a chunk the worker lacks just fails in MySQL. The "replica in use"
protection is currently vacuous: `wcontrol::ResourceMonitor` is never incremented
(TODO DM-53240); live per-chunk usage is instead tracked by `QueriesAndChunks` and
exposed at `/chunkusecounts`.

## Gotchas worth knowing before editing

- `qserv-worker-http.cc` `#include`s `WorkerMain.cc` (the .cc!) — ODR trap.
- `BlendScheduler`'s steady-state scheduler polling order is group, **slow, fast,
  med**, snail (construction order), contradicting its own class doc; the configured
  `priority_*` values only apply when `results.prioritize_by_inflight = true`
  (default false).
- Lock ordering: never take `BlendScheduler::_schedMtx` before
  `util::CommandQueue::_mx`; sub-schedulers reuse the base-class queue mutex.
- Dead code that looks live: `Foreman::_workerCommandPool` (threads with no work),
  `wbase::SendChannel` (legacy), `ResourceMonitor` counts, `ChunkInventory::has()`,
  `useFlexibleLock` plumbing in wsched.
- `replication.num_http_threads` is unused on the worker; `czar.ComNumHttpThreads`
  (default 40) sizes the REST service, and `results.num_http_threads` (default **1**)
  sizes the result-file server.
