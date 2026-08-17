# The replication/ingest control plane (`src/replica/`)

*Current as of 2026-08, derived from code inspection. Trust the code over this doc if
they diverge; update this doc when you change the code. `doc/ingest/` is the current,
trustworthy user-facing documentation for the ingest API — this doc covers the
internals.*

The replication system is a control plane separate from the query path: a singleton
**Replication Controller** (`qserv-replica-master-http`), a stateless **Registry**
(`qserv-replica-registry`), and a **replica-worker daemon** (`qserv-replica-worker`)
colocated with every qserv worker. Shared state lives in the `qservReplica` MySQL DB.
It owns data placement, replication level, catalog ingest, the director index, row
counters, and the publish step that makes catalogs visible to the czar.

## Module layout (`src/replica/`)

`apps/` (CLI/daemon application classes) · `config/` (the Configuration service backed
by `qservReplica`) · `contr/` (Controller, its long-running tasks, and the REST API
modules) · `ingest/` (worker-side ingest services) · `jobs/` (cluster-wide composite
operations) · `requests/` (single-worker operations + the Messenger transport) ·
`mysql/` (its own MySQL client layer + `QueryGenerator`) · `proto/` (protobuf wire
schema) · `qserv/` (controller→qserv-worker/czar management requests, HTTP) ·
`registry/` (the Registry service + client) · `services/` (`ServiceProvider`
singletons, `DatabaseServices` persistence, `ChunkMap` publisher) · `worker/` (the
replica-worker daemon) · `tools/` (binary entry points) · `util/`, `tests/`.

## Controller

`MasterControllerHttpApp` runs three things:

- **ReplicationTask** — the linear replication loop, each cycle:
  `FindAllJob` (rescan replicas on all workers) → `FixUpJob` (repair/colocation) →
  `ReplicateJob` (raise chunks to the family replication level) → `RebalanceJob` →
  optional `PurgeJob` (drop excess), each followed by a `QservSyncJob` that pushes the
  "good replica" view into qserv workers' `ChunkInventory` (`POST /replicas` on the
  worker management port). With `--qserv-chunk-map-update` it also republishes the
  chunk map (below).
- **HealthMonitorTask** — probes both the qserv service and the replication service on
  every worker; a worker becomes an eviction candidate only when *both* are silent
  past the timeout, and only one worker may be evicted at a time (a second concurrent
  failure demands human intervention). Eviction = stop ReplicationTask, run
  `DeleteWorkerJob` (redistribute its chunks), restart.
- **The REST API** (`contr/HttpProcessor` + `Http*Module` classes) on
  `controller.http-server-port` (25081): `/replication/*` (config, workers, levels,
  jobs, requests, qserv monitoring), `/ingest/*` (databases, tables, transactions,
  chunk allocation, director index, table stats), `/export/*`. It also serves the
  `www/` dashboard as static content.

A tracker thread reconciles Registry heartbeats into the Configuration,
auto-registering new workers/czars if configured.

### Jobs → requests → transport

Controller *jobs* (`jobs/`) fan out into per-worker *requests* (`requests/`).
**Current controller↔replica-worker transport is protobuf over persistent TCP**
(`proto/protocol.proto`, 4-byte length-prefixed frames, multiplexed per-worker by
`requests/Messenger`). A parallel **HTTP backend** on the replica worker
(`worker/WorkerHttpSvc`, `/worker/*` routes) is implemented and served but **nothing
calls it yet** — the controller-side migration hasn't happened (easy to misread the
tree as HTTP-only). By contrast, controller→*qserv* management traffic (`qserv/`
request classes) is already fully HTTP against the qserv workers'/czars' management
ports.

## Registry (`registry/`)

A small qhttp service (port 25082) holding a purely **in-memory** JSON map — no
persistence, **no TTL/expiry**; consumers judge staleness via `update-time-ms`.
Registrants (all on ~1 s heartbeat loops): replica workers (`POST /worker`), qserv
workers (`POST /qserv-worker` — name is the UUID from `qservw_worker.Id`, plus
management-port and result-file data-port), czars (`POST /czar` — note a *denied*
czar registration calls `abort()`), and the controller (`POST /controller`).
Consumers: the controller (worker/czar discovery + health) and each czar
(`czar::CzarRegistry` polls `GET /services` every 15 s to build its worker contact
map).

## Configuration and database families (`config/`)

`replica::Configuration` is backed by `qservReplica` (schema version asserted at load;
`--schema-upgrade-wait` supports rolling smig upgrades). Durable tables hold general
params, worker enable/read-only flags (`config_worker` — host/ports come from the
Registry at runtime, *not* the DB), families, databases, tables, and table schemas.

A **database family** (`config_database_family`) = databases sharing identical
partitioning (`num_stripes`, `num_sub_stripes`, `overlap`) plus a
`min_replication_level`; same-numbered chunks are spatially identical across the
family and are kept colocated so joins work worker-locally. Families are created
implicitly when a database is registered with a new parameter combination. Effective
replication level = `min(family level, controller.max-repl-level, #eligible workers)`.
Changed via `PUT /replication/level`.

## Ingest workflow (internals)

Matches `doc/ingest/api/concepts/overview.rst`. Key mechanics:

1. **Register database/tables** (`POST /ingest/database`, `/ingest/table`): stores
   metadata + schema in `qservReplica`; database starts unpublished.
2. **Transactions** ("super-transactions", `POST /ingest/trans`): every ingested row
   is tagged with a `qserv_trans_id` column and each transaction gets its own MySQL
   partition in every affected table, making abort cheap (`AbortTransactionJob` drops
   the partition cluster-wide). Explicit transitional states
   (`IS_STARTING/STARTED/IS_ABORTING/...`) in the `transaction` table.
3. **Chunk allocation** (`POST /ingest/chunk`): the controller picks the worker —
   reusing an existing replica's worker, else preferring workers already holding that
   chunk number for *any* database in the family (colocation), else least-loaded —
   and returns that worker's loader endpoints. The workflow then **pushes
   contribution data directly to workers**, bypassing the controller.
4. **Contributions**: worker-side `IngestHttpSvc` (`POST /ingest/csv|data|file|
   file-async`, sync and async by-reference modes; the async loader is a per-database
   queued thread pool with per-database concurrency limits). `IngestFileSvc` stages
   CSV (prepending the transaction id field) and runs `LOAD DATA INFILE`. Every
   contribution is a row in `transaction_contrib` with full status/error/retry
   bookkeeping — the `retry_allowed` flag tells workflows whether an in-transaction
   retry is safe or the transaction must be aborted. (A legacy protobuf push protocol
   and `qserv-replica-file INGEST` client also exist.)
5. **Commit/abort transactions**, then **publish** (`PUT /ingest/database/:db`):
   consolidate the director index, optionally scan/deploy row counters, create
   missing chunk tables, **drop the per-transaction MySQL partitions**, then
   `_publishDatabaseInMaster`: create the database/prototype tables in the czar's
   MariaDB, register everything **in CSS** (striping params from the family, match
   tables, scan params), and rebuild the CSS **empty-chunk list** (all chunks the
   sphgeom Chunker allows minus chunks actually ingested). Finally flip
   `is_published`, reconfigure workers, qserv-sync, and notify czars.

Czar-facing notifications are `POST /event` with a `DataManagementEvent`
(`DATABASE_PUBLISHED`, `CHUNK_MAP_REBUILT`, ...) → czar `EventService` → empty-chunks
cache invalidation and family-map re-read.

## Director index and row counters

- **Director index**: `qservMeta.<db>__<DirectorTable>` mapping director key →
  (chunkId, subChunkId). Built incrementally during ingest (per-transaction
  partitions, consolidated at publish; vetoable via `auto_build_secondary_index=0`)
  or bulk-rebuilt via `POST /ingest/index/secondary` → `DirectorIndexJob`, which
  harvests per-(worker, chunk) extracts (`SELECT ... INTO OUTFILE`, streamed back)
  and `LOAD DATA LOCAL INFILE`s them into the index table.
- **Row counters** (`POST /ingest/table-stats`): per-chunk row counts persisted in
  `qservReplica.stats_table_rows` and optionally *deployed* to
  `qservMeta.<db>__<table>__rows` — which is exactly what the czar's
  `SELECT COUNT(*)` optimization reads.

## Chunk map: how placement reaches the czar

`replica::ChunkMap::update()` reads COMPLETE replicas from `DatabaseServices`, and
atomically rewrites the `chunkMap` + `chunkMapStatus` tables in the czar's
**qservMeta** DB (full delete + bulk insert in one transaction). Triggered by the
replication loop (with `--qserv-chunk-map-update`) and on ingest events, followed by
`CHUNK_MAP_REBUILT` events to all registered czars. The czar reads it back through
`qmeta::QMeta::getChunkMap()` into `czar::CzarFamilyMap`/`CzarChunkMap`, which greedily
assigns each chunk a primary scan worker (largest chunks first, least-loaded
replica-holder wins). **Caveat:** czar-side "family" is currently a stub — one
pseudo-family per database (`CzarFamilyMap::getFamilyNameFromDbName` returns the db
name, TODO DM-53239) — so the czar does not yet see real family grouping.

## Replica-worker daemon (`worker/`)

Reads its identity from the colocated qserv worker's `qservw_worker.Id`, then runs
**six services in one process**: protobuf/TCP replica-management (`worker.svc-port`),
HTTP replica-management (served, unused — see above), the file server (`fs-port`,
serves replica files to *other* workers during replication), legacy binary ingest
(`loader-port`), HTTP ingest (`http-loader-port`), and the exporter
(`exporter-port`). Plus the Registry heartbeat.

It does **not** talk to the qserv worker's REST API — the *controller* does that
(`QservSyncJob` → `POST /replicas` etc. against `wcomms/HttpReplicaMgtModule`) to keep
`ChunkInventory` in sync with what the replication system considers good replicas.

## Binaries

`qserv-replica-master-http` (controller), `qserv-replica-worker`,
`qserv-replica-registry`, `qserv-replica-config[-test]`, `qserv-replica-controller-cmd`,
`qserv-replica-worker-notify`, `qserv-replica-calc-cs`, and multi-verb tools
`qserv-replica-job` (REPLICATE, PURGE, REBALANCE, FIXUP, SYNC, INDEX, DELETE-WORKER,
...), `qserv-replica-file` (INGEST client, file server, S3), `qserv-replica-test`.

## Known gaps / in-flight work

- HTTP worker backend served but unconsumed (protobuf path still authoritative).
- Czar family stub (DM-53239) — `chunkMap` carries no family column yet.
- `Czar::_monitor()` currently forces a family-map re-read every cycle (`|| true`
  with a TODO) despite the event-driven path existing.
- `ChunkMap::update()` rewrites the whole table each time; no incremental path.
- Registry entries never expire; explicit DELETE only.
