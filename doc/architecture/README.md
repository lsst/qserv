# Qserv architecture notes

*Developer/agent-oriented architecture documentation, written 2026-08 from direct code
inspection. These files are deliberately **not** part of the published Sphinx guide
(see `exclude_patterns` in `doc/conf.py`); unlike much of the rest of `doc/`, they are
current — but the code always wins. Please update them alongside code changes.*

| Doc | Covers |
| --- | --- |
| [query-lifecycle.md](query-lifecycle.md) | SQL in → merged results out: frontends, parsing, IR, analysis plugins, chunking, dispatch, merging, async queries |
| [worker.md](worker.md) | Worker internals: task building, shared-scan scheduling, subchunk handling, result files, worker↔czar status protocol |
| [replication-ingest.md](replication-ingest.md) | The replication/ingest control plane: controller, registry, replica workers, ingest transactions, director index |
| [metadata-and-state.md](metadata-and-state.md) | Who stores what: CSS, QMeta, worker inventory, replication DB, director index, smig migrations |
| [deployment.md](deployment.md) | Images, compose, Helm chart, qserv-deployments/Argo CD, **the PVC hazard** |
| [build-and-test.md](build-and-test.md) | Containerized build, test layers, CI, release versioning |

## What Qserv is

Qserv is a shared-nothing MPP SQL database for Rubin Observatory's LSST catalogs.
The design bet: astronomical catalogs are read-mostly, spatially partitionable, and
queried either by small regions/objects (interactive) or by full-sky scans (batch).
So: partition every large table spherically into **chunks** (and chunks into
**subchunks**, materialized on demand), replicate chunks across workers each backed by
a plain MariaDB, rewrite each user query into per-chunk queries that are answerable
using only worker-local data, run them in parallel with heavy **shared scan**
optimization, and merge/aggregate the partial results on a frontend ("**czar**") node.
Plain MariaDB does all actual SQL execution; Qserv's own code is the distributed query
planner, dispatcher, scheduler, result merger, and data-management control plane around
it.

## Component map

```
                       SQL (mysql protocol)          REST/JSON
  users ──────────────► mysql-proxy + Lua ─┐   ┌─── qserv-czar-http (:4048)
                              (:14040)     │   │      sync + async + user-table ingest
                                           ▼   ▼
                                     ┌──── CZAR ────┐        czar MariaDB:
                                     │ parse → IR   │◄─────  qservCssData (CSS metadata)
                                     │ qana plugins │        qservMeta (QMeta + director index)
                                     │ chunk spec   │        result tables
                                     │ UberJob disp │
                                     └──┬───────▲───┘
                    UberJobs (HTTP/JSON)│       │ result files (HTTP pull)
                        ┌───────────────┼───────┼──────────────┐
                        ▼                                      ▼
              ┌── WORKER pod ×N ──────────┐          ┌── WORKER pod ×N ──┐
              │ qserv-worker-http (:25010)│          │        ...        │
              │   tasks → BlendScheduler  │          └───────────────────┘
              │   → chunk queries against │
              │ worker MariaDB (chunk DBs,│
              │   qservw_worker inventory)│
              │ qserv-replica-worker      │
              └───────────▲───────────────┘
                          │ replication/ingest control (+ ingest data pushes)
        ┌─────────────────┴──────────────────┐
        │ REPLICATION CONTROLLER (:25081)    │◄── repl MariaDB: qservReplica
        │   replication, ingest REST API,    │    (config, replicas, transactions)
        │   chunk-map publish → qservMeta,   │
        │   health monitor, web dashboard    │
        └─────────────────▲──────────────────┘
                          │ heartbeats/contact info (also from workers & czar)
                REGISTRY (:25082) — service discovery
```

## The 30-second query lifecycle

1. A frontend receives SQL (or an HTTP query request) and hands it to
   `ccontrol::UserQueryFactory`, which classifies it (SELECT, COUNT(*) shortcut,
   async SUBMIT, admin statements…) and for real SELECTs builds a
   `ccontrol::UserQuerySelect`.
2. The statement is parsed (Hyrise parser by default) into the `query::` IR, then
   `qproc::QuerySession` runs the `qana::` plugin sequence: table metadata lookup
   (CSS), join admissibility via `qana::RelationGraph` (**read the long comment in
   `src/qana/RelationGraph.h`**), spatial/secondary-index restrictor extraction,
   aggregation split into parallel (worker) + merge (czar) statements.
3. Chunk numbers are chosen (area restrictors → sphgeom region → chunks; objectId
   restrictors → director-index lookup; else all chunks minus empty ones), giving one
   job per chunk. Jobs are grouped per target worker into **UberJobs** using the
   chunk→worker map (from the replication system via QMeta), and POSTed to workers.
4. Each worker turns an UberJob into per-chunk `wbase::Task`s, schedules them
   (interactive queries on the group scheduler; scans on chunk-ordered shared-scan
   schedulers), materializes subchunk/overlap in-memory tables when needed, runs the
   SQL against local MariaDB, and streams rows into a CSV result file. It then tells
   the czar the file is ready.
5. The czar pulls each result file over HTTP and `rproc::InfileMerger` loads it into
   the query's result table (applying the merge/aggregation statement when needed).
   When all jobs finish, the client gets the result — the proxy path runs a final
   "result query" against the result table; the HTTP path returns/streams it directly.
   QMeta tracks status/progress throughout; async queries return a QueryId
   immediately and results are fetched later.

## Module ownership cheat-sheet

Czar side: `ccontrol` (orchestration) → `parser`/`query` (SQL→IR) → `qana` (analysis
plugins) → `qproc` (chunking, templates) → `qdisp` (Executive/UberJobs) → `rproc`
(merge). `czar` = service wiring + HTTP frontend + chunk-map/registry clients.
Worker side: `wmain` (startup), `wcomms` (REST), `wbase` (tasks/result channels),
`wsched` (schedulers), `wdb` (execution, subchunks), `wcontrol` (Foreman, limits),
`wconfig`, `wpublish` (inventory, stats). Control plane: `replica` (+ `registry`
within it). Shared: `css`, `qmeta`, `cconfig`, `global`, `util`, `http` (client),
`qhttp` (server), `mysql`/`sql` (DB access), `protojson` (czar↔worker messages),
`proxy` (mysql-proxy glue), `partition` (offline partitioner), `www` (dashboard).

## History you'll trip over

- **XRootD/SSI is gone** (removed 2025–2026, tags `2026.8.1-xrd-*` mark the last
  XRootD-era line). Any doc/comment mentioning xrootd, SSI, or `xrdsvc` is historical.
  Czar↔worker transport is HTTP/JSON (`protojson`) with file-based result delivery.
- **The ANTLR parser is being replaced by Hyrise** (`extern/hyrise-sql-parser`,
  `ccontrol/HyriseAdapter`); ANTLR remains as a build-time option and for comparison
  tests.
- The **wmgr**, **qserv-ingest**, and other older admin layers referenced in `doc/`
  predate the current replication-system ingest API; trust `doc/ingest/` (current) and
  the code.
