# Query lifecycle: SQL in → merged results out

*Current as of 2026-08, derived from code inspection. Trust the code over this doc if
they diverge; update this doc when you change the code.*

## Frontends

Two czar processes run the same `czar::Czar` core, differing only in name/transport;
both also run a control HTTP server (`czar::HttpSvc`, `replication.http_port`) that
**workers call back to** (`/queryjob-ready`, `/queryjob-error`, `/workerczarcomissue`,
`/querystatus` replies, `/event` from the replication system).

**mysql-proxy path** (`src/proxy/`): `mysql-proxy` runs `mysqlProxy.lua`, which calls
into czar code via the `czarProxy` Lua extension. The Lua classifies each statement
(local passthrough, disallowed, KILL/CANCEL, or send-to-qserv) and calls
`Czar::submitQuery`. Synchronization is a neat trick: the czar pre-creates an
`ENGINE=MEMORY` *message table* (`message_<n>` in the result db) and holds a
`LOCK TABLES ... WRITE` on it; the proxy immediately issues a SELECT against that
table, which blocks until the czar's finalizer thread (submit → join → unlock) releases
the lock. The proxy then checks the message rows for errors, streams the czar-provided
*result query* (`SELECT ... FROM <resultdb>.result_<qid> [ORDER BY ...]`) straight to
the client, and drops both tables (except for `SELECT * FROM qserv_result(<id>)`,
which must not drop the async result table).

**HTTP frontend** (`czar/HttpCzarSvc`, TLS-only, port 4048): `POST /query` (sync),
`POST /query-async` (literally prefixes the SQL with `SUBMIT ` and reuses the same
path), `GET /query-async/status/:qid`, `GET /query-async/result/:qid`,
`DELETE /query-async/:qid` (cancel) and `.../result/:qid` (delete result), plus
user-table ingest (`/ingest/csv`, `/ingest/data`, ...). Results are returned as JSON
`{schema, rows}` with configurable binary-column encoding (hex/b64/array). See
`doc/user/http-frontend*.rst` (current).

**Async SQL API** (`doc/user/async.rst`, current): `SUBMIT <select>` returns a row
`jobId | resultLocation` immediately; progress via `information_schema.processlist` /
`.queries`; results via `SELECT * FROM qserv_result(<id>)`; cleanup via
`CALL qserv_result_delete(<id>)`; cancel via `CANCEL <id>`. Unclaimed async results
are GC'd after `resultdb.oldestAsyncResultKeptSeconds` (default 1 h).

## Classification: `ccontrol::UserQueryFactory`

Order matters (`UserQueryFactory::newUserQuery`): strip `SUBMIT` (→ async) → if
SELECT: parse; `information_schema.PROCESSLIST`/`QUERIES` → process-list query types;
**COUNT(\*) shortcut** — a bare `SELECT COUNT(*) FROM t` (no WHERE/ORDER/GROUP/HAVING/
JOIN) with row-counter data available in `qservMeta.<db>__<table>__rows` becomes
`UserQuerySelectCountStar`, which never touches workers (toggle:
`SET GLOBAL QSERV_ROW_COUNTER_OPTIMIZATION=0|1`); otherwise the full
`UserQuerySelect` pipeline. Non-SELECT: `SELECT * FROM qserv_result(N)` →
`UserQueryAsyncResult`; `SHOW PROCESSLIST`; `CALL qserv_result_delete(N)`;
`SET GLOBAL` (recognized vars: `QSERV_ROW_COUNTER_OPTIMIZATION`,
`QSERV_DEBUG_CZAR_NO_MERGE`); anything else → `UserQueryInvalid`.

## Parsing → IR

Build-time choice (`QSERV_USE_HYRISE_SQL_PARSER`, default ON): the Hyrise parser
(`extern/hyrise-sql-parser` + `ccontrol/HyriseAdapter`, ~800 lines of conversion) or
the legacy ANTLR4 grammar (`parser/*.g4` + `ccontrol/ParseListener`). Both produce the
same IR: `query::SelectStmt` owning SelectList, FromList (TableRef/JoinRef), WhereClause
(BoolTerm tree + a *restrictor side-channel* where `qserv_areaspec_box|circle|ellipse|
poly` hints are lifted), OrderBy/GroupBy/Having, DISTINCT, LIMIT. `query::ValueExpr` /
`ValueFactor` model expressions; `query::QueryTemplate` renders IR to SQL text with
alias-mode control and substitutable entries (how chunk numbers get patched in later).
`CASE` is rejected; area restrictors may not appear under OR/NOT.

## Analysis: `qproc::QuerySession` + `qana` plugins

`analyzeQuery` runs the plugin sequence (hardcoded order in
`QuerySession::_preparePlugins`):

1. **DuplSelectExprPlugin** — reject duplicate select-list names (result table is a
   real MySQL table).
2. **WherePlugin** — simplify/normalize the boolean tree.
3. **AggregatePlugin** — split each select item into *parallel* (worker) and *merge*
   (czar) forms: `COUNT→COUNT/SUM`, `SUM/MIN/MAX→same-over-alias`,
   `AVG→SUM(SUM)/SUM(COUNT)`. Sets `needsMerge` (and all-chunks-required) for
   aggregates/DISTINCT.
4. **TablePlugin** — resolve default dbs, assign table/value aliases, compute dominant
   dbs, canonicalize column refs (ORDER BY items **must** match a select-list entry —
   source of a well-known user error), then per parallel statement build
   `qana::RelationGraph` and `rewrite()` — join admissibility, overlap analysis, and
   fan-out into up to 2^N statements with chunk/subchunk/overlap table-name patterns
   (**read the long comment in `src/qana/RelationGraph.h`**). Also snapshots the
   *preflight* statement used to derive the result-table schema.
5. **MatchTablePlugin** — for single match-table queries, add the duplicate-row
   filter (`dir1 IS NULL OR flag <> 2`).
6. **QservRestrictorPlugin** — turn `qserv_areaspec_*` hints into real
   `scisql_s2PtIn*` predicates on each chunked table, or *infer* one area restrictor
   from an existing `scisql_s2PtIn*(...)=1` / `scisql_angSep(...) < r` predicate on
   partitioning columns; collect director-index restrictors from `=`/`IN` predicates
   on director-key columns.
7. **PostPlugin** — LIMIT/ORDER BY: strip per-chunk LIMIT when GROUP BY present;
   LIMIT+chunks forces a merge; without LIMIT, ORDER BY is removed from parallel and
   merge statements entirely (ordering happens only in the final *result query*).
8. **ScanTablePlugin** — classify as shared scan (any column refs in SELECT or WHERE
   → all partitioned FROM tables are scan tables; rating = max CSS `scanRating`);
   `applyFinal` de-classifies queries under `tuning.interactiveChunkLimit` (default
   10 chunks) as interactive.

The split: parallel statements (HAVING removed) run on workers; `_stmtMerge`
(select-list only, retargeted at the merge table) runs on the czar iff `needsMerge`;
the **result query** (built from aliases + the *original* ORDER BY, persisted in
`QInfo.resultQuery`) is what the frontend runs against the final result table.

## Chunk selection

`UserQuerySelect::_setupChunking`: area restrictors → sphgeom regions →
`IndexMap`/`sphgeom::Chunker` intersection; director-index restrictors →
`SELECT chunkId, subChunkId FROM qservMeta.<db>__<table> WHERE ...`; both present →
intersection (AND only); neither → all chunks. Then subtract the CSS empty-chunk set.
A chunk-less query gets the dummy chunk (1234567890). >interactiveChunkLimit chunks →
not interactive.

## Dispatch (`qdisp/`)

`UserQuerySelect::submit()` creates one `JobDescription` per chunk (chunk tag left
unsubstituted — workers do that), then `buildAndSendUberJobs()` groups unassigned
chunks **in numerical order** (keeps worker shared scans aligned) by their *primary
scan worker* — assigned by `czar::CzarChunkMap::organize()` (chunks sorted by size,
greedily placed on the least-loaded replica-holding worker; placement data ultimately
from the replication system via `qservMeta.chunkMap`). Up to `uberjob.maxChunks`
(default 10000) jobs per UberJob; each UberJob is POSTed as JSON to the worker's
`/queryjob` (`protojson::UberJobMsg` — with query-template and db-table interning to
keep messages small; includes rowlimit, maxtablesizemb, scan info, czar contact info).

Failure handling: transmit failure → jobs unassigned and reassigned next cycle;
worker "unknown table" errors → mark that worker avoided for those jobs (cleared when
a newer family map arrives) and reassign; worker restart/death (from registry
liveness + `w-startup-time`) → kill incomplete UberJobs and reassign — *unless* the
result file merge already started, in which case reassignment would corrupt results
and is refused. Per-job attempt cap `tuning.jobMaxAttempts` (default 150) → query
squashed.

**LIMIT squash:** for `LIMIT` without ORDER BY/GROUP BY/aggregates, once merged rows
reach the limit the executive squashes remaining jobs as "superfluous", tells workers
to drop results, and treats the query as successfully complete. Workers also stop
early (rowlimit is in the UberJob message).

**Cancellation:** `KILL`/`CANCEL`/HTTP DELETE → `Executive::squash` → cancel all
jobs + `endUserQueryOnWorkers` (workers drop tasks and delete files via the status
protocol).

## Result path (czar side)

Worker POSTs `/queryjob-ready` with `{fileUrl, rowCount, fileSize}`. The czar queues a
file-collect command (priority pool), GETs the file (libcurl, shared connection pool
of `resultdb.maxhttpconnections`), buffers it through `mysql::CsvMemDisk` (in-memory
up to a process-wide `resultdb.maxTransferMemMB` budget, spilling to
`resultdb.transferDir`), and merges via `rproc::InfileMerger`:
`LOAD DATA LOCAL INFILE` (virtual file via `mysql::LocalInfile` handler,
`FIELDS ENCLOSED BY '\''` — the worker writes quoted-TSV) into the merge table
(`result_<qid>_m` when aggregating, else directly `result_<qid>`, ENGINE=MyISAM,
schema derived by running the preflight statement). On success the czar DELETEs the
file from the worker. When all jobs finish, `finalize()` runs the merge statement
(`CREATE TABLE result_<qid> AS <merge select>` + drop `_m`) if needed. A failed merge
that already wrote bytes marks the result **contaminated** (query fails; no silent
partial results). Result-size cap `resultdb.maxtablesize_mb` (default ~5 GB) is
enforced both per worker response and on the accumulated total (`FAILED_LR` status).

## Bookkeeping (qmeta)

`QInfo` row per query (type, status EXECUTING→COMPLETED/FAILED/FAILED_LR/ABORTED,
query text, parallel/merge templates, result location, result query, timestamps);
`QTable` (tables touched); `QProgress` + history (chunk completion, throttled
updates); `QMessages` (per-job/error messages, capped per source); `QCzar` (czar
registry; czar restart aborts its stale EXECUTING queries at startup and can notify
workers to cancel pre-restart query ids). The proxy's transient message/result tables
live in `resultdb` (`qservResult`), not qmeta.

## Config quick reference (`cconfig::CzarConfig`)

`uberjob.maxChunks` 10000 · `tuning.interactiveChunkLimit` 10 ·
`tuning.jobMaxAttempts` 150 · `resultdb.maxtablesize_mb` 5001 ·
`resultdb.maxTransferMemMB` 10000 · `resultdb.oldestResultKeptDays` 30 ·
`resultdb.oldestAsyncResultKeptSeconds` 3600 · `qdisppool.*` (dispatch pool shape) ·
`activeworker.*` (worker liveness: 300 s questionable / 600 s dead) ·
`familymap.maxUpdateWaitSecs` 120 · `css.database` qservCssData · `qmeta.db` qservMeta
· `qstatus.db` qservStatusData · `resultdb.db` qservResult.

## Gotchas

- The ANTLR path still compiles and behaves differently for non-SELECT statements;
  don't assume Hyrise-only semantics when touching `UserQueryFactory`.
- ORDER BY without LIMIT never reaches workers or the merge — only the final result
  query orders. `copyMerge()` deliberately drops ORDER BY.
- The COUNT(\*) shortcut silently depends on row counters having been deployed at
  ingest time; without them the full scan path runs.
- `ScanTablePlugin` has a disabled (`#if 0`) branch that would de-classify
  director-index point lookups as scans — such queries are currently still rated as
  scans until `applyFinal`'s chunk-count check rescues small ones.
- Czar-side "family" == database today (DM-53239 stub); see replication-ingest.md.
