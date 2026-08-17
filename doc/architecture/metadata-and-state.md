# Metadata and state stores

*Current as of 2026-08, derived from code inspection. Trust the code over this doc if
they diverge; update this doc when you change the code.*

Qserv keeps its bookkeeping in a handful of MySQL/MariaDB databases spread across
three MariaDB roles (czar, per-worker, replication), plus one in-memory registry
service. Knowing which store owns which fact is the fastest way to orient when
debugging.

## The databases

| Database | Lives on | Owner code | Contents |
| --- | --- | --- | --- |
| `qservCssData` | czar MariaDB | `src/css/` | CSS — the "Central State System": global catalog metadata. Databases/tables known to Qserv, table types (director/child/match/RefMatch), partitioning params (stripes, overlap), scan ratings, empty-chunk lists. Stored as a key-value tree in a MySQL table (`KvInterfaceImplMySql`) |
| `qservMeta` | czar MariaDB | `src/qmeta/` | QMeta — per-query metadata: `QInfo` (query text, status, timing, result location), `QTable`, `QMessages`, `QCzar`, the chunk map published by the replication system (`chunkMap` + `chunkMapStatus` tables), director-index tables (`<db>__<table>`), row-counter tables (`<db>__<table>__rows`), and user-table ingest bookkeeping |
| `qservStatusData` | czar MariaDB | `src/qmeta/QProgress*` | Transient query progress (`QProgress`, `QProgressHistory`) — split from `qservMeta` so high-rate updates don't touch the durable store |
| `qservResult` | czar MariaDB | `src/rproc/`, `src/czar/MessageTable` | Result tables (`result_<queryId>`, plus `_m` merge tables during aggregation) and transient `message_<n>` / `result_async_<n>` tables used by the proxy protocol |
| `qservw_worker` | each worker MariaDB | `src/wpublish/ChunkInventory` | Worker-local inventory: which databases/chunks this worker serves (`Dbs`/`Chunks` tables), worker identity/UUID |
| `qservReplica` | repl MariaDB | `src/replica/config/` | Replication system config + state: worker registry, database families, databases/tables being ingested, replica disposition, ingest transactions and contributions, controller event log |

Chunk *data* itself lives in per-catalog databases on each worker MariaDB (chunk tables
named `Table_<chunkId>`, overlap tables `TableFullOverlap_<chunkId>`, plus transient
`Subchunks_<db>_<chunk>` MEMORY databases built on demand).

## Who writes what

- **CSS is written by the replication/ingest system** (e.g.
  `src/replica/contr/HttpIngestModule.cc` writes `qservCssData` directly when a
  database is published) and **read by the czar** at query-analysis time
  (`css::CssAccess`, used by `qproc::QuerySession` to get table types, partitioning
  params, and empty chunks).
- **The chunk map** (`chunkMap`/`chunkMapStatus` in `qservMeta`) is atomically
  rewritten by the replication controller (`replica::ChunkMap::update()`) and read by
  the czar's `czar::CzarFamilyMap` / `czar::CzarChunkMap`, which turn it into the
  chunk→worker assignment used to build UberJobs. So: replication decides placement;
  the czar consumes it via QMeta.
- **Worker contact info** flows through the **Registry** service
  (`src/replica/registry/`, deployed as `qserv-registry`): workers and czars POST their
  identity/host/port there periodically; the czar polls it
  (`czar::CzarRegistry::waitForWorkerContactMap`) to learn how to reach workers, and the
  replication controller uses it for health monitoring. The registry is soft state —
  it repopulates from heartbeats after a restart.
- **QMeta** is written by the czar throughout the query lifecycle (registration,
  status transitions, progress, final stats, result-query text for the proxy to run).

## Database families

Partitioned databases belong to a *family* (defined in the replication system's config)
that shares identical partitioning parameters (num stripes/sub-stripes, overlap). All
tables joined in one query must be partition-compatible, which in practice means their
databases are in the same family; chunk placement/replication is managed per-family so
joins can always be satisfied worker-locally (see `src/qana/RelationGraph.h` for why).
**Caveat:** on the czar side "family" is currently a stub — `czar::CzarFamilyMap`
treats each database as its own family (`getFamilyNameFromDbName` returns the db name;
TODO DM-53239), because the published chunk map carries no family column yet.

## The director index (a.k.a. secondary index)

For point lookups (`WHERE objectId = ...` / `IN (...)` on a director table's primary
key), the czar avoids querying every chunk by consulting the *director index*: a table
`<db>__<DirectorTable>` in the `qservMeta` database on the czar MariaDB, mapping
objectId → (chunk, subchunk). It is built by the replication system during ingest
(`src/replica/contr/HttpDirectorIndexModule.cc`) and queried at analysis time via
`qproc::SecondaryIndex` restrictors. See `doc/admin/director-index.rst`.

## Schema migration (smig)

Each database's schema is versioned; migration scripts live in
`python/lsst/qserv/schema/migrations/{czar,worker,repl}/migrate-N-to-M.sql` (czar →
`qservMeta` + CSS, worker → `qservw_worker`, repl → `qservReplica`). The `smig`
machinery (`python/lsst/qserv/schema/`, CLI `bin/qserv-smig`, entrypoint command
`smig-update`) applies them. In Kubernetes, per-component smig Jobs
(`deploy/helm/templates/*-smig-job.yaml`) run migrations on upgrade; services block at
startup until the schema version matches expectations (`--schema-upgrade-wait`).
**Any schema change to these databases needs a migration script plus a bump of the
expected version constant in the owning module.**

## Configuration files vs databases

Czar and worker process configuration is file-based (INI-style read into
`cconfig::CzarConfig` / `wconfig::WorkerConfig` via `util::ConfigValMap`; templates
rendered by the container `entrypoint` from `src/admin/templates/`). The replication
system is different: its configuration lives *in* `qservReplica`
(`replica::Configuration`), bootstrapped from a JSON/SQL seed and edited through the
controller's REST API or `qserv-replica-config` tooling.
