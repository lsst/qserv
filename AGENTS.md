# Qserv — guide for AI coding agents

Qserv is a petascale, shared-nothing, distributed SQL database (MPP) built to host the
astronomical catalogs of Rubin Observatory's LSST survey. A **czar** frontend parses and
analyzes each incoming SQL query, rewrites it into per-chunk queries, and dispatches them
over HTTP to **workers** that each hold a shard ("chunks") of the data in a colocated
MariaDB; the czar then collects per-worker result files and merges/aggregates them into a
result table. A separate **replication/ingest system** (controller + registry + per-worker
daemons) manages data placement, replication, and catalog ingest.

Read `doc/architecture/README.md` for the system overview and a map of the deeper
architecture docs (query lifecycle, worker internals, replication/ingest, metadata
stores, deployment, build/test). These docs are current as of 2026-08 and are the
fastest way to orient before touching unfamiliar subsystems.

## Critical cautions

- **PVC safety (operations/deployment work):** The Helm chart's StatefulSets create PVCs
  via `volumeClaimTemplates` (`worker-data`, `czar-data`, `repl-data`). The persistent
  volumes behind them hold catalog data that takes **days to weeks** to re-ingest.
  Inadvertent release of these PVCs is the single largest operational risk of running
  Qserv on Kubernetes. Never propose or perform chart changes, StatefulSet
  renames/deletions, `helm uninstall`, Argo CD sync/prune operations, or namespace
  deletions that could collaterally delete or orphan these PVCs without explicitly
  flagging the risk and getting human confirmation. Renames are dangerous even when
  nothing is deleted: chart/StatefulSet renames change the generated PVC names, and
  even a release rename forces an STS recreate. Argo CD sync is intentionally
  **manual** for this reason. Mechanics in `doc/architecture/deployment.md`.
- **Trust the code over `doc/`:** Most of `doc/` is severely dated. The exceptions,
  which are current and trustworthy, are the user-guide sections *Asynchronous Query
  API* (`doc/user/async.rst`), *HTTP Frontend* (`doc/user/http-frontend*.rst`), and
  *Ingesting Catalogs* (`doc/ingest/`). Everything else: verify against the code.
  Updating stale docs alongside code changes is encouraged.
- **XRootD is gone:** older docs/comments reference XRootD/SSI for czar–worker
  communication. That was removed (2025/2026); all czar↔worker traffic is now HTTP +
  JSON (`src/protojson/`) with results pulled as files over HTTP.
- **`qana/RelationGraph.h`** contains a long, authoritative comment on how Qserv decides
  whether a query is evaluable (join admissibility, overlap, query rewriting). Read it
  before touching query analysis (`src/qana/`, `src/qproc/`).

## Repository map

| Path | Contents |
| --- | --- |
| `src/` | C++ sources, one directory per module (see below) |
| `python/lsst/qserv/` | Python tooling: `admin/` (CLI, in-container entrypoint), `schema/` (smig migrations), `testing/` (kraken load tester) |
| `bin/` | Host-side CLIs: `qserv` (build/test driver), `entrypoint` (in-container), `qserv-smig`, `qserv-kraken` |
| `deploy/` | `docker/` (image Dockerfiles), `compose/` (dev/CI test cluster), `helm/` (the Qserv chart) |
| `doc/` | Sphinx docs (published to qserv.lsst.io; mostly dated — see cautions). `doc/architecture/` holds current agent/developer architecture notes (markdown, not published) |
| `data/` | Integration-test datasets and reference query results (`case01`…, `test101`) |
| `extern/` | Git submodules: `sphgeom`, `log`, `hyrise-sql-parser` — run `git submodule update --init` after clone |
| `../qserv-deployments/` | Separate repo (github.com/lsst/qserv-deployments), conventionally checked out as a sibling of this one: Argo CD Applications + per-deployment Helm values. It has its own AGENTS.md |

C++ module prefixes: modules starting with `w` are worker-side (`wbase`, `wcomms`,
`wconfig`, `wcontrol`, `wdb`, `wmain`, `wpublish`, `wsched`); czar-side query processing
is `ccontrol` (user query orchestration), `parser`/`query` (SQL → IR), `qana` (analysis
plugins), `qproc` (chunking/templates), `qdisp` (dispatch/UberJobs), `rproc` (result
merge), `czar` (service + HTTP frontend). Shared: `css` (global table metadata), `qmeta`
(query metadata), `cconfig`/`global`/`util`/`http`/`qhttp`/`mysql`/`sql`/`protojson`.
`replica/` is the replication/ingest control plane (largest module). `partition/` is the
offline data partitioner (`sph-partition` etc.). `www/` is the web dashboard.

## Build, test, lint

Everything builds **inside containers** via the host-side `./bin/qserv` CLI (needs
Python 3 with `click`, `pyyaml`, `requests`, plus Docker). There is no supported
bare-metal build. Image names are derived from `git describe` (see `qserv env`).

```sh
git submodule update --init                 # once, after clone
./bin/qserv build-images                    # build base/build images (slow, rarely needed — CI reuses GHCR)
./bin/qserv build-user-build-image          # personalized build image (once per user)
./bin/qserv build -j8                       # cmake+make+unit tests+clang-format, then package qserv image
./bin/qserv build -j8 --no-build-image      # compile + unit tests only, skip image packaging
./bin/qserv run-build                       # shell inside build container; then: make -j8 install test
./bin/qserv up / down [-v]                  # start/stop docker-compose test cluster
./bin/qserv itest                           # integration tests (query correctness vs reference MySQL)
./bin/qserv itest-http / itest-http-ingest  # HTTP frontend / user-table ingest integration tests
./bin/qserv itest-rm                        # remove integration test volumes
```

- C++ unit tests are Boost.Test binaries wired into CTest; `make test` inside the build
  container runs them (`ARGS=-jN` parallelizes). A single suite can be run from the
  build dir, e.g. `ctest -R testCss`.
- Formatting is enforced: clang-format via `src/.clang-format` (Google-based, 110 cols,
  4-space indent, includes NOT sorted); CI runs `--clang-format CHECK`, use
  `--clang-format REFORMAT` locally. Python: ruff (line length 110, py312) and
  strict-ish mypy — configs in `pyproject.toml`.
- Docs build: `tox -e docs` (sphinx/documenteer, warnings are errors).

## Conventions

- Branches: `tickets/DM-NNNNN` matching a Rubin Jira ticket
  (https://rubinobs.atlassian.net/browse/DM-NNNNN). CI (`rebase_checker`) rejects PRs
  that merge `main` into the ticket branch — rebase instead.
- Commit subjects: short, imperative. PRs merge into `main`.
- C++20 (`set(CMAKE_CXX_STANDARD 20)`): follow surrounding style. LSST-ish naming: private members
  `_underscorePrefixed`, classes `UpperCamel`, methods `lowerCamel`. Logging via
  `LOGS(_log, LOG_LVL_*, ...)` (lsst/log). Doxygen-style `///` comments on public APIs.
- Every C++ file gets the GPL header block; keep module-local `#include` grouping
  (system / third-party / LSST / Qserv) as in existing files.
- Config values flow through `cconfig::CzarConfig` / `wconfig::WorkerConfig`
  (`util::ConfigValMap`) — add new tunables there, not ad-hoc.
- Schema changes to the czar/worker/replication MySQL databases require a smig
  migration: add `migrate-N-to-N+1.sql` under `python/lsst/qserv/schema/migrations/{czar,worker,repl}/`
  and bump the expected version in the matching code.

## Deployments (context, not something to do casually)

USDF deployments (dev/int/prod) are Argo CD Applications in the `qserv-deployments`
repo, each pinning a published chart version (`ghcr.io/lsst/charts/qserv`, which
carries matching image tags) plus per-deployment values. Release flow: push a tag like
`2026.8.1-rc2` → CI publishes images → chart published to `ghcr.io/lsst/charts` →
bump `targetRevision` in qserv-deployments → human syncs in Argo CD (deliberately
manual). See `doc/architecture/deployment.md` first, and re-read the PVC caution.
