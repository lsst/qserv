# Build system, tests, and CI

*Current as of 2026-08, derived from code inspection. Trust the code over this doc if
they diverge; update this doc when you change the code.*

## Build system

CMake at the root builds `src/` (one static-ish library + test binaries per module
directory, wired together into the final executables) plus the vendored submodules in
`extern/` (`sphgeom` spatial library, `log`, `hyrise-sql-parser`). The
`QSERV_USE_HYRISE_SQL_PARSER` option (default `ON`) selects the Hyrise-based SQL parser
for SELECT statements over the legacy ANTLR4 one.

You never run cmake on the host. The `./bin/qserv` CLI
(`python/lsst/qserv/admin/qservCli/`) wraps every step in Docker:

- `qserv env` — shows the image names (derived from `git describe`) and the `QSERV_*`
  env vars that override them (`QSERV_IMAGE`, `QSERV_BUILD_IMAGE`, ...).
- `qserv build` — runs clang-format (CHECK/REFORMAT/OFF), cmake (if `build/` absent),
  `make install`, unit tests (`make test`, i.e. CTest), mypy; then optionally packages
  the run image. `-jN` parallelizes both make and ctest.
- `qserv run-build` — drops you into the build container for incremental `make`.
- `qserv build-images` / `build-user-build-image` / `build-mariadb-image` /
  `build-run-base-image` / `build-ssl-proxy-image` — the image hierarchy; base images
  are content-addressed by their Dockerfiles, so CI only rebuilds them when
  `deploy/docker/*` changes.

Binaries produced (installed into the run image): `qserv-czar-http`, `mysql-proxy` glue
(`czarProxy` lib + Lua), `qserv-worker-http`, `qserv-replica-master-http`,
`qserv-replica-registry`, `qserv-replica-worker`, various `qserv-replica-*` admin
tools, the partitioner tools (`sph-partition`, `sph-partition-matches`, ...), and the
Python tooling under `/usr/local/python`, entered via `entrypoint`.

## Test layers

1. **C++ unit tests** — Boost.Test `test*.cc` files in each `src/<module>/`,
   registered with CTest; run by `make test` inside the build container or
   `ctest -R <name>` in `build/`. Notable suites: `qproc/testQueryAna*` (query
   analysis golden tests), `ccontrol/testAntlr4GeneratedIR` / `testHyriseGeneratedIR` /
   `testParserCorpus` (parser → IR corpus tests, with a `.sql` stress corpus in
   `ccontrol/testdata/`), `wsched/testSchedulers`. A few suites need a live MySQL and
   are excluded from CTest (e.g. `qmeta/testQMeta` — its `add_test` is commented out).
2. **Python unit tests** — under `python/lsst/qserv/*/tests`; mypy+ruff gate style
   (`pyproject.toml`).
3. **Integration tests** — real compose cluster + dataset cases in `data/caseNN/`
   comparing Qserv results against a reference MariaDB running the same queries
   unpartitioned. Three suites: `itest` (SQL via proxy), `itest-http` (HTTP frontend),
   `itest-http-ingest` (user-table ingest). Config: `etc/integration_tests.yaml`;
   runner: `python/lsst/qserv/admin/cli/_integration_test.py`. See the `itest` skill /
   `doc/architecture/deployment.md` for the workflow.
4. **Load testing** — `bin/qserv-kraken` (`python/lsst/qserv/testing/`) replays query
   mixes against a deployment (not part of CI).

## CI (`.github/workflows/`)

- `ci.yml` — on every push: recompute image names from `git describe`; rebuild base /
  mariadb / ssl-proxy images only if missing from GHCR; build the qserv image
  (clang-format CHECK enforced, unit tests run inside the build); then run the full
  compose-based integration suite (all three itest flavors) with container logs dumped
  on failure. Publishes images to GHCR (`packages: write`).
- `docs.yaml` — builds Sphinx docs via `tox -e docs` (documenteer; **warnings are
  errors**) and uploads to LSST-the-Docs (qserv.lsst.io) on pushes to `main`,
  published releases, and PRs from `tickets/*` branches. Note: `doc/architecture/*.md`
  is excluded from the Sphinx build via `exclude_patterns` in `doc/conf.py` — if you
  add *published* docs, wire them into a toctree and expect `-W` strictness.
- `rebase_checker.yaml` — fails PRs whose ticket branch has `main` merged in; rebase
  ticket branches instead of merging main into them.

## Release versioning

Annotated tags `YYYY.M.P[-rcN]` (e.g. `2026.8.1-rc2`) define releases; everything else
is tagged `<last-tag>-<n>-g<sha>` via `git describe` (`--dirty` locally). The CI job
summary prints the exact image names for pasting into deployment `values.yaml`.
