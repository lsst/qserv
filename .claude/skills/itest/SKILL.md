---
name: itest
description: Run Qserv integration tests — launch the docker-compose test cluster, run query-correctness suites (itest), HTTP frontend tests, and ingest tests, then tear down. Use when verifying end-to-end behavior.
---

# Qserv integration tests

Integration tests run real queries against a small docker-compose Qserv cluster
(2 workers, czar with both frontends, replication controller/registry, MariaDB
instances — see `deploy/compose/docker-compose.yml`) and compare results against a
plain reference MySQL loaded with the same data. Datasets and expected outputs live in
`data/case01..case04`, `data/test101`; the runner is
`python/lsst/qserv/admin/cli/_integration_test.py` driven through the in-container
`entrypoint` CLI.

Requires: docker + docker-compose, the qserv and mariadb images (built by
`/build-qserv` or pulled from GHCR; pass `--qserv-image`/`--mariadb-image` or set
`QSERV_IMAGE`/`QSERV_MARIADB_IMAGE` to use specific ones).

## Workflow

```sh
./bin/qserv up                 # start the compose cluster (give it ~1-3 min to settle)
docker ps -a                   # sanity check: all containers up, none restarting

./bin/qserv itest              # classic path: load test datasets, run per-case queries,
                               #   compare qserv vs reference MySQL results
./bin/qserv itest-http --reload --load-http   # same suites through the HTTP frontend
./bin/qserv itest-http-ingest  # user-table ingest via the HTTP frontend

./bin/qserv itest-rm           # remove integration-test data volumes
./bin/qserv down -v            # stop cluster and remove its volumes
```

- `itest` accepts `--case caseNN` (repeatable) to run a subset, `--tests-yaml` for an
  alternate test config (default `etc/integration_tests.yaml`), and
  `--reload`/`--load`/`--unload` for dataset management — see `./bin/qserv itest --help`.
- Test queries are `data/caseNN/queries/*.sql`; a failing comparison prints the
  difference between qserv and reference outputs. Add new cases by extending a case's
  `queries/` + expected data rather than inventing a new harness.
- On failure, container logs are the first stop:
  `docker logs $USER-czar-proxy-1`, `$USER-czar-http-1`, `$USER-worker-svc-{0,1}-1`,
  `$USER-repl-controller-1` (names are prefixed with `$USER`, suffixed `-1` by compose).
- CI runs exactly this sequence (`.github/workflows/ci.yml`), so a green local run is a
  good predictor.

Compose state is per-user (project name includes `$USER`), so parallel checkouts don't
collide, but only one cluster per user at a time.
