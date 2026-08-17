# Deployment: containers, compose, Helm, Argo CD

*Current as of 2026-08, derived from code inspection. Trust the code over this doc if
they diverge; update this doc when you change the code.*

## ⚠️ The one thing you must not break

The Helm chart's StatefulSets create PVCs via `volumeClaimTemplates` — `worker-data`
(chunk data, per worker), `czar-data`, `repl-data`. The volumes behind them hold
catalog data that takes **days to weeks** to re-ingest. Releasing those PVCs by
accident — through a rename that changes generated PVC names, an Argo CD prune, an STS
recreate, or storage-class reclaim — is the largest operational risk of running Qserv
on Kubernetes. Argo CD sync is **intentionally manual** as a guard. Note that the
chart builds StatefulSet names from the **chart name** (`qserv-worker` → PVC
`worker-data-qserv-worker-N`), not the Helm release name — so a release rename keeps
PVC names, but it changes the immutable selector labels and forces an STS
delete/recreate, while a chart or StatefulSet rename does change PVC names. Treat
every change to
`deploy/helm/templates/*-sts.yaml`, release/chart names, labels/selectors, or
qserv-deployments as PVC-affecting until proven otherwise, and say so in the PR.

## Images

Five images, built by `.github/workflows/ci.yml` via `./bin/qserv` and published to
GHCR, all tagged from `git describe` (release tags look like `2026.8.1-rc2`):

| Image | Dockerfile | Role |
| --- | --- | --- |
| `ghcr.io/lsst/qserv` | `deploy/docker/run/Dockerfile` | The application image: all C++ binaries + Python tooling, entered via `entrypoint` |
| `ghcr.io/lsst/qserv-mariadb` | `deploy/docker/mariadb/Dockerfile` | MariaDB with Qserv extras (scisql UDFs) |
| `ghcr.io/lsst/qserv-build-base` | `deploy/docker/base/Dockerfile` (target `qserv-build-base`) | Build toolchain (also used as `ingestHelper` image) |
| `ghcr.io/lsst/qserv-run-base` | `deploy/docker/base/Dockerfile` (target `qserv-run-base`) | Runtime base for the qserv image |
| ssl-proxy | `deploy/docker/ssl-proxy/Dockerfile` | TLS terminator for the mysql-proxy frontend |

The container `entrypoint` (`bin/entrypoint` → `python/lsst/qserv/admin/cli/entrypoint.py`)
is how every service starts: `entrypoint czar-http | proxy | worker-svc | worker-repl |
replication-controller | replication-registry | smig-update | ...`. It renders config
templates from `src/admin/templates/`, waits for/migrates schemas, then execs the C++
binary.

## Process/service topology

The same topology appears in docker-compose (dev/CI) and Kubernetes (production):

- **Czar** — three colocated processes: MariaDB (`qservMeta`, `qservCssData`,
  `qservStatusData`, `qservResult` result tables);
  `mysql-proxy` (port 14040) embedding Lua glue (`src/proxy/mysqlProxy.lua`) that calls
  into czar code for the SQL frontend; `qserv-czar-http` (port 4048) — the HTTP/JSON
  frontend (sync/async queries, user-table ingest). Both frontends run the full czar
  query machinery in-process.
- **Worker ×N** — three colocated processes: MariaDB (chunk data, `qservw_worker`);
  `qserv-worker-http` (port 25010, the query-serving worker daemon);
  `qserv-replica-worker` (replication/ingest agent).
- **Replication controller** — `qserv-replica-master-http` (port 25081) + its MariaDB
  (`qservReplica`). Singleton.
- **Registry** — `qserv-replica-registry` (port 25082), stateless service discovery:
  workers/czars self-register; czar and controller poll it.
- **Dashboard** — the web UI in `www/` is served by the replication controller's HTTP
  server.

## docker-compose (dev/CI)

`deploy/compose/docker-compose.yml`, driven by `./bin/qserv up|down`. Two workers,
one czar (proxy + http), repl controller/registry + their MariaDBs. Container names are
`$USER-<service>-1`. Note the proxy listens on **4040** here (deployments use 14040).
This is what integration tests (`./bin/qserv itest*`) run against.
Note the compose file passes fixed test passwords/keys (`CHANGEME`, `replauthkey`) —
fine locally, never a pattern to copy elsewhere.

## Helm chart (production shape)

Chart source: `deploy/helm/` (`Chart.yaml`, `values.yaml`, `templates/`). Rendered
resources:

- `czar-sts.yaml`, `worker-sts.yaml`, `repl-sts.yaml` — StatefulSets described above,
  each with a data PVC from `volumeClaimTemplates`; worker STS supports node-tier
  affinity (`qserv.lsst.io/tier` label) and host anti-affinity.
- `registry-deploy.yaml` — stateless registry Deployment.
- `ingest-sts.yaml` (optional, `ingest.enable`) — ingest helper.
- `*-smig-job.yaml` — schema-migration Jobs run per upgrade (czar, worker, repl).
- `*-cm.yaml` / secrets — config files and passwords/auth keys mounted into pods.
- `czar-external-svc.yaml` (optional) — LoadBalancer exposing the SQL/HTTP frontends
  outside the cluster (MetalLB at USDF).
- `mode: full | db-only` in values — `db-only` runs just the MariaDBs (used
  operationally for maintenance; see `qserv.enable` helper in `_helpers.tpl`).

`deploy/helm/environments/usdf-{dev,int,prod}.yaml` are in-repo presets, but the values
actually deployed live in **qserv-deployments** (below).

## qserv-deployments + Argo CD (USDF gitops)

Repo: `github.com/lsst/qserv-deployments` — a **separate git repo**, conventionally
checked out as a sibling of this one (`../qserv-deployments/`); it carries its own
AGENTS.md with the deployment-change workflow. Layout:
`deployments/usdf-qserv-{dev,int,prod}/`, each with:

- `application.yaml` — an Argo CD `Application` with two sources: the packaged chart
  from `ghcr.io/lsst/charts` (`chart: qserv`, pinned `targetRevision`, e.g.
  `2026.8.1-rc2-23-g750048b57`) and this git repo for `values.yaml` (multi-source
  `$values` ref).
- `values.yaml` — deployment overrides: node tiers, worker replica count (70 in dev,
  35 in int and prod as of 2026-08), ingest enablement, external LoadBalancer
  IP/allowed ranges. Image names and storage (class `rubin-qserv-storage`, sizes such
  as 10 Ti per worker) are **not** set here — they come from the chart's own
  `values.yaml`, pinned per chart release. (The deployment values used to override
  image names; that was deliberately removed — see qserv-deployments commit "Remove
  image values".)

Argo CD watches `main` of qserv-deployments and reconciles — but **sync is manual**: a
human reviews the diff in Argo CD and syncs. Deploying a new Qserv version means: tag
qserv → CI publishes images → chart published to `ghcr.io/lsst/charts` (this publish
step is *not* in this repo's workflows as of 2026-08 — verify how before relying on
it) → PR to qserv-deployments bumping `targetRevision` and image names → merge → manual
sync. Smig jobs handle schema upgrades during rollout.

## Ports quick reference

| Port | Service |
| --- | --- |
| 14040 | czar mysql-proxy (SQL frontend) |
| 4048 | czar HTTP frontend (REST) |
| 25010 | worker qserv-worker-http |
| 25081 | replication controller REST + dashboard |
| 25082 | registry |
| 3306 | each colocated MariaDB |
