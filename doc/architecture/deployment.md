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
every change to `deploy/helm/templates/*-sts.yaml`, release/chart names,
labels/selectors, or qserv-deployments as PVC-affecting until proven otherwise, and
say so in the PR.

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

Chart source: `deploy/helm/` (`Chart.yaml`, `values.yaml`, `templates/`, `files/`).
Rendered resources:

- `czar-sts.yaml`, `worker-sts.yaml`, `repl-sts.yaml` — StatefulSets described above,
  each with a data PVC from `volumeClaimTemplates`; worker STS supports node-tier
  affinity (`qserv.lsst.io/tier` label) and host anti-affinity.
- `registry-deploy.yaml` — stateless registry Deployment.
- `ingest-sts.yaml` (optional, `ingest.enable`) — ingest helper.
- `*-smig-job.yaml` — schema-migration Jobs run per upgrade (czar, worker, repl).
- `*-cm.yaml` / secrets — config files and passwords/auth keys mounted into pods.
- `czar-external-svc.yaml` (optional) — LoadBalancer exposing the SQL/HTTP frontends
  outside the cluster (MetalLB at USDF).
- `mode: full | db-only | mariadb-upgrade` in values — `db-only` runs just the MariaDBs
  (used operationally for maintenance); `mariadb-upgrade` additionally replaces the
  MariaDB container command with an in-place `mariadb-upgrade` run (see
  *MariaDB major-version upgrades* below, and `mariadbUpgrade.force` there). Both gate
  off every Qserv sidecar; see the `qserv.enable` helper in `_helpers.tpl`. An
  unrecognised mode fails the render.
- `files/mariadb-upgrade.sh` — the upgrade-mode container command, kept as a real shell
  script rather than inline YAML (so `shellcheck`/`bash -n` apply) and inlined verbatim
  into all three StatefulSets by the `qserv.mariadbUpgradeScript` helper. It takes no
  template substitutions: `mariadbUpgrade.force` reaches it as the
  `QSERV_MARIADB_UPGRADE_FORCE` environment variable, so the file is never passed
  through `tpl`.

## MariaDB major-version upgrades

After a MariaDB major-version bump, each MariaDB instance needs `mariadb-upgrade` run
against its own data. `mariadb-upgrade` learns the datadir from the server
(`SHOW VARIABLES LIKE 'datadir'`) but then writes its version stamp on its *own*
filesystem, so it cannot be run usefully from a floating Job the way the smig jobs are —
it needs the data volume mounted. `mode: mariadb-upgrade` solves this by reusing the
StatefulSets: each pod already has its own PVC attached on the right node, so the
upgrade runs locally with no co-scheduling or PVC-name games.

Operating sequence (the teardown/redeploy cycle already used for every upgrade, which
leaves PVCs intact):

1. Delete the deployed release (pods go away, PVCs remain).
2. Deploy with `mode: mariadb-upgrade`. MariaDB pods come up, upgrade their own data,
   then park with `mariadbd` running so the probes keep passing.
3. Audit completion (below), then delete and redeploy with `mode: full`.

Notes on the design, all deliberate:

- The upgrade runs the **full** `mariadb-upgrade`, *not* `--upgrade-system-tables`. That
  means `mariadb-check` runs over all databases with `--auto-repair` and
  `--process-views=UPGRADE`, which remediates drift accumulated across MariaDB
  revisions. On workers this is expected to take minutes and to hammer the disks; run it
  in a planned maintenance window.
- No full data snapshot is taken — at worker scale that would be hundreds of terabytes
  of copying. Back up separately if you want one.
- Upstream's `MARIADB_AUTO_UPGRADE` is deliberately **not** used: it hardcodes
  `--upgrade-system-tables` (so it cannot do the full check) and its backup is only
  suppressible wholesale. The mode bypasses the image's `docker-entrypoint.sh` entirely.
- Repair scratch space is pointed at `/qserv/data/tmp` rather than the default `/tmp`,
  which is a small `emptyDir` a large MyISAM repair could fill.
- `livenessProbe.failureThreshold` is relaxed in this mode so a long `REPAIR TABLE`
  cannot get the container killed mid-repair.
- Because the entrypoint is bypassed, the upgrade script — not `mariadbd` — is PID 1, so
  it traps `SIGTERM`/`SIGINT` and shuts the server down itself; otherwise the teardown in
  step 3 would `SIGKILL` `mariadbd`. Bash defers a trap until the running foreground
  command returns, so this covers a pod parked after its upgrade, *not* one deleted while
  `mariadb-upgrade` is still repairing. Audit completion before deleting pods, and note
  that a graceful shutdown still has to fit inside the pod's termination grace period
  (the chart leaves this at the 30s default).

**Pod phase is not the completion signal** — pods stay Running and Ready by design. Use:

- `/qserv/data/mysql-upgrade.status` on each PVC — the durable record. One line of
  `key=value` fields, the same vocabulary as the result line below:

  ```
  status=ok version=12.3.3-MariaDB-ubu2404 started=2026-11-19T20:00:00+00:00 finished=2026-11-19T20:05:00+00:00 pod=qserv-worker-0
  ```

  Those five fields are all it carries, deliberately: the question it answers is "is this
  volume upgraded for this server version", and the log below is authoritative for *what*
  the run did. It is **not** JSON — nothing consumes it as JSON, and the script reads it
  by splitting the line into fields, so the format is one that can actually be parsed
  rather than one that merely looks structured and gets matched with substrings. This is
  the source of truth and it survives the teardown in step 3 (pod logs do not). It is
  written atomically (staged alongside, then renamed), so a reader never sees a
  half-written marker, and a write that fails — most plausibly the data volume filling,
  since the repair scratch space lives on it — parks the pod for a human rather than
  being ignored. Startup failures (bad root password, `mariadbd` dying before it accepts
  connections) are deliberately *not* recorded here — they carry no version and no repair
  work, and are usually transient, so the container just exits non-zero and crashloops
  visibly instead of parking a healthy-looking pod.
- A `QSERV-UPGRADE-RESULT status=…` line in the container log — the convenient view:

  ```sh
  for c in czar repl worker; do
      kubectl logs -l app.kubernetes.io/component=$c --tail=-1 --prefix \
        | grep QSERV-UPGRADE-RESULT
  done
  ```

  Count the lines against each component's replica count: every volume should report `ok`
  or `already-done`. `--tail=-1` matters: with a selector, `kubectl logs` otherwise shows
  only the last 10 lines per pod. The statuses are `ok`, `already-done`, `failed exit=N`,
  `needs-attention reason=…` (see below), and `failed-startup` — the last of which
  crashloops rather than parking, so it writes no marker.
- `/qserv/data/mysql-upgrade.log` — the full `mariadb-check` output, retained per volume
  and appended to by every run. This is where you look to see *which* tables were checked
  and repaired; the marker deliberately does not try to summarise it.

**Audit all three components, not just the workers.** Every one of the three
StatefulSets runs the upgrade against its own PVC and writes its own marker, so a czar or
repl volume that silently failed to upgrade is as broken as a worker that did. The czar
and repl databases are small and finish long before the workers, which makes them easy to
miss when scrolling worker output. Marker sweep:

```sh
for c in czar repl worker; do
    for p in $(kubectl get pods -l app.kubernetes.io/component=$c -o name); do
        echo "== $p"; kubectl exec $p -- cat /qserv/data/mysql-upgrade.status
    done
done
```

Scoping by release instead of by component (`-l app.kubernetes.io/instance=<release>`)
also works, but needs `--all-containers`: the ingest StatefulSet is gated on
`ingest.enable` rather than `qserv.enable`, so its multi-container pods can still be
running in this mode.

### Marker semantics and re-running

The script is deliberately blunt: it succeeds quietly on the simple path and parks for a
human on everything else. Nothing is ever retried unattended, because re-running repairs
on a multi-terabyte volume is not a decision a container command should make on its own.
One rule, keyed on the **running** server version from `SELECT VERSION()` at startup:

- No marker, or a marker from an *earlier* server version → the upgrade runs. An old
  marker describes the version that wrote it, not this one.
- Marker records `ok` for the running version → skipped (`already-done`), so the mode can
  be re-applied to finish a partially-completed multi-worker run.
- Marker exists for the running version and does **not** say `ok` — a recorded `failed`,
  or a `running` left by a pod killed mid-repair — → parks with
  `needs-attention reason=previous-run-did-not-complete`.
- Marker exists but is not exactly one record of the five fields the script writes —
  truncated, reordered, garbage, unknown status value, extra fields, more than one line →
  parks with `needs-attention reason=marker-unreadable` rather than assuming anything.
  Because the line is split into fields and the decision is made from the parsed values,
  no partial or concatenated file can authorise a skip or a repair by happening to
  contain a plausible-looking fragment.

Everything else that goes wrong parks the same way, with the reason naming the cause:
`marker-not-writable`, `log-not-writable`, `log-write-failed`,
`upgrade-ok-but-marker-not-writable`. A parked pod stays Running and Ready, so look for
the result line, not the pod phase.

The version keying is why **no cleanup tooling is needed before the next major upgrade**:
the marker self-invalidates as soon as the image's `VERSION()` changes, whatever it
records. There is nothing to sweep, and a stale marker cannot block a future upgrade.
(Note this also means re-selecting the mode after a mere *patch* bump will re-run the
full check/repair — only an operator deliberately choosing the mode can trigger that, so
it errs on the conservative side.)

To clear any parked volume — a recorded failure, an interrupted run, an unreadable
marker — on the same server version, set `mariadbUpgrade.force: true`
in values and redeploy. That ignores the marker entirely, on every volume in the release —
there is no way to scope it to one pod or one component, so a force aimed at a single
failed worker also re-runs the full check/repair on the czar and repl volumes. Investigate
the recorded failure first, and unset it afterwards. It exists so recovery is a values
change rather than a human exec'ing into each pod to `rm` marker files.

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
  `values.yaml`, pinned per chart release (image overrides were deliberately removed;
  see qserv-deployments commit "Remove image values").

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
