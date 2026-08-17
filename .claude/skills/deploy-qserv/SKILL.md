---
name: deploy-qserv
description: Qserv release and deployment workflow — version tags, GHCR images, the Helm chart, qserv-deployments values, and Argo CD at USDF. Use for any release, chart, values, or deployment-config task. Contains the mandatory PVC safety checklist.
---

# Releasing and deploying Qserv

## ⚠️ PVC safety checklist — read before ANY chart or deployment change

The StatefulSets (`worker`, `czar`, `repl` in `deploy/helm/templates/*-sts.yaml`)
create PVCs from `volumeClaimTemplates` (`worker-data-qserv-worker-N`, etc.). Those PVs
hold catalog data that takes **days to weeks** to re-ingest. Before proposing any
change, confirm it cannot cause Kubernetes or Argo CD to delete, recreate, or orphan
those PVCs:

- Renaming the chart, a StatefulSet, or a volumeClaimTemplate changes generated PVC
  names → new empty volumes get bound and the old ones are stranded (or reclaimed,
  depending on the StorageClass reclaim policy). STS names derive from the **chart
  name**, not the Helm release name, so a release rename keeps PVC names — but it
  changes the immutable selector labels and forces an STS delete/recreate.
- Most `volumeClaimTemplates` spec fields are immutable; changes there force STS
  delete/recreate.
- Argo CD prune/auto-sync could delete resources removed from the rendered chart. Sync
  is intentionally **manual** — never suggest enabling auto-sync or running a prune
  without explicit human sign-off naming the resources to be pruned.
- `helm uninstall` / namespace deletion do not delete PVCs by default, but cluster
  policy or manual cleanup afterwards can. Treat PVC deletion as unrecoverable.

Flag the risk explicitly in any PR/plan that touches these files, and say what happens
to existing PVCs.

## The pipeline

1. **Code → images.** Every push runs `.github/workflows/ci.yml`, which (re)builds only
   missing images and publishes `ghcr.io/lsst/qserv:<git-describe>` plus
   `qserv-mariadb`, `qserv-build-base`, `qserv-run-base`, ssl-proxy images. Release
   versions come from annotated tags like `2026.8.1-rc2`; between tags the version is
   `<tag>-<n>-g<sha>` from `git describe`. The CI job summary prints ready-to-paste
   `values.yaml` image names.
2. **Chart.** The chart source is `deploy/helm/` (`Chart.yaml` version is set as part
   of the release process); the packaged chart is consumed from `ghcr.io/lsst/charts`
   as OCI. Note: no workflow in this repo pushes the chart (verified 2026-08) — the
   publish step happens outside this repo; ask before assuming how it runs.
3. **Deployment config.** `qserv-deployments` repo (a separate git repo,
   conventionally checked out as a sibling: `../qserv-deployments/`; see its
   AGENTS.md): `deployments/usdf-qserv-{dev,int,prod}/application.yaml` pins chart
   `targetRevision`; `values.yaml` overrides node tiers, replica counts, ingest
   enablement, and the external LoadBalancer. Image names and storage class/size come
   from the chart's own `values.yaml`, pinned per chart release — the deployments
   deliberately no longer override images. Update by PR to that repo.
4. **Argo CD.** Watches `qserv-deployments` `main`. A human performs the sync in the
   Argo CD UI/CLI (manual by design). Rollout order matters: smig Jobs
   (`*-smig-job.yaml`) migrate DB schemas; StatefulSets restart pods against the
   migrated schemas.

## Topology reference (what a deployment looks like)

- `qserv-czar` STS: containers `mariadb` (result tables + qmeta/css), `mysql-proxy`
  (port 14040, SQL frontend), `czar-http` (port 4048, HTTP/REST frontend). PVC `czar-data`.
- `qserv-worker` STS ×N: `mariadb` (chunk data), `worker-svc` (`qserv-worker-http`,
  port 25010), `repl-worker` (`qserv-replica-worker`). PVC `worker-data`.
- `qserv-repl` STS: `mariadb` (replication config/state DB) + `repl-controller`
  (`qserv-replica-master-http`, port 25081). PVC `repl-data`.
- `qserv-registry` Deployment (`qserv-replica-registry`, port 25082): service
  discovery for workers/czars.
- Optional `ingest` STS and `czar-external-svc` (MetalLB LoadBalancer at USDF).
- Environment presets also exist in-repo: `deploy/helm/environments/usdf-{dev,int,prod}.yaml`
  (the qserv-deployments values are the ones actually deployed).

## Local sanity checks for chart changes

```sh
helm template deploy/helm -f ../qserv-deployments/deployments/usdf-qserv-dev/values.yaml > /tmp/rendered.yaml
helm lint deploy/helm
# then diff rendered output before/after your change; scrutinize anything touching
# volumeClaimTemplates, StatefulSet names, or labels/selectors.
```
