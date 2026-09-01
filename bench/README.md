# RA Bench Harness

Measures resource requirements and validates SLOs for Orbit on self-managed deployments. Replays a datalake dump against a standalone ClickHouse and an e2e GKG stack on GKE, optionally with code indexing against a mock git server backed by real gitlab-org archives.

## Prerequisites

- `gcloud` authenticated with access to `gl-knowledgegraph-prj-f2eec59d`
- `terraform` >= 1.5
- `helm` 3
- `yq` (for reading config YAML)
- `docker` with `buildx` (only for code indexing, to build the mock server image)
- SSH key registered with gitlab.com (only for fetching the code corpus)

## From scratch

Use this when there is no existing datalake snapshot or code corpus. This is the first-time setup.

### 0. Create the cluster

The GKE cluster and node pools are managed by Terraform in `bench/infra/`, wrapped by `infra.sh`. The tier variable controls the machine type, node count (from `tiers.yaml`), and cluster name (defaults to `ra-bench-{tier}`).

First-time setup requires creating the Terraform state bucket (run once):

```bash
cd bench/infra/bootstrap
terraform init && terraform apply
```

Then create the cluster:

```bash
bash bench/scripts/infra.sh init
bash bench/scripts/infra.sh apply   # reads tier from bench.yaml, creates "ra-bench-small"
```

The tier is read from `bench.yaml`. Change it there, or override with `TIER=large`. `KCTX` is auto-derived from `terraform output` by `lib.sh`.

### 1. Provision the stack

Deploys standalone ClickHouse, the full e2e stack (GitLab, NATS, Siphon, GKG), imports the datalake dump from GCS (~15 minutes), and starts SDLC indexing.

```bash
RUN_ID=bench8 bash bench/scripts/provision.sh
```

### 2. Create a golden snapshot

Once the import finishes and the graph starts building, snapshot the CH PVC so future runs skip the 15-minute import:

```bash
RUN_ID=bench8 DUMP_PREFIX=core-2026-06-25-1115 bash bench/scripts/snapshot-datalake.sh
```

### 3. Build the code corpus

Dump the project list from the bench datalake and fetch archives from gitlab.com:

```bash
RUN_ID=bench8 bash bench/scripts/dump-project-list.sh > projects.tsv
bash bench/scripts/fetch-code-corpus.sh projects.tsv
```

This streams archives via SSH directly to GCS. 7,736 repos takes a few hours. Test with a subset first:

```bash
FETCH_MAX=50 bash bench/scripts/fetch-code-corpus.sh projects.tsv
```

### 4. Deploy the mock git server

The GCS FUSE CSI driver must be enabled on the cluster (one-time). Use the cluster name from the infra output:

```bash
CLUSTER=$(bash bench/scripts/infra.sh output -raw cluster_name)
gcloud container clusters update "$CLUSTER" \
  --project gl-knowledgegraph-prj-f2eec59d \
  --zone us-central1-a \
  --update-addons GcsFuseCsiDriver=ENABLED
```

Then deploy the mock server and point the indexer at it:

```bash
RUN_ID=bench8 bash bench/scripts/deploy-mock-git-server.sh
```

This builds the image, deploys the pod with GCS FUSE, upgrades the GKG helm release, resets code indexing checkpoints, and restarts the indexer. Code indexing starts immediately.

### 5. Check SLOs

```bash
RUN_ID=bench8 bash bench/scripts/slos.sh
```

## From snapshot (fast path)

Use this when a golden snapshot and code corpus already exist. Takes ~10 minutes instead of ~30.

### 1. Provision from snapshot

If the snapshot is on the current cluster:

```bash
RUN_ID=bench9 RA_DATALAKE_SNAPSHOT=golden-core-2026-06-25-1115 \
  bash bench/scripts/provision.sh
```

If the snapshot was created on a different cluster (e.g. provisioning a fresh
Terraform-managed cluster from a snapshot on the old `ra-bench-smoke`):

```bash
RUN_ID=bench9 RA_DATALAKE_SNAPSHOT=golden-core-2026-06-25-1115 \
  RA_SNAPSHOT_SOURCE_CTX=gke_gl-knowledgegraph-prj-f2eec59d_us-central1-a_ra-bench-smoke \
  bash bench/scripts/provision.sh
```

You can also pass the GCE disk snapshot handle directly with `RA_SNAPSHOT_HANDLE`
to skip the VolumeSnapshot lookup entirely.

The datalake restores from the snapshot (~5 minutes). The graph database is dropped and rebuilt from scratch. SDLC indexing starts immediately.

### 2. Deploy the mock git server

The code corpus is already in `gs://gkg-code-corpus` from the initial scrape. Just deploy the mock server:

```bash
RUN_ID=bench9 bash bench/scripts/deploy-mock-git-server.sh
```

Code indexing starts against the existing 7K repo corpus. Both SDLC and code index in parallel.

### 3. Check SLOs

```bash
RUN_ID=bench9 bash bench/scripts/slos.sh
```

## Cluster lifecycle

Each tier gets its own cluster (`ra-bench-small`, `ra-bench-medium`, `ra-bench-large`). The full lifecycle is create, use, destroy.

```bash
bash bench/scripts/infra.sh apply                              # create ra-bench-{tier}
bash bench/scripts/infra.sh apply -var dedicated_ch_pool=true  # add CH pool
bash bench/scripts/infra.sh destroy                            # tear down everything
```

To change the tier, edit `bench.yaml`. Each tier produces an isolated cluster.

## Iterating on code changes

Two commands support the inner development loop without re-provisioning
the full stack or re-importing the datalake.

### Reload (wipe graph, keep datalake)

Drops and recreates the `gkg` database, then restarts GKG. The
checkpoint tables live in the graph DB, so dropping it implicitly
resets them. The indexer re-creates the schema on boot and re-indexes
from the existing datalake data. Grants persist across the drop.

```bash
RUN_ID=bench1 bash bench/scripts/infra.sh reload
```

### Deploy new code

Builds a debug GKG image from your local working tree, pushes it to the
registry, upgrades the Helm release, and restarts the pods. To skip the
build and use an existing image (e.g. a CI dev tag), set `GKG_IMAGE_TAG`.

```bash
RUN_ID=bench1 bash bench/scripts/infra.sh deploy                    # build locally
GKG_IMAGE_TAG=latest RUN_ID=bench1 bash bench/scripts/infra.sh deploy  # use pre-built
```

To deploy and wipe the graph in one step:

```bash
RUN_ID=bench1 bash bench/scripts/infra.sh deploy --reload
```

The image is tagged `bench-<short-sha>-dirty` when the working tree has
uncommitted changes, or `bench-<short-sha>` for a clean tree.

## Dedicated ClickHouse pool

For isolated CH sizing runs, set `dedicated_ch_pool = true` at plan time. This creates a tainted node pool that only the CH StatefulSet schedules on. `provision.sh` reads this from `terraform output` and sets the node selector and tolerations automatically.

## Corpus management

```bash
# Retry failed fetches with longer timeout
RETRY=fail ARCHIVE_TIMEOUT=300 bash bench/scripts/fetch-code-corpus.sh

# Retry both failures and skips
RETRY=fail,skip bash bench/scripts/fetch-code-corpus.sh

# Re-fetch everything (overwrites existing archives)
RETRY=all bash bench/scripts/fetch-code-corpus.sh
```

The fetch script auto-downloads `projects.tsv` from GCS if no file is given.

## Configuration

All config lives in two YAML files:

- `bench/config/bench.yaml` -- active tier, GCP project, region, bucket names, image tags, corpus settings
- `bench/config/tiers.yaml` -- per-tier resource limits, concurrency, SLO targets

Env vars (`TIER`, `RUN_ID`) override the YAML values when set.

## Scripts

| Script | What it does |
|---|---|
| `infra.sh` | Terraform lifecycle wrapper: init, apply, plan, destroy, output |
| `provision.sh` | Full stack deploy: CH, e2e, import, checkpoint reset, GMP |
| `slos.sh` | SLO report from Cloud Monitoring + CH query_log |
| `deploy-mock-git-server.sh` | Build, deploy mock, upgrade GKG, reset code checkpoints |
| `dump-project-list.sh` | Export project IDs from the datalake |
| `fetch-code-corpus.sh` | Fetch repo archives from gitlab.com to GCS |
| `snapshot-datalake.sh` | VolumeSnapshot of a loaded CH PVC |
| `render-tier.sh` | Generate GKG helm values from tiers.yaml |
| `validate.sh` | Post-provision readiness checks |
| `import-dump-job.sh` | Submit GCS datalake import K8s job |

## Known issues

- CH system logs grow unbounded without TTLs. `provision.sh` sets 1-day TTLs and truncates existing bloat on each run, but a long-running bench can still accumulate logs between provisions.
- The indexer needs a 12 GiB memory limit for the WorkItem extract (peaks at 7.4 GiB sorting 1M rows with description text). Set in tiers.yaml.
- Self-managed CH defaults `max_bytes_before_external_sort` to 0 (disabled). The bench sets it to 8 GiB via a ConfigMap override. Without it, wide-column sorts OOM the server.
- The mock git server does not return commit SHAs, so `last_commit` in code indexing checkpoints stays empty. The code graph data is written correctly.
