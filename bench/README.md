# RA Bench Harness

Measures resource requirements and validates SLOs for Orbit on self-managed deployments. Replays a datalake dump against a standalone ClickHouse and an e2e GKG stack on GKE, optionally with code indexing against a mock git server backed by real gitlab-org archives.

## Prerequisites

- `gcloud` authenticated with access to `gl-knowledgegraph-prj-f2eec59d`
- `kubectl` with a context pointing at the `ra-bench-smoke` cluster
- `helm` 3
- `yq` (for reading config YAML)
- `docker` with `buildx` (only for code indexing, to build the mock server image)
- SSH key registered with gitlab.com (only for fetching the code corpus)

Set `KCTX` to your kubectl context before running any script:

```bash
export KCTX="gke_gl-knowledgegraph-prj-f2eec59d_us-central1-a_ra-bench-smoke"
```

## Quick start (SDLC only)

Provision a bench run with a golden datalake snapshot. Takes ~10 minutes.

```bash
TIER=small RUN_ID=bench8 \
  RA_DATALAKE_SNAPSHOT=golden-core-2026-06-25-1115 \
  bash bench/scripts/provision.sh
```

This deploys standalone ClickHouse, the full e2e stack (GitLab, NATS, Siphon, GKG), restores the datalake from the snapshot, drops the graph database, and lets the indexer rebuild it from scratch.

Check progress:

```bash
kubectl --context "${KCTX}" exec -n ra-ch-bench8 clickhouse-0 -- \
  clickhouse-client -q "SELECT name, total_rows FROM system.tables WHERE database='gkg' AND total_rows > 0 ORDER BY total_rows DESC FORMAT PrettyCompact"
```

Run the SLO report:

```bash
RUN_ID=bench8 TIER=small bash bench/scripts/slos.sh
```

## Adding code indexing

Code indexing requires a corpus of repository archives in GCS and a mock git server to serve them.

### 1. Build the corpus

Dump the project list from the bench datalake and fetch archives from gitlab.com:

```bash
# Dump project IDs (stored in GCS automatically)
RUN_ID=bench8 bash bench/scripts/dump-project-list.sh > projects.tsv

# Fetch archives (streams via SSH, no local disk for GCS path)
bash bench/scripts/fetch-code-corpus.sh projects.tsv
```

The fetch script reads defaults from `bench/config/bench.yaml`. Override with env vars:

```bash
FETCH_MAX=100 bash bench/scripts/fetch-code-corpus.sh      # test with 100 repos
ARCHIVE_TIMEOUT=300 bash bench/scripts/fetch-code-corpus.sh # longer timeout for big repos
RETRY=fail bash bench/scripts/fetch-code-corpus.sh          # retry failures from previous run
RETRY=all bash bench/scripts/fetch-code-corpus.sh           # re-fetch everything
```

### 2. Deploy the mock server and start indexing

```bash
RUN_ID=bench8 bash bench/scripts/deploy-mock-git-server.sh
```

This builds and pushes the mock server image, deploys it in the CH namespace with GCS FUSE mounting the corpus bucket, upgrades the GKG helm release to point at the mock, resets code indexing checkpoints, and restarts the indexer.

The GCS FUSE CSI driver must be enabled on the cluster (one-time setup):

```bash
gcloud container clusters update ra-bench-smoke \
  --project gl-knowledgegraph-prj-f2eec59d \
  --zone us-central1-a \
  --update-addons GcsFuseCsiDriver=ENABLED
```

## Cluster management

```bash
bash bench/scripts/cluster.sh sleep     # resize to 0 nodes (~$2.40/day)
bash bench/scripts/cluster.sh wake      # resize back to 3 nodes
bash bench/scripts/cluster.sh wake 5    # resize to 5 nodes
bash bench/scripts/cluster.sh teardown  # delete all namespaces for this run
```

## Dedicated node pools

For isolated CH sizing runs, add `RA_DEDICATED_POOL=1` to provision. This creates a tainted node pool that only the CH pod schedules on.

```bash
RA_DEDICATED_POOL=1 RUN_ID=bench8 TIER=small bash bench/scripts/provision.sh
```

Teardown deletes the pool:

```bash
RA_DEDICATED_POOL=1 RUN_ID=bench8 bash bench/scripts/cluster.sh teardown
```

## Golden snapshots

After a successful import, snapshot the CH PVC so future runs skip the 15-minute import:

```bash
RUN_ID=bench8 DUMP_PREFIX=core-2026-06-25-1115 bash bench/scripts/snapshot-datalake.sh
```

Then provision from the snapshot:

```bash
RA_DATALAKE_SNAPSHOT=golden-core-2026-06-25-1115 RUN_ID=bench9 bash bench/scripts/provision.sh
```

The snapshot restores in ~5 minutes. The datalake is preserved, the graph is dropped and rebuilt.

## Configuration

All defaults live in two YAML files:

- `bench/config/bench.yaml` -- GCP project, bucket names, image tags, IAM accounts, corpus settings
- `bench/config/tiers.yaml` -- per-tier resource limits, concurrency, SLO targets

Every env var can override the YAML defaults. The scripts never hardcode GCP-specific values.

## Scripts

| Script | What it does |
|---|---|
| `provision.sh` | Full stack deploy: CH, e2e, import, checkpoint reset, GMP |
| `slos.sh` | SLO report from Cloud Monitoring + CH query_log |
| `cluster.sh` | sleep / wake / teardown |
| `deploy-mock-git-server.sh` | Build, deploy mock, upgrade GKG, reset code checkpoints |
| `dump-project-list.sh` | Export project IDs from the datalake |
| `fetch-code-corpus.sh` | Fetch repo archives from gitlab.com to GCS |
| `snapshot-datalake.sh` | VolumeSnapshot of a loaded CH PVC |
| `render-tier.sh` | Generate GKG helm values from tiers.yaml |
| `validate.sh` | Post-provision readiness checks |
| `import-dump-job.sh` | Submit GCS datalake import K8s job |

## Known issues

- CH system logs grow unbounded without TTLs. `provision.sh` sets 1-day TTLs via ALTER TABLE on each run, but a fresh CH from a snapshot may have existing bloat. The script truncates the biggest tables on provision.
- The indexer needs a 12 GiB memory limit for the WorkItem extract (sorts 1M rows with description text, peaks at 7.4 GiB). The default in tiers.yaml is set accordingly.
- Self-managed CH defaults `max_bytes_before_external_sort` to 0 (disabled). The bench sets it to 8 GiB via a ConfigMap override. Without it, wide-column sorts OOM the server.
