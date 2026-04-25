# Release process runbook

Operations guide for cutting a GKG release and rolling it to gkg-orbit-stg and gkg-orbit-prd. Covers where things live, how versions propagate across three repos, schema migration handling, observability, and the failure modes we have already hit in production.

## TL;DR

A release is three coordinated changes:

1. Tag a new `vX.Y.Z` on [knowledge-graph](https://gitlab.com/gitlab-org/orbit/knowledge-graph) by triggering the manual `semantic-release` job on `main`. The tag pipeline builds and pushes `registry.gitlab.com/gitlab-org/orbit/knowledge-graph/gkg:X.Y.Z`.
2. Bump the chart in [gkg-helm-charts](https://gitlab.com/gitlab-org/orbit/gkg-helm-charts) only when chart templates need a fix. Tag `vA.B.C`; the pipeline publishes the OCI artifact.
3. Open an MR on [argocd/apps](https://gitlab.com/gitlab-com/gl-infra/argocd/apps) bumping `services/gkg/values.yaml` `image.tag` and, if the chart changed, `gkg.chart.version` in `services/gkg/service.yaml`. ArgoCD syncs gkg-orbit-stg first, then gkg-orbit-prd.

A schema-version bump triggers an additional gate in production: the indexer will not promote the new active version until at least one namespace is enabled.

## Where everything lives

### Repos

| Repo | Role | Key files |
|---|---|---|
| [gitlab-org/orbit/knowledge-graph](https://gitlab.com/gitlab-org/orbit/knowledge-graph) | Rust source for the `gkg` binary; builds and tags the image | [.releaserc.json](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/blob/main/.releaserc.json), [.gitlab-ci.yml](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/blob/main/.gitlab-ci.yml), [crates/indexer/src/schema/completion.rs](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/blob/main/crates/indexer/src/schema/completion.rs) |
| [gitlab-org/orbit/gkg-helm-charts](https://gitlab.com/gitlab-org/orbit/gkg-helm-charts) | Helm chart for indexer, webserver, dispatcher, healthcheck | [chart/Chart.yaml](https://gitlab.com/gitlab-org/orbit/gkg-helm-charts/-/blob/main/chart/Chart.yaml), [chart/templates/webserver/configmap.yaml](https://gitlab.com/gitlab-org/orbit/gkg-helm-charts/-/blob/main/chart/templates/webserver/configmap.yaml), [scripts/publish-helm-chart.sh](https://gitlab.com/gitlab-org/orbit/gkg-helm-charts/-/blob/main/scripts/publish-helm-chart.sh) |
| [gitlab-com/gl-infra/argocd/apps](https://gitlab.com/gitlab-com/gl-infra/argocd/apps) | GitOps source for gkg-orbit-stg and gkg-orbit-prd | [services/gkg/values.yaml](https://gitlab.com/gitlab-com/gl-infra/argocd/apps/-/blob/main/services/gkg/values.yaml), [services/gkg/service.yaml](https://gitlab.com/gitlab-com/gl-infra/argocd/apps/-/blob/main/services/gkg/service.yaml), [services/gkg/env/orbit-prd/values.yaml](https://gitlab.com/gitlab-com/gl-infra/argocd/apps/-/blob/main/services/gkg/env/orbit-prd/values.yaml), [services/gkg/env/orbit-stg/values.yaml](https://gitlab.com/gitlab-com/gl-infra/argocd/apps/-/blob/main/services/gkg/env/orbit-stg/values.yaml) |

### Production change request template

Use [gitlab-com/gl-infra/production#21860](https://gitlab.com/gitlab-com/gl-infra/production/-/issues/21860) as the prior-art template. A new CR is required for instance-wide ops-flag changes (for example flipping `knowledge_graph_infra`); subsequent capacity tunes and image bumps do not require a fresh CR if no flag state changes.

### Slack and chatops

- `#g_orbit` for release coordination.
- `/chatops run knowledge_graph enable <namespace_id>` enables a namespace on .com. This is the preferred entry point. Alternatives are the admin REST API ([ee/lib/api/admin/knowledge_graph.rb](https://gitlab.com/gitlab-org/gitlab/-/blob/master/ee/lib/api/admin/knowledge_graph.rb)) and the orbit dashboard at <https://gitlab.com/dashboard/orbit/configuration>.

## Release flow end to end

1. Conventional commits land on `knowledge-graph` `main`. `feat:` bumps minor, `fix:` bumps patch. `chore`, `refactor`, `perf`, `test:` also bump patch per `releaseRules` in [.releaserc.json](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/blob/main/.releaserc.json). Commits whose subject starts with `chore(release):` are skipped.
2. The release captain triggers the manual `semantic-release` job. It generates the changelog, commits the schema config, creates the GitLab release, pushes `vX.Y.Z` plus the matching `clients/gkgpb/vX.Y.Z` proto-gem tag, and posts to Slack.
3. The tag pipeline runs `release-build-amd64`, `release-build-arm64`, then `release-manifest`, which runs `docker buildx imagetools create` to publish the multi-arch image. `publish-proto-gem` pushes the Ruby gem.
4. If templates need a fix, open an MR on `gkg-helm-charts`, push a `vA.B.C` git tag, and the `publish-helm-chart` job publishes the OCI artifact at `oci://registry.gitlab.com/gitlab-org/orbit/gkg-helm-charts/gkg`. The `version: 0.1.0` in `chart/Chart.yaml` is a placeholder; the tag supplies the real version via `helm package --version "${CI_COMMIT_TAG#v}"`.
5. Open the argocd-apps MR. Title format: `chore(gkg): bump image to vX.Y.Z`. Bump `image.tag` in `services/gkg/values.yaml`. Bump `gkg.chart.version` in `services/gkg/service.yaml` only if a new chart was tagged. Capacity tunes (replicas, concurrency, resources) belong in `services/gkg/env/orbit-prd/values.yaml`. Examples to model on: [!1500](https://gitlab.com/gitlab-com/gl-infra/argocd/apps/-/merge_requests/1500), [!1502](https://gitlab.com/gitlab-com/gl-infra/argocd/apps/-/merge_requests/1502), [!1504](https://gitlab.com/gitlab-com/gl-infra/argocd/apps/-/merge_requests/1504), [!1528](https://gitlab.com/gitlab-com/gl-infra/argocd/apps/-/merge_requests/1528).
6. ArgoCD reconciles gkg-orbit-stg first. Confirm webserver is `Ready 3/3` and indexer pods are pulling the new image before letting prd reconcile. If a schema bump is involved, watch the migration sequence in stg before signing off on prd.
7. After prd is healthy, run schema migration verification (next section), then enable code indexing for the target namespaces if this release expands rollout.

## Schema migration process

`schema_watcher` polls the active schema row in ClickHouse. When the deployed binary's compiled schema version differs from the active row, the row is marked `migrating`, the indexer dispatcher applies DDL, and the row flips back to `ready` once the migration completes.

Two facts about how this is gated:

- Promotion is gated on SDLC coverage only. Code coverage is observable but does not block promotion. The `migration-completion` schedule in the chart confirms SDLC state with a small buffer (~5%) before promoting.
- The `enabled namespace count is 0` check in [crates/indexer/src/schema/completion.rs](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/blob/main/crates/indexer/src/schema/completion.rs) blocks promotion in any environment with no enabled namespaces. This is a safety guard, not a Siphon misconfiguration. Stg self-promotes because its datalake already has enabled-namespace rows; prd will sit in `migrating` until at least one namespace is enabled.

### Verifying a migration

1. In stg, watch the `gkg_schema_version` row in the graph DB flip from `pending` to `active` at the new version. The shape-2 plus shape-3 transition we tested in stg ran `ready` to `pending` in about 5 seconds and `pending` to `ready` in about 3 seconds.
2. Webserver readiness gate flips 3/3 only after the migration completes. If pods stay `0/3` after deploy, check the schema row first.
3. Indexer logs show `downloading repository archive` once the dispatcher fires the backfill path. NATS consumer lag for `code-indexing-task` should drain.
4. Confirm `v7_code_indexing_checkpoint` rows are not all sitting at `_version: 0`. See "Risks" below.

### Rolling back a migration

If a release ships a broken schema, revert the argocd-apps MR and manually reset the `gkg_schema_version` active row in ClickHouse to the pre-release version. Treat this as last-resort surgery and coordinate with the indexer crate owner before running it.

## Capacity reference (orbit-prd as of HEAD)

Sourced from [services/gkg/env/orbit-prd/values.yaml](https://gitlab.com/gitlab-com/gl-infra/argocd/apps/-/blob/main/services/gkg/env/orbit-prd/values.yaml). Values change per release; treat this as the shape, not the live numbers. The shape below has been stable from v0.32.0 through v0.33.0.

| Knob | Value | Reasoning |
|---|---|---|
| `indexer.replicas` | 5 | Throughput for code backfill; multiplies fleet-wide Gitaly fetch cap |
| `engine.max_concurrent_workers` | 3 | Total per-pod worker slots |
| `engine.concurrency_groups.code` | 2 | Per-pod cap on code-indexing tasks; combined with replicas gives fleet cap of 10 concurrent Gitaly archive fetches. History: shipped at 1 in !1467 (GKG 0.29.0), bumped to 3 with the v0.30.0 capacity tune in !1500, lowered to 2 the same day to enforce the fleet cap of 10 |
| `engine.concurrency_groups.sdlc` | 1 | One SDLC dispatcher pass per pod; concurrency is at the top-level namespace |
| `indexer.tmpSizeLimit` | 30Gi | Plan for ~10Gi disk per concurrent code task |
| Indexer `requests` | cpu 2, memory 16Gi, ephemeral-storage 30Gi | Guaranteed QoS to avoid noisy-neighbor OOM |
| Indexer `limits` | cpu 8, memory 16Gi, ephemeral-storage 40Gi | Ephemeral-storage limit must exceed request to leave headroom for logs and the writable layer |
| `schedule.tasks.global.cron`, `namespace.cron` | `*/30 * * * * *` | 30s SDLC dispatcher tick. The chart default in [config/default.yaml](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/blob/main/config/default.yaml) is `0 */1 * * * *` (1 minute); the 30s value is a prd argocd-apps override. ClickHouse query is fast and NATS dedupes any double-publish |

## Observability

### Grafana

- Dashboard: <https://dashboards.gitlab.net/dashboards/f/orbit-playground/orbit>
- Useful panels during a release: bytes per second indexed, rows per minute, code events per second, skip-checkpoint rate (should stay low), repository fetch duration seconds (watch the tail), NATS consumer lag for `code-indexing-task`.
- The Rails KG dashboard has the `gitlab_knowledge_graph_*` histograms (grpc, jwt, auth_context, redaction). It was added in [gitlab-org/gitlab!229398](https://gitlab.com/gitlab-org/gitlab/-/merge_requests/229398).
- Sidekiq view, filtered to `worker=Analytics::KnowledgeGraph::CodeIndexingWorker`, shows enqueue, completion, failure rate, and queue latency.

### Logs

- UI: <https://log.gprd.gitlab.net/app/logs>
- Useful indices: `pubsub-application-inf-gprd` for indexer and webserver pod logs filtered to `kubernetes.namespace_name: gkg`. `pubsub-gitaly-inf-gprd` for `GetArchive` calls filtered to the GKG indexer client.

### k8s

- orbit-prd cluster, namespace `gkg`. Watch indexer pod memory and `OOMKilled` events on the namespace dashboard.

### Metrics naming rule

`gkg-observability` rejects unit suffixes on otel metric names: `_total`, `_seconds`, `_bytes`, `_bucket`, `_count`, `_sum`, `_milliseconds`. New panels and instrumentation must follow this.

## What to watch for during rollout

These are real failure modes we have hit. Each one has a specific check.

### Webserver crashloop on chart-template gaps

A new image can introduce an unconditional dependency that older chart templates do not render. Symptom: webserver pods crashloop with a connection or address error immediately after the bump.

Past incident: v0.31.x made the NATS connection unconditional, but [chart/templates/webserver/configmap.yaml](https://gitlab.com/gitlab-org/orbit/gkg-helm-charts/-/blob/main/chart/templates/webserver/configmap.yaml) at chart 0.18.1 had no `nats:` block (and the webserver `deployment.yaml` did not include the `gkg.natsTlsVolume` and `gkg.natsTlsVolumeMount` partials), so webserver pods could not reach NATS and crashlooped with `Cannot assign requested address (os error 99)`. The team opened a revert MR ([argocd-apps!1503](https://gitlab.com/gitlab-com/gl-infra/argocd/apps/-/merge_requests/1503)) to roll the image back to 0.30.0, then closed it in favor of rolling forward via a chart fix in [argocd-apps!1504](https://gitlab.com/gitlab-com/gl-infra/argocd/apps/-/merge_requests/1504) (chart 0.18.2). The fix is on lines 16-22 of [chart/templates/webserver/configmap.yaml](https://gitlab.com/gitlab-org/orbit/gkg-helm-charts/-/blob/main/chart/templates/webserver/configmap.yaml).

Default response: roll forward with a chart fix instead of reverting the image. Image reverts are slower and lose the migration work that already promoted.

### Ephemeral-storage eviction

If `limits.ephemeral-storage` equals `tmpSizeLimit` there is no headroom for pod logs or the writable layer, and a legitimate emptyDir fill will evict pods. Keep `limits` above `requests` (current shape: 30Gi request, 40Gi limit). [argocd-apps!1501](https://gitlab.com/gitlab-com/gl-infra/argocd/apps/-/merge_requests/1501) is the precedent.

### `enabled namespace count is 0`

A new schema version will not promote in prd until at least one namespace is enabled. Confirm via the chatops command above before expecting the migration to land. If the row count is 0, run a `disable` then `enable` to force replication of the row; this also recovers from manual ClickHouse edits that drop the row.

### Checkpoint `_version: 0` rows

`set_checkpoint` in [crates/indexer/src/modules/code/checkpoint_store.rs](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/blob/main/crates/indexer/src/modules/code/checkpoint_store.rs) does not set `_version` on the INSERT, so every `v7_code_indexing_checkpoint` row defaults to 0. ReplacingMergeTree cannot pick a latest because all rows tie at version 0; merges happen by physical position. Any tombstone with `_version > 0` wins permanently, and the project becomes un-checkpointable. Verify after a rollout that checkpoints advance, not just that they exist.

### NATS abandoned messages after a schema bump

A NATS message that fails `max_deliver` times (5 by default) is abandoned: it stays in the stream but is no longer eligible for redelivery, and `delivered.stream_seq` advances past it. During the v0.31.x rollout, the migration-completion check failed because the stream contained abandoned messages from before the schema bump. Recovery is to purge the stream's pending messages or temporarily raise `max_deliver` so the queue drains. Watch NATS consumer lag and `num_redelivered` on `code-indexing-task` and `migration-completion` after any schema bump.

### Manual ClickHouse edits

We have hit cases where a Siphon-side hotfix recreates a table directly in ClickHouse and does not repopulate the rows. The v0.31.x rollout symptom was a `enabled_namespaces` table with no rows after Adam's hotfix on the Siphon side, which dropped the row J-G had added. The migration ran but produced an empty backfill. The fix is to disable then re-enable the namespace via chatops, which re-publishes the row through Siphon.

### Eviction risk at scale

Indexer pods scaled to 5 replicas with sustained code-indexing tasks have run with 0 restarts so far through v0.33.0. Bohdan flagged the risk at v0.30.0 rollout when `limits.ephemeral-storage` equaled `tmpSizeLimit`; widening the limit in !1501 closed the gap. Check eviction rate on the namespace dashboard at the start of every prd rollout anyway.

## Worked example: cutting v0.33.0

This is the actual sequence used for v0.33.0 on 2026-04-25.

1. Conventional commits on `main` since v0.32.0: 2 `feat:` (jsonnet dashboard generator, admin-gated User columns), 3 `fix:` (DLQ subjects for wildcard deliveries, empty 200-OK archives classified as `indexed-empty`, Date32 clamp on `WorkItem`), 1 `chore(schema):` bump to v8. Next semver: minor bump because of the two `feat:` commits.
2. Trigger the manual `semantic-release` job on the `main` pipeline. The job creates `v0.33.0` and `clients/gkgpb/v0.33.0`, pushes the image, and publishes the proto gem.
3. No chart change required for this release.
4. Open [argocd-apps!1528](https://gitlab.com/gitlab-com/gl-infra/argocd/apps/-/merge_requests/1528) bumping `image.tag` from `0.32.0` to `0.33.0`. No capacity changes.
5. Watch stg, then prd. Verify the v7 to v8 migration promotes (this is the first schema bump validated via the stg shape-2 plus shape-3 test).

## References

- Production CR: [gitlab-com/gl-infra/production#21860](https://gitlab.com/gitlab-com/gl-infra/production/-/issues/21860)
- Last image bumps in argocd-apps: [!1467](https://gitlab.com/gitlab-com/gl-infra/argocd/apps/-/merge_requests/1467) (0.29.0, prior-art template for the rollout shape), [!1500](https://gitlab.com/gitlab-com/gl-infra/argocd/apps/-/merge_requests/1500) (0.30.0 + capacity tune), [!1501](https://gitlab.com/gitlab-com/gl-infra/argocd/apps/-/merge_requests/1501) (ephemeral-storage limit widen), [!1502](https://gitlab.com/gitlab-com/gl-infra/argocd/apps/-/merge_requests/1502) (0.31.1), [!1503](https://gitlab.com/gitlab-com/gl-infra/argocd/apps/-/merge_requests/1503) (closed; abandoned revert in favor of !1504), [!1504](https://gitlab.com/gitlab-com/gl-infra/argocd/apps/-/merge_requests/1504) (chart 0.18.2 NATS fix), [!1505](https://gitlab.com/gitlab-com/gl-infra/argocd/apps/-/merge_requests/1505) (0.32.0), [!1528](https://gitlab.com/gitlab-com/gl-infra/argocd/apps/-/merge_requests/1528) (0.33.0)
- Related runbooks: [code indexing](code_indexing.md), [SDLC indexing](sdlc_indexing.md), [server configuration](server_configuration.md)
