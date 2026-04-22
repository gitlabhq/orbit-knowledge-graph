# Orbit dashboards

Per-component Grafana dashboards for the Orbit stack: Knowledge Graph (GKG)
indexer + webserver, Siphon producers/consumers, NATS, and the Rails
monolith's `gitlab_knowledge_graph_*` integration metrics.

These live alongside the existing focused dashboards in `dashboards/`
(`etl-engine.json`, `gkg-overview.json`, `query-pipeline.json`,
`sdlc-indexing.json`) and provide a broader first-pass "dump every
metric, prune into SLIs later" view — Bohdan's suggestion from the
2026-04-21 KG Weekly.

## Live dashboards

All live in the **Orbit** subfolder under **Playground — FOR TESTING
PURPOSES ONLY** on dashboards.gitlab.net:
<https://dashboards.gitlab.net/dashboards/f/orbit-playground/orbit>

| File | UID | Scope |
|---|---|---|
| `orbit-overview.dashboard.json` | `orbit-overview` | Golden-signal stats across every component |
| `orbit-gkg-webserver.dashboard.json` | `orbit-gkg-webserver` | HTTP + gRPC transport, query pipeline, content resolution, schema watcher, query-engine threat counters |
| `orbit-gkg-indexer.dashboard.json` | `orbit-gkg-indexer` | ETL engine, code pipeline, SDLC, namespace deletion, scheduler, schema migration |
| `orbit-siphon.dashboard.json` | `orbit-siphon` | Producer + ClickHouse consumer metrics |
| `orbit-nats.dashboard.json` | `orbit-nats` | NATS varz + JetStream stream/consumer |
| `orbit-rails-kg.dashboard.json` | `orbit-rails-kg` | Rails → GKG gRPC client, redaction, JWT build, traversal-ID compaction |
| `orbit-all-metrics.dashboard.json` | `orbit-all-metrics` | Kitchen-sink dump, one page. Ad-hoc discovery only. |

Every dashboard has:

- An **Orbit** dropdown in the top bar (keep-time, keep-vars) to jump
  between siblings.
- Four template variables — `ORBIT_DS` (defaults to "Mimir - Analytics
  Eventsdot"), `RAILS_DS` (defaults to "Mimir - Gitlab Gstg"), `cluster`
  (orbit-stg / orbit-prd / all), `rails_env` (gstg / gprd / both).

## Files

- `generate.py` — source of truth for the panel list. Edit the metric
  lists (`GKG_ETL`, `GKG_QUERY`, `SIPHON_*`, `RAILS_KG_*`, …) and re-run
  to regenerate every dashboard JSON.
- `orbit-*.dashboard.json` — importable via Grafana's **Dashboards →
  New → Import** flow.

## Regenerate

```bash
cd dashboards/orbit
python3 generate.py
```

## Import flow

### Quick path — UI import

1. <https://dashboards.gitlab.net/dashboard/import>
2. Paste the contents of one of the `orbit-*.dashboard.json` files, or
   upload the file.
3. Choose folder **Orbit** (under Playground). Save.
4. Repeat for each dashboard.

### Batch path — `POST /api/dashboards/db`

Per file, with:

```json
{
  "dashboard": <contents of orbit-X.dashboard.json>,
  "folderUid": "orbit-playground",
  "overwrite": true,
  "message": "Regenerated from generate.py"
}
```

Google SSO cookies carry the session, so no API token is needed while
authoring dashboards in the Playground folder.

## Selectors

Queries use the labels actually emitted by the services (verified
against `mimir-analytics-eventsdot` and `mimir-gitlab-gstg` on
2026-04-22):

- **GKG webserver**: `container="gkg-webserver"`
- **GKG indexer**: `container="gkg-indexer"`
- **Siphon**: `namespace="siphon"`
- **NATS**: `cluster=~"$cluster"`
- **Rails KG**: `env=~"$rails_env"`

At time of writing:

- `orbit-stg` runs GKG + Siphon + NATS end-to-end.
- `orbit-prd` only runs Siphon (`siphon_main_1`, 1 producer).
- Rails `gitlab_knowledge_graph_*` series are live in `gstg` only;
  `gprd` is empty until the Rails integration is rolled out.

Existing dashboards in this directory (`query-pipeline.json`,
`gkg-overview.json`, `etl-engine.json`, `sdlc-indexing.json`) use
`job="gkg-webserver"` / `job="gkg-indexer"` which does **not** match
the real scrape label (the real value is `gkg/gkg-webserver` /
`gkg/gkg-indexer`). `container` is the stable join dimension.

## Cleanup exemption

Every dashboard carries the `protected` tag, which the runbooks sweep
(`runbooks/dashboards/delete-orphaned-dashboards.sh`) honors via
`protected-grafana-dashboards.jsonnet → protectedTags: ['protected']`.
That means the cleanup script will not delete them even though they
live under the Playground folder.

## Promotion path

These live under Playground for now. Even with the `protected` tag
preventing auto-deletion, the proper long-term home is the top-level
`orbit` folder on dashboards.gitlab.net.
When the signal set stabilises:

1. Translate the JSON into JSONnet under `runbooks/dashboards/orbit/`
   (`main.dashboard.jsonnet` currently only calls
   `serviceDashboard.overview('orbit')`).
2. MR against `gitlab-com/runbooks` so the top-level `orbit` Grafana
   folder (owned by `sa-autogen-gitlab-com-runbooks-ci-admin`) gets
   populated via CI. Top-level folder writes require Admin and cannot
   be done from a Viewer account.

## References

- Generator: `generate.py` (metric lists + panel templates).
- Source of metric names: `crates/**/metrics.rs` in this repo.
- Rails metric module: `ee/lib/gitlab/metrics/knowledge_graph/` in the
  monolith.
- Runbooks service catalogue:
  `runbooks/metrics-catalog/services/{orbit,siphon}.jsonnet`.
