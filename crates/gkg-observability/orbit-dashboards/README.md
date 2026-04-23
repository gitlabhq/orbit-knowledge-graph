# orbit-dashboards

Vendored library consumed by `gitlab-com/runbooks` via jsonnet-bundler.

## Contents

- `gkg-metrics.json` - generated catalog of every metric emitted by the GKG
  Rust service. Built by `cargo xtask metrics-catalog` from the static
  `gkg_observability::catalog()`. Do not hand-edit.
- `orbit-panels.libsonnet` - hand-authored panel helpers that consume the
  catalog. See the module header for the import + usage contract.

## Consuming from runbooks

Add the following entry to `gitlab-com/runbooks/jsonnetfile.json`:

```json
{
  "source": {
    "git": {
      "remote": "https://gitlab.com/gitlab-org/orbit/knowledge-graph.git",
      "subdir": "crates/gkg-observability/orbit-dashboards"
    }
  },
  "version": "main",
  "name": "orbit-dashboards"
}
```

Runbooks already has `legacyImports: true` set in its jsonnetfile, so dashboards can then import the vendored files with:

```jsonnet
local gkg   = import 'orbit-dashboards/gkg-metrics.json';
local orbit = import 'orbit-dashboards/orbit-panels.libsonnet';
```

## Updating

Changes to metric names, descriptions, labels, units, or buckets happen in
`crates/gkg-observability/src/**/*.rs`. After editing the Rust source, run:

```
mise run metrics:catalog
```

to regenerate `gkg-metrics.json`, and commit both the Rust change and the
regenerated JSON in the same MR. CI enforces this via the
`metrics-catalog-check` job.
