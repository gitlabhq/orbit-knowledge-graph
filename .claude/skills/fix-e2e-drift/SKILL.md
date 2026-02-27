---
name: fix-e2e-drift
description: Diagnose and fix e2e configuration drift caused by helm-dev/gkg/ changes. Use when helm-dev templates or values were modified and e2e files may be stale, or when e2e tests fail due to config mismatches.
allowed-tools: Read, Edit, Write, Glob, Grep, Bash(git *), Bash(cargo *), Bash(python3 *), Bash(./scripts/check-e2e-drift.sh *)
---

# Fix E2E drift

When `helm-dev/gkg/` changes (templates, values, secrets), the e2e environment can silently drift out of sync. This skill detects the drift and fixes it.

## Step 1: Detect what changed

Run the drift detection script with no arguments. It automatically finds the last MR on `origin/main` that touched e2e files and checks if `helm-dev/gkg/` has changed since then.

```bash
# Default: what infra changed since the last MR that synced e2e?
./scripts/check-e2e-drift.sh

# Or compare against a specific ref
./scripts/check-e2e-drift.sh origin/main
```

If the script exits 0, there is no drift. Stop here.

If it exits 1, get the full diff for analysis. Use the baseline commit from the script output:

```bash
git diff <baseline>..HEAD -- helm-dev/gkg/
```

Also review the commits to understand intent:

```bash
git log --oneline <baseline>..HEAD -- helm-dev/gkg/
```

## Step 2: Understand the mapping

Each helm-dev file maps to specific e2e files. Read both sides before making changes.

| helm-dev source | e2e target | What to sync |
|---|---|---|
| `helm-dev/gkg/values.yaml` | `e2e/helm-values.yaml` | Top-level value sections, defaults |
| `helm-dev/gkg/values-local.yaml` | `e2e/helm-values.yaml` | Feature flags, service URLs |
| `helm-dev/gkg/values-sandbox.yaml` | `e2e/helm-values.yaml` | Feature flags (ignore sandbox-specific IPs) |
| `helm-dev/gkg/templates/gkg-indexer.yaml` | `e2e/helm-values.yaml`, `config/e2e.yaml` | Config structure, env vars, ports, probes |
| `helm-dev/gkg/templates/gkg-webserver.yaml` | `config/e2e.yaml` | gRPC port, bind addresses |
| `helm-dev/gkg/templates/secrets.yaml` | `crates/xtask/src/e2e/utils.rs` | Secret names and keys |
| `helm-dev/gkg/templates/gkg-dispatch-indexing.yaml` | `e2e/templates/dispatch-indexing-job.yaml.tmpl` | Job spec, env vars, volume mounts |

## Step 3: Categorize and fix each change

Work through the diff systematically. Each type of change has a specific fix pattern.

### New or renamed value section

Example: `gitaly:` removed, `gitlab:` added in `values.yaml`.

1. Read `helm-dev/gkg/values.yaml` for the new section's defaults
2. Read `helm-dev/gkg/values-local.yaml` for the local override
3. Add the equivalent section to `e2e/helm-values.yaml` with e2e-appropriate values
4. Remove any stale section from `e2e/helm-values.yaml`

E2E values should use in-cluster service names, not `host.docker.internal` (that's for local dev). The e2e CNG GitLab webservice is at `http://gitlab-webservice-default.gitlab.svc.cluster.local:8181` in the `gitlab` namespace.

### Port change

Example: gRPC port `50051` → `50054`.

1. Check `config/e2e.yaml` field `gkg.grpc_endpoint` — update the port
2. Grep for the old port across e2e files:
   ```bash
   git grep -n '<old_port>' -- 'config/e2e.yaml' 'e2e/' 'crates/xtask/src/e2e/'
   ```
3. Update all occurrences

### Environment variable change

Example: `GKG_GITALY__TOKEN` → `GKG_GITLAB__JWT_SECRET`.

1. Check if the env var is referenced in `e2e/templates/dispatch-indexing-job.yaml.tmpl`
2. Check if it's set in `crates/xtask/src/e2e/pipeline/gkg.rs` via `--set` flags
3. Check if the secret source changed (e.g., different secret name or key)

### Secret reference change

Example: new secret `gkg-server-credentials` replacing `gitaly-credentials`.

1. Read `crates/xtask/src/e2e/utils.rs` function `create_k8s_secrets()`
2. Verify it creates all secrets the templates now reference
3. Add or update secret entries as needed
4. Check `config/e2e.yaml` for secret name/key fields

### Probe change (liveness/readiness)

Example: new health endpoint on port 4202.

1. Check if `config/e2e.yaml` `gkg_pod_readiness` entries need updating
2. New probes generally don't require e2e changes unless they change pod readiness behavior

### Helm `--set` overrides

The `deploy_gkg_chart()` function in `crates/xtask/src/e2e/pipeline/gkg.rs` passes `--set` flags that override `e2e/helm-values.yaml`. If a value section was renamed, the `--set` keys must match.

1. Read `crates/xtask/src/e2e/pipeline/gkg.rs` — find the `install_with_sets` call
2. Check if any `--set` keys reference removed/renamed values
3. Update the keys to match the new value names

### ConfigMap structure change

The indexer ConfigMap is generated from `helm-dev/gkg/templates/gkg-indexer.yaml`. If config keys change:

1. The server's `config.rs` must accept the new structure (check `crates/gkg-server/src/config.rs`)
2. The dispatch job template at `e2e/templates/dispatch-indexing-job.yaml.tmpl` mounts the same ConfigMap — ensure it still works

## Step 4: Validate

After making changes:

1. Verify the e2e YAML is valid:
   ```bash
   python3 -c "import yaml; yaml.safe_load(open('e2e/helm-values.yaml'))"
   python3 -c "import yaml; yaml.safe_load(open('config/e2e.yaml'))"
   ```

2. If Rust files were changed, check they compile:
   ```bash
   cargo check -p xtask
   ```

3. Run the drift check again to confirm it passes:
   ```bash
   ./scripts/check-e2e-drift.sh
   ```

## Common e2e service addresses

When adding service URLs to `e2e/helm-values.yaml`, use these in-cluster addresses:

| Service | Address in e2e |
|---|---|
| GitLab webservice (Workhorse) | `http://gitlab-webservice-default.gitlab.svc.cluster.local:8181` |
| ClickHouse HTTP | `gkg-e2e-clickhouse:8123` |
| ClickHouse native | `gkg-e2e-clickhouse:9000` |
| PostgreSQL | `postgresql.gitlab.svc.cluster.local:5432` |
| NATS | Managed by sub-chart, auto-discovered |
| GKG webserver | `gkg-webserver.default.svc.cluster.local` |
| GKG webserver gRPC | `gkg-webserver.default.svc.cluster.local:50054` |

## Self-improvement

When you encounter a new type of drift not covered above, add it to the "Categorize and fix each change" section with the fix pattern. If a mapping between helm-dev and e2e files is missing from the table, add it.
