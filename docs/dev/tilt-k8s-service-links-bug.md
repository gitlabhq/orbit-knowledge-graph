# Bug: Kubernetes service env var injection conflicts with `config` crate

## Summary

When running GKG components via Tilt/Kubernetes locally, all GKG pods
(`gkg-webserver`, `gkg-indexer`, `gkg-health-check`) crash on startup with
errors like:

```
Error: configuration error: invalid type: sequence, expected a string for key `grpc_bind_address` in the environment
Error: configuration error: invalid type: sequence, expected a string for key `graph.url` in the environment
Error: configuration error: invalid type: sequence, expected a string for key `metrics.otlp_endpoint` in the environment
```

## Root Cause

Two interacting issues:

### 1. Kubernetes service link injection

By default, Kubernetes injects environment variables for every service in the
namespace into every pod. Since the observability chart deploys services with
names like `gkg-obs-alloy`, `gkg-obs-loki`, etc., K8s injects vars such as:

```
GKG_OBS_ALLOY_SERVICE_PORT_OTLP_GRPC=4317
GKG_WEBSERVER_SERVICE_PORT_GRPC=50051
GKG_WEBSERVER_PORT=tcp://10.43.x.x:8080
```

All of these start with `GKG_`, which the `config` crate picks up as
configuration keys under the `GKG` prefix.

### 2. `try_parsing(true)` misparses URL values

The `config` crate (v0.15) with `try_parsing(true)` and `list_separator(",")`
misinterprets URL values containing `://` (e.g. `http://host.docker.internal:8123`)
as sequences rather than plain strings. This causes deserialization to fail
when the config struct expects a `String`.

The combination of (1) and (2) means:
- K8s-injected vars pollute the config namespace
- URL-valued env vars get misinterpreted as sequences

## Workarounds

### Short-term: `enableServiceLinks: false`

Add `enableServiceLinks: false` to the pod spec in all GKG Helm templates
(`gkg-webserver.yaml`, `gkg-indexer.yaml`, `gkg-health-check.yaml`):

```yaml
spec:
  enableServiceLinks: false
  containers:
    ...
```

This prevents K8s from injecting service discovery env vars into GKG pods.
However, this alone does not fix the `try_parsing(true)` + URL issue.

### Short-term: Run the server directly (no Kubernetes)

For local development and manual E2E testing, run the GKG server directly on
the host instead of via Tilt/Kubernetes:

```shell
# From the gkg repo root
GKG_JWT_SECRET=$(cat ~/Desktop/Code/gdk/gitlab/.gitlab_shell_secret) \
  mise run server:start
```

See `config/default.yaml` for all available configuration options and their
defaults.

## Applied Fix

Three changes made:

1. **`crates/gkg-server/src/config.rs`** — changed `prefix_separator` from
   `"_"` to `"__"`. This means only env vars named `GKG__*` are picked up as
   config keys. K8s-injected service vars are named `GKG_WEBSERVER_*`,
   `GKG_OBS_ALLOY_*`, etc. — all single-underscore — so they are now ignored.
   `try_parsing(true)` and `list_separator(",")` are restored so nested structs
   and list values deserialize correctly.

2. **Helm templates** — updated all `GKG_*` env var names to `GKG__*` (double
   underscore) to match the new prefix separator.

3. **Helm templates** — added `enableServiceLinks: false` to pod specs in
   `gkg-webserver.yaml`, `gkg-indexer.yaml`, and `gkg-health-check.yaml` as
   defence-in-depth.

The `health_check.services` list (the original reason `try_parsing(true)` was
added) should be handled via a dedicated env var with explicit list parsing
rather than relying on `try_parsing`, or configured via `config/default.yaml`
rather than env vars.

## References

- `crates/gkg-server/src/config.rs` — config builder
- `crates/health-check/src/config.rs` — health check config
- `helm-dev/gkg/templates/gkg-webserver.yaml`
- `helm-dev/gkg/templates/gkg-indexer.yaml`
- `helm-dev/gkg/templates/gkg-health-check.yaml`
- Fix attempt: commit `18deca4` (`fix(config): fix env variables handling`)
