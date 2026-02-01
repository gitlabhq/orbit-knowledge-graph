# Migrate to shared labkit-rs crate

## Summary

This MR replaces the in-repo `crates/labkit-rs` with the shared [gitlab-org/rust/labkit-rs](https://gitlab.com/gitlab-org/rust/labkit-rs) crate, consolidating observability utilities across GitLab Rust projects.

## What's changing

### Dependencies
- Removed `crates/labkit-rs` from workspace members
- Added `labkit` dependency from `gitlab.com/gitlab-org/rust/labkit-rs`
- Updated `deny.toml` to allow the new crate and git source

### API Changes

| Before | After |
|--------|-------|
| `labkit_rs::correlation::http::CorrelationIdLayer` | `labkit::correlation::CorrelationLayer` |
| `labkit_rs::correlation::http::PropagateCorrelationIdLayer` | _(merged into CorrelationLayer)_ |
| `labkit_rs::metrics::http::HttpMetricsLayer` | `labkit::metrics::MetricsLayer` |
| `labkit_rs::logging::init()` | `labkit::log::init_default()` |
| `labkit_rs::metrics::grpc::GrpcMetrics` | _(removed, use OTel-based metrics)_ |

### Code changes
- `router.rs`: Simplified layer setup with single `CorrelationLayer`
- `service.rs`: Removed per-method metrics wrapper (use OTel Collector instead)
- `server.rs`: Updated import path
- `main.rs`: Updated logging initialization

## Benefits of shared labkit-rs

1. **URL Masking** - Automatic sensitive parameter filtering (was issue #26)
2. **Health Endpoints** - Standard `/-/liveness`, `/-/readiness` endpoints available
3. **Mutation-tested** - Higher confidence in correctness
4. **Maintained** - Shared maintenance across GitLab Rust projects

## Metrics note

The per-method `GrpcMetrics` wrapper has been removed. For metrics collection:
- **Recommended**: Use OTel Collector to scrape metrics
- **Alternative**: Add `MetricsInterceptor` at the Tonic service level

## Migration guide

See: https://gitlab.com/gitlab-org/rust/labkit-rs/-/blob/main/docs/migration-from-knowledge-graph.md

## Checklist

- [ ] CI passes
- [ ] No functional regressions in correlation ID propagation
- [ ] Logging works correctly with correlation IDs
- [ ] Metrics (if using OTel Collector) are emitted

## Related

- Closes #23 (Prometheus metrics) - available via `prometheus-export` feature
- Closes #25 (Tower layers and Tonic interceptors) - available in shared crate
- Closes #26 (URL masking) - available via `labkit::mask`
