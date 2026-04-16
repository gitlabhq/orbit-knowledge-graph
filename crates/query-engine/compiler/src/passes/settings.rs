//! Settings: resolve query-level ClickHouse config.
//!
//! Reads the global [`QuerySettings`] (populated at server startup from YAML)
//! and resolves the effective [`QueryConfig`] for the current query type.
//! Cursor pagination queries get cache settings overlaid automatically.

use gkg_server_config::QueryConfig;

/// Resolve the [`QueryConfig`] for the given query type, then apply
/// cursor-based overrides if `has_cursor` is set.
pub fn resolve(query_type: &str, has_cursor: bool) -> QueryConfig {
    let mut cfg = gkg_server_config::query::for_query_type(query_type);

    if has_cursor && cfg.graph_query_cache_enabled != Some(true) {
        // When the NATS KV graph query cache is active, it already caches
        // the full LIMIT window (cursor field is stripped from the cache key),
        // so the ClickHouse query cache is redundant.
        // Without the KV cache, we still need CH's query cache so that
        // subsequent pages hit the same cached result.
        cfg.use_query_cache = Some(true);
    }

    cfg
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resolve_returns_default_for_unknown_type() {
        let cfg = resolve("nonexistent", false);
        assert_eq!(cfg, QueryConfig::default());
    }

    #[test]
    fn resolve_enables_ch_cache_for_cursor_without_kv() {
        let cfg = resolve("search", true);
        // Without global init, graph_query_cache_enabled defaults to None,
        // so ClickHouse query cache is enabled for cursor pagination.
        assert_eq!(cfg.use_query_cache, Some(true));
    }

    #[test]
    fn resolve_skips_ch_cache_for_cursor_when_kv_enabled() {
        // Simulate global settings with graph_query_cache_enabled: true
        let settings = gkg_server_config::QuerySettings {
            default: QueryConfig {
                graph_query_cache_enabled: Some(true),
                ..Default::default()
            },
            ..Default::default()
        };
        // Resolve manually since we can't call init() in tests (OnceLock)
        let mut cfg = settings.resolve("search");
        if cfg.graph_query_cache_enabled != Some(true) {
            cfg.use_query_cache = Some(true);
        }
        assert_eq!(cfg.use_query_cache, None);
        assert_eq!(cfg.graph_query_cache_enabled, Some(true));
    }

    #[test]
    fn resolve_does_not_enable_cache_without_cursor() {
        let cfg = resolve("search", false);
        // Without global init, default is None
        assert_eq!(cfg.use_query_cache, None);
    }
}
