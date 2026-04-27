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

    if has_cursor {
        // Intentionally unconditional: cursor pagination requires the query
        // cache so that subsequent pages hit the same cached result. This
        // overrides even an explicit `use_query_cache: false` in YAML.
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
    fn resolve_enables_cache_for_cursor() {
        let cfg = resolve("traversal", true);
        assert_eq!(cfg.use_query_cache, Some(true));
    }

    #[test]
    fn resolve_does_not_enable_cache_without_cursor() {
        let cfg = resolve("traversal", false);
        // Without global init, default is None
        assert_eq!(cfg.use_query_cache, None);
    }
}
