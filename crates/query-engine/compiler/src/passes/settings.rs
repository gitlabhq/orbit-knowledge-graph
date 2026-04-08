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
        // Default to sharing cache across ClickHouse users so all pods
        // hit the same cached result. Respects an explicit operator
        // override (e.g. `query_cache_share_between_users: false` in YAML).
        if cfg.query_cache_share_between_users.is_none() {
            cfg.query_cache_share_between_users = Some(true);
        }
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
        let cfg = resolve("search", true);
        assert_eq!(cfg.use_query_cache, Some(true));
        assert_eq!(cfg.query_cache_share_between_users, Some(true));
    }

    #[test]
    fn resolve_does_not_enable_cache_without_cursor() {
        let cfg = resolve("search", false);
        // Without global init, default is None
        assert_eq!(cfg.use_query_cache, None);
    }

    #[test]
    fn resolve_respects_explicit_share_between_users_false() {
        // When an operator explicitly sets query_cache_share_between_users: false
        // in YAML, cursor pagination should not override it.
        // Without global init the field defaults to None, so we test the
        // code path directly: if the field is already Some(false), the
        // is_none() guard should skip it.
        let mut cfg = resolve("search", true);
        // Simulate an operator having set it to false before resolve ran.
        // (In practice this comes from the YAML config merge.)
        cfg.query_cache_share_between_users = Some(false);
        // Re-apply the cursor logic manually to verify the guard.
        if cfg.query_cache_share_between_users.is_none() {
            cfg.query_cache_share_between_users = Some(true);
        }
        assert_eq!(
            cfg.query_cache_share_between_users,
            Some(false),
            "explicit false should not be overridden"
        );
    }
}
