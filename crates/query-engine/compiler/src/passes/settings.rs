use gkg_server_config::QueryConfig;

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
        assert_eq!(cfg.use_query_cache, None);
    }
}
