use gkg_server_config::QueryConfig;

pub fn resolve(query_type: &str) -> QueryConfig {
    gkg_server_config::query::for_query_type(query_type)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resolve_returns_default_for_unknown_type() {
        let cfg = resolve("nonexistent");
        assert_eq!(cfg, QueryConfig::default());
    }
}
