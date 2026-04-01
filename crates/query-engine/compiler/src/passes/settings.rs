//! Settings phase: builds a QueryConfig from global defaults and
//! input-specific overrides (e.g. cursor pagination enables query cache).

use gkg_config::QueryConfig;
use gkg_config::global::DEFAULT_QUERY_CONFIG;

use crate::input::Input;

/// Build a QueryConfig for this query based on global defaults and input.
pub fn build_query_config(input: &Input) -> QueryConfig {
    let mut config = DEFAULT_QUERY_CONFIG;

    if input.cursor.is_some() {
        config.use_query_cache = Some(true);
    }

    config
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::passes::{normalize, validate};

    fn validated_input(json: &str) -> Input {
        let ontology = ontology::Ontology::load_embedded().unwrap();
        let input = crate::input::parse_input(json).unwrap();
        validate::Validator::new(&ontology)
            .check_references(&input)
            .unwrap();
        normalize::normalize(input, &ontology).unwrap()
    }

    #[test]
    fn cursor_enables_query_cache() {
        let input = validated_input(
            r#"{
            "query_type": "search",
            "node": {"id": "u", "entity": "User"},
            "limit": 100,
            "cursor": {"offset": 0, "page_size": 20}
        }"#,
        );

        let config = build_query_config(&input);
        assert_eq!(
            config.use_query_cache,
            Some(true),
            "cursor should enable query cache"
        );
    }

    #[test]
    fn no_cursor_no_query_cache() {
        let input = validated_input(
            r#"{
            "query_type": "search",
            "node": {"id": "u", "entity": "User"},
            "limit": 100
        }"#,
        );

        let config = build_query_config(&input);
        assert_ne!(
            config.use_query_cache,
            Some(true),
            "no cursor should not enable query cache"
        );
    }
}
