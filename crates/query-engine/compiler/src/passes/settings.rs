//! Settings phase: builds a QueryConfig from global defaults and
//! input-specific overrides (e.g. cursor pagination enables query cache).

use gkg_config::QueryConfig;

use crate::input::Input;

/// Build a QueryConfig for this query based on global defaults and input.
pub fn build_query_config(input: &Input) -> QueryConfig {
    let mut config = QueryConfig::default();

    if input.cursor.is_some() {
        config.use_query_cache = true;
    }

    config
}
