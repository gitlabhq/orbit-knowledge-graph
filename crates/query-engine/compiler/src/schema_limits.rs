// Mirrors of `config/schemas/graph_query.schema.json`, asserted against it by
// `build.rs`. `include!`d into `build.rs`: no `use` statements or crate paths.

pub const MAX_HOPS_CAP: u32 = 3;
pub const MAX_DEPTH_CAP: u32 = 3;
pub const MAX_NODES_CAP: usize = 5;
pub const MAX_RELS_CAP: usize = 5;
pub const MAX_NODE_IDS: usize = 500;
pub const MAX_IN_VALUES: usize = 100;
pub const MAX_FILTERS_PER_NODE: usize = 10;
pub const MAX_FILTERS_PER_REL: usize = 5;
pub const MAX_COLUMNS: usize = 50;
pub const MAX_REL_TYPES: usize = 10;
pub const MAX_FILTER_ENTRIES_PER_PROPERTY: usize = 10;

#[allow(dead_code, reason = "consumed by build.rs via include! and by tests")]
pub const EXPECTED_FILTER_OPS: &[&str] = &[
    "eq",
    "gt",
    "lt",
    "gte",
    "lte",
    "in",
    "contains",
    "starts_with",
    "ends_with",
    "is_null",
    "is_not_null",
    "token_match",
    "all_tokens",
    "any_tokens",
];

#[allow(dead_code, reason = "consumed by build.rs via include! and by tests")]
pub const EXPECTED_PATH_TYPES: &[&str] = &["shortest"];

#[cfg(test)]
mod tests {
    use super::{EXPECTED_FILTER_OPS, EXPECTED_PATH_TYPES};
    use crate::input::{FilterOp, PathType};
    use strum::VariantNames;

    #[test]
    fn expected_filter_ops_match_enum() {
        assert_eq!(EXPECTED_FILTER_OPS, FilterOp::VARIANTS);
    }

    #[test]
    fn expected_path_types_match_enum() {
        assert_eq!(EXPECTED_PATH_TYPES, PathType::VARIANTS);
    }
}
