pub const EXPECTED_FILTER_OPS: &[&str] = &[
    "eq",
    "ne",
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
