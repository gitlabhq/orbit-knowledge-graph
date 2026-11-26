include!(concat!(env!("OUT_DIR"), "/icon_types_generated.rs"));

#[cfg(test)]
pub const HANDLED_CROSS_REFERENCE_ACTIONS: &[&str] = &[
    "cross_reference",
    "relate",
    "unrelate",
    "relate_to_parent",
    "relate_to_child",
    "unrelate_from_parent",
    "unrelate_from_child",
    "moved",
    "cloned",
    "duplicate",
    "commit",
    "merge",
];

#[cfg(test)]
pub const HANDLED_LIFECYCLE_ACTIONS: &[&str] = &["closed", "merged", "opened"];

#[cfg(test)]
pub const HANDLED_REVIEW_ACTIONS: &[&str] = &["requested_changes"];

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn handled_cross_reference_actions_are_subset_of_icon_types() {
        for action in HANDLED_CROSS_REFERENCE_ACTIONS {
            assert!(
                ICON_TYPES.contains(action),
                "handled cross-ref action `{action}` is not in vendored ICON_TYPES"
            );
        }
    }

    #[test]
    fn handled_lifecycle_actions_are_subset_of_icon_types() {
        for action in HANDLED_LIFECYCLE_ACTIONS {
            assert!(
                ICON_TYPES.contains(action),
                "handled lifecycle action `{action}` is not in vendored ICON_TYPES"
            );
        }
    }

    #[test]
    fn handled_review_actions_are_subset_of_icon_types() {
        for action in HANDLED_REVIEW_ACTIONS {
            assert!(
                ICON_TYPES.contains(action),
                "handled review action `{action}` is not in vendored ICON_TYPES"
            );
        }
    }

    #[test]
    fn icon_types_has_no_duplicates() {
        let mut seen = std::collections::HashSet::new();
        for action in ICON_TYPES {
            assert!(
                seen.insert(*action),
                "duplicate action `{action}` in ICON_TYPES"
            );
        }
    }
}
