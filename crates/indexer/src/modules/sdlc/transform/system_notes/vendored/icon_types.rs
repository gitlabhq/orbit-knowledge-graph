//! Vendored copy of Rails `SystemNoteMetadata::ICON_TYPES`.
//!
//! Source-of-truth:
//! `gitlab-org/gitlab` `app/models/system_note_metadata.rb`.
//! The pinned commit SHA and capture date are recorded at the top of
//! `config/vendored/system_note_metadata.actions`.
//!
//! `ICON_TYPES` is generated at build time from that file by `build.rs`;
//! To update: edit the `.actions` file, refresh the pinned SHA
//! comment of the file, and rebuild.
//!
//! The two `_HANDLED_*` constants below make the parser's coverage explicit.
//! The subset tests assert they remain subsets of `ICON_TYPES` so any future
//! update to the vendor file immediately surfaces coverage gaps at test time.

include!(concat!(env!("OUT_DIR"), "/icon_types_generated.rs"));

/// Subset of `ICON_TYPES` the parser dispatcher knows how to handle as
/// cross-reference / lifecycle actions. Asserted to be a subset of
/// `ICON_TYPES` by the unit test below; future actions Rails adds default to
/// "log and drop" until added here explicitly.
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

/// Lifecycle actions whose body is a fixed verb and which the parser emits
/// `User → Noteable` edges for directly.
///
/// `reopened` is intentionally absent: reopen is never a system-note action.
/// It is a `resource_state_events` row (state = 5), and REOPENED edges are
/// emitted by the standalone `reopened.yaml` ETL. See ADR 013.
#[cfg(test)]
pub const HANDLED_LIFECYCLE_ACTIONS: &[&str] = &["closed", "merged", "opened"];

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
