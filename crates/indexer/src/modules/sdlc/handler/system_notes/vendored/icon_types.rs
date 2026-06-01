//! Vendored copy of Rails `SystemNoteMetadata::ICON_TYPES`.
//!
//! Source-of-truth:
//! `gitlab-org/gitlab` `app/models/system_note_metadata.rb`.
//! Pinned to `7ca75b3c001b1bd19387fea78fe67032322da436` (refs/heads/master,
//! captured 2026-05-27).
//!
//! Why vendor: the parser dispatcher in `parse.rs` only handles a subset of
//! these actions (see `parse::Action`). When Rails adds a new action that
//! encodes a graph relationship, we need to notice — the
//! `scripts/check-system-note-actions.sh` drift check diffs this list against
//! the upstream file and fails CI if a new action appears upstream that is
//! not present here. The two `_HANDLED_*` constants below make the parser's
//! coverage explicit so the same script can also verify "every handled
//! action is in ICON_TYPES".
//!
//! The pattern mirrors ADR 012's vendored-constant + CI drift-check approach
//! for the GOON format version
//! (`scripts/check-goon-format-version.sh`).

/// All known `system_note_metadata.action` values from Rails. Read by the
/// `scripts/check-system-note-actions.sh` drift check and the subset tests;
/// the parser dispatches off the `HANDLED_*` subsets, not this full list.
#[allow(dead_code, reason = "consumed by the drift-check script and tests")]
pub const ICON_TYPES: &[&str] = &[
    "commit",
    "description",
    "merge",
    "confidential",
    "visible",
    "label",
    "assignee",
    "cross_reference",
    "designs_added",
    "designs_modified",
    "designs_removed",
    "designs_discussion_added",
    "title",
    "time_tracking",
    "branch",
    "milestone",
    "discussion",
    "task",
    "moved",
    "cloned",
    "opened",
    "closed",
    "merged",
    "duplicate",
    "locked",
    "unlocked",
    "outdated",
    "reviewer",
    "tag",
    "due_date",
    "start_date_or_due_date",
    "pinned_embed",
    "cherry_pick",
    "health_status",
    "approved",
    "unapproved",
    "status",
    "alert_issue_added",
    "relate",
    "unrelate",
    "new_alert_added",
    "severity",
    "contact",
    "timeline_event",
    "issue_type",
    "relate_to_child",
    "unrelate_from_child",
    "relate_to_parent",
    "unrelate_from_parent",
    "override",
    "issue_email_participants",
    "requested_changes",
    "reviewed",
    "custom_field",
    "duo_agent_started",
    "duo_agent_completed",
    "duo_agent_failed",
];

/// Subset of `ICON_TYPES` the parser dispatcher knows how to handle as
/// cross-reference / lifecycle actions. Asserted to be a subset of
/// `ICON_TYPES` by the unit test below; future actions Rails adds default to
/// "log and drop" until added here explicitly.
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
pub const HANDLED_LIFECYCLE_ACTIONS: &[&str] = &["closed", "reopened", "merged", "opened"];

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
    fn handled_lifecycle_actions_are_subset_of_icon_types_except_reopened() {
        // `reopened` is a known omission from Rails ICON_TYPES: the action
        // value is emitted by the lifecycle service but Rails doesn't render
        // an icon for it (the timeline shows the entity reopening visually
        // via the state change instead). Document the exception so the drift
        // check doesn't flag it.
        for action in HANDLED_LIFECYCLE_ACTIONS {
            if *action == "reopened" {
                continue;
            }
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
