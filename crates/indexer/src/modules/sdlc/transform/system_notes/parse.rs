//! GFM reference extraction for system note bodies.
//!
//! System notes in the GitLab monolith are authored by
//! [`SystemNoteService`](https://gitlab.com/gitlab-org/gitlab/-/blob/master/app/services/system_note_service.rb)
//! and sub-services. The body templates encode cross-entity references as
//! [GitLab Flavored Markdown](https://docs.gitlab.com/user/markdown/) reference
//! tokens (`#`, `!`, `@`). This module is a regex-based extractor that pulls the
//! references back out without rendering the full Banzai pipeline.
//!
//! Body templates were lifted from `app/services/system_notes/*.rb` in the Rails
//! monolith. The patterns are deliberately permissive on the *verb phrase* and
//! strict on the *reference token*, so future copy-edits on the Rails side do
//! not silently break extraction.

use std::sync::LazyLock;

use regex::Regex;

/// A target entity referenced from a system note body.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Reference {
    pub kind: RefKind,
    /// Full namespace path of the *owning project* of the referenced entity,
    /// when the reference is cross-project. `None` for same-project references
    /// — in that case the resolver substitutes the note's own project path.
    pub project_path: Option<String>,
    /// For Issue / MR references, the internal id. For Commit references,
    /// `None` (use `commit_sha`).
    pub iid: Option<i64>,
    /// For Commit references, the SHA (7–40 hex chars). `None` otherwise.
    pub commit_sha: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum RefKind {
    Issue,
    MergeRequest,
    Commit,
}

/// The Rails `system_note_metadata.action` value. Mirrors `ICON_TYPES` in
/// [`app/models/system_note_metadata.rb`](https://gitlab.com/gitlab-org/gitlab/-/blob/master/app/models/system_note_metadata.rb).
/// Vendored as `&'static str` so the dispatcher is exhaustive at compile time.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Action {
    CrossReference,
    Relate,
    Unrelate,
    RelateToParent,
    RelateToChild,
    UnrelateFromParent,
    UnrelateFromChild,
    Moved,
    Cloned,
    Duplicate,
    EpicIssueAdded,
    IssueAddedToEpic,
    EpicIssueMoved,
    Task,
    Commit,
    Merge,
    Closed,
    Reopened,
    Merged,
    Opened,
}

impl Action {
    pub fn parse(s: &str) -> Option<Self> {
        Some(match s {
            "cross_reference" => Self::CrossReference,
            "relate" => Self::Relate,
            "unrelate" => Self::Unrelate,
            "relate_to_parent" => Self::RelateToParent,
            "relate_to_child" => Self::RelateToChild,
            "unrelate_from_parent" => Self::UnrelateFromParent,
            "unrelate_from_child" => Self::UnrelateFromChild,
            "moved" => Self::Moved,
            "cloned" => Self::Cloned,
            "duplicate" => Self::Duplicate,
            "epic_issue_added" => Self::EpicIssueAdded,
            "issue_added_to_epic" => Self::IssueAddedToEpic,
            "epic_issue_moved" => Self::EpicIssueMoved,
            "task" => Self::Task,
            "commit" => Self::Commit,
            "merge" => Self::Merge,
            "closed" => Self::Closed,
            "reopened" => Self::Reopened,
            "merged" => Self::Merged,
            "opened" => Self::Opened,
            _ => return None,
        })
    }

    /// Inverse of [`Action::parse`]. Exercised by the round-trip test; kept
    /// as the canonical action→string mapping. Test-only — production code
    /// carries the raw `siphon_system_note_metadata.action` string instead.
    #[cfg(test)]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::CrossReference => "cross_reference",
            Self::Relate => "relate",
            Self::Unrelate => "unrelate",
            Self::RelateToParent => "relate_to_parent",
            Self::RelateToChild => "relate_to_child",
            Self::UnrelateFromParent => "unrelate_from_parent",
            Self::UnrelateFromChild => "unrelate_from_child",
            Self::Moved => "moved",
            Self::Cloned => "cloned",
            Self::Duplicate => "duplicate",
            Self::EpicIssueAdded => "epic_issue_added",
            Self::IssueAddedToEpic => "issue_added_to_epic",
            Self::EpicIssueMoved => "epic_issue_moved",
            Self::Task => "task",
            Self::Commit => "commit",
            Self::Merge => "merge",
            Self::Closed => "closed",
            Self::Reopened => "reopened",
            Self::Merged => "merged",
            Self::Opened => "opened",
        }
    }

    /// Lifecycle actions whose body is a fixed verb (`"closed"`, `"merged"`,
    /// `"reopened"`, `"opened"`) and which do not need text parsing. The
    /// resolved edge is `User --ACTION--> Noteable` from the note row alone.
    ///
    /// Test-only: `emit::build_edges` matches on the concrete `Action`
    /// variants directly, so this predicate is documentation/assertion sugar
    /// rather than a production branch.
    #[cfg(test)]
    pub fn is_lifecycle(self) -> bool {
        matches!(
            self,
            Self::Closed | Self::Reopened | Self::Merged | Self::Opened
        )
    }
}

// Reference token grammar, derived from Rails:
//
// * `Project.reference_pattern` in `app/models/project.rb` accepts an optional
//   namespace prefix (up to 20 segments) followed by a project path. Each
//   path segment matches `[a-zA-Z0-9_][a-zA-Z0-9_\-\.]{0,254}`.
// * `Issue.reference_pattern` is `(<project>?)#<digits>`.
// * `MergeRequest.reference_pattern` is `(<project>?)!<digits>`.
// * `Commit.reference_pattern` is `(<project>@)?<sha 7..40>`.
//
// We bound the namespace depth at 20 segments to match
// `Namespace::NUMBER_OF_ANCESTORS_ALLOWED`. The IID is bounded at 20 digits to
// fit `Gitlab::Database::MAX_INT_VALUE`.

const PATH_SEGMENT: &str = r"[A-Za-z0-9_][A-Za-z0-9_\-\.]{0,254}";

fn project_path_re() -> String {
    // Up to 20 path segments separated by `/`. The first segment doesn't
    // require a leading `/`; subsequent segments do.
    format!(r"(?:{PATH_SEGMENT})(?:/{PATH_SEGMENT}){{0,19}}")
}

static ISSUE_REF: LazyLock<Regex> = LazyLock::new(|| {
    let project = project_path_re();
    Regex::new(&format!(
        r"(?:(?P<project>{project})?)#(?P<iid>\d{{1,20}})\b"
    ))
    .expect("issue regex compiles")
});

static MR_REF: LazyLock<Regex> = LazyLock::new(|| {
    let project = project_path_re();
    Regex::new(&format!(
        r"(?:(?P<project>{project})?)!(?P<iid>\d{{1,20}})\b"
    ))
    .expect("merge request regex compiles")
});

// Commit SHA refs always include a project prefix when cross-project, but the
// project-less form is just a hex run. We require a word boundary on both sides
// so we don't grab the middle of an HTTP URL fragment.
static COMMIT_REF: LazyLock<Regex> = LazyLock::new(|| {
    let project = project_path_re();
    Regex::new(&format!(
        r"\b(?:(?P<project>{project})@)?(?P<sha>[0-9a-f]{{7,40}})\b"
    ))
    .expect("commit regex compiles")
});

/// Extract all entity references from `body` according to `action`.
///
/// The dispatcher applies action-specific filters to avoid false positives
/// from prose tokens that happen to match the GFM grammar (e.g. an issue
/// title containing `#123`). For free-form actions like `cross_reference`,
/// only one reference appears in the body by construction (the template is
/// `"mentioned in {ref}"`), so we take the first match.
pub fn extract(action: Action, body: &str) -> Vec<Reference> {
    match action {
        // Lifecycle: body is the bare verb, no cross-entity ref.
        Action::Closed | Action::Reopened | Action::Merged | Action::Opened => Vec::new(),

        // Body: "mentioned in <ref>".
        Action::CrossReference => first_ref_any(body).into_iter().collect(),

        // Body: "marked as related to <ref>[, <ref>...]".
        // Multiple refs separated by commas are possible per the Rails template.
        Action::Relate => all_refs_any(body),

        // Body: "removed the relation with <ref>".
        Action::Unrelate => first_ref_any(body).into_iter().collect(),

        // Bodies (hierarchies_service):
        //   "added #1234 as child item"     (relate_to_child)
        //   "added group/proj#1234 as parent item"   (relate_to_parent)
        //   "removed child item #1234"      (unrelate_from_child)
        //   "removed parent item group/proj#1234"    (unrelate_from_parent)
        Action::RelateToParent
        | Action::RelateToChild
        | Action::UnrelateFromParent
        | Action::UnrelateFromChild => first_ref_any(body).into_iter().collect(),

        // Bodies:
        //   "moved to <ref>"     "moved from <ref>"
        //   "cloned to <ref>"    "cloned from <ref>"
        //   "added issue <ref>" / "added to epic <ref>" / "moved issue <ref>"
        Action::Moved
        | Action::Cloned
        | Action::EpicIssueAdded
        | Action::IssueAddedToEpic
        | Action::EpicIssueMoved => first_ref_any(body).into_iter().collect(),

        Action::Task => extract_task_ref(body),

        // Bodies:
        //   "marked this issue as a duplicate of <ref>"
        //   "marked <ref> as a duplicate of this issue"
        Action::Duplicate => first_ref_any(body).into_iter().collect(),

        // Body: "added N commit(s)\n\n* <short_sha> - <title>\n..."
        // Each `<li>` line starts with a 7-40 hex run.
        Action::Commit => extract_commit_shas_from_list(body),

        // Body: variants
        //   "enabled an automatic merge when all merge checks for <sha> pass"
        //   "merged"  (lifecycle — handled above by Action::Merged)
        //   "canceled the automatic merge"
        //   "created merge request <ref> to address this issue"
        //
        // For the SHA-bearing variants we want the commit; for the "created
        // merge request <ref>" variant we want the MR. Both can be present
        // (in theory), but extract_first_sha returns the commit when present
        // and falls back to MR refs otherwise.
        Action::Merge => extract_merge_ref(body),
    }
}

fn extract_task_ref(body: &str) -> Vec<Reference> {
    if body.starts_with("marked the checklist item ")
        || body.starts_with("marked the task table item ")
    {
        return Vec::new();
    }

    static TASK_PARENT: LazyLock<Regex> = LazyLock::new(|| {
        Regex::new(r"(?i)\bparent\s+task\b|\btask\s+parent\b").expect("task parent regex compiles")
    });
    if !TASK_PARENT.is_match(body) {
        return Vec::new();
    }

    first_ref(&ISSUE_REF, body, RefKind::Issue)
        .into_iter()
        .collect()
}

fn first_ref_any(body: &str) -> Option<Reference> {
    // Priority: cross-project (longer, more specific) hits first, otherwise
    // the regex `match` order picks whichever the engine finds first.
    if let Some(r) = first_ref(&MR_REF, body, RefKind::MergeRequest) {
        return Some(r);
    }
    if let Some(r) = first_ref(&ISSUE_REF, body, RefKind::Issue) {
        return Some(r);
    }
    if let Some(r) = first_ref(&COMMIT_REF, body, RefKind::Commit) {
        return Some(r);
    }
    None
}

fn all_refs_any(body: &str) -> Vec<Reference> {
    let mut out = Vec::new();
    out.extend(all_refs(&MR_REF, body, RefKind::MergeRequest));
    out.extend(all_refs(&ISSUE_REF, body, RefKind::Issue));
    out.extend(all_refs(&COMMIT_REF, body, RefKind::Commit));
    out
}

fn first_ref(re: &Regex, body: &str, kind: RefKind) -> Option<Reference> {
    re.captures(body).map(|caps| capture_to_ref(&caps, kind))
}

fn all_refs(re: &Regex, body: &str, kind: RefKind) -> Vec<Reference> {
    re.captures_iter(body)
        .map(|caps| capture_to_ref(&caps, kind))
        .collect()
}

fn capture_to_ref(caps: &regex::Captures, kind: RefKind) -> Reference {
    let project_path = caps.name("project").map(|m| m.as_str().to_owned());
    match kind {
        RefKind::Issue | RefKind::MergeRequest => Reference {
            kind,
            project_path,
            iid: caps
                .name("iid")
                .and_then(|m| m.as_str().parse::<i64>().ok()),
            commit_sha: None,
        },
        RefKind::Commit => Reference {
            kind,
            project_path,
            iid: None,
            commit_sha: caps.name("sha").map(|m| m.as_str().to_owned()),
        },
    }
}

fn extract_commit_shas_from_list(body: &str) -> Vec<Reference> {
    // Rails formats the list as `<li>SHORTSHA - TITLE</li>` lines (HTML, not
    // markdown — see `app/services/system_notes/commit_service.rb`). After
    // serialization to the note body, each entry begins with the short SHA
    // followed by ` - `. We anchor on that delimiter so commit-message-style
    // hex words inside titles don't trigger false positives.
    static LINE: LazyLock<Regex> =
        LazyLock::new(|| Regex::new(r"(?m)^[^\w]*\b(?P<sha>[0-9a-f]{7,40})\b\s*-\s").unwrap());

    LINE.captures_iter(body)
        .filter_map(|caps| {
            caps.name("sha").map(|m| Reference {
                kind: RefKind::Commit,
                project_path: None,
                iid: None,
                commit_sha: Some(m.as_str().to_owned()),
            })
        })
        .collect()
}

fn extract_merge_ref(body: &str) -> Vec<Reference> {
    // "enabled an automatic merge when all merge checks for <SHA> pass"
    // SHA appears after "for " and before " pass".
    static SHA_AFTER_FOR: LazyLock<Regex> = LazyLock::new(|| {
        Regex::new(r"\bfor\s+(?P<sha>[0-9a-f]{7,40})\b").expect("sha-after-for regex compiles")
    });
    if let Some(caps) = SHA_AFTER_FOR.captures(body) {
        return vec![Reference {
            kind: RefKind::Commit,
            project_path: None,
            iid: None,
            commit_sha: caps.name("sha").map(|m| m.as_str().to_owned()),
        }];
    }

    // Fall back to an MR reference (e.g. "created merge request !123 to address
    // this issue").
    if let Some(r) = first_ref(&MR_REF, body, RefKind::MergeRequest) {
        return vec![r];
    }
    Vec::new()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn issue_ref(project: Option<&str>, iid: i64) -> Reference {
        Reference {
            kind: RefKind::Issue,
            project_path: project.map(String::from),
            iid: Some(iid),
            commit_sha: None,
        }
    }

    fn mr_ref(project: Option<&str>, iid: i64) -> Reference {
        Reference {
            kind: RefKind::MergeRequest,
            project_path: project.map(String::from),
            iid: Some(iid),
            commit_sha: None,
        }
    }

    fn commit_ref(project: Option<&str>, sha: &str) -> Reference {
        Reference {
            kind: RefKind::Commit,
            project_path: project.map(String::from),
            iid: None,
            commit_sha: Some(sha.to_owned()),
        }
    }

    // --- Action::parse / Action::as_str --------------------------------------

    #[test]
    fn action_parse_known_actions_roundtrip() {
        for s in [
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
            "epic_issue_added",
            "issue_added_to_epic",
            "epic_issue_moved",
            "task",
            "commit",
            "merge",
            "closed",
            "reopened",
            "merged",
            "opened",
        ] {
            let a = Action::parse(s).unwrap_or_else(|| panic!("action {s} should parse"));
            assert_eq!(a.as_str(), s);
        }
    }

    #[test]
    fn action_parse_returns_none_for_unknown() {
        for s in [
            "label",
            "milestone",
            "branch",
            "designs_added",
            "duo_agent_started",
        ] {
            assert!(Action::parse(s).is_none(), "{s} should not parse");
        }
    }

    #[test]
    fn action_lifecycle_classification() {
        assert!(Action::Closed.is_lifecycle());
        assert!(Action::Merged.is_lifecycle());
        assert!(Action::Reopened.is_lifecycle());
        assert!(Action::Opened.is_lifecycle());
        assert!(!Action::CrossReference.is_lifecycle());
        assert!(!Action::Merge.is_lifecycle());
    }

    // --- cross_reference -----------------------------------------------------

    #[test]
    fn cross_reference_same_project_issue() {
        let refs = extract(Action::CrossReference, "mentioned in #123");
        assert_eq!(refs, vec![issue_ref(None, 123)]);
    }

    #[test]
    fn cross_reference_same_project_merge_request() {
        let refs = extract(Action::CrossReference, "mentioned in !456");
        assert_eq!(refs, vec![mr_ref(None, 456)]);
    }

    #[test]
    fn cross_reference_cross_project_issue() {
        let refs = extract(Action::CrossReference, "mentioned in gitlab-org/gitlab#789");
        assert_eq!(refs, vec![issue_ref(Some("gitlab-org/gitlab"), 789)]);
    }

    #[test]
    fn cross_reference_cross_project_merge_request() {
        let refs = extract(Action::CrossReference, "mentioned in gitlab-org/gitlab!42");
        assert_eq!(refs, vec![mr_ref(Some("gitlab-org/gitlab"), 42)]);
    }

    #[test]
    fn cross_reference_commit_sha_only() {
        let refs = extract(Action::CrossReference, "mentioned in 54f7727c");
        assert_eq!(refs, vec![commit_ref(None, "54f7727c")]);
    }

    #[test]
    fn cross_reference_commit_with_full_sha() {
        let body = "mentioned in 0123456789abcdef0123456789abcdef01234567";
        let refs = extract(Action::CrossReference, body);
        assert_eq!(
            refs,
            vec![commit_ref(None, "0123456789abcdef0123456789abcdef01234567")]
        );
    }

    #[test]
    fn cross_reference_deep_namespace() {
        let body = "mentioned in gitlab-org/sub/subsub/project#1";
        let refs = extract(Action::CrossReference, body);
        assert_eq!(
            refs,
            vec![issue_ref(Some("gitlab-org/sub/subsub/project"), 1)]
        );
    }

    // --- relate / unrelate ---------------------------------------------------

    #[test]
    fn relate_single_reference() {
        let refs = extract(Action::Relate, "marked as related to gitlab-foss#9001");
        assert_eq!(refs, vec![issue_ref(Some("gitlab-foss"), 9001)]);
    }

    #[test]
    fn relate_multiple_references() {
        let refs = extract(
            Action::Relate,
            "marked as related to gitlab-foss#9001, gitlab-foss#9002, and gitlab-foss#9003",
        );
        assert_eq!(
            refs,
            vec![
                issue_ref(Some("gitlab-foss"), 9001),
                issue_ref(Some("gitlab-foss"), 9002),
                issue_ref(Some("gitlab-foss"), 9003),
            ]
        );
    }

    #[test]
    fn relate_mixed_mr_and_issue_refs_are_both_collected() {
        // `all_refs_any` runs MR_REF first then ISSUE_REF; both kinds need
        // to land in the output for a body that mixes them. Documents the
        // emission order (MR refs before Issue refs) which downstream
        // edge-emission stability relies on.
        let refs = extract(Action::Relate, "marked this issue as related to !42 and #9");
        assert_eq!(refs.len(), 2);
        assert_eq!(refs[0], mr_ref(None, 42));
        assert_eq!(refs[1], issue_ref(None, 9));
    }

    #[test]
    fn unrelate_single_reference() {
        let refs = extract(Action::Unrelate, "removed the relation with !456");
        assert_eq!(refs, vec![mr_ref(None, 456)]);
    }

    // --- hierarchy (relate_to_parent / child) --------------------------------

    #[test]
    fn relate_to_child_same_project() {
        let refs = extract(Action::RelateToChild, "added #1234 as child item");
        assert_eq!(refs, vec![issue_ref(None, 1234)]);
    }

    #[test]
    fn relate_to_parent_cross_project() {
        let refs = extract(
            Action::RelateToParent,
            "added group/project#1234 as parent item",
        );
        assert_eq!(refs, vec![issue_ref(Some("group/project"), 1234)]);
    }

    #[test]
    fn unrelate_from_child_same_project() {
        let refs = extract(Action::UnrelateFromChild, "removed child item #1234");
        assert_eq!(refs, vec![issue_ref(None, 1234)]);
    }

    #[test]
    fn unrelate_from_parent_cross_project() {
        let refs = extract(
            Action::UnrelateFromParent,
            "removed parent item group/proj#9",
        );
        assert_eq!(refs, vec![issue_ref(Some("group/proj"), 9)]);
    }

    // --- moved / cloned ------------------------------------------------------

    #[test]
    fn moved_to() {
        let refs = extract(Action::Moved, "moved to other_namespace/project_new#11");
        assert_eq!(
            refs,
            vec![issue_ref(Some("other_namespace/project_new"), 11)]
        );
    }

    #[test]
    fn moved_from() {
        let refs = extract(Action::Moved, "moved from gitlab-org/old-project#42");
        assert_eq!(refs, vec![issue_ref(Some("gitlab-org/old-project"), 42)]);
    }

    #[test]
    fn cloned_to() {
        let refs = extract(Action::Cloned, "cloned to other_namespace/project_new#11");
        assert_eq!(
            refs,
            vec![issue_ref(Some("other_namespace/project_new"), 11)]
        );
    }

    #[test]
    fn epic_issue_added_extracts_issue_reference() {
        let refs = extract(Action::EpicIssueAdded, "added issue group/project#11");
        assert_eq!(refs, vec![issue_ref(Some("group/project"), 11)]);
    }

    #[test]
    fn issue_added_to_epic_extracts_epic_work_item_reference() {
        let refs = extract(Action::IssueAddedToEpic, "added to epic #42");
        assert_eq!(refs, vec![issue_ref(None, 42)]);
    }

    #[test]
    fn epic_issue_moved_extracts_issue_reference() {
        let refs = extract(
            Action::EpicIssueMoved,
            "moved issue group/project#11 from another epic",
        );
        assert_eq!(refs, vec![issue_ref(Some("group/project"), 11)]);
    }

    #[test]
    fn task_hierarchy_extracts_work_item_reference() {
        let refs = extract(Action::Task, "added parent task group/project#11");
        assert_eq!(refs, vec![issue_ref(Some("group/project"), 11)]);
    }

    #[test]
    fn task_hierarchy_extracts_task_parent_word_order() {
        let refs = extract(Action::Task, "set task parent to group/project#11");
        assert_eq!(refs, vec![issue_ref(Some("group/project"), 11)]);
    }

    #[test]
    fn task_checklist_status_extracts_nothing() {
        let refs = extract(
            Action::Task,
            "marked the checklist item **Follow up in #11** as completed",
        );
        assert!(refs.is_empty());
    }

    // --- duplicate -----------------------------------------------------------

    #[test]
    fn duplicate_marked_this_as_duplicate_of() {
        let refs = extract(
            Action::Duplicate,
            "marked this issue as a duplicate of other_project#5678",
        );
        assert_eq!(refs, vec![issue_ref(Some("other_project"), 5678)]);
    }

    #[test]
    fn duplicate_marked_other_as_duplicate_of_this() {
        let refs = extract(
            Action::Duplicate,
            "marked #1234 as a duplicate of this issue",
        );
        assert_eq!(refs, vec![issue_ref(None, 1234)]);
    }

    // --- commit --------------------------------------------------------------

    #[test]
    fn commit_action_extracts_short_shas_from_li_list() {
        // Rails body format: `added N commits\n\n<ul><li>SHORTSHA - title</li>...</ul>`
        // The HTML tags are present in the stored note body.
        let body = "added 3 commits\n\n\
                    * abc1234 - Fix the bug\n\
                    * def5678 - Add a test\n\
                    * 0123456 - Refactor the foo\n\
                    \n[Compare with previous version](/diffs)";
        let refs = extract(Action::Commit, body);
        let shas: Vec<&str> = refs
            .iter()
            .filter_map(|r| r.commit_sha.as_deref())
            .collect();
        assert_eq!(shas, vec!["abc1234", "def5678", "0123456"]);
    }

    #[test]
    fn commit_action_ignores_compare_link_hex_in_url() {
        // The trailing `Compare with previous version` link's URL may contain
        // hex fragments. We anchor the SHA on the start-of-line + ` - ` and
        // refuse hex words elsewhere on the line.
        let body = "added 1 commit\n\n\
                    * deadbee - Add feature\n\
                    \n[Compare with previous version](/-/compare/abc1234...def5678)";
        let refs = extract(Action::Commit, body);
        let shas: Vec<&str> = refs
            .iter()
            .filter_map(|r| r.commit_sha.as_deref())
            .collect();
        assert_eq!(shas, vec!["deadbee"]);
    }

    // --- merge ---------------------------------------------------------------

    #[test]
    fn merge_extracts_auto_merge_sha() {
        let refs = extract(
            Action::Merge,
            "enabled an automatic merge when all merge checks for 1a2b3c4d5e pass",
        );
        assert_eq!(refs, vec![commit_ref(None, "1a2b3c4d5e")]);
    }

    #[test]
    fn merge_extracts_mr_reference_when_no_sha() {
        let refs = extract(
            Action::Merge,
            "created merge request !123 to address this issue",
        );
        assert_eq!(refs, vec![mr_ref(None, 123)]);
    }

    // --- lifecycle (no parsing) ---------------------------------------------

    #[test]
    fn lifecycle_actions_extract_nothing() {
        for action in [
            Action::Closed,
            Action::Reopened,
            Action::Merged,
            Action::Opened,
        ] {
            assert!(extract(action, "closed").is_empty());
            assert!(extract(action, "merged").is_empty());
            // Even a body that happens to look like a cross-reference is
            // ignored for lifecycle actions — the action discriminator wins.
            assert!(extract(action, "mentioned in !1").is_empty());
        }
    }

    // --- negative / edge cases ----------------------------------------------

    #[test]
    fn empty_body_returns_no_refs() {
        for action in [
            Action::CrossReference,
            Action::Relate,
            Action::Moved,
            Action::Commit,
        ] {
            assert!(extract(action, "").is_empty(), "{:?} on empty body", action);
        }
    }

    #[test]
    fn body_with_project_path_only_yields_nothing() {
        // No `#`, `!`, or `@` token — purely a project path mention.
        let refs = extract(Action::CrossReference, "mentioned in gitlab-org/gitlab");
        assert!(refs.is_empty());
    }

    #[test]
    fn body_with_too_short_sha_is_rejected() {
        // 6 hex chars — below the 7-char minimum for a commit SHA.
        let refs = extract(Action::CrossReference, "mentioned in abc123");
        assert!(refs.is_empty(), "got refs: {refs:?}");
    }

    #[test]
    fn commit_action_with_no_list_yields_nothing() {
        let refs = extract(Action::Commit, "added 0 commits\n\n");
        assert!(refs.is_empty());
    }

    #[test]
    fn iid_extracts_at_most_20_digits() {
        // The IID regex bounds at `\d{1,20}`. A 21-digit run produces a
        // partial match (the first or last 20 digits) — this is
        // intentional: a real-world Rails IID never exceeds
        // `Gitlab::Database::MAX_INT_VALUE` (~19 digits), so the cap exists
        // purely to prevent unbounded backtracking on adversarial input.
        // The downstream resolver will fail to find an entity with that IID
        // and the edge will be dropped.
        let body = "mentioned in #123456789012345678901";
        let refs = extract(Action::CrossReference, body);
        // Whether the engine returns one match (20-digit prefix or suffix)
        // or none, the contract is that the parser does not panic, does not
        // hang, and produces at most one ref.
        assert!(refs.len() <= 1, "got {refs:?}");
    }
}
