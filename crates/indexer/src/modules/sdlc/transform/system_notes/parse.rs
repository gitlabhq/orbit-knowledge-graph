use std::sync::LazyLock;

use regex::Regex;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Reference {
    pub kind: RefKind,
    pub project_path: Option<String>,
    pub iid: Option<i64>,
    pub commit_sha: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum RefKind {
    Issue,
    MergeRequest,
    Commit,
}

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
    Commit,
    Merge,
    Closed,
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
            "commit" => Self::Commit,
            "merge" => Self::Merge,
            "closed" => Self::Closed,
            "merged" => Self::Merged,
            "opened" => Self::Opened,
            _ => return None,
        })
    }

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
            Self::Commit => "commit",
            Self::Merge => "merge",
            Self::Closed => "closed",
            Self::Merged => "merged",
            Self::Opened => "opened",
        }
    }

    #[cfg(test)]
    pub fn is_lifecycle(self) -> bool {
        matches!(self, Self::Closed | Self::Merged | Self::Opened)
    }
}

// Reference token grammar, derived from Rails:
//
// * `Project.reference_pattern` in `app/models/project.rb` accepts an optional
//   namespace prefix (up to 20 segments) followed by a project path. Each
//   path segment matches `[a-zA-Z0-9_][a-zA-Z0-9_\-\.]{0,254}`.
// * `Issue.reference_pattern` is `(<project>?)#<digits>`.
// * `MergeRequest.reference_pattern` is `(<project>?)!<digits>`.
// * `Commit.reference_pattern` is `(<project>@)?<sha 7..64>`.
//
// We bound the namespace depth at 20 segments to match
// `Namespace::NUMBER_OF_ANCESTORS_ALLOWED`. The IID is bounded at 20 digits to
// fit `Gitlab::Database::MAX_INT_VALUE`.

const PATH_SEGMENT: &str = r"[A-Za-z0-9_][A-Za-z0-9_\-\.]{0,254}";

fn project_path_re() -> String {
    format!(r"(?:{PATH_SEGMENT})(?:/{PATH_SEGMENT}){{0,19}}")
}

#[expect(
    clippy::expect_used,
    reason = "static regex literal is verified by tests and cannot fail at runtime"
)]
static ISSUE_REF: LazyLock<Regex> = LazyLock::new(|| {
    let project = project_path_re();
    Regex::new(&format!(
        r"(?:(?P<project>{project})?)#(?P<iid>\d{{1,20}})\b"
    ))
    .expect("issue regex compiles")
});

#[expect(
    clippy::expect_used,
    reason = "static regex literal is verified by tests and cannot fail at runtime"
)]
static MR_REF: LazyLock<Regex> = LazyLock::new(|| {
    let project = project_path_re();
    Regex::new(&format!(
        r"(?:(?P<project>{project})?)!(?P<iid>\d{{1,20}})\b"
    ))
    .expect("merge request regex compiles")
});

#[expect(
    clippy::expect_used,
    reason = "static regex literal is verified by tests and cannot fail at runtime"
)]
static COMMIT_REF: LazyLock<Regex> = LazyLock::new(|| {
    let project = project_path_re();
    Regex::new(&format!(
        r"\b(?:(?P<project>{project})@)?(?P<sha>[0-9a-f]{{7,64}})\b"
    ))
    .expect("commit regex compiles")
});

pub fn extract(action: Action, body: &str) -> Vec<Reference> {
    match action {
        Action::Closed | Action::Merged | Action::Opened => Vec::new(),

        Action::CrossReference => first_ref_any(body).into_iter().collect(),

        Action::Relate => all_refs_any(body),

        Action::Unrelate => first_ref_any(body).into_iter().collect(),

        Action::RelateToParent
        | Action::RelateToChild
        | Action::UnrelateFromParent
        | Action::UnrelateFromChild => first_ref_any(body).into_iter().collect(),

        Action::Moved | Action::Cloned => first_ref_any(body).into_iter().collect(),

        Action::Duplicate => first_ref_any(body).into_iter().collect(),

        Action::Commit => extract_commit_shas_from_list(body),

        Action::Merge => extract_merge_ref(body),
    }
}

fn first_ref_any(body: &str) -> Option<Reference> {
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
    #[expect(
        clippy::unwrap_used,
        reason = "static regex literal is verified by tests and cannot fail at runtime"
    )]
    static LINE: LazyLock<Regex> =
        LazyLock::new(|| Regex::new(r"(?m)^[^\w]*\b(?P<sha>[0-9a-f]{7,64})\b\s*-\s").unwrap());

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
    #[expect(
        clippy::expect_used,
        reason = "static regex literal is verified by tests and cannot fail at runtime"
    )]
    static SHA_AFTER_FOR: LazyLock<Regex> = LazyLock::new(|| {
        Regex::new(r"\bfor\s+(?P<sha>[0-9a-f]{7,64})\b").expect("sha-after-for regex compiles")
    });
    if let Some(caps) = SHA_AFTER_FOR.captures(body) {
        return vec![Reference {
            kind: RefKind::Commit,
            project_path: None,
            iid: None,
            commit_sha: caps.name("sha").map(|m| m.as_str().to_owned()),
        }];
    }

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
            "commit",
            "merge",
            "closed",
            "merged",
            "opened",
        ] {
            let a = Action::parse(s).unwrap_or_else(|| panic!("action {s} should parse"));
            assert_eq!(a.as_str(), s);
        }
    }

    #[test]
    fn reopened_is_not_a_system_note_action() {
        assert!(Action::parse("reopened").is_none());
    }

    #[test]
    fn action_parse_returns_none_for_unknown() {
        for s in [
            "label",
            "milestone",
            "branch",
            "designs_added",
            "duo_agent_started",
            "epic_issue_added",
            "issue_added_to_epic",
            "epic_issue_moved",
            "task",
        ] {
            assert!(Action::parse(s).is_none(), "{s} should not parse");
        }
    }

    #[test]
    fn action_lifecycle_classification() {
        assert!(Action::Closed.is_lifecycle());
        assert!(Action::Merged.is_lifecycle());
        assert!(Action::Opened.is_lifecycle());
        assert!(!Action::CrossReference.is_lifecycle());
        assert!(!Action::Merge.is_lifecycle());
    }

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

    #[test]
    fn cross_reference_commit_with_full_sha256() {
        let sha = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
        let refs = extract(Action::CrossReference, &format!("mentioned in {sha}"));
        assert_eq!(refs, vec![commit_ref(None, sha)]);
    }

    #[test]
    fn cross_reference_cross_project_commit_with_full_sha256() {
        let sha = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
        let body = format!("mentioned in gitlab-org/gitlab@{sha}");
        let refs = extract(Action::CrossReference, &body);
        assert_eq!(refs, vec![commit_ref(Some("gitlab-org/gitlab"), sha)]);
    }

    #[test]
    fn body_with_too_long_sha_is_rejected() {
        let body = format!("mentioned in {}", "a".repeat(65));
        let refs = extract(Action::CrossReference, &body);
        assert!(refs.is_empty(), "got refs: {refs:?}");
    }

    #[test]
    fn commit_action_extracts_full_sha256_from_li_list() {
        let sha = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
        let body = format!("added 1 commit\n\n* {sha} - Fix the bug\n");
        let refs = extract(Action::Commit, &body);
        assert_eq!(refs, vec![commit_ref(None, sha)]);
    }

    #[test]
    fn merge_extracts_full_sha256_auto_merge_sha() {
        let sha = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
        let body = format!("enabled an automatic merge when all merge checks for {sha} pass");
        let refs = extract(Action::Merge, &body);
        assert_eq!(refs, vec![commit_ref(None, sha)]);
    }

    #[test]
    fn commit_action_extracts_short_shas_from_li_list() {
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

    #[test]
    fn lifecycle_actions_extract_nothing() {
        for action in [Action::Closed, Action::Merged, Action::Opened] {
            assert!(extract(action, "closed").is_empty());
            assert!(extract(action, "merged").is_empty());
            assert!(extract(action, "mentioned in !1").is_empty());
        }
    }

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
        let refs = extract(Action::CrossReference, "mentioned in gitlab-org/gitlab");
        assert!(refs.is_empty());
    }

    #[test]
    fn body_with_too_short_sha_is_rejected() {
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
        let body = "mentioned in #123456789012345678901";
        let refs = extract(Action::CrossReference, body);
        assert!(refs.len() <= 1, "got {refs:?}");
    }
}
