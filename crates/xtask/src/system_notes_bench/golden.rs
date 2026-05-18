//! Golden corpus of system-note bodies, sourced verbatim from Rails
//! `app/services/system_notes/*.rb` example comments.
//!
//! These are not "real" production notes — they're the canonical templates
//! the monolith uses to author system notes. The implementation MR's
//! integration tests will swap in real bodies pulled from staging; the
//! corpus here is the contract for what the parser must handle by spec.

use super::parser::Action;

pub struct Sample {
    pub action: Action,
    pub body: &'static str,
    pub description: &'static str,
}

pub const SAMPLES: &[Sample] = &[
    Sample {
        action: Action::CrossReference,
        body: "mentioned in !123",
        description: "same-project MR reference",
    },
    Sample {
        action: Action::CrossReference,
        body: "mentioned in gitlab-org/gitlab#456",
        description: "cross-project issue reference",
    },
    Sample {
        action: Action::CrossReference,
        body: "mentioned in 54f7727c",
        description: "commit short-SHA reference",
    },
    Sample {
        action: Action::Relate,
        body: "marked as related to gitlab-foss#9001",
        description: "single related-to reference",
    },
    Sample {
        action: Action::Relate,
        body: "marked as related to gitlab-foss#9001, gitlab-foss#9002, and gitlab-foss#9003",
        description: "multiple related-to references",
    },
    Sample {
        action: Action::Unrelate,
        body: "removed the relation with gitlab-foss#9001",
        description: "unrelate by reference",
    },
    Sample {
        action: Action::RelateToParent,
        body: "added group/project#1234 as parent item",
        description: "hierarchy: added parent",
    },
    Sample {
        action: Action::RelateToChild,
        body: "added #1234 as child item",
        description: "hierarchy: added child (same-project)",
    },
    Sample {
        action: Action::UnrelateFromParent,
        body: "removed parent item group/proj#9",
        description: "hierarchy: removed parent",
    },
    Sample {
        action: Action::UnrelateFromChild,
        body: "removed child item #1234",
        description: "hierarchy: removed child",
    },
    Sample {
        action: Action::Moved,
        body: "moved to other_namespace/project_new#11",
        description: "noteable moved to another project",
    },
    Sample {
        action: Action::Cloned,
        body: "cloned from other_namespace/project_new#11",
        description: "noteable cloned from another project",
    },
    Sample {
        action: Action::Duplicate,
        body: "marked this issue as a duplicate of other_project#5678",
        description: "duplicate of canonical",
    },
    Sample {
        action: Action::Duplicate,
        body: "marked #1234 as a duplicate of this issue",
        description: "canonical of duplicate",
    },
    Sample {
        action: Action::Commit,
        body: "added 2 commits\n\n* abc1234 - Fix the bug\n* def5678 - Add a test\n\n[Compare with previous version](/-/compare/abc...def)",
        description: "MR commits added",
    },
    Sample {
        action: Action::Merge,
        body: "enabled an automatic merge when all merge checks for 1a2b3c4d5e pass",
        description: "auto-merge enabled with SHA",
    },
    Sample {
        action: Action::Merge,
        body: "created merge request !123 to address this issue",
        description: "MR created from issue",
    },
    Sample {
        action: Action::Closed,
        body: "closed",
        description: "lifecycle: closed",
    },
    Sample {
        action: Action::Reopened,
        body: "reopened",
        description: "lifecycle: reopened",
    },
    Sample {
        action: Action::Merged,
        body: "merged",
        description: "lifecycle: merged",
    },
    Sample {
        action: Action::Opened,
        body: "opened",
        description: "lifecycle: opened (rarely persisted)",
    },
];

#[cfg(test)]
mod tests {
    use super::*;
    use crate::system_notes_bench::parser::extract;

    #[test]
    fn every_sample_parses_without_panic() {
        for sample in SAMPLES {
            let _ = extract(sample.action, sample.body);
        }
    }

    #[test]
    fn cross_reference_samples_yield_one_ref_each() {
        for sample in SAMPLES
            .iter()
            .filter(|s| s.action == Action::CrossReference)
        {
            let refs = extract(sample.action, sample.body);
            assert_eq!(refs.len(), 1, "{}: got {:?}", sample.description, refs);
        }
    }

    #[test]
    fn lifecycle_samples_yield_zero_refs() {
        for sample in SAMPLES.iter().filter(|s| s.action.is_lifecycle()) {
            let refs = extract(sample.action, sample.body);
            assert!(refs.is_empty(), "{}: got {:?}", sample.description, refs);
        }
    }
}
