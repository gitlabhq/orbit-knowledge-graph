use crate::api_client::{Issue, MergeRequest, Project, PublishedDiscussion, PublishedNote};
use rand::seq::SliceRandom;
use std::sync::{Arc, RwLock};

/// Thread-safe shared state for cross-agent resource interaction.
/// Agents publish their resources here so other agents can discover and interact with them.
#[derive(Debug, Default)]
struct SharedStateInner {
    projects: Vec<Project>,
    issues: Vec<Issue>,
    merge_requests: Vec<MergeRequest>,
    issue_notes: Vec<PublishedNote>,
    mr_discussions: Vec<PublishedDiscussion>,
}

#[derive(Debug, Clone, Default)]
pub struct SharedState {
    inner: Arc<RwLock<SharedStateInner>>,
}

impl SharedState {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(RwLock::new(SharedStateInner::default())),
        }
    }

    // =========================================================================
    // Publishing (agents add their resources to the shared pool)
    // =========================================================================

    pub fn publish_project(&self, project: Project) {
        if let Ok(mut state) = self.inner.write() {
            state.projects.push(project);
        }
    }

    pub fn publish_issue(&self, issue: Issue) {
        if let Ok(mut state) = self.inner.write() {
            state.issues.push(issue);
        }
    }

    pub fn publish_merge_request(&self, mr: MergeRequest) {
        if let Ok(mut state) = self.inner.write() {
            state.merge_requests.push(mr);
        }
    }

    pub fn publish_issue_note(&self, note: PublishedNote) {
        if let Ok(mut state) = self.inner.write() {
            state.issue_notes.push(note);
        }
    }

    pub fn publish_mr_discussion(&self, discussion: PublishedDiscussion) {
        if let Ok(mut state) = self.inner.write() {
            state.mr_discussions.push(discussion);
        }
    }

    // =========================================================================
    // Querying (agents discover resources from other agents)
    // =========================================================================

    pub fn random_issue(&self) -> Option<Issue> {
        let state = self.inner.read().ok()?;
        state.issues.choose(&mut rand::thread_rng()).cloned()
    }

    pub fn random_open_issue(&self) -> Option<Issue> {
        let state = self.inner.read().ok()?;
        let open: Vec<_> = state.issues.iter().filter(|i| i.state == "opened").collect();
        open.choose(&mut rand::thread_rng()).map(|i| (*i).clone())
    }

    pub fn random_issues(&self, count: usize) -> Vec<Issue> {
        let state = match self.inner.read() {
            Ok(s) => s,
            Err(_) => return Vec::new(),
        };
        state
            .issues
            .choose_multiple(&mut rand::thread_rng(), count.min(state.issues.len()))
            .cloned()
            .collect()
    }

    pub fn random_merge_request(&self) -> Option<MergeRequest> {
        let state = self.inner.read().ok()?;
        state.merge_requests.choose(&mut rand::thread_rng()).cloned()
    }

    pub fn random_open_merge_request(&self) -> Option<MergeRequest> {
        let state = self.inner.read().ok()?;
        let open: Vec<_> = state
            .merge_requests
            .iter()
            .filter(|m| m.state == "opened")
            .collect();
        open.choose(&mut rand::thread_rng()).map(|m| (*m).clone())
    }

    /// Get a random open MR that wasn't created by the given user.
    /// This is needed for approvals since you can't approve your own MR.
    pub fn random_open_merge_request_not_by(&self, user_id: u64) -> Option<MergeRequest> {
        let state = self.inner.read().ok()?;
        let candidates: Vec<_> = state
            .merge_requests
            .iter()
            .filter(|m| m.state == "opened" && m.author_id() != user_id)
            .collect();
        candidates.choose(&mut rand::thread_rng()).map(|m| (*m).clone())
    }

    /// Get a random issue note from another agent to reply to
    pub fn random_issue_note_not_by(&self, user_id: u64) -> Option<PublishedNote> {
        let state = self.inner.read().ok()?;
        let candidates: Vec<_> = state
            .issue_notes
            .iter()
            .filter(|n| n.author_id != user_id)
            .collect();
        candidates.choose(&mut rand::thread_rng()).map(|n| (*n).clone())
    }

    /// Get a random MR discussion from another agent to reply to
    pub fn random_mr_discussion_not_by(&self, user_id: u64) -> Option<PublishedDiscussion> {
        let state = self.inner.read().ok()?;
        let candidates: Vec<_> = state
            .mr_discussions
            .iter()
            .filter(|d| d.author_id != user_id)
            .collect();
        candidates.choose(&mut rand::thread_rng()).map(|d| (*d).clone())
    }

    // =========================================================================
    // State updates (closing issues/MRs)
    // =========================================================================

    pub fn close_issue(&self, project_id: u64, iid: u64) {
        if let Ok(mut state) = self.inner.write() {
            if let Some(issue) = state
                .issues
                .iter_mut()
                .find(|i| i.project_id == project_id && i.iid == iid)
            {
                issue.state = "closed".to_string();
            }
        }
    }

    pub fn close_merge_request(&self, project_id: u64, iid: u64) {
        if let Ok(mut state) = self.inner.write() {
            if let Some(mr) = state
                .merge_requests
                .iter_mut()
                .find(|m| m.project_id == project_id && m.iid == iid)
            {
                mr.state = "closed".to_string();
            }
        }
    }

    // =========================================================================
    // Stats
    // =========================================================================

    pub fn has_issues(&self) -> bool {
        self.inner
            .read()
            .map(|s| !s.issues.is_empty())
            .unwrap_or(false)
    }

    pub fn has_multiple_issues(&self) -> bool {
        self.inner
            .read()
            .map(|s| s.issues.len() >= 2)
            .unwrap_or(false)
    }

    pub fn has_merge_requests(&self) -> bool {
        self.inner
            .read()
            .map(|s| !s.merge_requests.is_empty())
            .unwrap_or(false)
    }

    pub fn has_open_merge_requests(&self) -> bool {
        self.inner
            .read()
            .map(|s| s.merge_requests.iter().any(|m| m.state == "opened"))
            .unwrap_or(false)
    }

    pub fn has_issue_notes_from_others(&self, user_id: u64) -> bool {
        self.inner
            .read()
            .map(|s| s.issue_notes.iter().any(|n| n.author_id != user_id))
            .unwrap_or(false)
    }

    pub fn has_mr_discussions_from_others(&self, user_id: u64) -> bool {
        self.inner
            .read()
            .map(|s| s.mr_discussions.iter().any(|d| d.author_id != user_id))
            .unwrap_or(false)
    }
}
