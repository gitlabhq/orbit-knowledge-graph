use crate::actions::{all_actions, Action};
use crate::api_client::{ApiClient, Issue, MergeRequest, Milestone, Project};
use crate::config::{ActionWeights, Config};
use crate::metrics::MetricsCollector;
use crate::shared_state::SharedState;
use rand::seq::SliceRandom;
use rand::Rng;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tracing::{debug, error, info};

#[derive(Debug, Clone)]
pub struct FileInfo {
    pub path: String,
    pub class_name: String,
    pub package: String,
}

#[derive(Debug, Clone)]
pub struct AgentState {
    pub user_id: u64,
    pub namespace_id: u64,
    pub projects: Vec<Project>,
    pub issues: Vec<Issue>,
    pub merge_requests: Vec<MergeRequest>,
    pub milestones: HashMap<u64, Vec<Milestone>>,
    pub files: HashMap<u64, Vec<FileInfo>>,
    /// Tracks branches created per project (project_id -> list of branch names)
    pub branches: HashMap<u64, Vec<String>>,
}

impl AgentState {
    pub fn new(user_id: u64, namespace_id: u64) -> Self {
        Self {
            user_id,
            namespace_id,
            projects: Vec::new(),
            issues: Vec::new(),
            merge_requests: Vec::new(),
            milestones: HashMap::new(),
            files: HashMap::new(),
            branches: HashMap::new(),
        }
    }

    pub fn add_project(&mut self, project: Project) {
        self.projects.push(project);
    }

    pub fn add_file(&mut self, project_id: u64, path: String, class_name: String, package: String) {
        self.files.entry(project_id).or_default().push(FileInfo {
            path,
            class_name,
            package,
        });
    }

    pub fn add_issue(&mut self, issue: Issue) {
        self.issues.push(issue);
    }

    pub fn add_merge_request(&mut self, mr: MergeRequest) {
        self.merge_requests.push(mr);
    }

    pub fn add_milestone(&mut self, project_id: u64, milestone: Milestone) {
        self.milestones.entry(project_id).or_default().push(milestone);
    }

    pub fn add_branch(&mut self, project_id: u64, branch_name: String) {
        self.branches.entry(project_id).or_default().push(branch_name);
    }

    pub fn random_branch(&self, project_id: u64) -> Option<String> {
        self.branches
            .get(&project_id)
            .and_then(|branches| branches.choose(&mut rand::thread_rng()).cloned())
    }

    pub fn has_branches(&self, project_id: u64) -> bool {
        self.branches.get(&project_id).map_or(false, |b| !b.is_empty())
    }

    pub fn random_project_with_branches(&self) -> Option<(Project, String)> {
        let projects_with_branches: Vec<_> = self
            .projects
            .iter()
            .filter(|p| self.has_branches(p.id))
            .collect();
        let project = projects_with_branches.choose(&mut rand::thread_rng())?;
        let branch = self.random_branch(project.id)?;
        Some(((*project).clone(), branch))
    }

    pub fn close_issue(&mut self, project_id: u64, iid: u64) {
        if let Some(issue) = self
            .issues
            .iter_mut()
            .find(|i| i.project_id == project_id && i.iid == iid)
        {
            issue.state = "closed".to_string();
        }
    }

    pub fn close_merge_request(&mut self, project_id: u64, iid: u64) {
        if let Some(mr) = self
            .merge_requests
            .iter_mut()
            .find(|m| m.project_id == project_id && m.iid == iid)
        {
            mr.state = "closed".to_string();
        }
    }

    pub fn random_project(&self) -> Option<Project> {
        self.projects.choose(&mut rand::thread_rng()).cloned()
    }

    pub fn random_project_with_files(&self) -> Option<Project> {
        let projects_with_files: Vec<_> = self
            .projects
            .iter()
            .filter(|p| self.files.get(&p.id).map_or(false, |f| !f.is_empty()))
            .collect();
        projects_with_files
            .choose(&mut rand::thread_rng())
            .map(|p| (*p).clone())
    }

    pub fn random_project_with_file(&self) -> Option<(Project, FileInfo)> {
        let project = self.random_project_with_files()?;
        let files = self.files.get(&project.id)?;
        let file = files.choose(&mut rand::thread_rng())?;
        Some((project, file.clone()))
    }

    pub fn has_files(&self) -> bool {
        self.files.values().any(|f| !f.is_empty())
    }

    pub fn has_open_issues(&self) -> bool {
        self.issues.iter().any(|i| i.state == "opened")
    }

    pub fn has_open_merge_requests(&self) -> bool {
        self.merge_requests.iter().any(|m| m.state == "opened")
    }

    pub fn random_issue(&self) -> Option<Issue> {
        self.issues.choose(&mut rand::thread_rng()).cloned()
    }

    pub fn random_open_issue(&self) -> Option<Issue> {
        let open: Vec<_> = self.issues.iter().filter(|i| i.state == "opened").collect();
        open.choose(&mut rand::thread_rng()).map(|i| (*i).clone())
    }

    pub fn random_issues(&self, count: usize) -> Vec<Issue> {
        let mut rng = rand::thread_rng();
        self.issues
            .choose_multiple(&mut rng, count.min(self.issues.len()))
            .cloned()
            .collect()
    }

    pub fn random_merge_request(&self) -> Option<MergeRequest> {
        self.merge_requests.choose(&mut rand::thread_rng()).cloned()
    }

    pub fn random_open_merge_request(&self) -> Option<MergeRequest> {
        let open: Vec<_> = self
            .merge_requests
            .iter()
            .filter(|m| m.state == "opened")
            .collect();
        open.choose(&mut rand::thread_rng()).map(|m| (*m).clone())
    }

    pub fn random_milestone(&self) -> Option<Milestone> {
        let all: Vec<_> = self.milestones.values().flatten().collect();
        all.choose(&mut rand::thread_rng()).map(|m| (*m).clone())
    }
}

pub struct Agent {
    pub id: usize,
    client: ApiClient,
    config: Arc<Config>,
    state: AgentState,
    shared: SharedState,
    metrics: MetricsCollector,
    actions: Vec<Box<dyn Action>>,
    weights: ActionWeights,
}

impl Agent {
    pub fn new(
        id: usize,
        client: ApiClient,
        config: Arc<Config>,
        user_id: u64,
        namespace_id: u64,
        shared: SharedState,
        metrics: MetricsCollector,
    ) -> Self {
        Self {
            id,
            client,
            config: config.clone(),
            state: AgentState::new(user_id, namespace_id),
            shared,
            metrics,
            actions: all_actions(),
            weights: config.action_weights(),
        }
    }

    pub async fn run(&mut self) {
        info!("Agent {} starting", self.id);

        while self.config.is_running() {
            if let Some(action_idx) = self.select_weighted_action_index() {
                // Check if action can execute using both local and shared state
                let can_execute = self.actions[action_idx].can_execute(&self.state, &self.shared);
                if can_execute {
                    self.execute_action_by_index(action_idx).await;
                }
            }

            self.random_delay().await;
        }

        info!("Agent {} finished", self.id);
    }

    fn select_weighted_action_index(&self) -> Option<usize> {
        let mut rng = rand::thread_rng();
        let total_weight = self.weights.total();
        if total_weight == 0 {
            return None;
        }

        let mut random_value = rng.gen_range(0..total_weight);

        let weights = [
            (self.weights.create_project, "create_project"),
            (self.weights.create_file, "create_file"),
            (self.weights.update_file, "update_file"),
            (self.weights.create_issue, "create_issue"),
            (self.weights.close_issue, "close_issue"),
            (self.weights.link_issues, "link_issues"),
            (self.weights.create_milestone, "create_milestone"),
            (self.weights.attach_milestone, "attach_milestone"),
            (self.weights.create_merge_request, "create_merge_request"),
            (self.weights.push_to_merge_request, "push_to_merge_request"),
            (self.weights.comment_on_issue, "comment_on_issue"),
            (self.weights.comment_on_merge_request, "comment_on_merge_request"),
            (self.weights.approve_merge_request, "approve_merge_request"),
            (self.weights.merge_merge_request, "merge_merge_request"),
            (self.weights.close_merge_request, "close_merge_request"),
            (self.weights.reply_to_issue_comment, "reply_to_issue_comment"),
            (self.weights.reply_to_mr_discussion, "reply_to_mr_discussion"),
        ];

        for (weight, name) in weights {
            if random_value < weight {
                return self.actions.iter().position(|a| a.name() == name);
            }
            random_value -= weight;
        }

        None
    }

    async fn execute_action_by_index(&mut self, action_idx: usize) {
        let action_name = self.actions[action_idx].name();
        let start = Instant::now();

        // Generate description and API endpoints for dry-run reporting
        let (description, api_endpoints) = self.describe_action(action_name);

        match self.actions[action_idx]
            .execute(&self.client, &mut self.state, &self.shared)
            .await
        {
            Ok(()) => {
                let duration = start.elapsed();
                self.metrics.record_success(action_name, duration);

                // Record dry-run action for reporting
                if self.config.dry_run {
                    self.metrics.record_dry_run_action(
                        self.id,
                        action_name,
                        &description,
                        api_endpoints,
                    );
                }

                debug!(
                    "[Agent {}] {}: success ({}ms)",
                    self.id,
                    action_name,
                    duration.as_millis()
                );
            }
            Err(e) => {
                let duration = start.elapsed();
                let error_msg = e.to_string();
                self.metrics.record_failure(action_name, &error_msg, duration);
                error!(
                    "[Agent {}] {}: failed - {}",
                    self.id, action_name, error_msg
                );
            }
        }
    }

    fn describe_action(&self, action_name: &str) -> (String, Vec<String>) {
        let ns = self.state.namespace_id;
        let project_count = self.state.projects.len();
        let issue_count = self.state.issues.len();
        let mr_count = self.state.merge_requests.len();

        match action_name {
            "create_project" => (
                format!("Create new project in namespace {}", ns),
                vec![
                    "POST /projects".to_string(),
                    "POST /projects/:id/repository/files/:path".to_string(),
                ],
            ),
            "create_file" => (
                format!("Create branch and add Java file in one of {} projects", project_count),
                vec![
                    "POST /projects/:id/repository/branches".to_string(),
                    "POST /projects/:id/repository/files/:path".to_string(),
                ],
            ),
            "update_file" => (
                "Create branch and update existing Java file".to_string(),
                vec![
                    "POST /projects/:id/repository/branches".to_string(),
                    "PUT /projects/:id/repository/files/:path".to_string(),
                ],
            ),
            "create_issue" => (
                format!("Create issue in one of {} projects", project_count),
                vec!["POST /projects/:id/issues".to_string()],
            ),
            "close_issue" => (
                format!("Close one of {} issues", issue_count),
                vec!["PUT /projects/:id/issues/:iid".to_string()],
            ),
            "link_issues" => (
                "Link two issues together".to_string(),
                vec!["POST /projects/:id/issues/:iid/links".to_string()],
            ),
            "create_milestone" => (
                "Create milestone in project".to_string(),
                vec!["POST /projects/:id/milestones".to_string()],
            ),
            "attach_milestone" => (
                "Attach milestone to issue".to_string(),
                vec!["PUT /projects/:id/issues/:iid".to_string()],
            ),
            "create_merge_request" => (
                "Create branch, add file, open MR".to_string(),
                vec![
                    "POST /projects/:id/repository/branches".to_string(),
                    "POST /projects/:id/repository/files/:path".to_string(),
                    "POST /projects/:id/merge_requests".to_string(),
                ],
            ),
            "push_to_merge_request" => (
                format!("Push commit to one of {} open MRs", mr_count),
                vec!["POST /projects/:id/repository/files/:path".to_string()],
            ),
            "comment_on_issue" => (
                "Comment on issue (cross-agent)".to_string(),
                vec!["POST /projects/:id/issues/:iid/notes".to_string()],
            ),
            "comment_on_merge_request" => (
                "Comment on MR (cross-agent)".to_string(),
                vec!["POST /projects/:id/merge_requests/:iid/notes".to_string()],
            ),
            "approve_merge_request" => (
                "Approve MR from another agent".to_string(),
                vec!["POST /projects/:id/merge_requests/:iid/approve".to_string()],
            ),
            "merge_merge_request" => (
                "Merge MR into target branch".to_string(),
                vec!["PUT /projects/:id/merge_requests/:iid/merge".to_string()],
            ),
            "close_merge_request" => (
                "Close MR without merging".to_string(),
                vec!["PUT /projects/:id/merge_requests/:iid".to_string()],
            ),
            "reply_to_issue_comment" => (
                "Reply to another agent's issue comment".to_string(),
                vec!["POST /projects/:id/issues/:iid/notes".to_string()],
            ),
            "reply_to_mr_discussion" => (
                "Reply to another agent's MR discussion".to_string(),
                vec!["POST /projects/:id/merge_requests/:iid/discussions/:id/notes".to_string()],
            ),
            _ => (
                format!("Unknown action: {}", action_name),
                vec![],
            ),
        }
    }

    async fn random_delay(&self) {
        let delay_secs = {
            let mut rng = rand::thread_rng();
            rng.gen_range(self.config.min_action_delay..=self.config.max_action_delay)
        };
        tokio::time::sleep(Duration::from_secs_f64(delay_secs)).await;
    }
}
