use clap::Parser;
use std::time::{Duration, Instant};

#[derive(Parser, Debug, Clone)]
#[command(name = "gitlab-load-testing")]
#[command(about = "Load testing framework for GitLab - simulates concurrent user behavior")]
pub struct Config {
    /// GitLab instance URL
    #[arg(long, env = "LOAD_TEST_BASE_URL", default_value = "http://localhost:3000")]
    pub base_url: String,

    /// Admin API token for creating users
    #[arg(long, env = "GITLAB_QA_ADMIN_ACCESS_TOKEN")]
    pub admin_token: String,

    /// Number of concurrent agents
    #[arg(long, env = "LOAD_TEST_AGENTS", default_value = "100")]
    pub agent_count: usize,

    /// Test duration in minutes
    #[arg(long, env = "LOAD_TEST_DURATION_MINUTES", default_value = "60")]
    pub duration_minutes: u64,

    /// Minimum delay between actions (seconds)
    #[arg(long, default_value = "0.5")]
    pub min_action_delay: f64,

    /// Maximum delay between actions (seconds)
    #[arg(long, default_value = "3.0")]
    pub max_action_delay: f64,

    /// Enable verbose logging
    #[arg(long, env = "LOAD_TEST_VERBOSE")]
    pub verbose: bool,

    /// Dry run mode - simulate actions without making API calls
    #[arg(long, env = "LOAD_TEST_DRY_RUN")]
    pub dry_run: bool,

    #[clap(skip)]
    start_time: Option<Instant>,
}

impl Config {
    pub const NAMESPACE_PREFIX: &'static str = "load-test-namespace";
    pub const USER_PREFIX: &'static str = "load-test-user";
    pub const NAMESPACE_COUNT: usize = 2;

    pub fn namespace_path(&self, index: usize) -> String {
        format!("{}-{}", Self::NAMESPACE_PREFIX, index)
    }

    pub fn namespace_name(&self, index: usize) -> String {
        format!("Load Test Namespace {}", index)
    }

    pub fn username(&self, index: usize) -> String {
        format!("{}-{:03}", Self::USER_PREFIX, index)
    }

    pub fn user_email(&self, index: usize) -> String {
        format!("{}@example.com", self.username(index))
    }

    pub fn start(&mut self) {
        self.start_time = Some(Instant::now());
    }

    pub fn end_time(&self) -> Instant {
        self.start_time
            .expect("Config not started")
            + Duration::from_secs(self.duration_minutes * 60)
    }

    pub fn is_running(&self) -> bool {
        Instant::now() < self.end_time()
    }

    pub fn elapsed(&self) -> Duration {
        self.start_time
            .map(|start| start.elapsed())
            .unwrap_or_default()
    }

    pub fn remaining(&self) -> Duration {
        let end = self.end_time();
        let now = Instant::now();
        if now < end {
            end - now
        } else {
            Duration::ZERO
        }
    }

    pub fn action_weights(&self) -> ActionWeights {
        ActionWeights::default()
    }
}

#[derive(Debug, Clone)]
pub struct ActionWeights {
    pub create_project: u32,
    pub create_file: u32,
    pub update_file: u32,
    pub create_issue: u32,
    pub close_issue: u32,
    pub link_issues: u32,
    pub create_milestone: u32,
    pub attach_milestone: u32,
    pub create_merge_request: u32,
    pub push_to_merge_request: u32,
    pub comment_on_issue: u32,
    pub comment_on_merge_request: u32,
    pub approve_merge_request: u32,
    pub merge_merge_request: u32,
    pub close_merge_request: u32,
    pub reply_to_issue_comment: u32,
    pub reply_to_mr_discussion: u32,
}

impl Default for ActionWeights {
    fn default() -> Self {
        Self {
            create_project: 5,
            create_file: 20,
            update_file: 15,
            create_issue: 15,
            close_issue: 10,
            link_issues: 5,
            create_milestone: 5,
            attach_milestone: 5,
            create_merge_request: 8,
            push_to_merge_request: 10,
            comment_on_issue: 5,
            comment_on_merge_request: 5,
            approve_merge_request: 5,
            merge_merge_request: 3,
            close_merge_request: 2,
            reply_to_issue_comment: 5,
            reply_to_mr_discussion: 5,
        }
    }
}

impl ActionWeights {
    pub fn total(&self) -> u32 {
        self.create_project
            + self.create_file
            + self.update_file
            + self.create_issue
            + self.close_issue
            + self.link_issues
            + self.create_milestone
            + self.attach_milestone
            + self.create_merge_request
            + self.push_to_merge_request
            + self.comment_on_issue
            + self.comment_on_merge_request
            + self.approve_merge_request
            + self.merge_merge_request
            + self.close_merge_request
            + self.reply_to_issue_comment
            + self.reply_to_mr_discussion
    }
}
