use anyhow::{Context, Result};
use clap::Parser;
use serde::Deserialize;
use std::fs;
use std::path::Path;
use std::time::{Duration, Instant};

#[derive(Parser, Debug, Clone)]
#[command(name = "gitlab-load-testing")]
#[command(about = "Load testing framework for GitLab - simulates concurrent user behavior")]
pub struct CliArgs {
    /// Path to YAML configuration file
    #[arg(long, short = 'c', env = "LOAD_TEST_CONFIG")]
    pub config: Option<String>,

    /// Configuration profile to use (defined in config file)
    #[arg(long, short = 'p')]
    pub profile: Option<String>,

    /// GitLab instance URL
    #[arg(long, env = "LOAD_TEST_BASE_URL")]
    pub base_url: Option<String>,

    /// Admin API token for creating users
    #[arg(long, env = "GITLAB_QA_ADMIN_ACCESS_TOKEN")]
    pub admin_token: Option<String>,

    /// Number of concurrent agents
    #[arg(long, env = "LOAD_TEST_AGENTS")]
    pub agent_count: Option<usize>,

    /// Test duration in minutes
    #[arg(long, env = "LOAD_TEST_DURATION_MINUTES")]
    pub duration_minutes: Option<u64>,

    /// Minimum delay between actions (seconds)
    #[arg(long)]
    pub min_action_delay: Option<f64>,

    /// Maximum delay between actions (seconds)
    #[arg(long)]
    pub max_action_delay: Option<f64>,

    /// Enable verbose logging
    #[arg(long, env = "LOAD_TEST_VERBOSE")]
    pub verbose: bool,

    /// Dry run mode - simulate actions without making API calls
    #[arg(long, env = "LOAD_TEST_DRY_RUN")]
    pub dry_run: bool,
}

#[derive(Debug, Deserialize, Default, Clone)]
pub struct YamlConfig {
    #[serde(default)]
    pub gitlab: GitLabConfig,
    #[serde(default)]
    pub execution: ExecutionConfig,
    #[serde(default)]
    pub timing: TimingConfig,
    #[serde(default)]
    pub namespaces: NamespaceConfig,
    #[serde(default)]
    pub users: UserConfig,
    #[serde(default)]
    pub action_weights: Option<ActionWeights>,
    #[serde(default)]
    pub profiles: std::collections::HashMap<String, ProfileConfig>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct GitLabConfig {
    #[serde(default = "default_base_url")]
    pub base_url: String,
    pub admin_token: Option<String>,
}

impl Default for GitLabConfig {
    fn default() -> Self {
        Self {
            base_url: default_base_url(),
            admin_token: None,
        }
    }
}

fn default_base_url() -> String {
    "http://localhost:3000".to_string()
}

#[derive(Debug, Deserialize, Clone)]
pub struct ExecutionConfig {
    #[serde(default = "default_agent_count")]
    pub agent_count: usize,
    #[serde(default = "default_duration_minutes")]
    pub duration_minutes: u64,
    #[serde(default)]
    pub dry_run: bool,
    #[serde(default)]
    pub verbose: bool,
}

impl Default for ExecutionConfig {
    fn default() -> Self {
        Self {
            agent_count: default_agent_count(),
            duration_minutes: default_duration_minutes(),
            dry_run: false,
            verbose: false,
        }
    }
}

fn default_agent_count() -> usize {
    100
}

fn default_duration_minutes() -> u64 {
    60
}

#[derive(Debug, Deserialize, Clone)]
pub struct TimingConfig {
    #[serde(default = "default_min_action_delay")]
    pub min_action_delay: f64,
    #[serde(default = "default_max_action_delay")]
    pub max_action_delay: f64,
    #[serde(default = "default_progress_interval")]
    pub progress_interval: u64,
    #[serde(default = "default_request_timeout")]
    pub request_timeout: u64,
}

impl Default for TimingConfig {
    fn default() -> Self {
        Self {
            min_action_delay: default_min_action_delay(),
            max_action_delay: default_max_action_delay(),
            progress_interval: default_progress_interval(),
            request_timeout: default_request_timeout(),
        }
    }
}

fn default_min_action_delay() -> f64 {
    0.5
}

fn default_max_action_delay() -> f64 {
    3.0
}

fn default_progress_interval() -> u64 {
    60
}

fn default_request_timeout() -> u64 {
    30
}

#[derive(Debug, Deserialize, Clone)]
pub struct NamespaceConfig {
    #[serde(default = "default_namespace_prefix")]
    pub prefix: String,
    #[serde(default = "default_namespace_count")]
    pub count: usize,
}

impl Default for NamespaceConfig {
    fn default() -> Self {
        Self {
            prefix: default_namespace_prefix(),
            count: default_namespace_count(),
        }
    }
}

fn default_namespace_prefix() -> String {
    "load-test-namespace".to_string()
}

fn default_namespace_count() -> usize {
    2
}

#[derive(Debug, Deserialize, Clone)]
pub struct UserConfig {
    #[serde(default = "default_user_prefix")]
    pub prefix: String,
    #[serde(default = "default_email_domain")]
    pub email_domain: String,
}

impl Default for UserConfig {
    fn default() -> Self {
        Self {
            prefix: default_user_prefix(),
            email_domain: default_email_domain(),
        }
    }
}

fn default_user_prefix() -> String {
    "load-test-user".to_string()
}

fn default_email_domain() -> String {
    "example.com".to_string()
}

#[derive(Debug, Deserialize, Default, Clone)]
pub struct ProfileConfig {
    pub agent_count: Option<usize>,
    pub duration_minutes: Option<u64>,
    pub timing: Option<TimingConfig>,
    pub action_weights: Option<ActionWeights>,
}

#[derive(Debug, Clone)]
pub struct Config {
    pub base_url: String,
    pub admin_token: String,
    pub agent_count: usize,
    pub duration_minutes: u64,
    pub min_action_delay: f64,
    pub max_action_delay: f64,
    pub progress_interval: u64,
    pub request_timeout: u64,
    pub verbose: bool,
    pub dry_run: bool,
    pub namespace_prefix: String,
    pub namespace_count: usize,
    pub user_prefix: String,
    pub email_domain: String,
    pub action_weights: ActionWeights,
    start_time: Option<Instant>,
}

impl Config {
    pub fn load() -> Result<Self> {
        let cli = CliArgs::parse();
        Self::from_cli(cli)
    }

    pub fn from_cli(cli: CliArgs) -> Result<Self> {
        let mut yaml_config = YamlConfig::default();

        if let Some(ref config_path) = cli.config {
            yaml_config = Self::load_yaml(config_path)?;
        }

        if let Some(ref profile_name) = cli.profile {
            let profile = yaml_config
                .profiles
                .get(profile_name)
                .cloned()
                .ok_or_else(|| anyhow::anyhow!("Profile '{}' not found in configuration", profile_name))?;
            yaml_config = Self::apply_profile(yaml_config, profile);
        }

        let admin_token = cli
            .admin_token
            .or(yaml_config.gitlab.admin_token.clone())
            .context("Admin token is required. Set via --admin-token or GITLAB_QA_ADMIN_ACCESS_TOKEN")?;

        Ok(Self {
            base_url: cli.base_url.unwrap_or(yaml_config.gitlab.base_url),
            admin_token,
            agent_count: cli.agent_count.unwrap_or(yaml_config.execution.agent_count),
            duration_minutes: cli
                .duration_minutes
                .unwrap_or(yaml_config.execution.duration_minutes),
            min_action_delay: cli
                .min_action_delay
                .unwrap_or(yaml_config.timing.min_action_delay),
            max_action_delay: cli
                .max_action_delay
                .unwrap_or(yaml_config.timing.max_action_delay),
            progress_interval: yaml_config.timing.progress_interval,
            request_timeout: yaml_config.timing.request_timeout,
            verbose: cli.verbose || yaml_config.execution.verbose,
            dry_run: cli.dry_run || yaml_config.execution.dry_run,
            namespace_prefix: yaml_config.namespaces.prefix,
            namespace_count: yaml_config.namespaces.count,
            user_prefix: yaml_config.users.prefix,
            email_domain: yaml_config.users.email_domain,
            action_weights: yaml_config.action_weights.unwrap_or_default(),
            start_time: None,
        })
    }

    fn load_yaml(path: &str) -> Result<YamlConfig> {
        let path = Path::new(path);
        let content = fs::read_to_string(path)
            .with_context(|| format!("Failed to read config file: {}", path.display()))?;
        serde_yaml::from_str(&content)
            .with_context(|| format!("Failed to parse config file: {}", path.display()))
    }

    fn apply_profile(mut config: YamlConfig, profile: ProfileConfig) -> YamlConfig {
        if let Some(agent_count) = profile.agent_count {
            config.execution.agent_count = agent_count;
        }
        if let Some(duration_minutes) = profile.duration_minutes {
            config.execution.duration_minutes = duration_minutes;
        }
        if let Some(timing) = profile.timing {
            config.timing = timing;
        }
        if let Some(weights) = profile.action_weights {
            config.action_weights = Some(weights);
        }
        config
    }

    pub fn namespace_path(&self, index: usize) -> String {
        format!("{}-{}", self.namespace_prefix, index)
    }

    pub fn namespace_name(&self, index: usize) -> String {
        format!(
            "{} {}",
            self.namespace_prefix
                .split('-')
                .map(|s| {
                    let mut c = s.chars();
                    match c.next() {
                        None => String::new(),
                        Some(f) => f.to_uppercase().collect::<String>() + c.as_str(),
                    }
                })
                .collect::<Vec<_>>()
                .join(" "),
            index
        )
    }

    pub fn username(&self, index: usize) -> String {
        format!("{}-{:03}", self.user_prefix, index)
    }

    pub fn user_email(&self, index: usize) -> String {
        format!("{}@{}", self.username(index), self.email_domain)
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
        self.action_weights.clone()
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct ActionWeights {
    #[serde(default = "default_create_project_weight")]
    pub create_project: u32,
    #[serde(default = "default_create_file_weight")]
    pub create_file: u32,
    #[serde(default = "default_update_file_weight")]
    pub update_file: u32,
    #[serde(default = "default_create_issue_weight")]
    pub create_issue: u32,
    #[serde(default = "default_close_issue_weight")]
    pub close_issue: u32,
    #[serde(default = "default_link_issues_weight")]
    pub link_issues: u32,
    #[serde(default = "default_create_milestone_weight")]
    pub create_milestone: u32,
    #[serde(default = "default_attach_milestone_weight")]
    pub attach_milestone: u32,
    #[serde(default = "default_create_merge_request_weight")]
    pub create_merge_request: u32,
    #[serde(default = "default_push_to_merge_request_weight")]
    pub push_to_merge_request: u32,
    #[serde(default = "default_comment_on_issue_weight")]
    pub comment_on_issue: u32,
    #[serde(default = "default_comment_on_merge_request_weight")]
    pub comment_on_merge_request: u32,
    #[serde(default = "default_approve_merge_request_weight")]
    pub approve_merge_request: u32,
    #[serde(default = "default_merge_merge_request_weight")]
    pub merge_merge_request: u32,
    #[serde(default = "default_close_merge_request_weight")]
    pub close_merge_request: u32,
    #[serde(default = "default_reply_to_issue_comment_weight")]
    pub reply_to_issue_comment: u32,
    #[serde(default = "default_reply_to_mr_discussion_weight")]
    pub reply_to_mr_discussion: u32,
}

fn default_create_project_weight() -> u32 {
    5
}
fn default_create_file_weight() -> u32 {
    20
}
fn default_update_file_weight() -> u32 {
    15
}
fn default_create_issue_weight() -> u32 {
    15
}
fn default_close_issue_weight() -> u32 {
    10
}
fn default_link_issues_weight() -> u32 {
    5
}
fn default_create_milestone_weight() -> u32 {
    5
}
fn default_attach_milestone_weight() -> u32 {
    5
}
fn default_create_merge_request_weight() -> u32 {
    8
}
fn default_push_to_merge_request_weight() -> u32 {
    10
}
fn default_comment_on_issue_weight() -> u32 {
    5
}
fn default_comment_on_merge_request_weight() -> u32 {
    5
}
fn default_approve_merge_request_weight() -> u32 {
    5
}
fn default_merge_merge_request_weight() -> u32 {
    3
}
fn default_close_merge_request_weight() -> u32 {
    2
}
fn default_reply_to_issue_comment_weight() -> u32 {
    5
}
fn default_reply_to_mr_discussion_weight() -> u32 {
    5
}

impl Default for ActionWeights {
    fn default() -> Self {
        Self {
            create_project: default_create_project_weight(),
            create_file: default_create_file_weight(),
            update_file: default_update_file_weight(),
            create_issue: default_create_issue_weight(),
            close_issue: default_close_issue_weight(),
            link_issues: default_link_issues_weight(),
            create_milestone: default_create_milestone_weight(),
            attach_milestone: default_attach_milestone_weight(),
            create_merge_request: default_create_merge_request_weight(),
            push_to_merge_request: default_push_to_merge_request_weight(),
            comment_on_issue: default_comment_on_issue_weight(),
            comment_on_merge_request: default_comment_on_merge_request_weight(),
            approve_merge_request: default_approve_merge_request_weight(),
            merge_merge_request: default_merge_merge_request_weight(),
            close_merge_request: default_close_merge_request_weight(),
            reply_to_issue_comment: default_reply_to_issue_comment_weight(),
            reply_to_mr_discussion: default_reply_to_mr_discussion_weight(),
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
