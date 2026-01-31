use crate::agent::Agent;
use crate::api_client::{ApiClient, Group, Member, PersonalAccessToken, User};
use crate::config::Config;
use crate::data_generator::DataGenerator;
use crate::metrics::MetricsCollector;
use crate::shared_state::SharedStateRegistry;
use anyhow::Result;
use serde_json::json;
use std::fs;
use std::sync::Arc;
use tokio::task::JoinHandle;
use tracing::{error, info, warn};

pub struct Orchestrator {
    config: Arc<Config>,
    metrics: MetricsCollector,
    admin_client: ApiClient,
    namespaces: Vec<Group>,
    shared_states: SharedStateRegistry,
}

impl Orchestrator {
    pub fn new(mut config: Config) -> Result<Self> {
        config.start();
        let admin_client = ApiClient::with_options(
            &config.base_url,
            &config.admin_token,
            config.dry_run,
            config.request_timeout,
        )?;

        Ok(Self {
            config: Arc::new(config),
            metrics: MetricsCollector::new(),
            admin_client,
            namespaces: Vec::new(),
            shared_states: SharedStateRegistry::new(),
        })
    }

    pub async fn run(&mut self) -> Result<()> {
        self.setup().await?;
        self.execute().await?;
        self.report();
        Ok(())
    }

    async fn setup(&mut self) -> Result<()> {
        info!("=== Load Test Setup ===");
        info!(
            "Configuration: {} agents, {} minutes",
            self.config.agent_count, self.config.duration_minutes
        );
        info!("Target: {}", self.config.base_url);

        self.create_namespaces().await?;
        Ok(())
    }

    async fn create_namespaces(&mut self) -> Result<()> {
        info!(
            "Finding or creating {} namespaces...",
            self.config.namespace_count
        );

        for i in 1..=self.config.namespace_count {
            let path = self.config.namespace_path(i);
            let name = self.config.namespace_name(i);

            let namespace = self.find_or_create_namespace(&path, &name).await?;
            self.enable_knowledge_graph(namespace.id).await?;
            info!(
                "Namespace ready: {} (id: {}, KG enabled)",
                namespace.path, namespace.id
            );
            self.namespaces.push(namespace);
        }

        Ok(())
    }

    async fn enable_knowledge_graph(&self, namespace_id: u64) -> Result<()> {
        let (status, _) = self
            .admin_client
            .put_with_status(
                &format!("/admin/knowledge_graph/namespaces/{}", namespace_id),
                &json!({}),
            )
            .await?;

        if !status.is_success() && status.as_u16() != 409 {
            warn!(
                "Failed to enable Knowledge Graph for namespace {}: {}",
                namespace_id, status
            );
        }

        Ok(())
    }

    async fn find_or_create_namespace(&self, path: &str, name: &str) -> Result<Group> {
        // Try to find existing namespace
        let groups: Vec<Group> = self
            .admin_client
            .get(&format!("/groups?search={}", path))
            .await?;

        if let Some(group) = groups.into_iter().find(|g| g.path == path) {
            return Ok(group);
        }

        // Create new namespace
        let group: Group = self
            .admin_client
            .post(
                "/groups",
                &json!({
                    "path": path,
                    "name": name,
                    "visibility": "public"
                }),
            )
            .await?;

        Ok(group)
    }

    async fn find_or_create_user(&self, index: usize) -> Result<(User, String)> {
        // Generate random legible username and full name
        let username = DataGenerator::random_username(index);
        let full_name = DataGenerator::random_full_name();
        let email = DataGenerator::random_email(&username);

        // Always create a new user with random name (no reuse for random names)
        let password = format!("Xy9#{}Zk!", uuid::Uuid::new_v4().simple());
        let user: User = self
            .admin_client
            .post(
                "/users",
                &json!({
                    "username": username,
                    "email": email,
                    "password": password,
                    "name": full_name,
                    "skip_confirmation": true
                }),
            )
            .await?;

        // Create PAT for the user
        let token: PersonalAccessToken = self
            .admin_client
            .post(
                &format!("/users/{}/personal_access_tokens", user.id),
                &json!({
                    "name": format!("load-test-token-{}", uuid::Uuid::new_v4()),
                    "scopes": ["api", "write_repository"],
                    "expires_at": (chrono::Utc::now() + chrono::Duration::days(7)).format("%Y-%m-%d").to_string()
                }),
            )
            .await?;

        Ok((user, token.token))
    }

    async fn ensure_membership(&self, namespace_id: u64, user_id: u64) -> Result<()> {
        // Check if already a member
        let member: Option<Member> = self
            .admin_client
            .get_optional(&format!("/groups/{}/members/{}", namespace_id, user_id))
            .await?;

        if member.is_some() {
            return Ok(());
        }

        // Add as maintainer (access_level = 50) to allow pushing to protected branches
        let (status, _) = self
            .admin_client
            .post_with_status(
                &format!("/groups/{}/members", namespace_id),
                &json!({
                    "user_id": user_id,
                    "access_level": 50
                }),
            )
            .await?;

        if !status.is_success() && status.as_u16() != 409 {
            warn!("Failed to add user {} to namespace {}", user_id, namespace_id);
        }

        Ok(())
    }

    async fn execute(&mut self) -> Result<()> {
        info!("=== Starting Load Test ===");
        info!("Running until {:?}", self.config.end_time());

        self.metrics.start();

        let mut handles: Vec<JoinHandle<()>> = Vec::new();

        for i in 1..=self.config.agent_count {
            let (user, token) = self.find_or_create_user(i).await?;

            // Add user only to their assigned namespace
            let namespace = &self.namespaces[(i - 1) % self.namespaces.len()];
            self.ensure_membership(namespace.id, user.id).await?;

            let client = ApiClient::with_options(
                &self.config.base_url,
                &token,
                self.config.dry_run,
                self.config.request_timeout,
            )?;
            // Get the SharedState for this agent's namespace
            let shared_state = self.shared_states.get(namespace.id);
            let mut agent = Agent::new(
                i,
                client,
                self.config.clone(),
                user.id,
                namespace.id,
                shared_state,
                self.metrics.clone(),
            );

            let handle = tokio::spawn(async move {
                agent.run().await;
            });

            handles.push(handle);

            if i % 10 == 0 || i == self.config.agent_count {
                info!("Created agent {}/{}", i, self.config.agent_count);
            }
        }

        // Progress reporting task
        let metrics_clone = self.metrics.clone();
        let config_clone = self.config.clone();
        let progress_handle = tokio::spawn(async move {
            loop {
                tokio::time::sleep(std::time::Duration::from_secs(60)).await;

                if !config_clone.is_running() {
                    break;
                }

                let remaining = config_clone.remaining().as_secs() / 60;
                let summary = metrics_clone.summary();
                info!(
                    "[Progress] {}min remaining | Requests: {} | Success: {:.1}%",
                    remaining, summary.total_requests, summary.success_rate
                );
            }
        });

        // Wait for all agents to complete
        for handle in handles {
            if let Err(e) = handle.await {
                error!("Agent task failed: {:?}", e);
            }
        }

        progress_handle.abort();
        info!("=== Load Test Complete ===");

        Ok(())
    }

    fn report(&self) {
        if self.config.dry_run {
            self.dry_run_report();
        } else {
            self.live_report();
        }
    }

    fn dry_run_report(&self) {
        let report = self.metrics.dry_run_report();

        println!();
        println!("=== DRY-RUN REPORT ===");
        println!("(No actual API calls were made)");
        println!();
        println!("Total Actions Simulated: {}", report.total_actions);
        println!();

        println!("--- Actions by Type ---");
        let mut actions: Vec<_> = report.actions_by_type.iter().collect();
        actions.sort_by_key(|(name, _)| name.as_str());
        for (action, count) in actions {
            println!("  {:<30} : {:>5}", action, count);
        }

        println!();
        println!("--- API Endpoints Required ---");
        let mut endpoints: Vec<_> = report.api_endpoints_called.iter().collect();
        endpoints.sort_by(|a, b| b.1.cmp(a.1)); // Sort by count descending
        for (endpoint, count) in endpoints.iter().take(20) {
            println!("  {:>5}x  {}", count, endpoint);
        }

        println!();
        println!("--- Agent Activity ---");
        println!("  Agents active: {}", report.agent_activity.len());
        let total: usize = report.agent_activity.values().sum();
        let avg = if !report.agent_activity.is_empty() {
            total / report.agent_activity.len()
        } else {
            0
        };
        println!("  Avg actions per agent: {}", avg);

        println!();
        println!("--- Sample Actions (last 10) ---");
        for action in report.action_log.iter().rev().take(10) {
            println!(
                "  [Agent {:>3}] {} - {}",
                action.agent_id, action.action_type, action.description
            );
        }

        // Save report to file
        let report_file = format!(
            "dry_run_report_{}.json",
            chrono::Utc::now().format("%Y%m%d_%H%M%S")
        );
        if let Err(e) = fs::write(&report_file, self.metrics.dry_run_report_json()) {
            error!("Failed to save report: {}", e);
        } else {
            println!();
            println!("Detailed dry-run report saved to: {}", report_file);
        }
    }

    fn live_report(&self) {
        let summary = self.metrics.summary();

        println!();
        println!("=== GitLab Simulator TEST RESULTS ===");
        println!("Duration: {:.1}s", summary.total_duration_seconds);
        println!("Total Requests: {}", summary.total_requests);
        println!("Success Rate: {:.1}%", summary.success_rate);
        println!();
        println!("--- By Action Type ---");

        let mut actions: Vec<_> = summary.actions.iter().collect();
        actions.sort_by_key(|(name, _)| name.as_str());

        for (action, data) in actions {
            println!(
                "{:<30} | Total: {:>5} | Success: {:>5} | Fail: {:>5} | Avg: {:>7.1}ms | P95: {:>7.1}ms",
                action,
                data.total_count,
                data.success_count,
                data.failure_count,
                data.avg_duration_ms,
                data.p95_duration_ms
            );
        }

        if !summary.error_sample.is_empty() {
            println!();
            println!("--- Recent Errors (last 10) ---");
            for error in &summary.error_sample {
                println!("{}: {}", error.action, error.error);
            }
        }

        // Save report to file
        let report_file = format!(
            "load_test_report_{}.json",
            chrono::Utc::now().format("%Y%m%d_%H%M%S")
        );
        if let Err(e) = fs::write(&report_file, self.metrics.to_json()) {
            error!("Failed to save report: {}", e);
        } else {
            println!();
            println!("Detailed report saved to: {}", report_file);
        }
    }
}
