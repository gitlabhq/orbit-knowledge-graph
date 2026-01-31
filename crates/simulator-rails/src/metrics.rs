use serde::Serialize;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

// =============================================================================
// Dry-Run Action Log
// =============================================================================

#[derive(Debug, Clone, Serialize)]
pub struct DryRunAction {
    pub timestamp: String,
    pub agent_id: usize,
    pub action_type: String,
    pub description: String,
    pub api_endpoints: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct DryRunReport {
    pub total_actions: usize,
    pub actions_by_type: HashMap<String, usize>,
    pub api_endpoints_called: HashMap<String, usize>,
    pub agent_activity: HashMap<usize, usize>,
    pub action_log: Vec<DryRunAction>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ActionMetrics {
    pub success_count: u64,
    pub failure_count: u64,
    pub total_count: u64,
    pub avg_duration_ms: f64,
    pub min_duration_ms: f64,
    pub max_duration_ms: f64,
    pub p95_duration_ms: f64,
}

#[derive(Debug, Clone, Serialize)]
pub struct ErrorSample {
    pub action: String,
    pub error: String,
    pub timestamp: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct MetricsSummary {
    pub total_duration_seconds: f64,
    pub total_requests: u64,
    pub total_successes: u64,
    pub total_failures: u64,
    pub success_rate: f64,
    pub actions: HashMap<String, ActionMetrics>,
    pub error_sample: Vec<ErrorSample>,
}

#[derive(Debug)]
struct ActionData {
    success: u64,
    failure: u64,
    durations: Vec<f64>,
}

impl Default for ActionData {
    fn default() -> Self {
        Self {
            success: 0,
            failure: 0,
            durations: Vec::new(),
        }
    }
}

#[derive(Debug)]
struct MetricsInner {
    start_time: Option<Instant>,
    actions: HashMap<String, ActionData>,
    errors: Vec<ErrorSample>,
    dry_run_actions: Vec<DryRunAction>,
}

#[derive(Clone)]
pub struct MetricsCollector {
    inner: Arc<Mutex<MetricsInner>>,
}

impl MetricsCollector {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(MetricsInner {
                start_time: None,
                actions: HashMap::new(),
                errors: Vec::new(),
                dry_run_actions: Vec::new(),
            })),
        }
    }

    pub fn start(&self) {
        let mut inner = self.inner.lock().unwrap();
        inner.start_time = Some(Instant::now());
    }

    pub fn record_success(&self, action_type: &str, duration: Duration) {
        let mut inner = self.inner.lock().unwrap();
        let data = inner
            .actions
            .entry(action_type.to_string())
            .or_insert_with(ActionData::default);
        data.success += 1;
        data.durations.push(duration.as_secs_f64());
    }

    pub fn record_failure(&self, action_type: &str, error: &str, duration: Duration) {
        let mut inner = self.inner.lock().unwrap();
        let data = inner
            .actions
            .entry(action_type.to_string())
            .or_insert_with(ActionData::default);
        data.failure += 1;
        if duration.as_secs_f64() > 0.0 {
            data.durations.push(duration.as_secs_f64());
        }
        inner.errors.push(ErrorSample {
            action: action_type.to_string(),
            error: error.to_string(),
            timestamp: chrono::Utc::now().to_rfc3339(),
        });
    }

    pub fn record_dry_run_action(
        &self,
        agent_id: usize,
        action_type: &str,
        description: &str,
        api_endpoints: Vec<String>,
    ) {
        let mut inner = self.inner.lock().unwrap();
        inner.dry_run_actions.push(DryRunAction {
            timestamp: chrono::Utc::now().to_rfc3339(),
            agent_id,
            action_type: action_type.to_string(),
            description: description.to_string(),
            api_endpoints,
        });
    }

    pub fn dry_run_report(&self) -> DryRunReport {
        let inner = self.inner.lock().unwrap();

        let mut actions_by_type: HashMap<String, usize> = HashMap::new();
        let mut api_endpoints_called: HashMap<String, usize> = HashMap::new();
        let mut agent_activity: HashMap<usize, usize> = HashMap::new();

        for action in &inner.dry_run_actions {
            *actions_by_type.entry(action.action_type.clone()).or_insert(0) += 1;
            *agent_activity.entry(action.agent_id).or_insert(0) += 1;

            for endpoint in &action.api_endpoints {
                // Normalize endpoint (remove IDs) for grouping
                let normalized = normalize_endpoint(endpoint);
                *api_endpoints_called.entry(normalized).or_insert(0) += 1;
            }
        }

        DryRunReport {
            total_actions: inner.dry_run_actions.len(),
            actions_by_type,
            api_endpoints_called,
            agent_activity,
            action_log: inner.dry_run_actions.clone(),
        }
    }

    pub fn summary(&self) -> MetricsSummary {
        let inner = self.inner.lock().unwrap();

        let total_duration = inner
            .start_time
            .map(|start| start.elapsed().as_secs_f64())
            .unwrap_or(0.0);

        let mut actions = HashMap::new();
        let mut total_requests = 0u64;
        let mut total_successes = 0u64;
        let mut total_failures = 0u64;

        for (action_type, data) in &inner.actions {
            let total = data.success + data.failure;
            total_requests += total;
            total_successes += data.success;
            total_failures += data.failure;

            let (avg, min, max, p95) = if data.durations.is_empty() {
                (0.0, 0.0, 0.0, 0.0)
            } else {
                let mut sorted = data.durations.clone();
                sorted.sort_by(|a, b| a.partial_cmp(b).unwrap());

                let avg = sorted.iter().sum::<f64>() / sorted.len() as f64;
                let min = *sorted.first().unwrap();
                let max = *sorted.last().unwrap();
                let p95_idx = ((sorted.len() as f64) * 0.95).ceil() as usize;
                let p95 = sorted[p95_idx.min(sorted.len() - 1)];

                (avg * 1000.0, min * 1000.0, max * 1000.0, p95 * 1000.0)
            };

            actions.insert(
                action_type.clone(),
                ActionMetrics {
                    success_count: data.success,
                    failure_count: data.failure,
                    total_count: total,
                    avg_duration_ms: (avg * 100.0).round() / 100.0,
                    min_duration_ms: (min * 100.0).round() / 100.0,
                    max_duration_ms: (max * 100.0).round() / 100.0,
                    p95_duration_ms: (p95 * 100.0).round() / 100.0,
                },
            );
        }

        let success_rate = if total_requests > 0 {
            (total_successes as f64 / total_requests as f64) * 100.0
        } else {
            0.0
        };

        let error_sample: Vec<ErrorSample> = inner.errors.iter().rev().take(10).cloned().collect();

        MetricsSummary {
            total_duration_seconds: (total_duration * 100.0).round() / 100.0,
            total_requests,
            total_successes,
            total_failures,
            success_rate: (success_rate * 100.0).round() / 100.0,
            actions,
            error_sample,
        }
    }

    pub fn to_json(&self) -> String {
        serde_json::to_string_pretty(&self.summary()).unwrap_or_default()
    }

    pub fn dry_run_report_json(&self) -> String {
        serde_json::to_string_pretty(&self.dry_run_report()).unwrap_or_default()
    }
}

impl Default for MetricsCollector {
    fn default() -> Self {
        Self::new()
    }
}

/// Normalize API endpoints by replacing numeric IDs with placeholders
/// e.g., "/projects/123/issues/456" -> "/projects/:id/issues/:id"
fn normalize_endpoint(endpoint: &str) -> String {
    endpoint
        .split('/')
        .map(|part| {
            if part.parse::<u64>().is_ok() {
                ":id"
            } else {
                part
            }
        })
        .collect::<Vec<_>>()
        .join("/")
}
