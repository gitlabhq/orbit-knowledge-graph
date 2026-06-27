use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::Path;
use std::time::Duration;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryMetadata {
    pub query_name: String,
    pub executed_at: DateTime<Utc>,
    pub original_query: serde_json::Value,
    pub substituted_query: serde_json::Value,
    pub sql: String,
    pub final_sql: String,
    pub params: HashMap<String, serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub query_plan: Option<QueryPlan>,
    pub runtime: RuntimeStats,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sample_data: Option<SampleData>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<ErrorInfo>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryPlan {
    pub explain_text: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pipeline: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub estimated_rows: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RuntimeStats {
    pub execution_time_ms: f64,
    pub success: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub row_count: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub memory_bytes: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rows_read: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bytes_read: Option<u64>,
}

impl RuntimeStats {
    pub fn from_duration(duration: Duration, success: bool, row_count: Option<u64>) -> Self {
        Self {
            execution_time_ms: duration.as_secs_f64() * 1000.0,
            success,
            row_count,
            memory_bytes: None,
            rows_read: None,
            bytes_read: None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SampleData {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<serde_json::Value>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ErrorInfo {
    pub message: String,
    pub category: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub code: Option<u32>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunMetadata {
    pub run_id: String,
    pub started_at: DateTime<Utc>,
    pub completed_at: DateTime<Utc>,
    pub config: RunConfig,
    pub queries: Vec<QueryMetadata>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunConfig {
    /// Sanitized, no credentials.
    pub clickhouse_url: String,
    pub iterations: usize,
    pub sample_size: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub filter: Option<String>,
}

impl RunMetadata {
    pub fn new(config: RunConfig) -> Self {
        let now = Utc::now();
        let run_id = format!("run_{}", now.format("%Y%m%d_%H%M%S"));
        Self {
            run_id,
            started_at: now,
            completed_at: now,
            config,
            queries: Vec::new(),
        }
    }

    pub fn complete(&mut self) {
        self.completed_at = Utc::now();
    }

    pub fn add_query(&mut self, metadata: QueryMetadata) {
        self.queries.push(metadata);
    }

    pub fn save_to_dir(&self, dir: &Path) -> anyhow::Result<()> {
        std::fs::create_dir_all(dir)?;

        let filename = format!("{}.json", self.run_id);
        let path = dir.join(filename);

        let json = serde_json::to_string_pretty(self)?;
        std::fs::write(&path, json)?;

        tracing::info!("Saved run metadata to {:?}", path);
        Ok(())
    }
}

#[derive(Debug, Default)]
pub struct QueryMetadataBuilder {
    query_name: String,
    original_query: Option<serde_json::Value>,
    substituted_query: Option<serde_json::Value>,
    sql: Option<String>,
    final_sql: Option<String>,
    params: HashMap<String, serde_json::Value>,
    query_plan: Option<QueryPlan>,
    execution_time: Option<Duration>,
    success: bool,
    row_count: Option<u64>,
    sample_data: Option<SampleData>,
    error: Option<ErrorInfo>,
}

impl QueryMetadataBuilder {
    pub fn new(query_name: impl Into<String>) -> Self {
        Self {
            query_name: query_name.into(),
            ..Default::default()
        }
    }

    pub fn original_query(mut self, query: serde_json::Value) -> Self {
        self.original_query = Some(query);
        self
    }

    pub fn substituted_query(mut self, query: serde_json::Value) -> Self {
        self.substituted_query = Some(query);
        self
    }

    pub fn sql(mut self, sql: impl Into<String>) -> Self {
        self.sql = Some(sql.into());
        self
    }

    pub fn final_sql(mut self, sql: impl Into<String>) -> Self {
        self.final_sql = Some(sql.into());
        self
    }

    pub fn params(mut self, params: HashMap<String, serde_json::Value>) -> Self {
        self.params = params;
        self
    }

    pub fn query_plan(mut self, plan: QueryPlan) -> Self {
        self.query_plan = Some(plan);
        self
    }

    pub fn execution_time(mut self, duration: Duration) -> Self {
        self.execution_time = Some(duration);
        self
    }

    pub fn success(mut self, row_count: u64) -> Self {
        self.success = true;
        self.row_count = Some(row_count);
        self
    }

    pub fn failure(mut self, error: ErrorInfo) -> Self {
        self.success = false;
        self.error = Some(error);
        self
    }

    pub fn sample_data(mut self, data: SampleData) -> Self {
        self.sample_data = Some(data);
        self
    }

    pub fn build(self) -> QueryMetadata {
        let runtime = RuntimeStats::from_duration(
            self.execution_time.unwrap_or_default(),
            self.success,
            self.row_count,
        );

        QueryMetadata {
            query_name: self.query_name,
            executed_at: Utc::now(),
            original_query: self.original_query.unwrap_or(serde_json::Value::Null),
            substituted_query: self.substituted_query.unwrap_or(serde_json::Value::Null),
            sql: self.sql.unwrap_or_default(),
            final_sql: self.final_sql.unwrap_or_default(),
            params: self.params,
            query_plan: self.query_plan,
            runtime,
            sample_data: self.sample_data,
            error: self.error,
        }
    }
}
