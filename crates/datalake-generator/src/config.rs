use std::collections::HashMap;
use std::path::Path;

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SimulatorConfig {
    pub datalake: ClickHouseConfig,
    pub generation: GenerationConfig,
    #[serde(default)]
    pub continuous: ContinuousConfig,
    #[serde(default)]
    pub metrics: MetricsConfig,
    #[serde(default)]
    pub state: StateConfig,
}

impl SimulatorConfig {
    pub fn load(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        let contents = std::fs::read_to_string(path)
            .with_context(|| format!("failed to read config: {}", path.display()))?;
        let config: Self = serde_yaml::from_str(&contents)
            .with_context(|| format!("failed to parse config: {}", path.display()))?;
        Ok(config)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ClickHouseConfig {
    pub url: String,
    pub database: String,
    pub username: String,
    pub password: Option<String>,
}

impl Default for ClickHouseConfig {
    fn default() -> Self {
        Self {
            url: "http://localhost:8123".to_string(),
            database: "default".to_string(),
            username: "default".to_string(),
            password: None,
        }
    }
}

impl ClickHouseConfig {
    pub fn build_client(&self) -> clickhouse_client::ArrowClickHouseClient {
        clickhouse_client::ArrowClickHouseClient::new(
            &self.url,
            &self.database,
            &self.username,
            self.password.as_deref(),
        )
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct GenerationConfig {
    pub seed: u64,
    pub batch_size: usize,
    pub organizations: usize,
    pub users: usize,
    pub groups: usize,
    pub subgroups: SubgroupConfig,
    pub per_group: PerGroupConfig,
    pub per_project: PerProjectConfig,
    pub field_overrides: HashMap<String, HashMap<String, Vec<serde_json::Value>>>,
}

impl Default for GenerationConfig {
    fn default() -> Self {
        Self {
            seed: 42,
            batch_size: 1_000_000,
            organizations: 1,
            users: 100,
            groups: 5,
            subgroups: SubgroupConfig::default(),
            per_group: PerGroupConfig::default(),
            per_project: PerProjectConfig::default(),
            field_overrides: HashMap::new(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct PerGroupConfig {
    pub projects: usize,
    pub members: usize,
}

impl Default for PerGroupConfig {
    fn default() -> Self {
        Self {
            projects: 3,
            members: 3,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct PerProjectConfig {
    pub merge_requests: usize,
    pub merge_request_diffs: usize,
    pub merge_request_diff_files: usize,
    pub work_items: usize,
    pub milestones: usize,
    pub labels: usize,
    pub notes: usize,
    pub pipelines: usize,
    pub stages: usize,
    pub jobs: usize,
    pub vulnerabilities: usize,
    pub security_scans: usize,
    pub security_findings: usize,
    pub members: usize,
}

impl Default for PerProjectConfig {
    fn default() -> Self {
        Self {
            merge_requests: 10,
            merge_request_diffs: 20,
            merge_request_diff_files: 60,
            work_items: 5,
            milestones: 2,
            labels: 3,
            notes: 25,
            pipelines: 3,
            stages: 9,
            jobs: 18,
            vulnerabilities: 2,
            security_scans: 3,
            security_findings: 6,
            members: 2,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct SubgroupConfig {
    pub max_depth: usize,
    pub per_group: usize,
}

impl Default for SubgroupConfig {
    fn default() -> Self {
        Self {
            max_depth: 3,
            per_group: 2,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ContinuousConfig {
    pub enabled: bool,
    pub cycles: usize,
    pub cycle_interval_secs: u64,
    pub inserts_per_cycle: HashMap<String, usize>,
    pub updates_per_cycle: HashMap<String, usize>,
    pub deletes_per_cycle: HashMap<String, usize>,
}

impl Default for ContinuousConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            cycles: 10,
            cycle_interval_secs: 5,
            inserts_per_cycle: HashMap::new(),
            updates_per_cycle: HashMap::new(),
            deletes_per_cycle: HashMap::new(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct MetricsConfig {
    pub enabled: bool,
    pub output_path: String,
}

impl Default for MetricsConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            output_path: "datalake-generator-report.json".to_string(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct StateConfig {
    pub dir: String,
}

impl Default for StateConfig {
    fn default() -> Self {
        Self {
            dir: "datalake-generator-state".to_string(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_config_roundtrips() {
        let yaml = r#"
datalake:
  url: http://localhost:8123
  database: datalake_test
generation:
  seed: 42
  batch_size: 100000
  organizations: 1
  users: 1000
  groups: 10
  subgroups:
    max_depth: 3
    per_group: 2
  per_group:
    projects: 5
  per_project:
    merge_requests: 20
"#;
        let config: SimulatorConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(config.datalake.database, "datalake_test");
        assert_eq!(config.generation.seed, 42);
        assert_eq!(config.generation.users, 1000);
        assert_eq!(config.generation.groups, 10);
        assert_eq!(config.generation.per_group.projects, 5);
        assert_eq!(config.generation.per_project.merge_requests, 20);
    }
}
