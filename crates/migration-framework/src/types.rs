use anyhow::Result;
use async_trait::async_trait;
use clickhouse_client::ArrowClickHouseClient;
use serde::{Deserialize, Serialize};

#[async_trait]
pub trait Migration: Send + Sync {
    fn version(&self) -> u64;
    fn name(&self) -> &str;
    fn migration_type(&self) -> MigrationType;
    async fn prepare(&self, ctx: &MigrationContext) -> Result<()>;
}

#[derive(Clone, Debug)]
pub struct MigrationContext {
    clickhouse: ArrowClickHouseClient,
}

impl MigrationContext {
    pub fn new(clickhouse: ArrowClickHouseClient) -> Self {
        Self { clickhouse }
    }

    pub fn clickhouse(&self) -> &ArrowClickHouseClient {
        &self.clickhouse
    }

    pub async fn execute_ddl(&self, sql: &str) -> Result<()> {
        self.clickhouse.execute(sql).await?;
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum MigrationType {
    Additive,
    Convergent,
    Finalization,
}

impl MigrationType {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Additive => "additive",
            Self::Convergent => "convergent",
            Self::Finalization => "finalization",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum MigrationStatus {
    Pending,
    Preparing,
    Completed,
    Failed,
}

impl MigrationStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Preparing => "preparing",
            Self::Completed => "completed",
            Self::Failed => "failed",
        }
    }
}
