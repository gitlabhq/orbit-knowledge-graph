use crate::configuration::ModuleConfiguration;

#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
pub struct SdlcIndexingConfig {
    /// Maximum rows per ClickHouse block in datalake queries.
    /// Controls peak memory via the max_block_size query setting.
    #[serde(default = "default_datalake_batch_size")]
    pub datalake_batch_size: u64,

    #[serde(flatten)]
    pub engine: ModuleConfiguration,
}

fn default_datalake_batch_size() -> u64 {
    1_000_000
}

impl Default for SdlcIndexingConfig {
    fn default() -> Self {
        Self {
            datalake_batch_size: default_datalake_batch_size(),
            engine: ModuleConfiguration {
                dead_letter_enabled: false,
                ..Default::default()
            },
        }
    }
}
