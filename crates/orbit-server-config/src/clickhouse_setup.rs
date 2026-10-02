use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Inputs of `--mode clickhouse-setup`. Each password is a secret file under
/// `/etc/secrets/clickhouse_setup/`.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize, JsonSchema)]
#[schemars(deny_unknown_fields)]
pub struct ClickHouseSetupConfig {
    pub admin_username: String,
    pub admin_password: Option<String>,
    pub writer_password: Option<String>,
    pub reader_password: Option<String>,
    pub siphon_reader_password: Option<String>,
}
