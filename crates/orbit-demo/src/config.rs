//! The demo's deployment config. Startup refuses any config with an endpoint that is not loopback.

use std::net::SocketAddr;
use std::path::{Path, PathBuf};

use anyhow::{Context, bail};
use serde::Deserialize;

use crate::lockdown::{BIND, Resolve, loopback};

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub page_port: u16,
    pub clickhouse: ClickHouse,
    pub providers: Providers,
    /// The seed directory, relative to the config file.
    pub seed: PathBuf,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClickHouse {
    pub url: String,
    pub database: String,
    pub reader: Login,
    pub seeder: Login,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Login {
    pub user: String,
    pub password: String,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Providers {
    pub orbit_next: String,
    pub slack: String,
}

/// A config whose every endpoint passed the loopback check, with the address each connects to.
#[derive(Debug, Clone)]
pub struct Checked {
    pub config: Config,
    pub clickhouse: SocketAddr,
    pub orbit_next: SocketAddr,
    pub slack: SocketAddr,
}

impl Config {
    pub fn read(path: &Path) -> anyhow::Result<Self> {
        let text =
            std::fs::read_to_string(path).with_context(|| format!("reading {}", path.display()))?;
        let mut config: Self = orbit_utils::yaml::from_str(&text)
            .with_context(|| format!("parsing {}", path.display()))?;
        let base = path.parent().unwrap_or(Path::new("."));
        config.seed = base.join(&config.seed);
        Ok(config)
    }

    pub fn check(self, resolve: &Resolve) -> anyhow::Result<Checked> {
        let mut errors = Vec::new();
        let mut check = |endpoint: &str, served: bool| match loopback(endpoint, resolve) {
            Ok(address) if served && address.ip() != BIND => {
                errors.push(format!(
                    "{endpoint}: the demo serves its fakes on {BIND} only"
                ));
                None
            }
            Ok(address) => Some(address),
            Err(error) => {
                errors.push(error.to_string());
                None
            }
        };
        let clickhouse = check(&self.clickhouse.url, false);
        let orbit_next = check(&self.providers.orbit_next, true);
        let slack = check(&self.providers.slack, true);
        match (clickhouse, orbit_next, slack) {
            (Some(clickhouse), Some(orbit_next), Some(slack)) if errors.is_empty() => Ok(Checked {
                config: self,
                clickhouse,
                orbit_next,
                slack,
            }),
            _ => bail!("the demo refuses to start: {}", errors.join("; ")),
        }
    }
}
