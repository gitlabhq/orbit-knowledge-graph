use std::collections::BTreeMap;
use std::num::NonZeroU32;
use std::time::Duration;

use serde::{Deserialize, Deserializer};

const DEFAULT_MAX_AGE: Duration = Duration::from_secs(24 * 60 * 60);

#[derive(Clone, Debug, Default, PartialEq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EngineConfig {
    #[serde(default)]
    pub lanes: BTreeMap<String, LaneConfig>,
    #[serde(default)]
    pub pools: BTreeMap<String, PoolConfig>,
    #[serde(default)]
    pub planner: bool,
}

#[derive(Clone, Debug, PartialEq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LaneConfig {
    pub slots: NonZeroU32,
    #[serde(deserialize_with = "parse_duration")]
    pub timeout: Duration,
    #[serde(default = "default_max_age", deserialize_with = "parse_duration")]
    pub max_age: Duration,
    #[serde(default)]
    pub on_timeout: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Deserialize)]
#[serde(untagged)]
pub enum PoolConfig {
    Pod(NonZeroU32),
    Cluster(ClusterPoolConfig),
}

#[derive(Clone, Debug, PartialEq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClusterPoolConfig {
    pub cluster: bool,
    pub per_key: NonZeroU32,
    pub unknown_key: NonZeroU32,
    #[serde(deserialize_with = "parse_duration")]
    pub max_wait: Duration,
}

impl EngineConfig {
    pub fn with_lane(mut self, lane: impl Into<String>, config: LaneConfig) -> Self {
        self.lanes.insert(lane.into(), config);
        self
    }

    pub fn with_pool(mut self, pool: impl Into<String>, config: PoolConfig) -> Self {
        self.pools.insert(pool.into(), config);
        self
    }
}

impl LaneConfig {
    pub fn new(slots: NonZeroU32, timeout: Duration) -> Self {
        Self {
            slots,
            timeout,
            max_age: DEFAULT_MAX_AGE,
            on_timeout: None,
        }
    }

    pub fn on_timeout(mut self, lane: impl Into<String>) -> Self {
        self.on_timeout = Some(lane.into());
        self
    }
}

fn default_max_age() -> Duration {
    DEFAULT_MAX_AGE
}

fn parse_duration<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Duration, D::Error> {
    let text = String::deserialize(deserializer)?;
    let unit_start = text
        .find(|c: char| !c.is_ascii_digit())
        .ok_or_else(|| serde::de::Error::custom(format!("duration {text:?} has no unit")))?;
    let (amount, unit) = text.split_at(unit_start);
    let amount: u64 = amount
        .parse()
        .map_err(|_| serde::de::Error::custom(format!("duration {text:?} has no amount")))?;
    let seconds = match unit {
        "ms" => return Ok(Duration::from_millis(amount)),
        "s" => amount,
        "m" => amount * 60,
        "h" => amount * 60 * 60,
        "d" => amount * 24 * 60 * 60,
        _ => {
            return Err(serde::de::Error::custom(format!(
                "duration {text:?} must end in ms, s, m, h or d"
            )));
        }
    };
    Ok(Duration::from_secs(seconds))
}

pub(crate) fn format_duration(duration: Duration) -> String {
    const UNITS: [(u64, &str); 4] = [(24 * 60 * 60, "d"), (60 * 60, "h"), (60, "m"), (1, "s")];
    if duration.subsec_millis() != 0 {
        return format!("{}ms", duration.as_millis());
    }
    let mut left = duration.as_secs();
    let mut text = String::new();
    for (size, unit) in UNITS {
        if left >= size {
            text.push_str(&format!("{}{unit}", left / size));
            left %= size;
        }
    }
    if text.is_empty() {
        text.push_str("0s");
    }
    text
}
