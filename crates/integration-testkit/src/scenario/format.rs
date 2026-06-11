use std::collections::BTreeMap;

use serde::Deserialize;

pub type Row = BTreeMap<String, serde_yaml::Value>;
pub type Seed = BTreeMap<String, Vec<Row>>;
pub type SeedSettings = BTreeMap<String, serde_yaml::Value>;
pub type RowMatcher = BTreeMap<String, Matcher>;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Scenario {
    #[serde(default)]
    pub description: Option<String>,
    #[serde(default)]
    pub scope: Option<Scope>,
    #[serde(default)]
    pub seed: Option<Seed>,
    #[serde(default)]
    pub seed_settings: SeedSettings,
    #[serde(default)]
    pub run: Option<RunSpec>,
    #[serde(default)]
    pub expect: Option<Expect>,
    #[serde(default)]
    pub steps: Vec<Step>,
}

impl Scenario {
    /// Normalize the single-step sugar (top-level seed/run/expect) and the
    /// explicit `steps:` form into one list of steps.
    pub fn into_steps(self) -> Vec<Step> {
        let has_inline = self.seed.is_some()
            || !self.seed_settings.is_empty()
            || self.run.is_some()
            || self.expect.is_some();
        match (has_inline, self.steps.is_empty()) {
            (true, true) => vec![Step {
                seed: self.seed.unwrap_or_default(),
                seed_settings: self.seed_settings,
                run: self.run,
                expect: self.expect,
            }],
            (false, false) => self.steps,
            (true, false) => {
                panic!("scenario declares both top-level seed/seed_settings/run/expect and steps:")
            }
            (false, true) => {
                panic!("scenario declares neither seed/seed_settings/run/expect nor steps:")
            }
        }
    }
}

#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Scope {
    #[serde(default = "default_organization")]
    pub organization: i64,
    pub namespace: i64,
}

fn default_organization() -> i64 {
    1
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Step {
    #[serde(default)]
    pub seed: Seed,
    /// ClickHouse INSERT settings applied to every seed insert in this step,
    /// e.g. `date_time_overflow_behavior: ignore`. Their presence switches the
    /// inserts from VALUES to JSONEachRow, where such settings take effect.
    #[serde(default)]
    pub seed_settings: SeedSettings,
    #[serde(default)]
    pub run: Option<RunSpec>,
    #[serde(default)]
    pub expect: Option<Expect>,
}

impl Step {
    pub fn handlers(&self) -> Vec<&str> {
        match &self.run {
            None => vec!["namespace"],
            Some(RunSpec::One(h)) => vec![h.as_str()],
            Some(RunSpec::Many(hs)) => hs.iter().map(String::as_str).collect(),
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
pub enum RunSpec {
    One(String),
    Many(Vec<String>),
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Expect {
    #[serde(default)]
    pub nodes: BTreeMap<String, NodeExpect>,
    #[serde(default)]
    pub edges: Vec<EdgeExpect>,
    #[serde(default)]
    pub totals: BTreeMap<String, usize>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NodeExpect {
    #[serde(default)]
    pub count: Option<usize>,
    #[serde(default)]
    pub rows: Vec<RowMatcher>,
}

impl NodeExpect {
    pub fn expected_count(&self) -> Option<usize> {
        self.count
            .or((!self.rows.is_empty()).then_some(self.rows.len()))
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EdgeExpect {
    pub kind: String,
    #[serde(default)]
    pub from: Option<String>,
    #[serde(default)]
    pub to: Option<String>,
    #[serde(default)]
    pub traversal_path: Option<String>,
    #[serde(default)]
    pub count: Option<usize>,
    #[serde(default)]
    pub rows: Vec<RowMatcher>,
    #[serde(default)]
    pub source_tags: BTreeMap<i64, Vec<String>>,
    #[serde(default)]
    pub target_tags: BTreeMap<i64, Vec<String>>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ContainsMatcher {
    pub contains: String,
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
pub enum Matcher {
    Contains(ContainsMatcher),
    Value(serde_yaml::Value),
}
