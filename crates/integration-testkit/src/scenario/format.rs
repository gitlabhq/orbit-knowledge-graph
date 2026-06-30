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
    pub dispatch: Option<DispatchSpec>,
    #[serde(default)]
    pub cdc: Vec<CdcEvent>,
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
            || self.dispatch.is_some()
            || !self.cdc.is_empty()
            || self.expect.is_some();
        match (has_inline, self.steps.is_empty()) {
            (true, true) => vec![Step {
                seed: self.seed.unwrap_or_default(),
                seed_settings: self.seed_settings,
                run: self.run,
                dispatch: self.dispatch,
                cdc: self.cdc,
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
    pub dispatch: Option<DispatchSpec>,
    #[serde(default)]
    pub cdc: Vec<CdcEvent>,
    #[serde(default)]
    pub expect: Option<Expect>,
}

impl Step {
    pub fn handlers(&self) -> Vec<&str> {
        let mut handlers: Vec<&str> = match &self.run {
            None => Vec::new(),
            Some(RunSpec::One(h)) => vec![h.as_str()],
            Some(RunSpec::Many(hs)) => hs.iter().map(String::as_str).collect(),
        };
        match &self.dispatch {
            None => {}
            Some(DispatchSpec::One(k)) => handlers.push(k.handler()),
            Some(DispatchSpec::Many(ks)) => handlers.extend(ks.iter().map(|k| k.handler())),
        }
        if handlers.is_empty() {
            handlers.push("namespace");
        }
        handlers
    }
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
pub enum DispatchSpec {
    One(DispatchKind),
    Many(Vec<DispatchKind>),
}

#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DispatchKind {
    Namespace,
    Global,
    EnabledNamespaces,
}

impl DispatchKind {
    pub fn handler(self) -> &'static str {
        match self {
            DispatchKind::Namespace => "dispatch_namespace",
            DispatchKind::Global => "dispatch_global",
            DispatchKind::EnabledNamespaces => "dispatch_enabled_namespace_cdc",
        }
    }
}

/// A Siphon logical-replication event fed to a CDC route, so a scenario can
/// exercise routing the same way the wire path does.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CdcEvent {
    pub table: String,
    pub operation: CdcOperation,
    pub rows: Vec<Row>,
}

#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CdcOperation {
    Insert,
    Update,
    Delete,
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
    #[serde(default)]
    pub dispatched: Vec<DispatchExpect>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DispatchExpect {
    pub kind: String,
    #[serde(default)]
    pub count: Option<usize>,
    #[serde(default)]
    pub rows: Vec<RowMatcher>,
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
