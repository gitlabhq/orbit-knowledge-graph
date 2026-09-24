use serde::Deserialize;
use serde::de;
use std::collections::HashMap;
use std::fmt;

#[derive(Debug, Clone, Deserialize)]
pub struct FixtureFile {
    pub path: String,
    pub content: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct TestSuite {
    pub name: String,
    #[serde(default)]
    pub pipeline: Option<String>,
    #[serde(default)]
    pub fixtures: Vec<FixtureFile>,
    /// Load all source files from this directory (relative to workspace root).
    /// Files are discovered recursively and written to the temp dir preserving
    /// relative paths. Combines with `fixtures` (inline files take precedence).
    #[serde(default)]
    pub fixture_dir: Option<String>,
    /// When true, emit detailed engine/resolver trace events to stderr.
    #[serde(default)]
    pub trace: bool,
    pub tests: Vec<TestCase>,
    /// Incremental re-index steps; only the tree-dsl runner executes them.
    #[serde(default)]
    pub steps: Vec<IncrementalStep>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct IncrementalStep {
    pub name: String,
    /// Save the graph and reload it from the snapshot before this step.
    #[serde(default)]
    pub snapshot: bool,
    #[serde(default)]
    pub add: Vec<FixtureFile>,
    #[serde(default)]
    pub modify: Vec<FixtureFile>,
    #[serde(default)]
    pub remove: Vec<String>,
    #[serde(default)]
    pub tests: Vec<TestCase>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct TestCase {
    pub name: String,
    #[serde(default)]
    pub severity: Severity,
    #[serde(default)]
    pub skip: bool,
    #[serde(default)]
    pub debug: bool,
    #[serde(default)]
    pub query: Option<String>,
    #[serde(default)]
    pub assert: Vec<Assert>,
    #[serde(default)]
    pub queries: Vec<QueryBlock>,
}

impl TestCase {
    pub fn all_queries(&self) -> Vec<QueryBlock> {
        let mut blocks = Vec::new();
        if let Some(q) = &self.query {
            blocks.push(QueryBlock {
                query: q.clone(),
                assert: self.assert.clone(),
            });
        }
        blocks.extend(self.queries.iter().cloned());
        blocks
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct QueryBlock {
    pub query: String,
    pub assert: Vec<Assert>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum Severity {
    #[default]
    Error,
    Warning,
}

impl fmt::Display for Severity {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Severity::Error => f.write_str("ERROR"),
            Severity::Warning => f.write_str("WARN"),
        }
    }
}

/// An assertion with optional `where` filter and `not` negation.
///
/// ```yaml
/// - { row_count: 3 }
/// - { where: { file: "main.py" }, row_count: 2 }
/// - { not: true, row: { name: "Foo" } }
/// - { not: true, match: { field: fqn, pattern: "bad.*" } }
/// ```
#[derive(Debug, Clone)]
pub struct Assert {
    pub filter: Option<HashMap<String, String>>,
    pub negate: bool,
    pub check: AssertCheck,
}

impl<'de> Deserialize<'de> for Assert {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: de::Deserializer<'de>,
    {
        let mut value: serde_json::Value = Deserialize::deserialize(deserializer)?;
        let filter = value
            .as_object_mut()
            .and_then(|m| m.remove("where"))
            .map(serde_json::from_value)
            .transpose()
            .map_err(de::Error::custom)?;
        let negate = value
            .as_object_mut()
            .and_then(|m| m.remove("not"))
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        let check: AssertCheck = serde_json::from_value(value).map_err(de::Error::custom)?;
        Ok(Assert {
            filter,
            negate,
            check,
        })
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
pub enum AssertCheck {
    Empty {
        empty: bool,
    },
    RowCount {
        row_count: i64,
    },
    CountEquals {
        count_equals: FieldValueArgs,
    },
    CountGte {
        count_gte: FieldValueArgs,
    },
    Match {
        #[serde(rename = "match")]
        match_args: MatchArgs,
    },
    Row {
        row: HashMap<String, serde_json::Value>,
    },
    NoNulls {
        no_nulls: String,
    },
    Unique {
        unique: String,
    },
    ColumnValues {
        column_values: ColumnValuesArgs,
    },
}

#[derive(Debug, Clone, Deserialize)]
pub struct FieldValueArgs {
    pub field: String,
    pub value: i64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct MatchArgs {
    pub field: String,
    pub pattern: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ColumnValuesArgs {
    pub field: String,
    pub values: Vec<String>,
}
