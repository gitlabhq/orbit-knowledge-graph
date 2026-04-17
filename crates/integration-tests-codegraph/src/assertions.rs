use serde::Deserialize;
use std::collections::HashMap;
use std::fmt;

#[derive(Debug, Deserialize)]
pub(crate) struct FixtureFile {
    pub path: String,
    pub content: String,
}

#[derive(Debug, Deserialize)]
pub(crate) struct TestSuite {
    pub name: String,
    /// Which pipeline to use. Absent or `"generic"` uses the standard
    /// `Pipeline::run()` dispatch. Named pipelines (e.g. `"ruby_prism"`)
    /// invoke the corresponding custom pipeline directly.
    #[serde(default)]
    pub pipeline: Option<String>,
    #[serde(default)]
    pub fixtures: Vec<FixtureFile>,
    pub tests: Vec<TestCase>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct TestCase {
    pub name: String,
    #[serde(default)]
    pub severity: Severity,
    #[serde(default)]
    pub skip: bool,
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
pub(crate) struct QueryBlock {
    pub query: String,
    pub assert: Vec<Assert>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub(crate) enum Severity {
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

#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
pub(crate) enum Assert {
    Empty {
        empty: bool,
    },
    NonEmpty {
        non_empty: bool,
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
    AllMatch {
        all_match: AllMatchArgs,
    },
    ContainsRow {
        contains_row: HashMap<String, String>,
    },
}

#[derive(Debug, Clone, Deserialize)]
pub(crate) struct FieldValueArgs {
    pub field: String,
    pub value: i64,
}

#[derive(Debug, Clone, Deserialize)]
pub(crate) struct AllMatchArgs {
    pub field: String,
    pub pattern: String,
}
