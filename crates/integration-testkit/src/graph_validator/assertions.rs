//! YAML-driven test suite types for graph validation.

use serde::Deserialize;
use std::collections::HashMap;

/// A source file to be parsed into the graph before running tests.
#[derive(Debug, Deserialize)]
pub struct FixtureFile {
    pub path: String,
    pub content: String,
}

#[derive(Debug, Deserialize)]
pub struct TestSuite {
    pub name: String,
    /// Source files that make up the fixture graph.
    #[serde(default)]
    pub fixtures: Vec<FixtureFile>,
    pub tests: Vec<TestCase>,
}

#[derive(Debug, Deserialize)]
pub struct TestCase {
    pub name: String,
    /// Optional longer description.
    #[serde(default)]
    pub description: Option<String>,
    #[serde(default)]
    pub severity: Severity,
    /// Skip this test (expected failure / not yet implemented).
    #[serde(default)]
    pub skip: bool,
    /// Single query (simple form).
    #[serde(default)]
    pub query: Option<String>,
    /// Assertions for the single query.
    #[serde(default)]
    pub assert: Vec<Assert>,
    /// Multiple queries, each with their own assertions (extended form).
    #[serde(default)]
    pub queries: Vec<QueryBlock>,
    #[serde(default)]
    pub params: HashMap<String, serde_json::Value>,
}

impl TestCase {
    /// Iterate over all query blocks — merges `query`+`assert` and `queries`.
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

/// A single query with its assertions.
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

#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
pub enum Assert {
    Empty {
        empty: bool,
    },
    NonEmpty {
        non_empty: bool,
    },
    CountEquals {
        count_equals: CountEqualsArgs,
    },
    CountGte {
        count_gte: CountGteArgs,
    },
    RowCount {
        row_count: i64,
    },
    AllMatch {
        all_match: AllMatchArgs,
    },
    ContainsRow {
        contains_row: std::collections::HashMap<String, String>,
    },
}

#[derive(Debug, Clone, Deserialize)]
pub struct CountEqualsArgs {
    pub field: String,
    pub value: i64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct CountGteArgs {
    pub field: String,
    pub value: i64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct AllMatchArgs {
    pub field: String,
    pub pattern: String,
}
