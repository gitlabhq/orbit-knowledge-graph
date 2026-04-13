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
    /// Parsed by the v2 pipeline to produce the CodeGraph under test.
    #[serde(default)]
    pub fixtures: Vec<FixtureFile>,
    pub tests: Vec<TestCase>,
}

#[derive(Debug, Deserialize)]
pub struct TestCase {
    pub name: String,
    #[serde(default)]
    pub severity: Severity,
    pub query: String,
    pub assert: Vec<Assert>,
    #[serde(default)]
    pub params: HashMap<String, serde_json::Value>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
#[derive(Default)]
pub enum Severity {
    #[default]
    Error,
    Warning,
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
pub enum Assert {
    Empty { empty: bool },
    NonEmpty { non_empty: bool },
    CountEquals { count_equals: CountEqualsArgs },
    AllMatch { all_match: AllMatchArgs },
}

#[derive(Debug, Deserialize)]
pub struct CountEqualsArgs {
    pub field: String,
    pub value: i64,
}

#[derive(Debug, Deserialize)]
pub struct AllMatchArgs {
    pub field: String,
    pub pattern: String,
}
