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
pub enum Severity {
    Error,
    Warning,
}

impl Default for Severity {
    fn default() -> Self {
        Self::Error
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Assert {
    Empty(bool),
    NonEmpty(bool),
    CountEquals { field: String, value: i64 },
    AllMatch { field: String, pattern: String },
}
