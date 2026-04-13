//! YAML-driven test suite types for graph validation.

use serde::Deserialize;
use std::collections::HashMap;

#[derive(Debug, Deserialize)]
pub struct TestSuite {
    pub name: String,
    pub tests: Vec<TestCase>,
}

#[derive(Debug, Deserialize)]
pub struct TestCase {
    pub name: String,
    #[serde(default)]
    pub severity: Severity,
    pub query: String,
    pub assert: Assert,
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
