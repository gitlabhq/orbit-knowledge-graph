use serde::Deserialize;
use serde::de;
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

/// An assertion with optional `where` filter and `not` negation.
///
/// ```yaml
/// - { row_count: 3 }
/// - { where: { file: "main.py" }, row_count: 2 }
/// - { not: true, row: { name: "Foo" } }
/// - { not: true, match: { field: fqn, pattern: "bad.*" } }
/// ```
#[derive(Debug, Clone)]
pub(crate) struct Assert {
    pub filter: Option<HashMap<String, String>>,
    pub negate: bool,
    pub check: AssertCheck,
}

impl<'de> Deserialize<'de> for Assert {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: de::Deserializer<'de>,
    {
        let mut value: serde_yaml::Value = Deserialize::deserialize(deserializer)?;
        let filter = value
            .as_mapping_mut()
            .and_then(|m| m.remove("where"))
            .map(serde_yaml::from_value)
            .transpose()
            .map_err(de::Error::custom)?;
        let negate = value
            .as_mapping_mut()
            .and_then(|m| m.remove("not"))
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        let check: AssertCheck = serde_yaml::from_value(value).map_err(de::Error::custom)?;
        Ok(Assert {
            filter,
            negate,
            check,
        })
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
pub(crate) enum AssertCheck {
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
        row: HashMap<String, String>,
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
pub(crate) struct FieldValueArgs {
    pub field: String,
    pub value: i64,
}

#[derive(Debug, Clone, Deserialize)]
pub(crate) struct MatchArgs {
    pub field: String,
    pub pattern: String,
}

#[derive(Debug, Clone, Deserialize)]
pub(crate) struct ColumnValuesArgs {
    pub field: String,
    pub values: Vec<String>,
}
