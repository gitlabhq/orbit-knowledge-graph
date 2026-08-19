use std::collections::HashMap;

use async_trait::async_trait;
use orbit_utils::arrow::ColumnValue;
use query_engine::pipeline::PipelineError;
use query_engine::shared::content::{ColumnResolver, PropertyRow, ResolverContext};
use serde::Deserialize;

const SEED_DIR: &str = env!("SEEDS_DIR");

#[derive(Debug, Deserialize)]
pub struct SeedEntry {
    #[serde(rename = "match")]
    pub match_fields: HashMap<String, serde_yaml::Value>,
    pub value: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(transparent)]
pub struct SeededContent {
    lookups: HashMap<String, Vec<SeedEntry>>,
}

pub struct SeededColumnResolver {
    content: SeededContent,
}

impl SeededColumnResolver {
    pub fn from_seed_file() -> Self {
        let path = format!("{SEED_DIR}/virtual_content.yaml");
        let yaml = std::fs::read_to_string(&path)
            .unwrap_or_else(|e| panic!("virtual content seed not found at {path}: {e}"));
        Self::from_yaml(&yaml)
    }

    pub fn from_yaml(yaml: &str) -> Self {
        let content: SeededContent =
            serde_yaml::from_str(yaml).expect("virtual content seed should parse");
        Self { content }
    }

    fn resolve_row(&self, lookup: &str, row: &PropertyRow) -> Option<ColumnValue> {
        let entries = self.content.lookups.get(lookup)?;
        let entry = entries.iter().find(|e| row_matches(e, row))?;
        entry.value.clone().map(ColumnValue::String)
    }
}

#[async_trait]
impl ColumnResolver for SeededColumnResolver {
    async fn resolve_batch(
        &self,
        lookup: &str,
        rows: &[&PropertyRow],
        _ctx: &ResolverContext,
    ) -> Result<Vec<Option<ColumnValue>>, PipelineError> {
        Ok(rows
            .iter()
            .map(|row| self.resolve_row(lookup, row))
            .collect())
    }
}

fn row_matches(entry: &SeedEntry, row: &PropertyRow) -> bool {
    entry.match_fields.iter().all(|(field, want)| {
        row.get(field)
            .is_some_and(|got| column_value_string(got) == yaml_scalar_string(want))
    })
}

fn column_value_string(value: &ColumnValue) -> String {
    match value {
        ColumnValue::Int64(v) => v.to_string(),
        ColumnValue::Float64(v) => v.to_string(),
        ColumnValue::String(v) => v.clone(),
        ColumnValue::Bool(v) => v.to_string(),
        ColumnValue::Null => String::new(),
    }
}

fn yaml_scalar_string(value: &serde_yaml::Value) -> String {
    match value {
        serde_yaml::Value::Number(n) => n.to_string(),
        serde_yaml::Value::String(s) => s.clone(),
        serde_yaml::Value::Bool(b) => b.to_string(),
        other => panic!("unsupported match value in virtual content seed: {other:?}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const YAML: &str = r#"
blob_content:
  - match: {project_id: 1000, path: src/lib.rs}
    value: "pub fn hello() {}"
  - match: {project_id: 1000, path: assets/logo.png}
    value: null
mr_raw_patch:
  - match: {project_id: 1000, iid: 5}
    value: "@@ -1,1 +1,2 @@"
"#;

    fn row(pairs: &[(&str, ColumnValue)]) -> PropertyRow {
        pairs
            .iter()
            .map(|(k, v)| ((*k).to_string(), v.clone()))
            .collect()
    }

    #[tokio::test]
    async fn resolves_matching_row() {
        let resolver = SeededColumnResolver::from_yaml(YAML);
        let r = row(&[
            ("project_id", ColumnValue::String("1000".into())),
            ("path", ColumnValue::String("src/lib.rs".into())),
        ]);

        let results = resolver
            .resolve_batch("blob_content", &[&r], &ResolverContext::default())
            .await
            .unwrap();

        assert_eq!(
            results[0],
            Some(ColumnValue::String("pub fn hello() {}".into()))
        );
    }

    #[tokio::test]
    async fn null_value_resolves_to_none() {
        let resolver = SeededColumnResolver::from_yaml(YAML);
        let r = row(&[
            ("project_id", ColumnValue::String("1000".into())),
            ("path", ColumnValue::String("assets/logo.png".into())),
        ]);

        let results = resolver
            .resolve_batch("blob_content", &[&r], &ResolverContext::default())
            .await
            .unwrap();

        assert_eq!(results[0], None);
    }

    #[tokio::test]
    async fn unmatched_row_resolves_to_none() {
        let resolver = SeededColumnResolver::from_yaml(YAML);
        let r = row(&[
            ("project_id", ColumnValue::String("1000".into())),
            ("path", ColumnValue::String("src/other.rs".into())),
        ]);

        let results = resolver
            .resolve_batch("blob_content", &[&r], &ResolverContext::default())
            .await
            .unwrap();

        assert_eq!(results[0], None);
    }

    #[tokio::test]
    async fn matches_numeric_props_against_stringified_values() {
        let resolver = SeededColumnResolver::from_yaml(YAML);
        let r = row(&[
            ("project_id", ColumnValue::Int64(1000)),
            ("iid", ColumnValue::Int64(5)),
        ]);

        let results = resolver
            .resolve_batch("mr_raw_patch", &[&r], &ResolverContext::default())
            .await
            .unwrap();

        assert_eq!(
            results[0],
            Some(ColumnValue::String("@@ -1,1 +1,2 @@".into()))
        );
    }

    #[tokio::test]
    async fn unknown_lookup_resolves_to_none() {
        let resolver = SeededColumnResolver::from_yaml(YAML);
        let r = row(&[("project_id", ColumnValue::String("1000".into()))]);

        let results = resolver
            .resolve_batch("unknown", &[&r], &ResolverContext::default())
            .await
            .unwrap();

        assert_eq!(results[0], None);
    }

    #[test]
    fn seed_file_parses() {
        SeededColumnResolver::from_seed_file();
    }
}
