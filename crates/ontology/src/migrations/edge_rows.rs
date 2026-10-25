use serde::{Deserialize, Serialize};

use super::Fingerprints;
use crate::Ontology;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EdgeRows {
    #[default]
    Changed,
    Unchanged,
}

impl EdgeRows {
    pub fn is_changed(&self) -> bool {
        *self == Self::Changed
    }

    pub fn covers(self, required: Self) -> bool {
        self.is_changed() || !required.is_changed()
    }

    pub fn widened_with(self, other: Self) -> Self {
        if self.is_changed() || other.is_changed() {
            Self::Changed
        } else {
            Self::Unchanged
        }
    }
}

pub fn derive_edge_rows(
    ontology: &Ontology,
    current: &Fingerprints,
    base: &Fingerprints,
) -> EdgeRows {
    let (changed_sources, changed_tables) = current.get_versioned_diff_keys_between(base);
    let edge_tables = ontology.edge_tables();

    let lowering_code_only = changed_sources.is_empty() && changed_tables.is_empty();
    let edge_table_ddl_changed = changed_tables
        .iter()
        .any(|table| edge_tables.contains(&table.as_str()));
    let edge_row_sources_changed = current.edge_rows != base.edge_rows;

    if lowering_code_only || edge_table_ddl_changed || edge_row_sources_changed {
        EdgeRows::Changed
    } else {
        EdgeRows::Unchanged
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;

    fn fingerprints(source: &str, ddl: &[(&str, &str)], edge_rows: &str) -> Fingerprints {
        Fingerprints {
            sources: BTreeMap::from([("nodes/core/note.yaml".to_string(), source.to_string())]),
            ddl: ddl
                .iter()
                .map(|(table, hash)| (table.to_string(), hash.to_string()))
                .collect(),
            edge_rows: edge_rows.to_string(),
            auxiliary_schema: BTreeMap::new(),
        }
    }

    #[test]
    fn node_only_change_keeps_edge_rows_unchanged() {
        let ontology = Ontology::load_embedded().expect("ontology loads");
        let base = fingerprints("old", &[("gl_note", "old")], "edges");
        let current = fingerprints("new", &[("gl_note", "new")], "edges");
        assert_eq!(
            derive_edge_rows(&ontology, &current, &base),
            EdgeRows::Unchanged
        );
    }

    #[test]
    fn any_edge_row_risk_marks_edge_rows_changed() {
        let ontology = Ontology::load_embedded().expect("ontology loads");
        let base = fingerprints("old", &[("gl_edge", "old")], "edges");
        let cases = [
            ("lowering code only", &base, base.clone()),
            (
                "edge table DDL",
                &base,
                fingerprints("new", &[("gl_edge", "new")], "edges"),
            ),
            (
                "edge row sources",
                &base,
                fingerprints("new", &[("gl_edge", "old")], "new edges"),
            ),
            (
                "base without an edge row hash",
                &fingerprints("old", &[("gl_edge", "old")], ""),
                fingerprints("new", &[("gl_edge", "old")], "edges"),
            ),
        ];
        for (risk, base, current) in cases {
            assert_eq!(
                derive_edge_rows(&ontology, &current, base),
                EdgeRows::Changed,
                "{risk}"
            );
        }
    }

    #[test]
    fn only_changed_covers_changed() {
        assert!(EdgeRows::Changed.covers(EdgeRows::Unchanged));
        assert!(EdgeRows::Unchanged.covers(EdgeRows::Unchanged));
        assert!(!EdgeRows::Unchanged.covers(EdgeRows::Changed));
        assert_eq!(
            EdgeRows::Unchanged.widened_with(EdgeRows::Changed),
            EdgeRows::Changed
        );
    }
}
