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
