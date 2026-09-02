//! Denormalized join tables: a pre-joined `gl_denorm_*` table per opted-in
//! edge variant. Its columns are the edge table's columns followed by every
//! column of both endpoint nodes under a side prefix, so the compiler's
//! edge-chain lowering can scan it exactly like an edge table while reading
//! node properties from the same row. Three `TO`-style materialized views (one
//! per source table: edge, source node, target node) keep it current; a change
//! to any side re-emits the affected rows and the `ReplacingMergeTree` keeps
//! the latest `_version`.
//!
//! This module only names the parts and fixes the column-naming contract. The
//! DDL generator composes the actual table from the generated definitions of
//! the three source tables.

use crate::constants::{
    DEFAULT_PRIMARY_KEY, DELETED_COLUMN, SOURCE_ID_COLUMN, TARGET_ID_COLUMN, TRAVERSAL_PATH_COLUMN,
    VERSION_COLUMN,
};
use crate::entities::{EdgeEntity, NodeEntity};

pub const SOURCE_PREFIX: &str = "src_";
pub const TARGET_PREFIX: &str = "tgt_";

/// `gl_denorm_reviewer__user__merge_request`
#[must_use]
pub fn table_name(relationship_kind: &str, source_kind: &str, target_kind: &str) -> String {
    format!(
        "gl_denorm_{}__{}__{}",
        relationship_kind.to_lowercase(),
        snake(source_kind),
        snake(target_kind)
    )
}

fn snake(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 4);
    for (i, c) in s.chars().enumerate() {
        if c.is_ascii_uppercase() {
            if i > 0 {
                out.push('_');
            }
            out.push(c.to_ascii_lowercase());
        } else {
            out.push(c);
        }
    }
    out
}

/// Which endpoint of the edge a column or alias belongs to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Side {
    Source,
    Target,
}

impl Side {
    #[must_use]
    pub fn prefix(self) -> &'static str {
        match self {
            Side::Source => SOURCE_PREFIX,
            Side::Target => TARGET_PREFIX,
        }
    }

    /// The edge column holding this side's node id.
    #[must_use]
    pub fn id_column(self) -> &'static str {
        match self {
            Side::Source => SOURCE_ID_COLUMN,
            Side::Target => TARGET_ID_COLUMN,
        }
    }

    #[must_use]
    pub fn other(self) -> Side {
        match self {
            Side::Source => Side::Target,
            Side::Target => Side::Source,
        }
    }

    /// The join-table column a node property on this side resolves to. The
    /// node's `id` is the edge's `source_id`/`target_id`; other node properties
    /// carry the side prefix.
    #[must_use]
    pub fn column_for(self, node_column: &str) -> String {
        if node_column == DEFAULT_PRIMARY_KEY {
            self.id_column().to_string()
        } else {
            format!("{}{node_column}", self.prefix())
        }
    }

    /// Inverse of [`Side::column_for`] for prefixed columns: which side a
    /// join-table column was copied from, and the node column it came from.
    #[must_use]
    pub fn of_column(column: &str) -> Option<(Side, &str)> {
        [Side::Source, Side::Target]
            .into_iter()
            .find_map(|side| column.strip_prefix(side.prefix()).map(|c| (side, c)))
    }
}

/// The parts of one denormalized join table: the three source tables and the
/// side whose id leads the sort key.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DenormalizedJoinTable {
    pub table: String,
    pub relationship_kind: String,
    pub edge_table: String,
    pub source_kind: String,
    pub source_table: String,
    pub target_kind: String,
    pub target_table: String,
    /// The scoped side when exactly one side is scoped, else the source.
    pub anchor: Side,
}

impl DenormalizedJoinTable {
    pub(crate) fn build(
        edge: &EdgeEntity,
        source: &NodeEntity,
        target: &NodeEntity,
    ) -> Option<Self> {
        let table = edge.denormalized_table.clone()?;
        let anchor = if source.global && !target.global {
            Side::Target
        } else {
            Side::Source
        };
        Some(Self {
            table,
            relationship_kind: edge.relationship_kind.clone(),
            edge_table: edge.destination_table.clone(),
            source_kind: edge.source_kind.clone(),
            source_table: source.destination_table.clone(),
            target_kind: edge.target_kind.clone(),
            target_table: target.destination_table.clone(),
            anchor,
        })
    }

    /// `(traversal_path, {anchor id}, {other id})`
    #[must_use]
    pub fn sort_key(&self) -> Vec<String> {
        vec![
            TRAVERSAL_PATH_COLUMN.to_string(),
            self.anchor.id_column().to_string(),
            self.anchor.other().id_column().to_string(),
        ]
    }

    /// Whether a node column is copied into the join table. `id` is already
    /// the edge's `source_id`/`target_id`; `traversal_path` is the edge's; the
    /// system columns are declared once for the joined row.
    #[must_use]
    pub fn copies_node_column(column: &str) -> bool {
        !matches!(
            column,
            DEFAULT_PRIMARY_KEY | TRAVERSAL_PATH_COLUMN | VERSION_COLUMN | DELETED_COLUMN
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn table_name_snake_cases_camel_kinds() {
        assert_eq!(
            table_name("REVIEWER", "User", "MergeRequest"),
            "gl_denorm_reviewer__user__merge_request"
        );
        assert_eq!(
            table_name("HAS_LABEL", "WorkItem", "Label"),
            "gl_denorm_has_label__work_item__label"
        );
    }

    #[test]
    fn node_id_resolves_to_the_edge_id_column() {
        assert_eq!(Side::Source.column_for("id"), "source_id");
        assert_eq!(Side::Target.column_for("id"), "target_id");
        assert_eq!(Side::Target.column_for("title"), "tgt_title");
        assert_eq!(Side::of_column("tgt_title"), Some((Side::Target, "title")));
        assert_eq!(Side::of_column("source_id"), None);
    }
}
