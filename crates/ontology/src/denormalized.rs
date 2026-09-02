//! denormalized join tables: a pre-joined `gl_denorm_*` table per opted-in edge
//! variant, holding the edge's `traversal_path` plus every storage column of
//! both endpoint nodes under a side prefix. Three `TO`-style materialized
//! views (one per source table: edge, source node, target node) keep it
//! current, so a change to any side re-emits the affected rows and the
//! `ReplacingMergeTree` keeps the latest `_version`.
//!
//! The compiler can answer a single-hop traversal or aggregation over such a
//! variant with one `FINAL` scan instead of an edge scan plus two node joins.

use crate::constants::{
    DELETED_COLUMN, RELATIONSHIP_KIND_COLUMN, TRAVERSAL_PATH_COLUMN, VERSION_COLUMN,
};
use crate::entities::{EdgeEntity, NodeEntity, StorageColumn};

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

    #[must_use]
    pub fn other(self) -> Side {
        match self {
            Side::Source => Side::Target,
            Side::Target => Side::Source,
        }
    }
}

/// Column layout and provenance for one denormalized join table. Built on
/// demand from the edge variant and its two node entities; not stored.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DenormalizedJoinTable {
    pub table: String,
    pub relationship_kind: String,
    pub edge_table: String,
    pub source_kind: String,
    pub source_table: String,
    pub target_kind: String,
    pub target_table: String,
    /// The side whose `id` leads the sort key after `traversal_path`. This is
    /// the scoped side when exactly one side is scoped, else the source.
    pub anchor: Side,
    /// Node storage columns copied under [`SOURCE_PREFIX`], in DDL order.
    pub source_columns: Vec<StorageColumn>,
    /// Node storage columns copied under [`TARGET_PREFIX`], in DDL order.
    pub target_columns: Vec<StorageColumn>,
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
            source_columns: copyable_columns(source),
            target_columns: copyable_columns(target),
        })
    }

    /// `(traversal_path, {anchor}_id, {other}_id)`
    #[must_use]
    pub fn sort_key(&self) -> Vec<String> {
        vec![
            TRAVERSAL_PATH_COLUMN.to_string(),
            format!("{}id", self.anchor.prefix()),
            format!("{}id", self.anchor.other().prefix()),
        ]
    }

    /// Whether `column` is one of the table's own columns rather than a
    /// prefixed copy of a node column. The compiler leaves these unrewritten.
    #[must_use]
    pub fn is_passthrough_column(column: &str) -> bool {
        matches!(
            column,
            TRAVERSAL_PATH_COLUMN | RELATIONSHIP_KIND_COLUMN | VERSION_COLUMN | DELETED_COLUMN
        )
    }
}

/// Node columns worth copying: everything except the edge-owned
/// `traversal_path` and the system columns, which the join table declares once.
fn copyable_columns(node: &NodeEntity) -> Vec<StorageColumn> {
    node.storage
        .columns
        .iter()
        .filter(|c| {
            !matches!(
                c.name.as_str(),
                TRAVERSAL_PATH_COLUMN | VERSION_COLUMN | DELETED_COLUMN
            )
        })
        .cloned()
        .collect()
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
}
