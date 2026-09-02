//! Denormalized joins: a declared linear chain of tables pre-joined into one
//! `gl_denorm_<name>` table, kept current by one materialized view per source
//! table. Adjacent tables join on the id or edge id column that links them. A
//! hop between two nodes is realized either through its edge table or, when
//! the variant has an FK column, directly node to node.
//!
//! Every scoped table keeps its own `traversal_path` in the row, exactly as
//! each scan alias keeps its own in an ordinary join, and the security pass
//! filters each of them. The first scoped table's is the row's unprefixed
//! `traversal_path`, which leads the sort key and drives partitioning.
//!
//! The DDL generator only sees `tables`; the compiler additionally uses
//! `hops` to map query hops and nodes onto table indices.

use crate::constants::{
    DEFAULT_PRIMARY_KEY, DELETED_COLUMN, TRAVERSAL_PATH_COLUMN, VERSION_COLUMN,
};

pub const TABLE_PREFIX: &str = "gl_denorm_";

#[must_use]
pub fn table_name(name: &str) -> String {
    format!("{TABLE_PREFIX}{name}")
}

/// Column prefix for table `i` in the chain.
#[must_use]
pub fn prefix(i: usize) -> String {
    format!("t{i}_")
}

/// Alias of table `i` inside the feeding views.
#[must_use]
pub fn alias(i: usize) -> String {
    format!("t{i}")
}

/// Whether `column` of a source table is copied into the row. The system
/// columns are declared once for the whole row.
#[must_use]
pub fn copies(column: &str) -> bool {
    !matches!(column, VERSION_COLUMN | DELETED_COLUMN)
}

/// How table `i` joins onto table `i - 1`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JoinOn {
    pub prev_column: String,
    pub this_column: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JoinedTable {
    pub table: String,
    pub has_traversal_path: bool,
    pub has_id: bool,
    /// `None` for the first table.
    pub join: Option<JoinOn>,
    /// Equality predicates selecting the rows that belong to this chain, e.g.
    /// an edge table's relationship and endpoint kinds.
    pub filter: Vec<(String, String)>,
}

/// How a declared hop is realized in the chain.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JoinHop {
    pub relationship_kind: String,
    pub source_kind: String,
    pub target_kind: String,
    /// Chain index of the source node's table.
    pub source_table: usize,
    /// Chain index of the target node's table.
    pub target_table: usize,
    /// Chain index of the edge table, or `None` when the hop is realized
    /// through the FK column.
    pub edge_table: Option<usize>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DenormalizedJoin {
    pub name: String,
    pub table: String,
    pub tables: Vec<JoinedTable>,
    pub hops: Vec<JoinHop>,
}

impl DenormalizedJoin {
    /// Chain index of the table whose `traversal_path` is the row's unprefixed
    /// one (the sort key and partition anchor).
    #[must_use]
    pub fn anchor_table(&self) -> usize {
        self.tables
            .iter()
            .position(|t| t.has_traversal_path)
            .expect("the loader requires a scoped table")
    }

    /// The denormalized column holding `column` of table `i`. Row-level system
    /// columns are unprefixed, as is the anchor table's `traversal_path`;
    /// everything else carries the table prefix.
    #[must_use]
    pub fn column_for(&self, i: usize, column: &str) -> String {
        let anchored = column == TRAVERSAL_PATH_COLUMN && i == self.anchor_table();
        if copies(column) && !anchored {
            format!("{}{column}", prefix(i))
        } else {
            column.to_string()
        }
    }

    /// Every `traversal_path` column in the row, one per scoped table, with
    /// that table's chain index. The security pass filters each of them.
    pub fn traversal_path_columns(&self) -> impl Iterator<Item = (usize, String)> + '_ {
        (0..self.tables.len())
            .filter(|&i| self.tables[i].has_traversal_path)
            .map(|i| (i, self.column_for(i, TRAVERSAL_PATH_COLUMN)))
    }

    /// `traversal_path`, then the `id` of every table that has one, with the
    /// first scoped table's id leading so seeks within a namespace land on it.
    #[must_use]
    pub fn sort_key(&self) -> Vec<String> {
        let mut ids: Vec<usize> = (0..self.tables.len())
            .filter(|&i| self.tables[i].has_id)
            .collect();
        if let Some(anchor) = ids.iter().position(|&i| self.tables[i].has_traversal_path) {
            let anchor = ids.remove(anchor);
            ids.insert(0, anchor);
        }
        std::iter::once(TRAVERSAL_PATH_COLUMN.to_string())
            .chain(
                ids.into_iter()
                    .map(|i| self.column_for(i, DEFAULT_PRIMARY_KEY)),
            )
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn table(has_traversal_path: bool, has_id: bool) -> JoinedTable {
        JoinedTable {
            table: String::new(),
            has_traversal_path,
            has_id,
            join: None,
            filter: vec![],
        }
    }

    fn reviewer_project() -> DenormalizedJoin {
        // User (global), REVIEWER edge (no id), MergeRequest, IN_PROJECT edge, Project.
        DenormalizedJoin {
            name: String::new(),
            table: String::new(),
            tables: vec![
                table(false, true),
                table(true, false),
                table(true, true),
                table(true, false),
                table(true, true),
            ],
            hops: vec![],
        }
    }

    #[test]
    fn only_the_anchor_traversal_path_is_unprefixed() {
        let join = reviewer_project();
        assert_eq!(join.anchor_table(), 1);
        assert_eq!(join.column_for(2, "title"), "t2_title");
        assert_eq!(join.column_for(1, "traversal_path"), "traversal_path");
        assert_eq!(join.column_for(2, "traversal_path"), "t2_traversal_path");
        assert_eq!(join.column_for(2, "_deleted"), "_deleted");
        let paths: Vec<(usize, String)> = join.traversal_path_columns().collect();
        assert_eq!(
            paths,
            [
                (1, "traversal_path".into()),
                (2, "t2_traversal_path".into()),
                (3, "t3_traversal_path".into()),
                (4, "t4_traversal_path".into()),
            ]
        );
    }

    #[test]
    fn sort_key_leads_with_the_first_scoped_id() {
        assert_eq!(
            reviewer_project().sort_key(),
            ["traversal_path", "t2_id", "t0_id", "t4_id"]
        );
    }
}
