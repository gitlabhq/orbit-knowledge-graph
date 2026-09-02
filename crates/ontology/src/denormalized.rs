//! Denormalized joins: a declared linear chain of tables pre-joined into one
//! `gl_denorm_<name>` table, kept current by one materialized view per source
//! table. Adjacent tables join on `traversal_path` when both carry it and
//! always on the id or edge id column that links them. A hop between two
//! nodes is realized either through its edge table or, when the variant has
//! an FK column, directly node to node.
//!
//! The row carries one `traversal_path`, the first scoped table's; the
//! `traversal_path` joins make every other scoped table agree with it, which
//! is what lets the security pass filter the whole row on that one column.
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

/// The denormalized column holding `column` of table `i`. Row-level columns
/// (`traversal_path`, `_version`, `_deleted`) exist once and are unprefixed;
/// everything else carries the table prefix.
#[must_use]
pub fn column_for(i: usize, column: &str) -> String {
    if copies(column) {
        format!("{}{column}", prefix(i))
    } else {
        column.to_string()
    }
}

/// Whether `column` of a source table is copied under its table prefix. The
/// system columns are declared once for the whole row and `traversal_path` is
/// taken once from the first scoped table (see
/// [`DenormalizedJoin::traversal_path_table`]).
#[must_use]
pub fn copies(column: &str) -> bool {
    !matches!(
        column,
        VERSION_COLUMN | DELETED_COLUMN | TRAVERSAL_PATH_COLUMN
    )
}

/// How table `i` joins onto table `i - 1`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JoinOn {
    pub prev_column: String,
    pub this_column: String,
    /// Both tables carry `traversal_path`, so the join also equates it. This
    /// keeps the row on one path and lets both sides seek their sort key.
    pub on_traversal_path: bool,
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
    /// Chain index of the table whose `traversal_path` the row carries.
    #[must_use]
    pub fn traversal_path_table(&self) -> usize {
        self.tables
            .iter()
            .position(|t| t.has_traversal_path)
            .expect("the loader requires a scoped table")
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
            .chain(ids.into_iter().map(|i| column_for(i, DEFAULT_PRIMARY_KEY)))
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

    #[test]
    fn columns_are_prefixed_except_the_shared_traversal_path() {
        assert_eq!(column_for(2, "title"), "t2_title");
        assert_eq!(column_for(2, "traversal_path"), "traversal_path");
        assert_eq!(column_for(2, "_deleted"), "_deleted");
        assert!(copies("title") && copies("id"));
        assert!(!copies("_version") && !copies("traversal_path"));
    }

    #[test]
    fn sort_key_leads_with_the_first_scoped_id() {
        // User (global), REVIEWER edge (no id), MergeRequest, IN_PROJECT edge, Project.
        let join = DenormalizedJoin {
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
        };
        assert_eq!(join.traversal_path_table(), 1);
        assert_eq!(
            join.sort_key(),
            ["traversal_path", "t2_id", "t0_id", "t4_id"]
        );
    }
}
