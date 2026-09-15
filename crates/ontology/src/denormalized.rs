//! A denormalized join is a linear chain of tables pre-joined into one `gl_denorm_<name>` table.
//! Each scoped table keeps its own `traversal_path` in the row; the first one is the sort-key anchor.

use crate::constants::{
    DEFAULT_PRIMARY_KEY, DELETED_COLUMN, TRAVERSAL_PATH_COLUMN, VERSION_COLUMN,
};

const TABLE_PREFIX: &str = "gl_denorm_";

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

/// System columns are declared once per row rather than copied per table.
#[must_use]
pub fn copies(column: &str) -> bool {
    !matches!(column, VERSION_COLUMN | DELETED_COLUMN)
}

/// Row-level columns (system, and the anchor's `traversal_path`) are unprefixed; the rest get `t{i}_`.
#[must_use]
pub fn column_for(anchor: usize, i: usize, column: &str) -> String {
    let anchored = column == TRAVERSAL_PATH_COLUMN && i == anchor;
    if copies(column) && !anchored {
        format!("{}{column}", prefix(i))
    } else {
        column.to_string()
    }
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
    /// Only tables with an `id` (nodes, not edges) enter the sort key.
    pub has_id: bool,
    /// `None` for the first table.
    pub join: Option<JoinOn>,
    /// Equality predicates selecting this chain's rows, e.g. an edge table's kinds.
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
    /// `None` when the hop is realized through the FK column instead of an edge table.
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
    /// The table whose `traversal_path` leads the sort key and drives partitioning.
    #[must_use]
    pub fn anchor_table(&self) -> usize {
        self.tables
            .iter()
            .position(|t| t.has_traversal_path)
            .expect("the loader requires a scoped table")
    }

    /// See [`column_for`].
    #[must_use]
    pub fn column_for(&self, i: usize, column: &str) -> String {
        column_for(self.anchor_table(), i, column)
    }

    /// One `traversal_path` column per scoped table, each filtered by the security pass.
    pub fn traversal_path_columns(&self) -> impl Iterator<Item = (usize, String)> + '_ {
        (0..self.tables.len())
            .filter(|&i| self.tables[i].has_traversal_path)
            .map(|i| (i, self.column_for(i, TRAVERSAL_PATH_COLUMN)))
    }

    /// `traversal_path`, then ids with the anchor's first so in-namespace seeks land on it.
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
