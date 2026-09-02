//! Denormalized paths: a pre-joined `gl_denorm_<name>` table per declared
//! chain of edge variants, holding every edge row on the path and every
//! column of every node on it. `k` hops give `k` edge blocks and `k + 1` node
//! blocks in one row, so the compiler can answer those hops with one scan.
//! One materialized view per source table (`2k + 1` of them) keeps the row
//! current; a change to any side re-emits the affected rows and the
//! `ReplacingMergeTree` keeps the latest `_version`.
//!
//! One row carries one `traversal_path`, the first hop's, and the security
//! pass filters on it. That is only sound when every node on the path lives
//! under that path, so the loader requires each hop to be scope-preserving or
//! to reach a global hub, the same rule the FK-chain lowering uses.
//!
//! This module names the parts and fixes the column contract. The DDL
//! generator composes the table from the source tables' generated
//! definitions.

use crate::constants::{
    DEFAULT_PRIMARY_KEY, DELETED_COLUMN, SOURCE_ID_COLUMN, TARGET_ID_COLUMN, TRAVERSAL_PATH_COLUMN,
    VERSION_COLUMN,
};

pub const TABLE_PREFIX: &str = "gl_denorm_";

/// `gl_denorm_reviewer_project`
#[must_use]
pub fn table_name(path_name: &str) -> String {
    format!("{TABLE_PREFIX}{path_name}")
}

/// One position on a path: edge `i` or node `j`. Nodes outnumber edges by
/// one; edge `i` connects node `i` to node `i + 1`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Position {
    Edge(usize),
    Node(usize),
}

impl Position {
    /// Column prefix: `e0_`, `n2_`.
    #[must_use]
    pub fn prefix(self) -> String {
        match self {
            Position::Edge(i) => format!("e{i}_"),
            Position::Node(j) => format!("n{j}_"),
        }
    }

    /// Alias for this position's source table inside the feeding views.
    #[must_use]
    pub fn view_alias(self) -> String {
        match self {
            Position::Edge(i) => format!("e{i}"),
            Position::Node(j) => format!("n{j}"),
        }
    }

    /// The join-table column a source column at this position resolves to.
    /// `traversal_path` is unprefixed (the row has exactly one, the first
    /// hop's). A node's `id` is an edge id column: node `j` is edge `j`'s
    /// source, or, for the last node, edge `j - 1`'s target. Everything else
    /// carries the position prefix.
    #[must_use]
    pub fn column_for(self, source_column: &str, hop_count: usize) -> String {
        match (self, source_column) {
            (_, TRAVERSAL_PATH_COLUMN) => TRAVERSAL_PATH_COLUMN.to_string(),
            (Position::Node(j), DEFAULT_PRIMARY_KEY) if j < hop_count => {
                format!("{}{SOURCE_ID_COLUMN}", Position::Edge(j).prefix())
            }
            (Position::Node(j), DEFAULT_PRIMARY_KEY) => {
                format!("{}{TARGET_ID_COLUMN}", Position::Edge(j - 1).prefix())
            }
            (pos, col) => format!("{}{col}", pos.prefix()),
        }
    }

    /// Inverse of [`Position::column_for`] for prefixed columns.
    #[must_use]
    pub fn of_column(column: &str) -> Option<(Position, &str)> {
        let (kind, rest) = column.split_at(1);
        let (index, col) = rest.split_once('_')?;
        let index: usize = index.parse().ok()?;
        match kind {
            "e" => Some((Position::Edge(index), col)),
            "n" => Some((Position::Node(index), col)),
            _ => None,
        }
    }

    /// Whether a source column at this position is copied into the row. The
    /// system columns are declared once for the whole row; a node's `id` is
    /// already an edge id column; only the first edge's `traversal_path` is
    /// kept.
    #[must_use]
    pub fn copies(self, source_column: &str) -> bool {
        match source_column {
            VERSION_COLUMN | DELETED_COLUMN => false,
            TRAVERSAL_PATH_COLUMN => self == Position::Edge(0),
            DEFAULT_PRIMARY_KEY => matches!(self, Position::Edge(_)),
            _ => true,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PathHop {
    pub relationship_kind: String,
    pub source_kind: String,
    pub target_kind: String,
    pub edge_table: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PathNode {
    pub kind: String,
    pub table: String,
    pub global: bool,
}

/// A declared denormalized path with its resolved source tables.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DenormalizedPath {
    pub name: String,
    pub table: String,
    pub hops: Vec<PathHop>,
    /// `hops.len() + 1` entries; node `j` is hop `j`'s source and hop
    /// `j - 1`'s target.
    pub nodes: Vec<PathNode>,
    pub sort_key: Vec<String>,
}

impl DenormalizedPath {
    #[must_use]
    pub fn hop_count(&self) -> usize {
        self.hops.len()
    }

    /// Every position in path order: `n0, e0, n1, e1, ..., nk`.
    pub fn positions(&self) -> impl Iterator<Item = Position> + '_ {
        (0..self.nodes.len()).flat_map(move |j| {
            std::iter::once(Position::Node(j))
                .chain((j < self.hops.len()).then_some(Position::Edge(j)))
        })
    }

    #[must_use]
    pub fn source_table(&self, pos: Position) -> &str {
        match pos {
            Position::Edge(i) => &self.hops[i].edge_table,
            Position::Node(j) => &self.nodes[j].table,
        }
    }

    /// Default sort key: `traversal_path`, then the first scoped node's id,
    /// then the remaining node ids in path order.
    #[must_use]
    pub fn default_sort_key(nodes: &[PathNode], hop_count: usize) -> Vec<String> {
        let anchor = nodes.iter().position(|n| !n.global).unwrap_or(0);
        let mut order: Vec<usize> = (0..nodes.len()).collect();
        order.remove(anchor);
        order.insert(0, anchor);
        std::iter::once(TRAVERSAL_PATH_COLUMN.to_string())
            .chain(
                order
                    .into_iter()
                    .map(|j| Position::Node(j).column_for(DEFAULT_PRIMARY_KEY, hop_count)),
            )
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn node_ids_resolve_to_edge_id_columns() {
        assert_eq!(Position::Node(0).column_for("id", 2), "e0_source_id");
        assert_eq!(Position::Node(1).column_for("id", 2), "e1_source_id");
        assert_eq!(Position::Node(2).column_for("id", 2), "e1_target_id");
        assert_eq!(Position::Node(1).column_for("title", 2), "n1_title");
        assert_eq!(
            Position::Edge(1).column_for("traversal_path", 2),
            "traversal_path"
        );
        assert_eq!(
            Position::Edge(1).column_for("target_kind", 2),
            "e1_target_kind"
        );
    }

    #[test]
    fn of_column_inverts_prefixes() {
        assert_eq!(
            Position::of_column("n12_title"),
            Some((Position::Node(12), "title"))
        );
        assert_eq!(
            Position::of_column("e0_source_id"),
            Some((Position::Edge(0), "source_id"))
        );
        assert_eq!(Position::of_column("traversal_path"), None);
        assert_eq!(Position::of_column("_version"), None);
    }

    #[test]
    fn default_sort_key_anchors_on_the_first_scoped_node() {
        let nodes = |globals: [bool; 3]| {
            globals
                .iter()
                .map(|&global| PathNode {
                    kind: String::new(),
                    table: String::new(),
                    global,
                })
                .collect::<Vec<_>>()
        };
        assert_eq!(
            DenormalizedPath::default_sort_key(&nodes([true, false, false]), 2),
            [
                "traversal_path",
                "e1_source_id",
                "e0_source_id",
                "e1_target_id"
            ]
        );
        assert_eq!(
            DenormalizedPath::default_sort_key(&nodes([false, false, true]), 2),
            [
                "traversal_path",
                "e0_source_id",
                "e1_source_id",
                "e1_target_id"
            ]
        );
    }
}
