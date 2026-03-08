//! Relation tree and chainable query builder.
//!
//! The plan is an abstract relation tree composed of [`Rel`] nodes that store
//! [`Expr`](crate::ir::expr::Expr) expressions directly — no positional
//! resolution, no Substrait types. Backends walk the tree to emit SQL or
//! encode to Substrait for DataFusion.
//!
//! ```text
//! Rel::read("gl_project", "p", &[("id", Int64), ("name", String)])
//!     .filter(col("p", "id").eq(int(42)))
//!     .project(&[(col("p", "name"), "name")])
//!     .fetch(10, None)
//!     .into_plan()
//! ```

use crate::ir::expr::{DataType, Expr, JoinType, SortDir};

// ---------------------------------------------------------------------------
// Relation tree
// ---------------------------------------------------------------------------

/// A node in the relation tree.
///
/// Each node holds its operator-specific data in `kind` and its child
/// relations in `inputs`. Input conventions:
/// - `Read`: no inputs
/// - `Filter`, `Project`, `Sort`, `Fetch`, `Aggregate`, `Subquery`, `Distinct`: `inputs[0]`
/// - `Join`: `inputs[0]` is left, `inputs[1]` is right
/// - `UnionAll`: `inputs` contains all union arms
#[derive(Debug, Clone)]
pub struct Rel {
    pub kind: RelKind,
    pub inputs: Vec<Rel>,
}

/// Operator-specific data for a relation node.
#[derive(Debug, Clone)]
pub enum RelKind {
    Read {
        table: String,
        alias: String,
        columns: Vec<ColumnDef>,
    },
    Filter {
        condition: Expr,
    },
    Project {
        expressions: Vec<(Expr, String)>,
    },
    Join {
        join_type: JoinType,
        condition: Expr,
    },
    Sort {
        sorts: Vec<SortSpec>,
    },
    Fetch {
        limit: u64,
        offset: Option<u64>,
    },
    Aggregate {
        group_by: Vec<Expr>,
        measures: Vec<Measure>,
    },
    UnionAll {
        alias: String,
    },
    Subquery {
        alias: String,
    },
    Distinct,
}

/// Sentinel table name for raw FROM clauses.
pub const RAW_FROM_TAG: &str = "__raw_from";

#[derive(Debug, Clone)]
pub struct ColumnDef {
    pub name: String,
    pub data_type: DataType,
}

impl ColumnDef {
    fn from_pairs(columns: &[(&str, DataType)]) -> Vec<Self> {
        columns
            .iter()
            .map(|(name, dt)| Self {
                name: (*name).into(),
                data_type: dt.clone(),
            })
            .collect()
    }
}

#[derive(Debug, Clone)]
pub struct SortSpec {
    pub expr: Expr,
    pub direction: SortDir,
}

#[derive(Debug, Clone)]
pub struct Measure {
    pub function: String,
    pub args: Vec<Expr>,
    pub alias: String,
    pub filter: Option<Expr>,
}

impl Measure {
    pub fn new(function: &str, args: &[Expr], alias: &str) -> Self {
        Self {
            function: function.into(),
            args: args.to_vec(),
            alias: alias.into(),
            filter: None,
        }
    }

    pub fn with_filter(mut self, filter: Expr) -> Self {
        self.filter = Some(filter);
        self
    }
}

// ---------------------------------------------------------------------------
// Chainable API
// ---------------------------------------------------------------------------

impl Rel {
    /// Table scan: `FROM table AS alias`
    pub fn read(table: &str, alias: &str, columns: &[(&str, DataType)]) -> Self {
        Rel {
            kind: RelKind::Read {
                table: table.into(),
                alias: alias.into(),
                columns: ColumnDef::from_pairs(columns),
            },
            inputs: Vec::new(),
        }
    }

    /// Raw FROM clause: verbatim SQL in the FROM position.
    ///
    /// Columns define the output schema for downstream references.
    /// Column references use empty table alias (unqualified).
    pub fn read_raw(raw_from: &str, columns: &[(&str, DataType)]) -> Self {
        Rel {
            kind: RelKind::Read {
                table: RAW_FROM_TAG.into(),
                alias: raw_from.into(),
                columns: ColumnDef::from_pairs(columns),
            },
            inputs: Vec::new(),
        }
    }

    /// `WHERE condition`
    pub fn filter(self, condition: Expr) -> Self {
        Rel {
            kind: RelKind::Filter { condition },
            inputs: vec![self],
        }
    }

    /// `SELECT expr1 AS alias1, expr2 AS alias2, ...`
    pub fn project(self, exprs: &[(Expr, &str)]) -> Self {
        Rel {
            kind: RelKind::Project {
                expressions: exprs
                    .iter()
                    .map(|(e, a)| (e.clone(), (*a).into()))
                    .collect(),
            },
            inputs: vec![self],
        }
    }

    /// `self JOIN right ON condition`
    pub fn join(self, join_type: JoinType, right: Rel, condition: Expr) -> Self {
        Rel {
            kind: RelKind::Join {
                join_type,
                condition,
            },
            inputs: vec![self, right],
        }
    }

    /// `ORDER BY key1 dir1, key2 dir2, ...`
    pub fn sort(self, keys: &[(Expr, SortDir)]) -> Self {
        Rel {
            kind: RelKind::Sort {
                sorts: keys
                    .iter()
                    .map(|(e, d)| SortSpec {
                        expr: e.clone(),
                        direction: *d,
                    })
                    .collect(),
            },
            inputs: vec![self],
        }
    }

    /// `LIMIT count [OFFSET offset]`
    pub fn fetch(self, limit: u64, offset: Option<u64>) -> Self {
        Rel {
            kind: RelKind::Fetch { limit, offset },
            inputs: vec![self],
        }
    }

    /// `SELECT agg(args) AS alias, ... FROM self GROUP BY group_exprs`
    pub fn aggregate(self, group_by: &[Expr], measures: &[Measure]) -> Self {
        Rel {
            kind: RelKind::Aggregate {
                group_by: group_by.to_vec(),
                measures: measures.to_vec(),
            },
            inputs: vec![self],
        }
    }

    /// `UNION ALL` of multiple relations, aliased for outer references.
    pub fn union_all(inputs: Vec<Rel>, alias: &str) -> Self {
        assert!(!inputs.is_empty(), "union_all requires at least one input");
        Rel {
            kind: RelKind::UnionAll {
                alias: alias.into(),
            },
            inputs,
        }
    }

    /// Wrap as `(SELECT ...) AS alias` derived table.
    pub fn subquery(self, alias: &str) -> Self {
        Rel {
            kind: RelKind::Subquery {
                alias: alias.into(),
            },
            inputs: vec![self],
        }
    }

    /// `SELECT DISTINCT ...`
    pub fn distinct(self) -> Self {
        Rel {
            kind: RelKind::Distinct,
            inputs: vec![self],
        }
    }
}

// ---------------------------------------------------------------------------
// Plan
// ---------------------------------------------------------------------------

/// A complete query plan: a relation tree with output column names and CTEs.
#[derive(Debug, Clone)]
pub struct Plan {
    pub root: Rel,
    pub output_names: Vec<String>,
    pub ctes: Vec<CteDef>,
}

/// A Common Table Expression for WITH clauses.
#[derive(Debug, Clone)]
pub struct CteDef {
    pub name: String,
    pub plan: Plan,
    pub recursive: bool,
}

impl Rel {
    /// Finalize into a [`Plan`]. Output names are derived from the top-level
    /// relation (project aliases, read columns, etc.).
    pub fn into_plan(self) -> Plan {
        let output_names = self.output_names();
        Plan {
            root: self,
            output_names,
            ctes: Vec::new(),
        }
    }

    /// Finalize into a [`Plan`] with explicit output names.
    pub fn into_plan_named(self, names: &[&str]) -> Plan {
        Plan {
            root: self,
            output_names: names.iter().map(|n| (*n).into()).collect(),
            ctes: Vec::new(),
        }
    }

    /// Finalize into a [`Plan`] with CTEs.
    pub fn into_plan_with_ctes(self, ctes: Vec<CteDef>) -> Plan {
        let output_names = self.output_names();
        Plan {
            root: self,
            output_names,
            ctes,
        }
    }

    /// Derive output column names from the relation tree.
    pub fn output_names(&self) -> Vec<String> {
        self.output_columns()
            .into_iter()
            .map(|(_, name)| name)
            .collect()
    }

    /// Derive `(table_alias, column_name)` pairs from the relation tree.
    ///
    /// Used by `output_names()` (drops the alias) and by the Substrait
    /// encoder (needs aliases for positional column resolution).
    pub fn output_columns(&self) -> Vec<(String, String)> {
        match &self.kind {
            RelKind::Read { alias, columns, .. } => columns
                .iter()
                .map(|c| (alias.clone(), c.name.clone()))
                .collect(),
            RelKind::Project { expressions } => expressions
                .iter()
                .map(|(expr, alias)| {
                    let table = match expr {
                        Expr::Column { table, .. } => table.clone(),
                        _ => String::new(),
                    };
                    (table, alias.clone())
                })
                .collect(),
            RelKind::Filter { .. }
            | RelKind::Sort { .. }
            | RelKind::Fetch { .. }
            | RelKind::Distinct => self.inputs[0].output_columns(),
            RelKind::Aggregate {
                group_by, measures, ..
            } => {
                let mut cols: Vec<(String, String)> = group_by
                    .iter()
                    .map(|e| match e {
                        Expr::Column { table, name } => (table.clone(), name.clone()),
                        _ => (String::new(), "_expr".into()),
                    })
                    .collect();
                cols.extend(measures.iter().map(|m| (String::new(), m.alias.clone())));
                cols
            }
            RelKind::Join { .. } => {
                let mut cols = self.inputs[0].output_columns();
                cols.extend(self.inputs[1].output_columns());
                cols
            }
            RelKind::UnionAll { alias } => {
                if let Some(first) = self.inputs.first() {
                    first
                        .output_columns()
                        .into_iter()
                        .map(|(_, name)| (alias.clone(), name))
                        .collect()
                } else {
                    Vec::new()
                }
            }
            RelKind::Subquery { alias } => self.inputs[0]
                .output_columns()
                .into_iter()
                .map(|(_, name)| (alias.clone(), name))
                .collect(),
        }
    }
}

// ---------------------------------------------------------------------------
// Plan mutation API (for passes)
// ---------------------------------------------------------------------------

impl Plan {
    /// Collect `(table_name, alias)` pairs for all ReadRels whose table name
    /// satisfies `predicate`.
    pub fn filterable_aliases(&self, predicate: impl Fn(&str) -> bool) -> Vec<(String, String)> {
        let mut aliases = Vec::new();
        walk_rel_for_aliases(&self.root, &predicate, &mut aliases);
        aliases
    }

    /// Inject a filter expression on top of the root rel.
    pub fn inject_filter(&mut self, condition: Expr) {
        let existing = self.take_root();
        self.root = Rel {
            kind: RelKind::Filter { condition },
            inputs: vec![existing],
        };
    }

    /// Append projection items to the outermost `Project`. Walks through
    /// `Fetch`/`Sort` to find it. If no project exists, wraps the root in one.
    /// Duplicates (by alias) are skipped.
    pub fn extend_project(&mut self, items: Vec<(Expr, String)>) {
        self.mutate_project(|exprs| {
            for (e, alias) in items {
                if !exprs.iter().any(|(_, a)| *a == alias) {
                    exprs.push((e, alias));
                }
            }
        });
    }

    /// Insert a projection item immediately after the item whose alias matches
    /// `after`. Falls back to appending if `after` is not found.
    pub fn insert_project_after(&mut self, after: &str, item: (Expr, String)) {
        self.mutate_project(|exprs| {
            if exprs.iter().any(|(_, a)| *a == item.1) {
                return;
            }
            let pos = exprs
                .iter()
                .position(|(_, a)| a == after)
                .map(|i| i + 1)
                .unwrap_or(exprs.len());
            exprs.insert(pos, item);
        });
    }

    /// Add group-by expressions to the `Aggregate` node in the tree.
    /// Walks through `Fetch`/`Sort`/`Filter` to find it.
    pub fn extend_aggregate_groups(&mut self, items: Vec<(Expr, String)>) {
        fn walk(rel: &mut Rel, items: &[(Expr, String)]) -> bool {
            match &mut rel.kind {
                RelKind::Aggregate { group_by, .. } => {
                    for (e, _) in items {
                        if !group_by.iter().any(|g| g == e) {
                            group_by.push(e.clone());
                        }
                    }
                    true
                }
                RelKind::Fetch { .. } | RelKind::Sort { .. } | RelKind::Filter { .. } => {
                    walk(&mut rel.inputs[0], items)
                }
                _ => false,
            }
        }
        walk(&mut self.root, &items);
    }

    fn take_root(&mut self) -> Rel {
        std::mem::replace(
            &mut self.root,
            Rel {
                kind: RelKind::Read {
                    table: String::new(),
                    alias: String::new(),
                    columns: Vec::new(),
                },
                inputs: Vec::new(),
            },
        )
    }

    /// Mutate the outermost project's expression list. Walks through Fetch/Sort
    /// to find it. If no project exists, wraps the root in one.
    fn mutate_project(&mut self, f: impl FnOnce(&mut Vec<(Expr, String)>)) {
        fn find_project(rel: &mut Rel) -> Option<&mut Vec<(Expr, String)>> {
            match &mut rel.kind {
                RelKind::Project { expressions } => Some(expressions),
                RelKind::Fetch { .. } | RelKind::Sort { .. } => find_project(&mut rel.inputs[0]),
                _ => None,
            }
        }

        if let Some(exprs) = find_project(&mut self.root) {
            f(exprs);
        } else {
            let existing = self.take_root();
            let mut expressions = Vec::new();
            f(&mut expressions);
            self.root = Rel {
                kind: RelKind::Project { expressions },
                inputs: vec![existing],
            };
        }

        // Keep output_names in sync
        self.output_names = self.root.output_names();
    }
}

fn walk_rel_for_aliases(
    rel: &Rel,
    predicate: &impl Fn(&str) -> bool,
    aliases: &mut Vec<(String, String)>,
) {
    rel.walk(&mut |r| match &r.kind {
        RelKind::Read { table, alias, .. } if table != RAW_FROM_TAG && predicate(table) => {
            aliases.push((table.clone(), alias.clone()));
            true
        }
        // Don't recurse into UnionAll — arms are derived tables
        // secured transitively through join conditions.
        RelKind::UnionAll { .. } => false,
        _ => true,
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ir::expr::*;

    #[test]
    fn chainable_select() {
        let plan = Rel::read(
            "users",
            "u",
            &[("id", DataType::Int64), ("name", DataType::String)],
        )
        .filter(col("u", "id").eq(int(42)))
        .project(&[(col("u", "name"), "name")])
        .fetch(10, None)
        .into_plan();

        assert_eq!(plan.output_names, vec!["name"]);
    }

    #[test]
    fn chainable_join() {
        let projects = Rel::read("gl_project", "p", &[("id", DataType::Int64)]);
        let mrs = Rel::read("gl_merge_request", "mr", &[("project_id", DataType::Int64)]);

        let plan = projects
            .join(
                JoinType::Inner,
                mrs,
                col("p", "id").eq(col("mr", "project_id")),
            )
            .project(&[(col("p", "id"), "id")])
            .into_plan();

        assert_eq!(plan.output_names, vec!["id"]);
    }

    #[test]
    fn chainable_aggregate() {
        let plan = Rel::read(
            "gl_project",
            "p",
            &[("namespace_id", DataType::Int64), ("id", DataType::Int64)],
        )
        .aggregate(
            &[col("p", "namespace_id")],
            &[Measure::new("count", &[col("p", "id")], "cnt")],
        )
        .into_plan();

        assert_eq!(plan.output_names, vec!["namespace_id", "cnt"]);
    }

    #[test]
    fn chainable_union_all() {
        let a = Rel::read("t1", "a", &[("id", DataType::Int64)]);
        let b = Rel::read("t2", "b", &[("id", DataType::Int64)]);

        let plan = Rel::union_all(vec![a, b], "combined")
            .project(&[(col("combined", "id"), "id")])
            .into_plan();

        assert_eq!(plan.output_names, vec!["id"]);
    }

    #[test]
    fn filterable_aliases() {
        let projects = Rel::read(
            "gl_project",
            "p",
            &[
                ("id", DataType::Int64),
                ("traversal_path", DataType::String),
            ],
        );
        let users = Rel::read("gl_user", "u", &[("id", DataType::Int64)]);
        let other = Rel::read("custom", "c", &[("id", DataType::Int64)]);

        let plan = projects
            .join(JoinType::Inner, users, col("p", "id").eq(col("u", "id")))
            .join(JoinType::Inner, other, col("p", "id").eq(col("c", "id")))
            .into_plan();

        let gl_aliases = plan.filterable_aliases(|t| t.starts_with("gl_"));
        assert_eq!(gl_aliases.len(), 2);
        assert!(gl_aliases.iter().any(|(t, _)| t == "gl_project"));
        assert!(gl_aliases.iter().any(|(t, _)| t == "gl_user"));
    }

    #[test]
    fn inject_filter() {
        let mut plan = Rel::read("gl_project", "p", &[("id", DataType::Int64)])
            .project(&[(col("p", "id"), "id")])
            .into_plan();

        plan.inject_filter(col("p", "id").eq(int(1)));

        assert!(matches!(plan.root.kind, RelKind::Filter { .. }));
    }

    #[test]
    fn cte_plan() {
        let cte_plan = Rel::read(
            "gl_project",
            "p",
            &[("id", DataType::Int64), ("name", DataType::String)],
        )
        .project(&[(col("p", "id"), "node_id")])
        .into_plan();

        let plan = Rel::read("base", "b", &[("node_id", DataType::Int64)])
            .project(&[(col("b", "node_id"), "node_id")])
            .into_plan_with_ctes(vec![CteDef {
                name: "base".into(),
                plan: cte_plan,
                recursive: false,
            }]);

        assert_eq!(plan.ctes.len(), 1);
        assert_eq!(plan.ctes[0].name, "base");
    }

    #[test]
    fn subquery() {
        let plan = Rel::read("gl_project", "p", &[("id", DataType::Int64)])
            .project(&[(col("p", "id"), "id")])
            .subquery("sq")
            .project(&[(col("sq", "id"), "id")])
            .into_plan();

        assert_eq!(plan.output_names, vec!["id"]);
        assert!(matches!(plan.root.kind, RelKind::Project { .. }));
    }

    #[test]
    fn extend_project_appends() {
        let mut plan = Rel::read(
            "gl_user",
            "u",
            &[("id", DataType::Int64), ("name", DataType::String)],
        )
        .project(&[(col("u", "name"), "name")])
        .fetch(10, None)
        .into_plan();

        plan.extend_project(vec![
            (col("u", "id"), "_gkg_u_id".into()),
            (string("User"), "_gkg_u_type".into()),
        ]);

        assert_eq!(plan.output_names, vec!["name", "_gkg_u_id", "_gkg_u_type"]);
    }

    #[test]
    fn extend_project_deduplicates() {
        let mut plan = Rel::read("t", "t", &[("id", DataType::Int64)])
            .project(&[(col("t", "id"), "id")])
            .into_plan();

        plan.extend_project(vec![(col("t", "id"), "id".into())]);
        assert_eq!(plan.output_names, vec!["id"]);
    }

    #[test]
    fn insert_project_after() {
        let mut plan = Rel::read("t", "t", &[("id", DataType::Int64)])
            .project(&[(col("t", "id"), "_gkg_u_id")])
            .into_plan();

        plan.insert_project_after("_gkg_u_id", (string("User"), "_gkg_u_type".into()));

        assert_eq!(plan.output_names, vec!["_gkg_u_id", "_gkg_u_type"]);
    }

    #[test]
    fn extend_aggregate_groups() {
        let mut plan = Rel::read(
            "gl_user",
            "u",
            &[("id", DataType::Int64), ("username", DataType::String)],
        )
        .aggregate(
            &[col("u", "username")],
            &[Measure::new("count", &[col("u", "id")], "cnt")],
        )
        .fetch(10, None)
        .into_plan();

        plan.extend_aggregate_groups(vec![(col("u", "id"), "_gkg_u_id".into())]);

        if let RelKind::Fetch { .. } = &plan.root.kind
            && let RelKind::Aggregate { group_by, .. } = &plan.root.inputs[0].kind
        {
            assert_eq!(group_by.len(), 2);
            return;
        }
        panic!("expected Fetch(Aggregate(...))");
    }

    #[test]
    fn extend_project_creates_project_when_missing() {
        let mut plan = Rel::read("t", "t", &[("id", DataType::Int64)]).into_plan();

        plan.extend_project(vec![(col("t", "id"), "_gkg_t_id".into())]);

        assert!(matches!(plan.root.kind, RelKind::Project { .. }));
        assert_eq!(plan.output_names, vec!["_gkg_t_id"]);
    }
}
