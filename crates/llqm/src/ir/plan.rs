//! Relation tree and chainable query builder.
//!
//! The plan is an abstract relation tree composed of [`Rel`] nodes that store
//! [`Expr`](crate::ir::expr::Expr) expressions directly — no positional
//! resolution, no Substrait types. Backends walk the tree to emit SQL or
//! encode to Substrait for DataFusion.
//!
//! ```text
//! Rel::read("gl_project", "p", &[("id", Int64), ("name", String)])
//!     .filter(eq(col("p", "id"), int(42)))
//!     .project(&[(col("p", "name"), "name")])
//!     .fetch(10, None)
//!     .into_plan()
//! ```

use crate::ir::expr::{DataType, Expr, JoinType, SortDir};

// ---------------------------------------------------------------------------
// Relation tree
// ---------------------------------------------------------------------------

/// A node in the relation tree.
#[derive(Debug, Clone)]
pub enum Rel {
    Read(ReadRel),
    Filter(FilterRel),
    Project(ProjectRel),
    Join(JoinRel),
    Sort(SortRel),
    Fetch(FetchRel),
    Aggregate(AggregateRel),
    UnionAll(UnionAllRel),
    Subquery(SubqueryRel),
}

#[derive(Debug, Clone)]
pub struct ReadRel {
    pub table: String,
    pub alias: String,
    pub columns: Vec<ColumnDef>,
}

/// Raw FROM clause — escape hatch for verbatim SQL in the FROM position.
#[derive(Debug, Clone)]
pub struct RawFrom(pub String);

#[derive(Debug, Clone)]
pub struct ColumnDef {
    pub name: String,
    pub data_type: DataType,
}

#[derive(Debug, Clone)]
pub struct FilterRel {
    pub input: Box<Rel>,
    pub condition: Expr,
}

#[derive(Debug, Clone)]
pub struct ProjectRel {
    pub input: Box<Rel>,
    pub expressions: Vec<(Expr, String)>,
}

#[derive(Debug, Clone)]
pub struct JoinRel {
    pub left: Box<Rel>,
    pub right: Box<Rel>,
    pub join_type: JoinType,
    pub condition: Expr,
}

#[derive(Debug, Clone)]
pub struct SortRel {
    pub input: Box<Rel>,
    pub sorts: Vec<SortSpec>,
}

#[derive(Debug, Clone)]
pub struct SortSpec {
    pub expr: Expr,
    pub direction: SortDir,
}

#[derive(Debug, Clone)]
pub struct FetchRel {
    pub input: Box<Rel>,
    pub limit: u64,
    pub offset: Option<u64>,
}

#[derive(Debug, Clone)]
pub struct AggregateRel {
    pub input: Box<Rel>,
    pub group_by: Vec<Expr>,
    pub measures: Vec<Measure>,
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

#[derive(Debug, Clone)]
pub struct UnionAllRel {
    pub inputs: Vec<Rel>,
    pub alias: String,
}

#[derive(Debug, Clone)]
pub struct SubqueryRel {
    pub input: Box<Rel>,
    pub alias: String,
}

// ---------------------------------------------------------------------------
// Chainable API
// ---------------------------------------------------------------------------

impl Rel {
    /// Table scan: `FROM table AS alias`
    pub fn read(table: &str, alias: &str, columns: &[(&str, DataType)]) -> Self {
        Rel::Read(ReadRel {
            table: table.into(),
            alias: alias.into(),
            columns: columns
                .iter()
                .map(|(name, dt)| ColumnDef {
                    name: (*name).into(),
                    data_type: dt.clone(),
                })
                .collect(),
        })
    }

    /// Raw FROM clause: verbatim SQL in the FROM position.
    ///
    /// Columns define the output schema for downstream references.
    /// Column references use empty table alias (unqualified).
    pub fn read_raw(raw_from: &str, columns: &[(&str, DataType)]) -> Self {
        Rel::Read(ReadRel {
            table: RawFrom::TAG.into(),
            alias: String::new(),
            columns: columns
                .iter()
                .map(|(name, dt)| ColumnDef {
                    name: (*name).into(),
                    data_type: dt.clone(),
                })
                .collect(),
        })
        .with_raw_from(raw_from)
    }

    /// `WHERE condition`
    pub fn filter(self, condition: Expr) -> Self {
        Rel::Filter(FilterRel {
            input: Box::new(self),
            condition,
        })
    }

    /// `SELECT expr1 AS alias1, expr2 AS alias2, ...`
    pub fn project(self, exprs: &[(Expr, &str)]) -> Self {
        Rel::Project(ProjectRel {
            input: Box::new(self),
            expressions: exprs
                .iter()
                .map(|(e, a)| (e.clone(), (*a).into()))
                .collect(),
        })
    }

    /// `self JOIN right ON condition`
    pub fn join(self, join_type: JoinType, right: Rel, condition: Expr) -> Self {
        Rel::Join(JoinRel {
            left: Box::new(self),
            right: Box::new(right),
            join_type,
            condition,
        })
    }

    /// `ORDER BY key1 dir1, key2 dir2, ...`
    pub fn sort(self, keys: &[(Expr, SortDir)]) -> Self {
        Rel::Sort(SortRel {
            input: Box::new(self),
            sorts: keys
                .iter()
                .map(|(e, d)| SortSpec {
                    expr: e.clone(),
                    direction: *d,
                })
                .collect(),
        })
    }

    /// `LIMIT count [OFFSET offset]`
    pub fn fetch(self, limit: u64, offset: Option<u64>) -> Self {
        Rel::Fetch(FetchRel {
            input: Box::new(self),
            limit,
            offset,
        })
    }

    /// `SELECT agg(args) AS alias, ... FROM self GROUP BY group_exprs`
    pub fn aggregate(self, group_by: &[Expr], measures: &[Measure]) -> Self {
        Rel::Aggregate(AggregateRel {
            input: Box::new(self),
            group_by: group_by.to_vec(),
            measures: measures.to_vec(),
        })
    }

    /// `UNION ALL` of multiple relations, aliased for outer references.
    pub fn union_all(inputs: Vec<Rel>, alias: &str) -> Self {
        assert!(!inputs.is_empty(), "union_all requires at least one input");
        Rel::UnionAll(UnionAllRel {
            inputs,
            alias: alias.into(),
        })
    }

    /// Wrap as `(SELECT ...) AS alias` derived table.
    pub fn subquery(self, alias: &str) -> Self {
        Rel::Subquery(SubqueryRel {
            input: Box::new(self),
            alias: alias.into(),
        })
    }
}

impl RawFrom {
    pub const TAG: &str = "__raw_from";
}

// Attach raw_from metadata. Stored as a Subquery wrapping a Read with
// a sentinel table name, so the backend can detect it.
impl Rel {
    fn with_raw_from(self, raw_from: &str) -> Self {
        // We encode raw_from as a special ReadRel where table == TAG
        // and alias is the raw SQL. This avoids needing extra fields.
        if let Rel::Read(mut r) = self {
            r.table = RawFrom::TAG.into();
            r.alias = raw_from.into();
            Rel::Read(r)
        } else {
            self
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
        match self {
            Rel::Read(r) => r.columns.iter().map(|c| c.name.clone()).collect(),
            Rel::Project(p) => p
                .expressions
                .iter()
                .map(|(_, alias)| alias.clone())
                .collect(),
            Rel::Filter(f) => f.input.output_names(),
            Rel::Sort(s) => s.input.output_names(),
            Rel::Fetch(f) => f.input.output_names(),
            Rel::Aggregate(a) => {
                let mut names: Vec<String> = a
                    .group_by
                    .iter()
                    .map(|e| match e {
                        Expr::Column { name, .. } => name.clone(),
                        _ => "_expr".into(),
                    })
                    .collect();
                names.extend(a.measures.iter().map(|m| m.alias.clone()));
                names
            }
            Rel::Join(j) => {
                let mut names = j.left.output_names();
                names.extend(j.right.output_names());
                names
            }
            Rel::UnionAll(u) => {
                if let Some(first) = u.inputs.first() {
                    first.output_names()
                } else {
                    Vec::new()
                }
            }
            Rel::Subquery(s) => s.input.output_names(),
        }
    }
}

// ---------------------------------------------------------------------------
// Plan mutation API (for passes)
// ---------------------------------------------------------------------------

impl Plan {
    /// Collect `(table_name, alias)` pairs for all ReadRels whose table name
    /// satisfies `predicate`.
    pub fn filterable_aliases(&self, predicate: fn(&str) -> bool) -> Vec<(String, String)> {
        let mut aliases = Vec::new();
        walk_rel_for_aliases(&self.root, predicate, &mut aliases);
        aliases
    }

    /// Inject a filter expression on top of the root rel.
    pub fn inject_filter(&mut self, condition: Expr) {
        let existing = self.take_root();
        self.root = Rel::Filter(FilterRel {
            input: Box::new(existing),
            condition,
        });
    }

    /// Add projection items to the outermost `Project`. If the root isn't a
    /// `Project` (or is wrapped in `Fetch`/`Sort`), the items are appended to
    /// the existing project or a new project is created.
    ///
    /// `position` controls where items are inserted: `None` appends, `Some(fn)`
    /// calls the function to find the insert index for each item.
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
            match rel {
                Rel::Aggregate(a) => {
                    for (e, _) in items {
                        if !a.group_by.iter().any(|g| g == e) {
                            a.group_by.push(e.clone());
                        }
                    }
                    true
                }
                Rel::Fetch(f) => walk(&mut f.input, items),
                Rel::Sort(s) => walk(&mut s.input, items),
                Rel::Filter(f) => walk(&mut f.input, items),
                _ => false,
            }
        }
        walk(&mut self.root, &items);
    }

    fn take_root(&mut self) -> Rel {
        std::mem::replace(
            &mut self.root,
            Rel::Read(ReadRel {
                table: String::new(),
                alias: String::new(),
                columns: Vec::new(),
            }),
        )
    }

    /// Mutate the outermost project's expression list. Walks through Fetch/Sort
    /// to find it. If no project exists, wraps the root in one.
    fn mutate_project(&mut self, f: impl FnOnce(&mut Vec<(Expr, String)>)) {
        fn find_project(rel: &mut Rel) -> Option<&mut Vec<(Expr, String)>> {
            match rel {
                Rel::Project(p) => Some(&mut p.expressions),
                Rel::Fetch(fe) => find_project(&mut fe.input),
                Rel::Sort(s) => find_project(&mut s.input),
                _ => None,
            }
        }

        if let Some(exprs) = find_project(&mut self.root) {
            f(exprs);
        } else {
            // Wrap root in a project
            let existing = self.take_root();
            let mut expressions = Vec::new();
            f(&mut expressions);
            self.root = Rel::Project(ProjectRel {
                input: Box::new(existing),
                expressions,
            });
        }

        // Keep output_names in sync
        self.output_names = self.root.output_names();
    }
}

fn walk_rel_for_aliases(
    rel: &Rel,
    predicate: fn(&str) -> bool,
    aliases: &mut Vec<(String, String)>,
) {
    match rel {
        Rel::Read(r) if r.table != RawFrom::TAG => {
            if predicate(&r.table) {
                aliases.push((r.table.clone(), r.alias.clone()));
            }
        }
        Rel::Read(_) => {}
        Rel::Filter(f) => walk_rel_for_aliases(&f.input, predicate, aliases),
        Rel::Project(p) => walk_rel_for_aliases(&p.input, predicate, aliases),
        Rel::Join(j) => {
            walk_rel_for_aliases(&j.left, predicate, aliases);
            walk_rel_for_aliases(&j.right, predicate, aliases);
        }
        Rel::Sort(s) => walk_rel_for_aliases(&s.input, predicate, aliases),
        Rel::Fetch(f) => walk_rel_for_aliases(&f.input, predicate, aliases),
        Rel::Aggregate(a) => walk_rel_for_aliases(&a.input, predicate, aliases),
        Rel::Subquery(s) => walk_rel_for_aliases(&s.input, predicate, aliases),
        // Don't recurse into UnionAll — arms are derived tables
        // secured transitively through join conditions.
        Rel::UnionAll(_) => {}
    }
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
        .filter(eq(col("u", "id"), int(42)))
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
                eq(col("p", "id"), col("mr", "project_id")),
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
            .join(JoinType::Inner, users, eq(col("p", "id"), col("u", "id")))
            .join(JoinType::Inner, other, eq(col("p", "id"), col("c", "id")))
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

        plan.inject_filter(eq(col("p", "id"), int(1)));

        assert!(matches!(plan.root, Rel::Filter(_)));
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
        assert!(matches!(plan.root, Rel::Project(_)));
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

        // Verify the aggregate now has 2 group-by expressions
        if let Rel::Fetch(f) = &plan.root
            && let Rel::Aggregate(a) = &*f.input
        {
            assert_eq!(a.group_by.len(), 2);
            return;
        }
        panic!("expected Fetch(Aggregate(...))");
    }

    #[test]
    fn extend_project_creates_project_when_missing() {
        let mut plan = Rel::read("t", "t", &[("id", DataType::Int64)]).into_plan();

        plan.extend_project(vec![(col("t", "id"), "_gkg_t_id".into())]);

        assert!(matches!(plan.root, Rel::Project(_)));
        assert_eq!(plan.output_names, vec!["_gkg_t_id"]);
    }
}
