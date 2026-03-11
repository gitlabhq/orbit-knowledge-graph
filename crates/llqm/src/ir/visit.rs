//! Generic tree traversal and rewriting for [`Rel`], [`Expr`], and [`Plan`].
//!
//! Passes import this module to walk or transform relation/expression trees
//! without hand-writing per-variant recursion.

use crate::ir::expr::Expr;
use crate::ir::plan::{CteDef, Plan, Rel};

// ---------------------------------------------------------------------------
// Immutable walk
// ---------------------------------------------------------------------------

impl Rel {
    /// Pre-order depth-first walk.
    ///
    /// The visitor returns `true` to recurse into children, `false` to skip
    /// the current node's subtree (siblings continue).
    ///
    /// ```ignore
    /// rel.walk(&mut |r| {
    ///     println!("{:?}", r.kind);
    ///     true // recurse into children
    /// });
    /// ```
    pub fn walk(&self, visitor: &mut impl FnMut(&Rel) -> bool) {
        if !visitor(self) {
            return;
        }
        for child in &self.inputs {
            child.walk(visitor);
        }
    }

    /// Pre-order mutable walk.
    ///
    /// The visitor may mutate the node's `kind` or `inputs` in place.
    /// Returns `true` to recurse into children (after mutation),
    /// `false` to skip the subtree.
    pub fn walk_mut(&mut self, visitor: &mut impl FnMut(&mut Rel) -> bool) {
        if !visitor(self) {
            return;
        }
        for child in &mut self.inputs {
            child.walk_mut(visitor);
        }
    }
}

// ---------------------------------------------------------------------------
// Bottom-up transform
// ---------------------------------------------------------------------------

impl Rel {
    /// Bottom-up (post-order) transform.
    ///
    /// Children are transformed first, then `f` receives the node with its
    /// already-rewritten children. Returns a new tree.
    ///
    /// ```ignore
    /// let new_plan = plan.root.transform(&mut |rel| {
    ///     // rel.inputs are already transformed at this point
    ///     rel
    /// });
    /// ```
    pub fn transform(self, f: &mut impl FnMut(Rel) -> Rel) -> Rel {
        let Rel { kind, inputs } = self;
        let inputs = inputs.into_iter().map(|c| c.transform(&mut *f)).collect();
        f(Rel { kind, inputs })
    }

    /// Fallible bottom-up transform.
    ///
    /// Stops and propagates the first error encountered.
    pub fn try_transform<E>(self, f: &mut impl FnMut(Rel) -> Result<Rel, E>) -> Result<Rel, E> {
        let Rel { kind, inputs } = self;
        let inputs = inputs
            .into_iter()
            .map(|c| c.try_transform(&mut *f))
            .collect::<Result<_, _>>()?;
        f(Rel { kind, inputs })
    }
}

// ---------------------------------------------------------------------------
// Expr traversal
// ---------------------------------------------------------------------------

/// Helper: call `visitor` on each child expression of an `Expr`.
fn walk_expr_children(expr: &Expr, visitor: &mut impl FnMut(&Expr) -> bool) {
    match expr {
        Expr::BinaryOp { left, right, .. } => {
            left.walk(visitor);
            right.walk(visitor);
        }
        Expr::UnaryOp { operand, .. } => operand.walk(visitor),
        Expr::FuncCall { args, .. } => {
            for arg in args {
                arg.walk(visitor);
            }
        }
        Expr::Cast { expr, .. } => expr.walk(visitor),
        Expr::IfThen { ifs, else_expr } => {
            for (cond, val) in ifs {
                cond.walk(visitor);
                val.walk(visitor);
            }
            if let Some(e) = else_expr {
                e.walk(visitor);
            }
        }
        Expr::InList { expr, list } => {
            expr.walk(visitor);
            for item in list {
                item.walk(visitor);
            }
        }
        Expr::Column { .. } | Expr::Literal(_) | Expr::Param { .. } | Expr::Raw(_) => {}
    }
}

/// Helper: call `visitor` on each child expression of an `Expr` (mutable).
fn walk_expr_children_mut(expr: &mut Expr, visitor: &mut impl FnMut(&mut Expr) -> bool) {
    match expr {
        Expr::BinaryOp { left, right, .. } => {
            left.walk_mut(visitor);
            right.walk_mut(visitor);
        }
        Expr::UnaryOp { operand, .. } => operand.walk_mut(visitor),
        Expr::FuncCall { args, .. } => {
            for arg in args {
                arg.walk_mut(visitor);
            }
        }
        Expr::Cast { expr, .. } => expr.walk_mut(visitor),
        Expr::IfThen { ifs, else_expr } => {
            for (cond, val) in ifs {
                cond.walk_mut(visitor);
                val.walk_mut(visitor);
            }
            if let Some(e) = else_expr {
                e.walk_mut(visitor);
            }
        }
        Expr::InList { expr, list } => {
            expr.walk_mut(visitor);
            for item in list {
                item.walk_mut(visitor);
            }
        }
        Expr::Column { .. } | Expr::Literal(_) | Expr::Param { .. } | Expr::Raw(_) => {}
    }
}

/// Transform each child expression, collecting into a new `Expr` of the same variant.
fn transform_expr_children(expr: Expr, f: &mut impl FnMut(Expr) -> Expr) -> Expr {
    match expr {
        Expr::BinaryOp { op, left, right } => Expr::BinaryOp {
            op,
            left: Box::new(left.transform(f)),
            right: Box::new(right.transform(f)),
        },
        Expr::UnaryOp { op, operand } => Expr::UnaryOp {
            op,
            operand: Box::new(operand.transform(f)),
        },
        Expr::FuncCall { name, args } => Expr::FuncCall {
            name,
            args: args.into_iter().map(|a| a.transform(&mut *f)).collect(),
        },
        Expr::Cast { expr, target_type } => Expr::Cast {
            expr: Box::new(expr.transform(f)),
            target_type,
        },
        Expr::IfThen { ifs, else_expr } => Expr::IfThen {
            ifs: ifs
                .into_iter()
                .map(|(c, v)| (c.transform(&mut *f), v.transform(&mut *f)))
                .collect(),
            else_expr: else_expr.map(|e| Box::new(e.transform(f))),
        },
        Expr::InList { expr, list } => Expr::InList {
            expr: Box::new(expr.transform(f)),
            list: list.into_iter().map(|i| i.transform(&mut *f)).collect(),
        },
        leaf @ (Expr::Column { .. } | Expr::Literal(_) | Expr::Param { .. } | Expr::Raw(_)) => leaf,
    }
}

/// Fallible version of `transform_expr_children`.
fn try_transform_expr_children<E>(
    expr: Expr,
    f: &mut impl FnMut(Expr) -> Result<Expr, E>,
) -> Result<Expr, E> {
    match expr {
        Expr::BinaryOp { op, left, right } => Ok(Expr::BinaryOp {
            op,
            left: Box::new(left.try_transform(f)?),
            right: Box::new(right.try_transform(f)?),
        }),
        Expr::UnaryOp { op, operand } => Ok(Expr::UnaryOp {
            op,
            operand: Box::new(operand.try_transform(f)?),
        }),
        Expr::FuncCall { name, args } => Ok(Expr::FuncCall {
            name,
            args: args
                .into_iter()
                .map(|a| a.try_transform(&mut *f))
                .collect::<Result<_, _>>()?,
        }),
        Expr::Cast { expr, target_type } => Ok(Expr::Cast {
            expr: Box::new(expr.try_transform(f)?),
            target_type,
        }),
        Expr::IfThen { ifs, else_expr } => Ok(Expr::IfThen {
            ifs: ifs
                .into_iter()
                .map(|(c, v)| Ok((c.try_transform(&mut *f)?, v.try_transform(&mut *f)?)))
                .collect::<Result<_, E>>()?,
            else_expr: else_expr
                .map(|e| e.try_transform(f).map(Box::new))
                .transpose()?,
        }),
        Expr::InList { expr, list } => Ok(Expr::InList {
            expr: Box::new(expr.try_transform(f)?),
            list: list
                .into_iter()
                .map(|i| i.try_transform(&mut *f))
                .collect::<Result<_, _>>()?,
        }),
        leaf @ (Expr::Column { .. } | Expr::Literal(_) | Expr::Param { .. } | Expr::Raw(_)) => {
            Ok(leaf)
        }
    }
}

impl Expr {
    /// Pre-order depth-first walk over expression tree.
    ///
    /// The visitor returns `true` to recurse into children, `false` to skip
    /// the current node's subtree.
    pub fn walk(&self, visitor: &mut impl FnMut(&Expr) -> bool) {
        if !visitor(self) {
            return;
        }
        walk_expr_children(self, visitor);
    }

    /// Pre-order mutable walk over expression tree.
    pub fn walk_mut(&mut self, visitor: &mut impl FnMut(&mut Expr) -> bool) {
        if !visitor(self) {
            return;
        }
        walk_expr_children_mut(self, visitor);
    }

    /// Bottom-up (post-order) transform of expression tree.
    ///
    /// Children are transformed first, then `f` receives the node with
    /// already-rewritten children.
    pub fn transform(self, f: &mut impl FnMut(Expr) -> Expr) -> Expr {
        let rewritten_children = transform_expr_children(self, &mut *f);
        f(rewritten_children)
    }

    /// Fallible bottom-up transform of expression tree.
    pub fn try_transform<E>(self, f: &mut impl FnMut(Expr) -> Result<Expr, E>) -> Result<Expr, E> {
        let rewritten_children = try_transform_expr_children(self, &mut *f)?;
        f(rewritten_children)
    }
}

// ---------------------------------------------------------------------------
// Plan-level traversal
// ---------------------------------------------------------------------------

impl Plan {
    /// Walk every `Rel` node in the plan (root + all CTE roots).
    pub fn walk_rels(&self, visitor: &mut impl FnMut(&Rel) -> bool) {
        self.root.walk(visitor);
        for cte in &self.ctes {
            cte.plan.walk_rels(visitor);
        }
    }

    /// Bottom-up transform of every `Rel` in the plan (root + CTEs).
    ///
    /// Output names are recomputed from the transformed root.
    pub fn transform_rels(self, f: &mut impl FnMut(Rel) -> Rel) -> Plan {
        let root = self.root.transform(&mut *f);
        let output_names = root.output_names();
        let ctes = self
            .ctes
            .into_iter()
            .map(|cte| CteDef {
                name: cte.name,
                plan: cte.plan.transform_rels(&mut *f),
                recursive: cte.recursive,
            })
            .collect();
        Plan {
            root,
            output_names,
            ctes,
        }
    }

    /// Fallible bottom-up transform of every `Rel` in the plan.
    pub fn try_transform_rels<E>(
        self,
        f: &mut impl FnMut(Rel) -> Result<Rel, E>,
    ) -> Result<Plan, E> {
        let root = self.root.try_transform(&mut *f)?;
        let output_names = root.output_names();
        let ctes = self
            .ctes
            .into_iter()
            .map(|cte| {
                Ok(CteDef {
                    name: cte.name,
                    plan: cte.plan.try_transform_rels(&mut *f)?,
                    recursive: cte.recursive,
                })
            })
            .collect::<Result<_, E>>()?;
        Ok(Plan {
            root,
            output_names,
            ctes,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ir::expr::*;
    use crate::ir::plan::RelKind;

    fn sample_tree() -> Rel {
        Rel::read("gl_project", "p", &[("id", DataType::Int64)])
            .filter(col("p", "id").eq(int(42)))
            .project(&[(col("p", "id"), "id")])
            .fetch(10, None)
    }

    // -- walk --

    #[test]
    fn walk_visits_all_nodes() {
        let rel = sample_tree();
        let mut kinds = Vec::new();
        rel.walk(&mut |r| {
            kinds.push(std::mem::discriminant(&r.kind));
            true
        });
        // Fetch → Project → Filter → Read
        assert_eq!(kinds.len(), 4);
    }

    #[test]
    fn walk_skips_children_on_false() {
        let rel = sample_tree();
        let mut count = 0;
        rel.walk(&mut |r| {
            count += 1;
            // Stop at Project, don't visit Filter or Read
            !matches!(r.kind, RelKind::Project { .. })
        });
        // Fetch → Project (stops here)
        assert_eq!(count, 2);
    }

    #[test]
    fn walk_handles_join() {
        let left = Rel::read("t1", "a", &[("id", DataType::Int64)]);
        let right = Rel::read("t2", "b", &[("id", DataType::Int64)]);
        let rel = left.join(JoinType::Inner, right, col("a", "id").eq(col("b", "id")));

        let mut count = 0;
        rel.walk(&mut |_| {
            count += 1;
            true
        });
        // Join → Read(t1), Read(t2)
        assert_eq!(count, 3);
    }

    #[test]
    fn walk_handles_union_all() {
        let a = Rel::read("t1", "a", &[("id", DataType::Int64)]);
        let b = Rel::read("t2", "b", &[("id", DataType::Int64)]);
        let c = Rel::read("t3", "c", &[("id", DataType::Int64)]);
        let rel = Rel::union_all(vec![a, b, c], "u");

        let mut count = 0;
        rel.walk(&mut |_| {
            count += 1;
            true
        });
        // UnionAll → Read(t1), Read(t2), Read(t3)
        assert_eq!(count, 4);
    }

    // -- walk_mut --

    #[test]
    fn walk_mut_can_modify_nodes() {
        let mut rel = Rel::read("old_table", "t", &[("id", DataType::Int64)])
            .filter(col("t", "id").eq(int(1)));

        rel.walk_mut(&mut |r| {
            if let RelKind::Read { table, .. } = &mut r.kind {
                *table = "new_table".into();
            }
            true
        });

        if let RelKind::Read { table, .. } = &rel.inputs[0].kind {
            assert_eq!(table, "new_table");
        } else {
            panic!("expected Read");
        }
    }

    // -- transform --

    #[test]
    fn transform_identity() {
        let rel = sample_tree();
        let original_names = rel.output_names();
        let transformed = rel.transform(&mut |r| r);
        assert_eq!(transformed.output_names(), original_names);
    }

    #[test]
    fn transform_rewrites_bottom_up() {
        // Inject a limit of 5 around every Read
        let rel = Rel::read("t", "t", &[("id", DataType::Int64)]).filter(col("t", "id").eq(int(1)));

        let transformed = rel.transform(&mut |r| {
            if matches!(r.kind, RelKind::Read { .. }) {
                r.fetch(5, None)
            } else {
                r
            }
        });

        // Filter → Fetch → Read (Fetch was injected between Filter and Read)
        assert!(matches!(transformed.kind, RelKind::Filter { .. }));
        assert!(matches!(
            transformed.inputs[0].kind,
            RelKind::Fetch { limit: 5, .. }
        ));
        assert!(matches!(
            transformed.inputs[0].inputs[0].kind,
            RelKind::Read { .. }
        ));
    }

    #[test]
    fn transform_rewrites_join_children() {
        let left = Rel::read("t1", "a", &[("id", DataType::Int64)]);
        let right = Rel::read("t2", "b", &[("id", DataType::Int64)]);
        let rel = left.join(JoinType::Inner, right, col("a", "id").eq(col("b", "id")));

        // Rename all tables to "renamed"
        let transformed = rel.transform(&mut |mut r| {
            if let RelKind::Read { table, .. } = &mut r.kind {
                *table = "renamed".into();
            }
            r
        });

        if let RelKind::Read { table, .. } = &transformed.inputs[0].kind {
            assert_eq!(table, "renamed");
        }
        if let RelKind::Read { table, .. } = &transformed.inputs[1].kind {
            assert_eq!(table, "renamed");
        }
    }

    // -- try_transform --

    #[test]
    fn try_transform_propagates_error() {
        let rel = sample_tree();

        let result = rel.try_transform(&mut |r| {
            if matches!(r.kind, RelKind::Filter { .. }) {
                Err("no filters allowed")
            } else {
                Ok(r)
            }
        });

        assert_eq!(result.unwrap_err(), "no filters allowed");
    }

    #[test]
    fn try_transform_succeeds() {
        let rel = sample_tree();
        let result: Result<Rel, &str> = rel.try_transform(&mut |r| Ok(r));
        assert!(result.is_ok());
    }

    // -- Plan-level --

    #[test]
    fn plan_walk_rels_includes_ctes() {
        let cte_plan = Rel::read("gl_project", "p", &[("id", DataType::Int64)])
            .project(&[(col("p", "id"), "id")])
            .into_plan();

        let plan = Rel::read("base", "b", &[("id", DataType::Int64)])
            .project(&[(col("b", "id"), "id")])
            .into_plan_with_ctes(vec![CteDef {
                name: "base".into(),
                plan: cte_plan,
                recursive: false,
            }]);

        let mut tables = Vec::new();
        plan.walk_rels(&mut |r| {
            if let RelKind::Read { table, .. } = &r.kind {
                tables.push(table.clone());
            }
            true
        });

        assert!(tables.contains(&"base".to_string()));
        assert!(tables.contains(&"gl_project".to_string()));
    }

    #[test]
    fn plan_transform_rels_updates_output_names() {
        let plan = Rel::read("t", "t", &[("id", DataType::Int64)])
            .project(&[(col("t", "id"), "id")])
            .into_plan();

        assert_eq!(plan.output_names, vec!["id"]);

        // Wrap root project in a new project that renames the column
        let plan = plan.transform_rels(&mut |r| {
            if let RelKind::Project { expressions } = &r.kind
                && expressions.iter().any(|(_, a)| a == "id")
            {
                return r.project(&[(col("t", "id"), "node_id")]);
            }
            r
        });

        // output_names should reflect the outermost project
        assert_eq!(plan.output_names, vec!["node_id"]);
    }

    #[test]
    fn plan_try_transform_rels_propagates_error() {
        let plan = Rel::read("t", "t", &[("id", DataType::Int64)])
            .project(&[(col("t", "id"), "id")])
            .into_plan();

        let result = plan.try_transform_rels(&mut |r| {
            if matches!(r.kind, RelKind::Read { .. }) {
                Err("boom")
            } else {
                Ok(r)
            }
        });

        assert_eq!(result.unwrap_err(), "boom");
    }

    // -----------------------------------------------------------------------
    // Expr traversal
    // -----------------------------------------------------------------------

    fn sample_expr() -> Expr {
        // (p.id = 42) AND startsWith(p.path, 'foo')
        col("p", "id")
            .eq(int(42))
            .and(col("p", "path").starts_with(string("foo")))
    }

    #[test]
    fn expr_walk_visits_all_nodes() {
        let expr = sample_expr();
        let mut count = 0;
        expr.walk(&mut |_| {
            count += 1;
            true
        });
        // AND(BinaryOp(Eq(Column, Literal)), FuncCall(Column, Literal))
        // = 1(AND) + 1(Eq) + 1(Column) + 1(Literal) + 1(FuncCall) + 1(Column) + 1(Literal) = 7
        assert_eq!(count, 7);
    }

    #[test]
    fn expr_walk_skips_children_on_false() {
        let expr = sample_expr();
        let mut count = 0;
        expr.walk(&mut |e| {
            count += 1;
            // Don't recurse into the Eq subtree
            !matches!(
                e,
                Expr::BinaryOp {
                    op: BinaryOp::Eq,
                    ..
                }
            )
        });
        // AND → Eq (stops) → FuncCall → Column → Literal = 5
        assert_eq!(count, 5);
    }

    #[test]
    fn expr_walk_handles_if_then() {
        let expr = if_then(
            vec![(col("t", "a").eq(int(1)), string("yes"))],
            Some(string("no")),
        );
        let mut count = 0;
        expr.walk(&mut |_| {
            count += 1;
            true
        });
        // IfThen → Eq(Column, Literal) → Literal("yes") → Literal("no") = 6
        assert_eq!(count, 6);
    }

    #[test]
    fn expr_walk_handles_in_list() {
        let expr = col("t", "id").in_list(vec![int(1), int(2), int(3)]);
        let mut count = 0;
        expr.walk(&mut |_| {
            count += 1;
            true
        });
        // InList → Column → Literal(1) → Literal(2) → Literal(3) = 5
        assert_eq!(count, 5);
    }

    #[test]
    fn expr_walk_handles_cast() {
        let expr = col("t", "id").cast(DataType::String);
        let mut count = 0;
        expr.walk(&mut |_| {
            count += 1;
            true
        });
        // Cast → Column = 2
        assert_eq!(count, 2);
    }

    #[test]
    fn expr_walk_mut_can_modify_literals() {
        let mut expr = col("t", "id").eq(int(42));
        expr.walk_mut(&mut |e| {
            if let Expr::Literal(LiteralValue::Int64(v)) = e {
                *v = 99;
            }
            true
        });
        let expected = col("t", "id").eq(int(99));
        assert_eq!(expr, expected);
    }

    #[test]
    fn expr_walk_mut_can_rename_columns() {
        let mut expr = col("old", "name").and(col("old", "id").eq(int(1)));
        expr.walk_mut(&mut |e| {
            if let Expr::Column { table, .. } = e
                && table == "old"
            {
                *table = "new".into();
            }
            true
        });
        let mut found_old = false;
        expr.walk(&mut |e| {
            if let Expr::Column { table, .. } = e {
                assert_eq!(table, "new");
                if table == "old" {
                    found_old = true;
                }
            }
            true
        });
        assert!(!found_old);
    }

    #[test]
    fn expr_transform_identity() {
        let expr = sample_expr();
        let cloned = expr.clone();
        let transformed = expr.transform(&mut |e| e);
        assert_eq!(transformed, cloned);
    }

    #[test]
    fn expr_transform_replaces_literals() {
        // Replace all Int64 literals with 0
        let expr = col("t", "id").eq(int(42)).and(col("t", "x").gt(int(10)));
        let transformed = expr.transform(&mut |e| {
            if matches!(e, Expr::Literal(LiteralValue::Int64(_))) {
                int(0)
            } else {
                e
            }
        });
        let expected = col("t", "id").eq(int(0)).and(col("t", "x").gt(int(0)));
        assert_eq!(transformed, expected);
    }

    #[test]
    fn expr_transform_is_bottom_up() {
        // Bottom-up: children are transformed before the parent.
        // Wrap every Column in a Cast — the cast should appear around the column,
        // not around some intermediate.
        let expr = col("t", "id");
        let transformed = expr.transform(&mut |e| {
            if matches!(e, Expr::Column { .. }) {
                e.cast(DataType::String)
            } else {
                e
            }
        });
        assert!(matches!(transformed, Expr::Cast { .. }));
        if let Expr::Cast { expr, .. } = &transformed {
            assert!(matches!(**expr, Expr::Column { .. }));
        }
    }

    #[test]
    fn expr_transform_rewrites_if_then() {
        let expr = if_then(
            vec![(col("t", "a").eq(int(1)), string("yes"))],
            Some(string("no")),
        );
        // Replace all string literals with "replaced"
        let transformed = expr.transform(&mut |e| {
            if let Expr::Literal(LiteralValue::String(_)) = &e {
                string("replaced")
            } else {
                e
            }
        });
        if let Expr::IfThen { ifs, else_expr } = &transformed {
            assert_eq!(ifs[0].1, string("replaced"));
            assert_eq!(**else_expr.as_ref().unwrap(), string("replaced"));
        } else {
            panic!("expected IfThen");
        }
    }

    #[test]
    fn expr_try_transform_succeeds() {
        let expr = sample_expr();
        let result: Result<Expr, &str> = expr.try_transform(&mut |e| Ok(e));
        assert!(result.is_ok());
    }

    #[test]
    fn expr_try_transform_propagates_error() {
        let expr = col("t", "id").eq(int(42));
        let result = expr.try_transform(&mut |e| {
            if matches!(e, Expr::Literal(LiteralValue::Int64(42))) {
                Err("no 42 allowed")
            } else {
                Ok(e)
            }
        });
        assert_eq!(result.unwrap_err(), "no 42 allowed");
    }

    #[test]
    fn expr_try_transform_stops_at_first_error() {
        let expr = col("t", "a").eq(int(1)).and(col("t", "b").eq(int(2)));
        let mut transform_count = 0;
        let result = expr.try_transform(&mut |e| {
            transform_count += 1;
            if matches!(e, Expr::Literal(LiteralValue::Int64(1))) {
                Err("stop")
            } else {
                Ok(e)
            }
        });
        assert_eq!(result.unwrap_err(), "stop");
        // Should not have visited all 7 nodes
        assert!(transform_count < 7);
    }

    // -----------------------------------------------------------------------
    // Plan mutation via walk_mut / transform_rels
    // -----------------------------------------------------------------------

    #[test]
    fn walk_mut_extends_project() {
        let mut plan = Rel::read(
            "gl_user",
            "u",
            &[("id", DataType::Int64), ("name", DataType::String)],
        )
        .project(&[(col("u", "name"), "name")])
        .fetch(10, None)
        .into_plan();

        assert_eq!(plan.output_names, vec!["name"]);

        plan.root.walk_mut(&mut |r| {
            if let RelKind::Project { expressions } = &mut r.kind {
                expressions.push((col("u", "id"), "_gkg_u_id".into()));
                expressions.push((string("User"), "_gkg_u_type".into()));
                return false;
            }
            matches!(r.kind, RelKind::Fetch { .. } | RelKind::Sort { .. })
        });
        plan.output_names = plan.root.output_names();

        assert_eq!(plan.output_names, vec!["name", "_gkg_u_id", "_gkg_u_type"]);
    }

    #[test]
    fn walk_mut_extends_aggregate_groups() {
        use crate::ir::plan::Measure;

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

        plan.root.walk_mut(&mut |r| {
            if let RelKind::Aggregate { group_by, .. } = &mut r.kind {
                let item = col("u", "id");
                if !group_by.contains(&item) {
                    group_by.push(item);
                }
                return false;
            }
            matches!(
                r.kind,
                RelKind::Fetch { .. } | RelKind::Sort { .. } | RelKind::Filter { .. }
            )
        });

        if let RelKind::Fetch { .. } = &plan.root.kind
            && let RelKind::Aggregate { group_by, .. } = &plan.root.inputs[0].kind
        {
            assert_eq!(group_by.len(), 2);
            return;
        }
        panic!("expected Fetch(Aggregate(...))");
    }

    #[test]
    fn transform_rels_injects_filters_on_reads() {
        let cte_plan = Rel::read(
            "gl_project",
            "p",
            &[
                ("id", DataType::Int64),
                ("traversal_path", DataType::String),
            ],
        )
        .project(&[(col("p", "id"), "node_id")])
        .into_plan();

        let plan = Rel::read(
            "gl_issue",
            "i",
            &[
                ("id", DataType::Int64),
                ("traversal_path", DataType::String),
            ],
        )
        .project(&[(col("i", "id"), "id")])
        .into_plan_with_ctes(vec![CteDef {
            name: "base".into(),
            plan: cte_plan,
            recursive: false,
        }]);

        let plan = plan.transform_rels(&mut |rel| {
            let cond = match &rel.kind {
                RelKind::Read { table, alias, .. } if table.starts_with("gl_") => {
                    Some(col(alias, "traversal_path").starts_with(string("42/")))
                }
                _ => None,
            };
            match cond {
                Some(c) => rel.filter(c),
                None => rel,
            }
        });

        // Both root and CTE should have filters injected
        let mut filter_count = 0;
        plan.walk_rels(&mut |r| {
            if matches!(r.kind, RelKind::Filter { .. }) {
                filter_count += 1;
            }
            true
        });
        assert_eq!(filter_count, 2);
    }
}
