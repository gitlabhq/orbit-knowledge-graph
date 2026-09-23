//! A plan as indented text, like `EXPLAIN`, for fixtures and
//! `orbit-devtools compile --plan`:
//!
//! ```text
//! (Limit 11
//!   (Project
//!       e0.source_id AS e0_src
//!       excerpt(mr.title) AS mr_title
//!     (Join ON e0.target_id = mr.id
//!       (Filter e0.relationship_kind = 'AUTHORED', !deleted(e0)
//!         (Scan gl_edge AS e0))
//!       (Filter mr.state = ?, !deleted(mr)
//!         (Scan gl_merge_request AS mr FINAL)))))
//! ```
//!
//! `?` is a user-supplied filter value; `excerpt(x)` is the text-truncation
//! expression the plan applies to long text columns.

use super::*;
use ontology::constants::DELETED_COLUMN;

/// Predicate or column lists longer than this go one per line.
const INLINE_WIDTH: usize = 90;

impl PhysOp {
    pub fn explain(&self) -> String {
        self.node(0)
    }

    /// `(Label items` on the first line, children indented below, closing
    /// paren on the last child's line.
    fn node(&self, depth: usize) -> String {
        let pad = "  ".repeat(depth);
        let (head, items, children): (String, Vec<String>, Vec<String>) = match self {
            PhysOp::Scan {
                table,
                alias,
                dedup,
            } => {
                let d = match dedup {
                    Dedup::None => "",
                    Dedup::Final => " FINAL",
                    Dedup::LimitBy => " LIMIT 1 BY sort key",
                };
                (format!("Scan {table} AS {alias}{d}"), vec![], vec![])
            }
            PhysOp::Filter { input, predicates } => (
                "Filter".into(),
                predicates.iter().map(PExpr::explain).collect(),
                vec![input.node(depth + 1)],
            ),
            PhysOp::Project { input, columns } => (
                "Project".into(),
                columns.iter().map(|(e, a)| named(e, a)).collect(),
                vec![input.node(depth + 1)],
            ),
            PhysOp::Join {
                left,
                right,
                on,
                kind,
            } => {
                let head = match kind {
                    JoinKind::Inner => {
                        let conds: Vec<String> = on
                            .iter()
                            .map(|(x, y)| format!("{}.{} = {}.{}", x.0, x.1, y.0, y.1))
                            .collect();
                        format!("Join ON {}", conds.join(" AND "))
                    }
                    JoinKind::Semi => {
                        let (x, y) = &on[0];
                        format!("SemiJoin {}.{} IN {}.{}", x.0, x.1, y.0, y.1)
                    }
                };
                (
                    head,
                    vec![],
                    vec![left.node(depth + 1), right.node(depth + 1)],
                )
            }
            PhysOp::Aggregate {
                input,
                group_by,
                metrics,
            } => (
                "Aggregate".into(),
                group_by
                    .iter()
                    .map(|(e, a)| format!("group {}", named(e, a)))
                    .chain(metrics.iter().map(|(e, a)| named(e, a)))
                    .collect(),
                vec![input.node(depth + 1)],
            ),
            PhysOp::Union { arms, alias } => (
                format!("Union AS {alias}"),
                vec![],
                arms.iter().map(|a| a.node(depth + 1)).collect(),
            ),
            PhysOp::Sort { input, keys } => {
                let keys: Vec<String> = keys
                    .iter()
                    .map(|(e, desc)| format!("{}{}", e.explain(), if *desc { " DESC" } else { "" }))
                    .collect();
                (
                    format!("Sort {}", keys.join(", ")),
                    vec![],
                    vec![input.node(depth + 1)],
                )
            }
            PhysOp::Limit { input, count } => (
                format!("Limit {count}"),
                vec![],
                vec![input.node(depth + 1)],
            ),
            PhysOp::With { ctes, input } => {
                let mut children: Vec<String> = ctes
                    .iter()
                    .map(|(name, body)| format!("{pad}  ({name} =\n{})", body.node(depth + 2)))
                    .collect();
                children.push(input.node(depth + 1));
                ("With".into(), vec![], children)
            }
        };

        let mut out = format!("{pad}({head}");
        let inline = items.join(", ");
        if items.len() <= 3 && inline.len() <= INLINE_WIDTH {
            if !inline.is_empty() {
                out.push(' ');
                out.push_str(&inline);
            }
        } else {
            for i in &items {
                out.push_str(&format!("\n{pad}    {i}"));
            }
        }
        for c in &children {
            out.push('\n');
            out.push_str(c);
        }
        out.push(')');
        out
    }
}

fn named(e: &PExpr, alias: &str) -> String {
    let text = e.explain();
    if text == alias {
        text
    } else {
        format!("{text} AS {alias}")
    }
}

impl PExpr {
    fn explain(&self) -> String {
        if let Some(inner) = self.as_excerpt() {
            return format!("excerpt({})", inner.explain());
        }
        match self {
            PExpr::Col(a, c) => format!("{a}.{c}"),
            PExpr::Ident(n) => n.clone(),
            PExpr::Lit(Lit::Int(i)) => i.to_string(),
            PExpr::Lit(Lit::Str(s)) => format!("'{s}'"),
            PExpr::Lit(Lit::Bool(b)) => b.to_string(),
            PExpr::Func(n, xs) => {
                let xs: Vec<String> = xs.iter().map(PExpr::explain).collect();
                if n == "array" {
                    format!("[{}]", xs.join(", "))
                } else {
                    format!("{n}({})", xs.join(", "))
                }
            }
            PExpr::Cmp(CmpOp::Eq, l, r) if matches!((l.as_ref(), r.as_ref()), (PExpr::Col(_, c), PExpr::Lit(Lit::Bool(false))) if c == DELETED_COLUMN) =>
            {
                let PExpr::Col(a, _) = l.as_ref() else {
                    unreachable!()
                };
                format!("!deleted({a})")
            }
            PExpr::Cmp(op, l, r) => {
                let op = match op {
                    CmpOp::Eq => "=",
                    CmpOp::Ne => "!=",
                    CmpOp::Lt => "<",
                    CmpOp::Le => "<=",
                    CmpOp::Gt => ">",
                    CmpOp::Ge => ">=",
                };
                format!("{} {op} {}", l.explain(), r.explain())
            }
            PExpr::And(xs) => xs
                .iter()
                .map(PExpr::explain)
                .collect::<Vec<_>>()
                .join(" AND "),
            PExpr::Or(xs) => format!(
                "({})",
                xs.iter()
                    .map(PExpr::explain)
                    .collect::<Vec<_>>()
                    .join(" OR ")
            ),
            PExpr::In(x, vs) => {
                let shown: Vec<String> = vs
                    .iter()
                    .take(5)
                    .map(|v| PExpr::Lit(v.clone()).explain())
                    .collect();
                let more = if vs.len() > 5 {
                    format!(", … {} more", vs.len() - 5)
                } else {
                    String::new()
                };
                format!("{} IN ({}{more})", x.explain(), shown.join(", "))
            }
            PExpr::Lambda(p, b) => format!("{p} -> {}", b.explain()),
            PExpr::NodeFilter {
                alias,
                property,
                filter,
            } => {
                let op = match filter.op.as_ref().map(|o| o.as_ref()).unwrap_or("eq") {
                    "eq" => "=".to_string(),
                    "ne" => "!=".to_string(),
                    "gt" => ">".to_string(),
                    "gte" => ">=".to_string(),
                    "lt" => "<".to_string(),
                    "lte" => "<=".to_string(),
                    other => other.to_uppercase(),
                };
                format!("{alias}.{property} {op} ?")
            }
            PExpr::Scope(a, _) => format!("scope({a})"),
            PExpr::ScopeResolved(_) => "scope_resolved".to_string(),
        }
    }

    /// The `concat(substringUTF8(x, 1, n), if(...))` shape `PlanCtx::property`
    /// builds for long text columns.
    fn as_excerpt(&self) -> Option<&PExpr> {
        let PExpr::Func(concat, args) = self else {
            return None;
        };
        if concat != "concat" || args.len() != 2 {
            return None;
        }
        let PExpr::Func(sub, sub_args) = &args[0] else {
            return None;
        };
        (sub == "substringUTF8" && matches!(args[1], PExpr::Func(ref f, _) if f == "if"))
            .then(|| &sub_args[0])
    }
}
