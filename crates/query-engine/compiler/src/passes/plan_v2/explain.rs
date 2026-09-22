//! A plan as indented text, like `EXPLAIN`, for fixtures and
//! `orbit-devtools compile --plan`:
//!
//! ```text
//! Limit 11
//!   Project
//!     e0.source_id AS e0_src
//!     excerpt(mr.title) AS mr_title
//!   Join ON e0.target_id = mr.id
//!     Filter e0.relationship_kind = 'AUTHORED', !deleted(e0)
//!       Scan gl_edge AS e0
//!     Filter mr.state = ?, !deleted(mr)
//!       Scan gl_merge_request AS mr FINAL
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
        let mut out = String::new();
        self.write(0, &mut out);
        out.trim_end().to_string()
    }

    fn write(&self, depth: usize, out: &mut String) {
        let pad = "  ".repeat(depth);
        let mut line = |s: String| {
            out.push_str(&pad);
            out.push_str(&s);
            out.push('\n');
        };
        match self {
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
                line(format!("Scan {table} AS {alias}{d}"));
            }
            PhysOp::Filter { input, predicates } => {
                let items: Vec<String> = predicates.iter().map(PExpr::explain).collect();
                list("Filter", &items, depth, out);
                input.write(depth + 1, out);
            }
            PhysOp::Project { input, columns } => {
                let items: Vec<String> = columns.iter().map(|(e, a)| named(e, a)).collect();
                list("Project", &items, depth, out);
                input.write(depth + 1, out);
            }
            PhysOp::Join {
                left,
                right,
                on,
                kind,
            } => {
                let conds: Vec<String> = on
                    .iter()
                    .map(|(x, y)| format!("{}.{} = {}.{}", x.0, x.1, y.0, y.1))
                    .collect();
                match kind {
                    JoinKind::Inner => line(format!("Join ON {}", conds.join(" AND "))),
                    JoinKind::Semi => {
                        let (x, y) = &on[0];
                        line(format!("SemiJoin {}.{} IN {}.{}", x.0, x.1, y.0, y.1));
                    }
                }
                left.write(depth + 1, out);
                right.write(depth + 1, out);
            }
            PhysOp::Aggregate {
                input,
                group_by,
                metrics,
            } => {
                let items: Vec<String> = group_by
                    .iter()
                    .map(|(e, a)| format!("group {}", named(e, a)))
                    .chain(metrics.iter().map(|(e, a)| named(e, a)))
                    .collect();
                list("Aggregate", &items, depth, out);
                input.write(depth + 1, out);
            }
            PhysOp::Union { arms, alias } => {
                line(format!("Union AS {alias}"));
                for a in arms {
                    a.write(depth + 1, out);
                }
            }
            PhysOp::Sort { input, keys } => {
                let items: Vec<String> = keys
                    .iter()
                    .map(|(e, desc)| format!("{}{}", e.explain(), if *desc { " DESC" } else { "" }))
                    .collect();
                line(format!("Sort {}", items.join(", ")));
                input.write(depth + 1, out);
            }
            PhysOp::Limit { input, count } => {
                line(format!("Limit {count}"));
                input.write(depth + 1, out);
            }
            PhysOp::With { ctes, input } => {
                line("With".to_string());
                for (name, body) in ctes {
                    out.push_str(&format!("{pad}  {name} ="));
                    out.push('\n');
                    body.write(depth + 2, out);
                }
                input.write(depth + 1, out);
            }
        }
    }
}

/// `label a, b, c` on one line when it fits; otherwise one item per line.
fn list(label: &str, items: &[String], depth: usize, out: &mut String) {
    let pad = "  ".repeat(depth);
    let inline = items.join(", ");
    if items.len() <= 3 && inline.len() <= INLINE_WIDTH {
        out.push_str(&format!("{pad}{label} {inline}\n"));
        return;
    }
    out.push_str(&format!("{pad}{label}\n"));
    for i in items {
        out.push_str(&format!("{pad}    {i}\n"));
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
