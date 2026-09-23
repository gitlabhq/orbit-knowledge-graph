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

/// One operator as text: `(head items children)`.
#[derive(Debug, Clone, PartialEq)]
pub struct PlanNode {
    /// Operator label, e.g. `Join`, then its inline arguments, e.g. `ON a.x = b.y`.
    pub label: String,
    pub head: String,
    /// Predicates, columns, or keys, one string each.
    pub items: Vec<String>,
    pub children: Vec<PlanNode>,
}

impl PhysOp {
    pub fn explain(&self) -> String {
        self.to_node().render(0)
    }

    pub fn to_node(&self) -> PlanNode {
        let node =
            |label: &str, head: String, items: Vec<String>, children: Vec<PlanNode>| PlanNode {
                label: label.into(),
                head,
                items,
                children,
            };
        let child = |op: &PhysOp| vec![op.to_node()];
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
                node("Scan", format!("{table} AS {alias}{d}"), vec![], vec![])
            }
            PhysOp::Filter { input, predicates } => node(
                "Filter",
                String::new(),
                predicates.iter().map(PExpr::explain).collect(),
                child(input),
            ),
            PhysOp::Project { input, columns } => node(
                "Project",
                String::new(),
                columns.iter().map(|(e, a)| named(e, a)).collect(),
                child(input),
            ),
            PhysOp::Join {
                left,
                right,
                on,
                kind,
            } => {
                let eq = |(x, y): &(Col, Col)| format!("{}.{} = {}.{}", x.0, x.1, y.0, y.1);
                let (label, head) = match kind {
                    JoinKind::Inner => (
                        "Join",
                        format!("ON {}", on.iter().map(eq).collect::<Vec<_>>().join(" AND ")),
                    ),
                    JoinKind::Semi => {
                        let (x, y) = &on[0];
                        ("SemiJoin", format!("{}.{} IN {}.{}", x.0, x.1, y.0, y.1))
                    }
                };
                node(label, head, vec![], vec![left.to_node(), right.to_node()])
            }
            PhysOp::Aggregate {
                input,
                group_by,
                metrics,
            } => node(
                "Aggregate",
                String::new(),
                group_by
                    .iter()
                    .map(|(e, a)| format!("group {}", named(e, a)))
                    .chain(metrics.iter().map(|(e, a)| named(e, a)))
                    .collect(),
                child(input),
            ),
            PhysOp::Union { arms, alias } => node(
                "Union",
                format!("AS {alias}"),
                vec![],
                arms.iter().map(PhysOp::to_node).collect(),
            ),
            PhysOp::Sort { input, keys } => node(
                "Sort",
                keys.iter()
                    .map(|(e, desc)| format!("{}{}", e.explain(), if *desc { " DESC" } else { "" }))
                    .collect::<Vec<_>>()
                    .join(", "),
                vec![],
                child(input),
            ),
            PhysOp::Limit { input, count } => {
                node("Limit", count.to_string(), vec![], child(input))
            }
            PhysOp::With { ctes, input } => {
                let mut children: Vec<PlanNode> = ctes
                    .iter()
                    .map(|(name, body)| node(name, "=".into(), vec![], child(body)))
                    .collect();
                children.push(input.to_node());
                node("With", String::new(), vec![], children)
            }
        }
    }
}

impl PlanNode {
    /// `(Label head` on the first line; items one per line when the list is
    /// long; children indented; closing paren on the last line.
    pub fn render(&self, depth: usize) -> String {
        let pad = "  ".repeat(depth);
        let mut out = format!("{pad}({}", self.label);
        if !self.head.is_empty() {
            out.push(' ');
            out.push_str(&self.head);
        }
        let inline = self.items.join(", ");
        if self.items.len() <= 3 && inline.len() <= INLINE_WIDTH {
            if !inline.is_empty() {
                out.push(' ');
                out.push_str(&inline);
            }
        } else {
            for i in &self.items {
                out.push_str(&format!("\n{pad}    {i}"));
            }
        }
        for c in &self.children {
            out.push('\n');
            out.push_str(&c.render(depth + 1));
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
    pub fn explain(&self) -> String {
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
                // Brackets, not parens: in plan text `(` after a space starts a child.
                format!("{} IN [{}{more}]", x.explain(), shown.join(", "))
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
