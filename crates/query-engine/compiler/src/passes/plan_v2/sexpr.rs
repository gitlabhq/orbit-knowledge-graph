//! The plan as an S-expression, one operator per line, for plan-shape
//! fixtures and `orbit-devtools compile --plan`.

use super::*;
use ontology::constants::DELETED_COLUMN;

impl PhysOp {
    pub fn to_sexpr(&self) -> String {
        self.fmt(0)
    }

    fn fmt(&self, indent: usize) -> String {
        let pad = "  ".repeat(indent);
        let list = |xs: &[String]| xs.join(" ");
        match self {
            PhysOp::Scan {
                table,
                alias,
                dedup,
            } => {
                let d = match dedup {
                    Dedup::None => "",
                    Dedup::Final => " FINAL",
                    Dedup::LimitBy => " LIMIT-BY",
                };
                format!("{pad}(Scan {table} {alias}{d})")
            }
            PhysOp::Filter { input, predicates } => {
                let ps: Vec<String> = predicates.iter().map(PExpr::to_sexpr).collect();
                format!("{pad}(Filter [{}]\n{})", list(&ps), input.fmt(indent + 1))
            }
            PhysOp::Project { input, columns } => {
                let cs: Vec<String> = columns.iter().map(|(e, a)| named_sexpr(e, a)).collect();
                format!("{pad}(Project [{}]\n{})", list(&cs), input.fmt(indent + 1))
            }
            PhysOp::Join {
                left,
                right,
                on,
                kind,
            } => {
                let k = match kind {
                    JoinKind::Inner => "Inner",
                    JoinKind::Semi => "Semi",
                };
                let on: Vec<String> = on
                    .iter()
                    .map(|(x, y)| format!("{}.{} = {}.{}", x.0, x.1, y.0, y.1))
                    .collect();
                format!(
                    "{pad}(Join {k} ({})\n{}\n{})",
                    on.join(" ∧ "),
                    left.fmt(indent + 1),
                    right.fmt(indent + 1)
                )
            }
            PhysOp::Aggregate {
                input,
                group_by,
                metrics,
            } => {
                let g: Vec<String> = group_by.iter().map(|(e, a)| named_sexpr(e, a)).collect();
                let m: Vec<String> = metrics.iter().map(|(e, a)| named_sexpr(e, a)).collect();
                format!(
                    "{pad}(Agg [group: {}] [metrics: {}]\n{})",
                    list(&g),
                    list(&m),
                    input.fmt(indent + 1)
                )
            }
            PhysOp::Union { arms, alias } => {
                let arms: Vec<String> = arms.iter().map(|a| a.fmt(indent + 1)).collect();
                format!("{pad}(Union {alias}\n{})", arms.join("\n"))
            }
            PhysOp::Sort { input, keys } => {
                let ks: Vec<String> = keys
                    .iter()
                    .map(|(e, desc)| format!("{}{}", e.to_sexpr(), if *desc { "↓" } else { "↑" }))
                    .collect();
                format!("{pad}(Sort [{}]\n{})", list(&ks), input.fmt(indent + 1))
            }
            PhysOp::Limit { input, count } => {
                format!("{pad}(Limit {count}\n{})", input.fmt(indent + 1))
            }
            PhysOp::With { ctes, input } => {
                let cs: Vec<String> = ctes
                    .iter()
                    .map(|(n, c)| format!("{pad}  ({n} =\n{})", c.fmt(indent + 2)))
                    .collect();
                format!("{pad}(With\n{}\n{})", cs.join("\n"), input.fmt(indent + 1))
            }
        }
    }
}

fn named_sexpr(e: &PExpr, alias: &str) -> String {
    format!("{}:{alias}", e.to_sexpr())
}

impl PExpr {
    fn to_sexpr(&self) -> String {
        match self {
            PExpr::Col(a, c) => format!("{a}.{c}"),
            PExpr::Ident(n) => n.clone(),
            PExpr::Lit(Lit::Int(i)) => i.to_string(),
            PExpr::Lit(Lit::Str(s)) => format!("\"{s}\""),
            PExpr::Lit(Lit::Bool(b)) => b.to_string(),
            PExpr::Func(n, xs) => {
                let xs: Vec<String> = xs.iter().map(PExpr::to_sexpr).collect();
                format!("{n}({})", xs.join(" "))
            }
            PExpr::Cmp(CmpOp::Eq, l, r) if matches!((l.as_ref(), r.as_ref()), (PExpr::Col(_, c), PExpr::Lit(Lit::Bool(false))) if c == DELETED_COLUMN) =>
            {
                let PExpr::Col(a, _) = l.as_ref() else {
                    unreachable!()
                };
                format!("!{a}.deleted")
            }
            PExpr::Cmp(op, l, r) => {
                let op = match op {
                    CmpOp::Eq => "=",
                    CmpOp::Ne => "≠",
                    CmpOp::Lt => "<",
                    CmpOp::Le => "≤",
                    CmpOp::Gt => ">",
                    CmpOp::Ge => "≥",
                };
                format!("{}{op}{}", l.to_sexpr(), r.to_sexpr())
            }
            PExpr::And(xs) => {
                let xs: Vec<String> = xs.iter().map(PExpr::to_sexpr).collect();
                format!("({})", xs.join(" ∧ "))
            }
            PExpr::Or(xs) => {
                let xs: Vec<String> = xs.iter().map(PExpr::to_sexpr).collect();
                format!("({})", xs.join(" ∨ "))
            }
            PExpr::In(x, vs) => {
                let shown: Vec<String> = vs
                    .iter()
                    .take(3)
                    .map(|v| PExpr::Lit(v.clone()).to_sexpr())
                    .collect();
                let more = if vs.len() > 3 {
                    format!(",…+{}", vs.len() - 3)
                } else {
                    String::new()
                };
                format!("{}∈[{}{more}]", x.to_sexpr(), shown.join(","))
            }
            PExpr::Lambda(p, b) => format!("{p} -> {}", b.to_sexpr()),
            PExpr::NodeFilter {
                alias,
                property,
                filter,
            } => {
                let op = filter.op.as_ref().map(|o| o.as_ref()).unwrap_or("eq");
                format!("{alias}.{property}:{op}")
            }
            PExpr::Scope(a, _) => format!("scope({a})"),
            PExpr::ScopeResolved(_) => "scope_resolved".to_string(),
        }
    }
}
