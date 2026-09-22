use super::plan_v2::*;

// ── S-expression serialization ──────────────────────────────────────────────

impl PhysOp {
    pub fn to_sexpr(&self) -> String {
        self.fmt_sexpr(0)
    }

    fn fmt_sexpr(&self, indent: usize) -> String {
        let pad = "  ".repeat(indent);

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
                let preds = predicates
                    .iter()
                    .map(|p| p.to_sexpr())
                    .collect::<Vec<_>>()
                    .join(" ");
                format!("{pad}(Filter [{preds}]\n{})", input.fmt_sexpr(indent + 1))
            }
            PhysOp::Project { input, columns } => {
                let cols = columns
                    .iter()
                    .map(|c| c.to_sexpr())
                    .collect::<Vec<_>>()
                    .join(" ");
                format!("{pad}(Project [{cols}]\n{})", input.fmt_sexpr(indent + 1))
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
                let on_str = on
                    .iter()
                    .map(|(x, y)| format!("{}.{} = {}.{}", x.0, x.1, y.0, y.1))
                    .collect::<Vec<_>>()
                    .join(" ∧ ");
                format!(
                    "{pad}(Join {k} ({on_str})\n{}\n{})",
                    left.fmt_sexpr(indent + 1),
                    right.fmt_sexpr(indent + 1)
                )
            }
            PhysOp::Aggregate {
                input,
                group_by,
                metrics,
            } => {
                let gk = group_by
                    .iter()
                    .map(|g| g.to_sexpr())
                    .collect::<Vec<_>>()
                    .join(" ");
                let ms = metrics
                    .iter()
                    .map(|m| m.to_sexpr())
                    .collect::<Vec<_>>()
                    .join(" ");
                format!(
                    "{pad}(Agg [group: {gk}] [metrics: {ms}]\n{})",
                    input.fmt_sexpr(indent + 1)
                )
            }
            PhysOp::Union { arms, alias } => {
                let arm_strs: Vec<String> = arms.iter().map(|a| a.fmt_sexpr(indent + 1)).collect();
                format!("{pad}(Union {alias}\n{})", arm_strs.join("\n"))
            }
            PhysOp::With { ctes, input } => {
                let cte_strs: Vec<String> = ctes
                    .iter()
                    .map(|(n, c)| format!("{pad}  ({n} =\n{})", c.fmt_sexpr(indent + 2)))
                    .collect();
                format!(
                    "{pad}(With\n{}\n{})",
                    cte_strs.join("\n"),
                    input.fmt_sexpr(indent + 1)
                )
            }
            PhysOp::Sort { input, keys } => {
                if keys.is_empty() {
                    return input.fmt_sexpr(indent);
                }
                let ks = keys
                    .iter()
                    .map(|k| k.to_sexpr())
                    .collect::<Vec<_>>()
                    .join(" ");
                format!("{pad}(Sort [{ks}]\n{})", input.fmt_sexpr(indent + 1))
            }
            PhysOp::Limit { input, count } => {
                format!("{pad}(Limit {count}\n{})", input.fmt_sexpr(indent + 1))
            }
        }
    }
}

impl Predicate {
    fn to_sexpr(&self) -> String {
        match self {
            Predicate::Eq { column, value } => {
                if column == "_deleted" && matches!(value, Value::Bool(false)) {
                    return "!deleted".to_string();
                }
                format!("{column}={}", value.to_sexpr())
            }
            Predicate::In { column, values } => {
                if values.len() > 5 {
                    let first3 = values[..3]
                        .iter()
                        .map(|v| v.to_sexpr())
                        .collect::<Vec<_>>()
                        .join(",");
                    format!("{column}∈[{first3},…+{}]", values.len() - 3)
                } else {
                    let vs = values
                        .iter()
                        .map(|v| v.to_sexpr())
                        .collect::<Vec<_>>()
                        .join(",");
                    format!("{column}∈[{vs}]")
                }
            }
            Predicate::Range { column, start, end } => format!("{column}∈{start}..{end}"),
            Predicate::NodeFilter { property, filter } => {
                let op = filter.op.as_ref().map(|o| o.as_ref()).unwrap_or("eq");
                format!("{property}:{op}")
            }
            Predicate::Func {
                name,
                column,
                value,
            } => format!("{name}({column},{value})", value = value.to_sexpr()),
            Predicate::ScopePrefix(_) => "scope(…)".to_string(),
            Predicate::Expr(_) => "expr(…)".to_string(),
        }
    }
}

impl Value {
    fn to_sexpr(&self) -> String {
        match self {
            Value::Int(i) => i.to_string(),
            Value::Str(s) => format!("\"{s}\""),
            Value::Bool(b) => b.to_string(),
            Value::Strs(ss) => {
                let items = ss
                    .iter()
                    .map(|s| format!("\"{s}\""))
                    .collect::<Vec<_>>()
                    .join(",");
                format!("[{items}]")
            }
        }
    }
}

impl ProjectedColumn {
    fn to_sexpr(&self) -> String {
        match self {
            ProjectedColumn::Ref {
                table,
                column,
                alias,
            } => {
                if table.is_empty() {
                    if column == alias {
                        column.clone()
                    } else {
                        format!("{column}:{alias}")
                    }
                } else {
                    format!("{table}.{column}:{alias}")
                }
            }
            ProjectedColumn::NodeProperty { node, property } => format!("@{node}.{property}"),
            ProjectedColumn::Computed { expr, alias } => match expr {
                ColumnExpr::Lit(v) => format!("{}:{alias}", v.to_sexpr()),
                _ => format!("({}):{alias}", expr.to_sexpr()),
            },
            ProjectedColumn::Expr { alias, .. } => format!("expr(…):{alias}"),
        }
    }
}

impl ColumnExpr {
    fn to_sexpr(&self) -> String {
        match self {
            ColumnExpr::Col(table, col) => format!("{table}.{col}"),
            ColumnExpr::Lit(v) => v.to_sexpr(),
            ColumnExpr::Array(items) => {
                let inner = items
                    .iter()
                    .map(|i| i.to_sexpr())
                    .collect::<Vec<_>>()
                    .join(" ");
                format!("[{inner}]")
            }
            ColumnExpr::Tuple(items) => {
                let inner = items
                    .iter()
                    .map(|i| i.to_sexpr())
                    .collect::<Vec<_>>()
                    .join(" ");
                format!("({inner})")
            }
        }
    }
}

impl GroupKey {
    fn to_sexpr(&self) -> String {
        let trunc = self
            .truncate
            .map(|t| format!("/{}", t.ch_function()))
            .unwrap_or_default();
        format!("{}.{}{trunc}:{}", self.node, self.property, self.alias)
    }
}

impl Metric {
    fn to_sexpr(&self) -> String {
        let func = self.function.as_sql();
        let prop = self.property.as_deref().unwrap_or("*");
        format!("{func}({}.{prop}):{}", self.node, self.alias)
    }
}

impl SortKey {
    fn to_sexpr(&self) -> String {
        let dir = if self.desc { "↓" } else { "↑" };
        format!("{}{dir}", self.column)
    }
}
