use super::{Dialect, Renderer};
use crate::implementations::relational::plan::{BinaryOp, Expr, Operation, UnaryOp};
use crate::plan::tracking::{NodeId, Ref};

impl<'p, D: Dialect> Renderer<'p, D> {
    pub fn expr(&mut self, expr: &Expr) -> String {
        match expr {
            Expr::Column(output) => self.column(*output),
            Expr::Variable(name) => name.clone(),
            Expr::Parameter { data_type, value } => self.dialect.parameter(*data_type, value),
            Expr::List(items) => format!("({})", self.list(items)),
            Expr::Null => "NULL".to_string(),
            Expr::Bool(value) => value.to_string(),
            Expr::Call { function, args } => D::call(self, *function, args),
            Expr::EmptyArray(types) => self.dialect.empty_array(types),
            Expr::TimeBucket { unit, value } => {
                let value = self.expr(value);
                self.dialect.time_bucket(*unit, &value)
            }
            Expr::TextSearch { mode, value, query } => {
                let value = self.expr(value);
                let query = self.expr(query);
                self.dialect.text_search(*mode, &value, &query)
            }
            Expr::Lambda { param, body } => {
                let body = self.expr(body);
                self.dialect.lambda(param, &body)
            }
            Expr::Binary { op, left, right } => {
                let left = self.expr(left);
                let right = self.expr(right);
                match op {
                    BinaryOp::In => format!("{left} IN {right}"),
                    op => format!("({left} {} {right})", binary(*op)),
                }
            }
            Expr::Unary { op, value } => {
                let value = self.expr(value);
                match op {
                    UnaryOp::Not => format!("(NOT {value})"),
                    UnaryOp::IsNull => format!("({value} IS NULL)"),
                    UnaryOp::IsNotNull => format!("({value} IS NOT NULL)"),
                }
            }
            Expr::In { value, query } => {
                let value = self.expr(value);
                format!("{value} IN ({})", self.subquery(*query))
            }
            Expr::Scalar(query) => format!("({})", self.subquery(*query)),
        }
    }

    pub fn list(&mut self, items: &[Expr]) -> String {
        let items: Vec<_> = items.iter().map(|item| self.expr(item)).collect();
        items.join(", ")
    }

    pub(super) fn direction(&mut self, value: &Expr, descending: bool) -> String {
        let value = self.expr(value);
        match descending {
            true => format!("{value} DESC"),
            false => format!("{value} ASC"),
        }
    }

    pub(super) fn column(&mut self, output: Ref) -> String {
        let plan = self.plan;
        let (owner, position) = self.owners[&output];
        let name = plan.plan.name(output).unwrap_or_default();
        match plan.plan.operation(owner) {
            Operation::Scan { alias, .. }
            | Operation::Read {
                alias: Some(alias), ..
            } => format!("{alias}.{name}"),
            Operation::Aggregate { groups, measures } => match groups.get(position) {
                Some(group) => self.expr(group),
                None => D::measure(self, &measures[position - groups.len()]),
            },
            Operation::Compute(_) if plan.plan.name(output).is_none() => {
                let value = self.computed(owner, position);
                self.expr(value)
            }
            _ => name.to_string(),
        }
    }

    pub(super) fn computed(&self, compute: NodeId, position: usize) -> &'p Expr {
        let plan = self.plan;
        let Operation::Compute(items) = plan.plan.operation(compute) else {
            unreachable!("computed outputs belong to a compute operation");
        };
        let declared = plan.plan.outputs(compute).count() - items.len();
        &items[position - declared]
    }
}

fn binary(op: BinaryOp) -> &'static str {
    match op {
        BinaryOp::Eq => "=",
        BinaryOp::Ne => "!=",
        BinaryOp::Lt => "<",
        BinaryOp::Le => "<=",
        BinaryOp::Gt => ">",
        BinaryOp::Ge => ">=",
        BinaryOp::In => "IN",
        BinaryOp::And => "AND",
        BinaryOp::Or => "OR",
        BinaryOp::Add => "+",
    }
}
