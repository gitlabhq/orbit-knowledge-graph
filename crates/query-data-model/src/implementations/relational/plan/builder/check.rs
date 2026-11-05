use super::RelationalBuilder;
use crate::implementations::relational::RelationalBackend;
use crate::implementations::relational::plan::{
    AggregateFunction, Expr, Measure, RelationalError, TextMatch,
};

impl<B: RelationalBackend> RelationalBuilder<'_, B> {
    pub(super) fn check(&mut self, expr: &Expr) -> Result<(), RelationalError> {
        let result = self.validate(expr, &mut Vec::new());
        self.guard(result)
    }

    pub(super) fn check_measure(&mut self, measure: &Measure) -> Result<(), RelationalError> {
        let count = measure.args.len();
        let valid = match measure.function {
            AggregateFunction::ArgMax | AggregateFunction::ArgMaxOrNull => {
                count == 2 && !measure.distinct && measure.condition.is_none()
            }
            AggregateFunction::Count if !measure.distinct => count <= 1,
            _ => count == 1,
        };
        self.guard(match valid {
            true => Ok(()),
            false => Err(RelationalError::Arity {
                function: format!("{:?}", measure.function),
                count,
            }),
        })?;
        for value in measure.args.iter().chain(&measure.condition) {
            self.check(value)?;
        }
        Ok(())
    }

    fn validate(&self, expr: &Expr, bound: &mut Vec<String>) -> Result<(), RelationalError> {
        match expr {
            Expr::Variable(name) if !bound.contains(name) => {
                return Err(RelationalError::Unbound(name.clone()));
            }
            Expr::Call { function, args } if !function.accepts(args.len()) => {
                return Err(RelationalError::Arity {
                    function: format!("{function:?}"),
                    count: args.len(),
                });
            }
            Expr::TextSearch { mode, .. }
                if *mode != TextMatch::Contains && !B::CAPABILITIES.token_search =>
            {
                return Err(RelationalError::Unsupported("token search"));
            }
            Expr::In { value, query } => self.width(query.index(), value.width())?,
            Expr::Scalar(query) => self.width(query.index(), 1)?,
            Expr::Lambda { param, body } => {
                bound.push(param.clone());
                let result = self.validate(body, bound);
                bound.pop();
                return result;
            }
            _ => {}
        }
        expr.children()
            .into_iter()
            .try_for_each(|child| self.validate(child, bound))
    }

    fn width(&self, body: usize, expected: usize) -> Result<(), RelationalError> {
        match self.widths.get(body) {
            Some(found) if *found == expected => Ok(()),
            Some(found) => Err(RelationalError::Width {
                expected,
                found: *found,
            }),
            None => Err(RelationalError::Rejected(
                crate::plan::tracking::Rejected::ForeignPlan,
            )),
        }
    }
}
