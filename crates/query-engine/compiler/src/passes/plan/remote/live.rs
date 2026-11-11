use query_data_model::implementations::live::Live;
use query_data_model::plan::live::{Calls, Conditions, Field, LiveError, LivePlan};
use query_data_model::plan::logical::{
    Columns, Comparison, Conjunct, Expr, Literal, LogicalOperation, LogicalPlan, Matching, Operand,
    Predicate, Projection,
};
use query_data_model::{Catalog, PropertyId, StorageBackend};

use super::unsupported;
use crate::error::Result;

const BACKEND: &str = Live::KIND;

enum Pushed {
    Equals(PropertyId, Literal),
    Contains(PropertyId, String),
}

#[derive(Default)]
struct Window {
    property: Option<PropertyId>,
    from: Option<String>,
    to: Option<String>,
}

pub(super) fn plan(logical: &LogicalPlan, live: &Catalog<Live>) -> Result<LivePlan> {
    let graph = live.graph();
    let mut entity = None;
    let mut pushed = Vec::new();
    let mut window = Window::default();
    let mut returned = Vec::new();
    let mut limit = None;
    for node in logical.nodes() {
        match logical.operation(node) {
            LogicalOperation::Entities(id) => entity = Some(*id),
            LogicalOperation::Filter(predicate) => conditions(predicate, &mut pushed, &mut window)?,
            LogicalOperation::Select(items) => {
                for item in items {
                    match item {
                        Projection::Graph {
                            columns: Columns::Listed(listed),
                            ..
                        } => returned.extend(listed.iter().copied()),
                        Projection::Graph { .. } => {}
                        Projection::Value(_) => {
                            return Err(unsupported(BACKEND, "return an expression"));
                        }
                    }
                }
            }
            LogicalOperation::Limit { rows, skip: None } => limit = Some(*rows),
            _ => return Err(unsupported(BACKEND, "run this query shape")),
        }
    }
    let entity = entity.ok_or_else(|| unsupported(BACKEND, "call without an entity"))?;
    let store = live
        .home(entity)
        .map(|(store, _)| store.name.clone())
        .ok_or_else(|| unsupported(BACKEND, "find the entity's operation"))?;
    let name = |property: PropertyId| graph.property(property).name.as_str();
    Ok(live.build(|q| {
        let calls = q.call(live.operation(&store, &graph.entity(entity).name)?)?;
        let mut filters: Option<Conditions> = None;
        for condition in &pushed {
            let next = match condition {
                Pushed::Equals(property, value) => calls.field(name(*property))?.eq(value.clone()),
                Pushed::Contains(property, text) => calls.field(name(*property))?.contains(text),
            };
            filters = Some(match filters {
                Some(all) => all.and(next),
                None => next,
            });
        }
        let calls = match filters {
            Some(filters) => q.filter(calls, filters)?,
            None => calls,
        };
        let calls = match window.property {
            Some(property) => {
                let field = calls.field(name(property))?;
                q.window(calls, &field, window.from.clone(), window.to.clone())?
            }
            None => calls,
        };
        let calls = match limit {
            Some(rows) => q.limit(calls, rows)?,
            None => calls,
        };
        let fields = selected(&calls, &returned, name)?;
        q.select(calls, fields)
    })?)
}

fn selected<'n>(
    calls: &Calls,
    returned: &[PropertyId],
    name: impl Fn(PropertyId) -> &'n str,
) -> std::result::Result<Vec<Field>, LiveError> {
    match returned {
        [] => Ok(calls.fields().to_vec()),
        properties => properties
            .iter()
            .map(|property| calls.field(name(*property)))
            .collect(),
    }
}

fn conditions(predicate: &Predicate, pushed: &mut Vec<Pushed>, window: &mut Window) -> Result<()> {
    for conjunct in predicate.conjuncts() {
        let Conjunct::Leaf {
            predicate,
            negated: false,
        } = conjunct
        else {
            return Err(unsupported(BACKEND, "push down a negated condition"));
        };
        let property = |left: &Expr| match left {
            Expr::Property { property, .. } => Ok(*property),
            _ => Err(unsupported(BACKEND, "push down an expression")),
        };
        match predicate {
            Predicate::Compare {
                left,
                comparison: Comparison::Eq,
                right: Operand::Literal(literal),
            } => pushed.push(Pushed::Equals(property(left)?, literal.clone())),
            Predicate::Compare {
                left,
                comparison,
                right: Operand::Literal(Literal::Temporal(bound) | Literal::String(bound)),
            } if *comparison != Comparison::Ne => {
                let property = property(left)?;
                if window.property.is_some_and(|bounded| bounded != property) {
                    return Err(unsupported(BACKEND, "window more than one time field"));
                }
                window.property = Some(property);
                match comparison {
                    Comparison::Gt | Comparison::Ge => window.from = Some(bound.clone()),
                    _ => window.to = Some(bound.clone()),
                }
            }
            Predicate::Text {
                left,
                matching: Matching::Contains,
                needle,
            } => pushed.push(Pushed::Contains(property(left)?, needle.clone())),
            _ => return Err(unsupported(BACKEND, "push down this condition")),
        }
    }
    Ok(())
}
