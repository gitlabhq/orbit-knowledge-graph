use super::{Backend, JoinStrategy, PhysicalOp, PhysicalPlan};
use crate::passes::logical_v3::{ColumnRef, CompareOp, Expr, NamedExpr, RelationId, SortKey};
use std::collections::{HashMap, HashSet};

pub struct JoinEditor<B: Backend> {
    inputs: Vec<PhysicalPlan<B>>,
    conditions: Vec<Expr>,
    strategy: JoinStrategy,
    required: HashSet<RelationId>,
    substitutions: HashMap<ColumnRef, Expr>,
    filters: Vec<Expr>,
    semi_joins: Vec<(Expr, PhysicalPlan<B>)>,
}

impl<B: Backend> JoinEditor<B> {
    fn new(plan: PhysicalPlan<B>, required: HashSet<RelationId>) -> Result<Self, PhysicalPlan<B>> {
        let PhysicalOp::Join {
            conditions,
            strategy,
        } = plan.op
        else {
            return Err(plan);
        };
        Ok(Self {
            inputs: plan.inputs,
            conditions,
            strategy,
            required,
            substitutions: HashMap::new(),
            filters: Vec::new(),
            semi_joins: Vec::new(),
        })
    }

    pub fn relations(&self) -> impl Iterator<Item = RelationId> + '_ {
        self.inputs.iter().filter_map(PhysicalPlan::relation_id)
    }

    pub fn relation(&self, relation: RelationId) -> Option<&PhysicalPlan<B>> {
        self.inputs
            .iter()
            .find(|input| input.relation_id() == Some(relation))
    }

    pub fn required(&self, relation: RelationId) -> bool {
        self.required.contains(&relation)
    }

    pub fn conditions_for(&self, relation: RelationId) -> Vec<&Expr> {
        self.conditions
            .iter()
            .filter(|condition| condition.relations().contains(&relation))
            .collect()
    }

    pub fn equality_for(&self, relation: RelationId) -> Option<(ColumnRef, ColumnRef)> {
        let mut equalities = self
            .conditions_for(relation)
            .into_iter()
            .filter_map(equality)
            .map(|(left, right)| {
                if left.relation == relation {
                    (left, right)
                } else {
                    (right, left)
                }
            });
        let equality = equalities.next()?;
        equalities.next().is_none().then_some(equality)
    }

    pub fn edge_equality(
        &self,
        left_relation: RelationId,
        right_relation: RelationId,
    ) -> Option<(ColumnRef, ColumnRef)> {
        self.conditions.iter().find_map(|condition| {
            let (left, right) = equality(condition)?;
            if left.relation == left_relation && right.relation == right_relation {
                Some((left, right))
            } else if left.relation == right_relation && right.relation == left_relation {
                Some((right, left))
            } else {
                None
            }
        })
    }

    pub fn remove(&mut self, relation: RelationId) -> Option<PhysicalPlan<B>> {
        let index = self
            .inputs
            .iter()
            .position(|input| input.relation_id() == Some(relation))?;
        self.conditions
            .retain(|condition| !condition.relations().contains(&relation));
        Some(self.inputs.remove(index))
    }

    pub fn ensure_input(
        &mut self,
        relation: RelationId,
        input: impl FnOnce() -> PhysicalPlan<B>,
    ) {
        if self.relation(relation).is_none() {
            self.inputs.push(input());
        }
    }

    pub fn add_condition(&mut self, condition: Expr) {
        if !self.conditions.contains(&condition) {
            self.conditions.push(condition);
        }
    }

    pub fn substitute(&mut self, columns: impl IntoIterator<Item = (ColumnRef, Expr)>) {
        self.substitutions.extend(columns);
    }

    pub fn add_filter(&mut self, predicate: Expr) {
        self.filters.push(predicate);
    }

    pub fn convert_to_semi(&mut self, relation: RelationId, condition: Expr) -> bool {
        let Some(lookup) = self.remove(relation) else {
            return false;
        };
        self.semi_joins.push((condition, lookup));
        true
    }

    pub fn fresh_relation(&self) -> RelationId {
        RelationId(
            self.inputs
                .iter()
                .flat_map(PhysicalPlan::visible_relations)
                .map(|relation| relation.0)
                .max()
                .unwrap_or(0)
                + 1,
        )
    }

    fn finish(mut self) -> PhysicalPlan<B> {
        self.conditions = self
            .conditions
            .into_iter()
            .map(|condition| condition.substitute(&self.substitutions))
            .filter(|condition| !tautology(condition))
            .collect();
        let mut plan = PhysicalPlan {
            op: PhysicalOp::Join {
                conditions: self.conditions,
                strategy: self.strategy,
            },
            inputs: self.inputs,
        }
        .map_expressions(&mut |expression| expression.substitute(&self.substitutions));
        if plan.inputs.len() == 1 {
            plan = plan.inputs.pop().unwrap();
        }
        if let Some(predicate) = Expr::and(self.filters) {
            plan = PhysicalPlan::unary(PhysicalOp::Filter(predicate), plan);
        }
        self.semi_joins
            .into_iter()
            .fold(plan, |left, (condition, right)| {
                PhysicalPlan::nary(PhysicalOp::SemiJoin(condition), [left, right])
            })
    }
}

pub fn rewrite_joins<B: Backend>(
    plan: PhysicalPlan<B>,
    rule: &mut impl FnMut(&mut JoinEditor<B>),
) -> PhysicalPlan<B> {
    rewrite(plan, HashSet::new(), rule)
}

fn rewrite<B: Backend>(
    mut plan: PhysicalPlan<B>,
    mut required: HashSet<RelationId>,
    rule: &mut impl FnMut(&mut JoinEditor<B>),
) -> PhysicalPlan<B> {
    if !matches!(plan.op, PhysicalOp::Join { .. }) {
        required.extend(plan.expressions().into_iter().flat_map(Expr::relations));
    }
    plan.inputs = plan
        .inputs
        .into_iter()
        .map(|input| rewrite(input, required.clone(), rule))
        .collect();
    let Ok(mut join) = JoinEditor::new(plan.clone(), required) else {
        return plan;
    };
    rule(&mut join);
    join.finish()
}

impl<B: Backend> PhysicalPlan<B> {
    pub fn relation_id(&self) -> Option<RelationId> {
        match &self.op {
            PhysicalOp::Scan { relation, .. } | PhysicalOp::Alias { relation, .. } => {
                Some(*relation)
            }
            PhysicalOp::Filter(_)
            | PhysicalOp::Project(_)
            | PhysicalOp::Sort(_)
            | PhysicalOp::Limit(_)
            | PhysicalOp::Deduplicate { .. } => self.inputs.first().and_then(Self::relation_id),
            _ => None,
        }
    }

    pub fn visible_relations(&self) -> HashSet<RelationId> {
        let mut relations = HashSet::new();
        self.visit(&mut |plan| match &plan.op {
            PhysicalOp::Scan { relation, .. } | PhysicalOp::Alias { relation, .. } => {
                relations.insert(*relation);
            }
            _ => {}
        });
        relations
    }

    pub fn read_relations(&self) -> HashSet<RelationId> {
        let mut relations = HashSet::new();
        self.visit(&mut |plan| {
            for expression in plan.expressions() {
                relations.extend(expression.relations());
            }
        });
        relations
    }

    pub fn map_expressions(mut self, map: &mut impl FnMut(Expr) -> Expr) -> Self {
        self.op = match self.op {
            PhysicalOp::Filter(expression) => PhysicalOp::Filter(expression.rewrite(map)),
            PhysicalOp::Project(columns) => PhysicalOp::Project(map_named(columns, map)),
            PhysicalOp::Join {
                conditions,
                strategy,
            } => PhysicalOp::Join {
                conditions: conditions
                    .into_iter()
                    .map(|condition| condition.rewrite(map))
                    .collect(),
                strategy,
            },
            PhysicalOp::SemiJoin(condition) => PhysicalOp::SemiJoin(condition.rewrite(map)),
            PhysicalOp::Aggregate { groups, metrics } => PhysicalOp::Aggregate {
                groups: map_named(groups, map),
                metrics: map_named(metrics, map),
            },
            PhysicalOp::Sort(keys) => PhysicalOp::Sort(
                keys.into_iter()
                    .map(|key| SortKey {
                        expression: key.expression.rewrite(map),
                        descending: key.descending,
                    })
                    .collect(),
            ),
            PhysicalOp::Deduplicate { keys, strategy } => PhysicalOp::Deduplicate {
                keys: keys.into_iter().map(|key| key.rewrite(map)).collect(),
                strategy,
            },
            PhysicalOp::FusedNeighbors {
                outgoing_predicate,
                incoming_predicate,
                columns,
            } => PhysicalOp::FusedNeighbors {
                outgoing_predicate: outgoing_predicate.rewrite(map),
                incoming_predicate: incoming_predicate.rewrite(map),
                columns: map_named(columns, map),
            },
            PhysicalOp::ScopeGuard(guard) => PhysicalOp::ScopeGuard(guard),
            PhysicalOp::ReadColumns(columns) => PhysicalOp::ReadColumns(columns),
            other => other,
        };
        self
    }

    pub fn rebind(self, from: RelationId, to: RelationId) -> Self {
        let mut plan = self.map_expressions(&mut |expression| expression.rebind(from, to));
        plan.op = match plan.op {
            PhysicalOp::Scan {
                relation,
                alias,
                access,
            } if relation == from => PhysicalOp::Scan {
                relation: to,
                alias,
                access,
            },
            PhysicalOp::Alias { relation, alias } if relation == from => {
                PhysicalOp::Alias {
                    relation: to,
                    alias,
                }
            }
            other => other,
        };
        plan.inputs = plan
            .inputs
            .into_iter()
            .map(|input| input.rebind(from, to))
            .collect();
        plan
    }

    pub fn expressions(&self) -> Vec<&Expr> {
        match &self.op {
            PhysicalOp::Filter(expression) | PhysicalOp::SemiJoin(expression) => vec![expression],
            PhysicalOp::Project(columns) => {
                columns.iter().map(|column| &column.expression).collect()
            }
            PhysicalOp::Join { conditions, .. } => conditions.iter().collect(),
            PhysicalOp::Aggregate { groups, metrics } => groups
                .iter()
                .chain(metrics)
                .map(|column| &column.expression)
                .collect(),
            PhysicalOp::Sort(keys) => keys.iter().map(|key| &key.expression).collect(),
            PhysicalOp::Deduplicate { keys, .. } => keys.iter().collect(),
            PhysicalOp::FusedNeighbors {
                outgoing_predicate,
                incoming_predicate,
                columns,
            } => std::iter::once(outgoing_predicate)
                .chain([incoming_predicate])
                .chain(columns.iter().map(|column| &column.expression))
                .collect(),
            PhysicalOp::ScopeGuard(_) => Vec::new(),
            PhysicalOp::ReadColumns(_) => Vec::new(),
            _ => Vec::new(),
        }
    }

    pub fn required_columns(&self) -> HashSet<ColumnRef> {
        self.expressions()
            .into_iter()
            .flat_map(columns)
            .chain(self.inputs.iter().flat_map(PhysicalPlan::required_columns))
            .collect()
    }
}

fn columns(expression: &Expr) -> HashSet<ColumnRef> {
    let mut columns = HashSet::new();
    expression.clone().rewrite(&mut |expression| {
        if let Expr::Column(column) = &expression {
            columns.insert(column.clone());
        }
        expression
    });
    columns
}

fn equality(expression: &Expr) -> Option<(ColumnRef, ColumnRef)> {
    let Expr::Compare {
        op: CompareOp::Eq,
        left,
        right,
    } = expression
    else {
        return None;
    };
    let (Expr::Column(left), Expr::Column(right)) = (left.as_ref(), right.as_ref()) else {
        return None;
    };
    Some((left.clone(), right.clone()))
}

fn tautology(expression: &Expr) -> bool {
    matches!(
        expression,
        Expr::Compare {
            op: CompareOp::Eq,
            left,
            right,
        } if left == right
    )
}

fn map_named(
    expressions: Vec<NamedExpr>,
    map: &mut impl FnMut(Expr) -> Expr,
) -> Vec<NamedExpr> {
    expressions
        .into_iter()
        .map(|expression| NamedExpr {
            expression: expression.expression.rewrite(map),
            name: expression.name,
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::passes::logical_v3::{column, literal, Plan};
    use crate::passes::physical_v3::{DuckDb, DuckDbAccess};

    fn scan(relation: u32) -> PhysicalPlan<DuckDb> {
        Plan::leaf(PhysicalOp::Scan {
            relation: RelationId(relation),
            alias: format!("r{relation}"),
            access: DuckDbAccess::Table(format!("t{relation}")),
        })
    }

    #[test]
    fn editor_rewrites_join_by_stable_relation_identity() {
        let plan = Plan::nary(
            PhysicalOp::Join {
                conditions: vec![column(RelationId(1), "id").eq(column(RelationId(2), "id"))],
                strategy: JoinStrategy::Default,
            },
            [scan(1), scan(2)],
        );
        let mut output = rewrite_joins(plan, &mut |join| {
            let (_, other) = join.equality_for(RelationId(2)).unwrap();
            join.remove(RelationId(2));
            join.add_filter(column(other.relation, other.name).eq(literal(1_i64)));
        });
        assert!(matches!(output.op, PhysicalOp::Filter(_)));
        assert_eq!(output.inputs.pop().unwrap().relation_id(), Some(RelationId(1)));
    }

    #[test]
    fn editor_converts_relation_to_semi_join() {
        let condition = column(RelationId(1), "id").eq(column(RelationId(2), "id"));
        let plan = Plan::nary(
            PhysicalOp::Join {
                conditions: vec![condition.clone()],
                strategy: JoinStrategy::Default,
            },
            [scan(1), scan(2)],
        );
        let output = rewrite_joins(plan, &mut |join| {
            join.convert_to_semi(RelationId(2), condition.clone());
        });
        assert!(matches!(output.op, PhysicalOp::SemiJoin(_)));
    }
}
