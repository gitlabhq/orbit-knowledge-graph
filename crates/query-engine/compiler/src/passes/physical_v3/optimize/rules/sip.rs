use super::super::super::*;
use crate::passes::logical_v3::{column, named, Plan};

pub fn apply<B: Backend>(plan: PhysicalPlan<B>, catalog: &PhysicalCatalog<'_>) -> PhysicalPlan<B> {
    rewrite_joins(plan, &mut |join| {
        let relations: Vec<_> = join.relations().collect();
        for pair in relations.windows(2) {
            let Some((previous, current)) = join.edge_equality(pair[0], pair[1]) else {
                continue;
            };
            let Some(relation) = join.relation(pair[0]).cloned() else {
                continue;
            };
            if !catalog.selective(pair[0]) {
                continue;
            }
            let lookup_relation = join.fresh_relation();
            let lookup = relation.rebind(pair[0], lookup_relation);
            let lookup = Plan::unary(
                PhysicalOp::Project(vec![named(
                    column(lookup_relation, &previous.name),
                    previous.name.clone(),
                )]),
                lookup,
            );
            join.ensure_input(lookup_relation, || lookup.clone());
            join.convert_to_semi(
                lookup_relation,
                column(current.relation, &current.name)
                    .eq(column(lookup_relation, &previous.name)),
            );
        }
    })
}
