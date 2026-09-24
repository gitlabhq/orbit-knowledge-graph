use super::super::super::*;
use crate::passes::logical_v3::{column, Expr};
use ontology::constants::DEFAULT_PRIMARY_KEY;

pub fn apply<B: Backend>(plan: PhysicalPlan<B>, catalog: &PhysicalCatalog<'_>) -> PhysicalPlan<B> {
    rewrite_joins(plan, &mut |join| {
        let relations: Vec<_> = join.relations().collect();
        for relation in relations {
            let Some(node) = catalog.node(relation) else {
                continue;
            };
            if join.required(relation) {
                continue;
            }
            let Some((own, other)) = join.equality_for(relation) else {
                continue;
            };
            if own.relation != relation || own.name != DEFAULT_PRIMARY_KEY {
                continue;
            }
            if node.filters.is_empty() && node.id_range.is_none() && !node.node_ids.is_empty() {
                let predicate = match node.node_ids.as_slice() {
                    [id] => column(other.relation, &other.name)
                        .eq(Expr::Literal((*id).into())),
                    ids => Expr::In {
                        value: Box::new(column(other.relation, &other.name)),
                        values: ids.iter().copied().map(Into::into).collect(),
                        data_type: Some(ontology::DataType::Int),
                    },
                };
                join.remove(relation);
                join.add_filter(predicate);
            } else if !node.filters.is_empty() || node.id_range.is_some() {
                join.convert_to_semi(
                    relation,
                    column(other.relation, &other.name)
                        .eq(column(relation, DEFAULT_PRIMARY_KEY)),
                );
            }
        }
    })
}
