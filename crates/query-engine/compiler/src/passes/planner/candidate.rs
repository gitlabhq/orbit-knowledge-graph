use super::*;
use ontology::constants::DEFAULT_PRIMARY_KEY;

pub(super) fn build<B: Flavor>(bound: &BoundCatalog, plan: Plan<B>, cost: Cost) -> Candidate<B> {
    let visible = visible_relations(&plan);
    let outputs = bound
        .relations
        .iter()
        .filter_map(|(relation, metadata)| {
            let RelationOrigin::Node { input } = metadata.origin else {
                return None;
            };
            if !visible.contains(relation) {
                return None;
            }
            let primary_key = bound.column_ids.get(&ColumnKey {
                relation: *relation,
                name: DEFAULT_PRIMARY_KEY.into(),
            })?;
            Some((
                input,
                OutputBinding {
                    relation: *relation,
                    primary_key: *primary_key,
                },
            ))
        })
        .collect();
    Candidate {
        properties: physical_properties(bound, &plan),
        plan,
        columns: ColumnBindings {
            columns: bound
                .columns
                .iter()
                .filter_map(|(id, column)| {
                    visible
                        .contains(&column.relation)
                        .then_some((*id, Expr::Column(*id)))
                })
                .collect(),
        },
        outputs: OutputBindings { nodes: outputs },
        cost,
    }
}

pub(super) fn relation_columns(bound: &BoundCatalog, relation: RelationId) -> BTreeSet<ColumnId> {
    bound
        .columns
        .iter()
        .filter_map(|(id, column)| (column.relation == relation).then_some(*id))
        .collect()
}

fn physical_properties<B: Flavor>(bound: &BoundCatalog, plan: &Plan<B>) -> PhysicalProperties {
    let mut properties = PhysicalProperties::default();
    plan.visit(&mut |plan| match &plan.operator {
        Operator::Sort(keys) => properties.ordered_by = keys.clone(),
        Operator::CurrentRows { keys, .. } => {
            for key in keys {
                if let Expr::Column(column) = key
                    && let Some(column) = bound.columns.get(column)
                {
                    properties.current_relations.insert(column.relation);
                }
            }
        }
        _ => {}
    });
    properties
}

fn visible_relations<B: Flavor>(plan: &Plan<B>) -> BTreeSet<RelationId> {
    let mut relations = BTreeSet::new();
    plan.visit(&mut |plan| {
        if let Operator::Scan(scan) = &plan.operator {
            relations.insert(scan.relation());
        }
    });
    relations
}
