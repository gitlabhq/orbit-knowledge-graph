use super::*;
use crate::error::Result;

pub fn plan_duckdb(bound: &BoundCatalog, logical: LogicalPlan) -> Result<PlanningResult<DuckDb>> {
    let plan = map_plan(bound, &logical.root);
    Ok(PlanningResult {
        logical,
        selected: SelectedPlan {
            candidate: candidate::build(bound, plan, Cost::default()),
        },
    })
}

fn map_plan(bound: &BoundCatalog, logical: &Plan<Logical>) -> Plan<DuckDb> {
    let inputs = logical
        .inputs
        .iter()
        .map(|input| map_plan(bound, input))
        .collect();
    let operator = match &logical.operator {
        Operator::Scan(scan) => Operator::Scan(PhysicalScan {
            relation: scan.relation,
            access: DuckDbAccess::Table(TableAccess {
                layout: table_layout(bound, scan.relation),
            }),
            columns: candidate::relation_columns(bound, scan.relation),
        }),
        Operator::Filter(expression) => Operator::Filter(expression.clone()),
        Operator::Project(columns) => Operator::Project(columns.clone()),
        Operator::Join(conditions) => Operator::Join(conditions.clone()),
        Operator::SemiJoin(condition) => Operator::SemiJoin(condition.clone()),
        Operator::Aggregate { groups, metrics } => Operator::Aggregate {
            groups: groups.clone(),
            metrics: metrics.clone(),
        },
        Operator::Union => Operator::Union,
        Operator::Bind(relation) => Operator::Bind(*relation),
        Operator::Sort(keys) => Operator::Sort(keys.clone()),
        Operator::Limit(limit) => Operator::Limit(*limit),
        Operator::CurrentRows { keys, .. } => Operator::CurrentRows {
            keys: keys.clone(),
            strategy: DuckDbCurrentRows,
        },
        Operator::Extension(()) => unreachable!(),
    };
    Plan { operator, inputs }
}

fn table_layout(bound: &BoundCatalog, relation: RelationId) -> TableLayout {
    let table = match bound.relation(relation) {
        BoundRelation {
            entity: Some(entity),
            ..
        } => bound
            .ontology
            .table_name(&bound.entities[entity].name)
            .unwrap_or_default(),
        metadata => metadata
            .relationships
            .first()
            .map(|relationship| {
                bound
                    .ontology
                    .edge_table_for_relationship(bound.relationship_name(*relationship))
            })
            .unwrap_or_else(|| bound.ontology.edge_table()),
    };
    TableLayout {
        table: TableName(table.into()),
        columns: bound
            .input
            .compiler
            .table_columns
            .get(table)
            .into_iter()
            .flatten()
            .cloned()
            .map(PhysicalColumn)
            .collect(),
        sort_key: vec![],
        global: bound.ontology.is_global_table(table),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::input::{Direction, InputNode, InputRelationship, QueryType};

    #[test]
    fn passes_logical_plan_directly_to_duckdb() {
        let input = Input {
            query_type: QueryType::Traversal,
            nodes: vec![
                InputNode {
                    id: "u".into(),
                    entity: Some("User".into()),
                    node_ids: vec![1],
                    ..Default::default()
                },
                InputNode {
                    id: "mr".into(),
                    entity: Some("MergeRequest".into()),
                    ..Default::default()
                },
            ],
            relationships: vec![InputRelationship {
                types: vec!["AUTHORED".into()],
                from: "u".into(),
                to: "mr".into(),
                hops: Default::default(),
                direction: Direction::Outgoing,
                filters: Default::default(),
                fk_column: None,
                scope_proof: None,
                scope_preserving: false,
            }],
            ..Default::default()
        };
        let ontology = Ontology::new()
            .with_nodes(["User", "MergeRequest"])
            .with_edges(["AUTHORED"]);
        let (bound, logical) = bind(input, Arc::new(ontology)).unwrap();

        let planned = plan_duckdb(&bound, logical).unwrap();

        assert_eq!(planned.selected.candidate.cost, Cost::default());
        assert_eq!(
            planned.selected.candidate.outputs.nodes.len(),
            bound.input.nodes.len()
        );
        let lowered = lower_duckdb(&bound, planned.selected);
        assert!(lowered.is_ok(), "{lowered:?}");
    }
}
