mod rules;

use super::*;

pub fn optimize_clickhouse(
    mut plan: PhysicalPlan<ClickHouse>,
    catalog: &PhysicalCatalog<'_>,
) -> PhysicalPlan<ClickHouse> {
    plan = rules::scope::apply(plan, catalog);
    loop {
        let previous = plan.clone();
        plan = rules::fk::apply(plan, catalog);
        plan = rules::prune::apply(plan, catalog);
        plan = rules::sip::apply(plan, catalog);
        if plan == previous {
            break;
        }
    }
    plan = rules::denorm::apply(plan, catalog);
    plan = rules::text_index::apply(plan, catalog);
    plan = rules::neighbors::apply(plan);
    plan = rules::dedup::apply(plan, catalog);
    rules::columns::apply(plan)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::input::{Direction, Input, InputRelationship};
    use crate::passes::logical_v3::{column, edge, join, node, LogicalPlan, LogicalRelation, LogicalRelationSource, RelationId};
    use ontology::constants::{SOURCE_ID_COLUMN, TARGET_ID_COLUMN};
    use std::collections::BTreeMap;

    #[test]
    fn complete_fk_chain_elides_edge_by_relation_id() {
        let edge_id = RelationId(0);
        let a_id = RelationId(1);
        let b_id = RelationId(2);
        let root = join(
            [edge(edge_id, vec!["REL".into()], "edge"), node(a_id, "A", "a"), node(b_id, "B", "b")],
            [
                column(edge_id, SOURCE_ID_COLUMN).eq(column(a_id, "id")),
                column(edge_id, TARGET_ID_COLUMN).eq(column(b_id, "id")),
            ],
        );
        let logical = LogicalPlan {
            root,
            relations: BTreeMap::from([
                (edge_id, LogicalRelation { alias: "edge".into(), source: LogicalRelationSource::Edge(Some(0)) }),
                (a_id, LogicalRelation { alias: "a".into(), source: LogicalRelationSource::Node(0) }),
                (b_id, LogicalRelation { alias: "b".into(), source: LogicalRelationSource::Node(1) }),
            ]),
        };
        let mut input = Input::default();
        input.nodes = vec![
            crate::input::InputNode { id: "a".into(), entity: Some("A".into()), ..Default::default() },
            crate::input::InputNode { id: "b".into(), entity: Some("B".into()), ..Default::default() },
        ];
        input.relationships = vec![InputRelationship {
            types: vec!["REL".into()],
            from: "a".into(),
            to: "b".into(),
            hops: Default::default(),
            direction: Direction::Outgoing,
            filters: Default::default(),
            fk_column: None,
            scope_prefix: None,
            scope_preserving: false,
        }];
        let ontology = ontology::Ontology::new()
            .with_nodes(["A", "B"])
            .with_edges(["REL"]);
        let catalog = PhysicalCatalog::new(&logical, &input, &ontology);
        let plan = plan_clickhouse(logical, &catalog);
        assert!(plan.visible_relations().contains(&edge_id));
    }

    #[test]
    fn catalog_uses_ontology_table_and_sort_key() {
        let relation = RelationId(0);
        let logical = LogicalPlan {
            root: node(relation, "A", "a"),
            relations: BTreeMap::from([(
                relation,
                LogicalRelation {
                    alias: "a".into(),
                    source: LogicalRelationSource::Node(0),
                },
            )]),
        };
        let input = Input {
            nodes: vec![crate::input::InputNode {
                id: "a".into(),
                entity: Some("A".into()),
                ..Default::default()
            }],
            ..Default::default()
        };
        let ontology = ontology::Ontology::new().with_nodes(["A"]);
        let catalog = PhysicalCatalog::new(&logical, &input, &ontology);
        assert_eq!(catalog.node_table(relation), Some("gl_a"));
        assert!(!catalog.sort_key(relation).is_empty());
    }

    #[test]
    fn dedup_rule_uses_catalog_scan_role() {
        let relation = RelationId(0);
        let logical = LogicalPlan {
            root: node(relation, "A", "a"),
            relations: BTreeMap::from([(
                relation,
                LogicalRelation {
                    alias: "a".into(),
                    source: LogicalRelationSource::Node(0),
                },
            )]),
        };
        let input = Input {
            nodes: vec![crate::input::InputNode {
                id: "a".into(),
                entity: Some("A".into()),
                ..Default::default()
            }],
            ..Default::default()
        };
        let ontology = ontology::Ontology::new().with_nodes(["A"]);
        let catalog = PhysicalCatalog::new(&logical, &input, &ontology);
        let plan = rules::dedup::apply(plan_clickhouse(logical, &catalog), &catalog);
        assert!(matches!(plan.op, PhysicalOp::Deduplicate { strategy: ClickHouseDedup::Final, .. }));
    }

    #[test]
    fn column_rule_attaches_only_required_columns() {
        let relation = RelationId(0);
        let plan: PhysicalPlan<ClickHouse> = crate::passes::logical_v3::Plan::unary(
            PhysicalOp::Project(vec![crate::passes::logical_v3::named(
                column(relation, "name"),
                "name",
            )]),
            crate::passes::logical_v3::Plan::leaf(PhysicalOp::Scan {
                relation,
                alias: "a".into(),
                access: ClickHouseAccess::Table("a".into()),
            }),
        );
        let plan = rules::columns::apply(plan);
        assert!(matches!(plan.inputs[0].op, PhysicalOp::ReadColumns(ref columns) if columns == &["name"]));
    }
}
