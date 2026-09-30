use std::collections::HashMap;

use ontology::constants::*;

use crate::ast::{Expr, JoinType, SelectExpr};
use crate::constants::{DEPTH_COLUMN, PATH_NODES_COLUMN};
use crate::input::Direction;
use crate::passes::shared::{deleted_false, rel_kind_filter};

use super::physical::{PhysicalPlan, PhysicalSource};
use super::{Hop, NodePlan};

pub(super) fn multi_hop(
    hop: &Hop,
    alias: &str,
    nodes: &HashMap<String, NodePlan>,
) -> PhysicalSource {
    let arms = (hop.min_hops.max(1)..=hop.max_hops)
        .map(|depth| depth_arm(hop, depth))
        .collect();
    let (from_kind, to_kind) = match hop.direction {
        Direction::Outgoing | Direction::Both => (SOURCE_KIND_COLUMN, TARGET_KIND_COLUMN),
        Direction::Incoming => (TARGET_KIND_COLUMN, SOURCE_KIND_COLUMN),
    };
    let mut predicates = Vec::new();
    for (node, column) in [(&hop.from_node, from_kind), (&hop.to_node, to_kind)] {
        if let Some(entity) = nodes.get(node).and_then(|node| node.entity.as_ref()) {
            predicates.push(Expr::eq(Expr::col(alias, column), Expr::string(entity)));
        }
    }
    predicates.push(deleted_false(alias));
    PhysicalSource::Filter {
        predicate: Expr::conjoin(predicates).expect("hop has a deletion predicate"),
        input: Box::new(PhysicalSource::Union {
            alias: alias.into(),
            arms,
            relationship: hop.input_index,
        }),
    }
}

fn depth_arm(hop: &Hop, depth: u32) -> PhysicalPlan {
    let (start, end) = hop.direction.edge_columns();
    let end_kind = match hop.direction {
        Direction::Outgoing | Direction::Both => TARGET_KIND_COLUMN,
        Direction::Incoming => SOURCE_KIND_COLUMN,
    };
    let scan = |alias: &str| PhysicalSource::Scan {
        relationship: None,
        table: hop.edge_table.clone(),
        alias: alias.into(),
        final_: false,
    };
    let mut predicate = vec![];
    if let Some(kind) = rel_kind_filter("e1", &hop.rel_types) {
        predicate.push(kind);
    }
    predicate.push(deleted_false("e1"));
    let mut source = scan("e1");
    for index in 2..=depth {
        let previous = format!("e{}", index - 1);
        let current = format!("e{index}");
        let mut condition = Expr::and(
            Expr::eq(Expr::col(&previous, end), Expr::col(&current, start)),
            deleted_false(&current),
        );
        if let Some(kind) = rel_kind_filter(&current, &hop.rel_types) {
            condition = Expr::and(condition, kind);
        }
        source = PhysicalSource::Join {
            kind: JoinType::Inner,
            condition,
            left: Box::new(source),
            right: Box::new(scan(&current)),
        };
    }
    let last = format!("e{depth}");
    let (source_alias, target_alias, kind_alias) = match hop.direction {
        Direction::Outgoing | Direction::Both => ("e1", last.as_str(), "e1"),
        Direction::Incoming => (last.as_str(), "e1", last.as_str()),
    };
    let path = Expr::func(
        "array",
        (1..=depth)
            .map(|index| {
                let alias = format!("e{index}");
                Expr::func(
                    "tuple",
                    vec![Expr::col(&alias, end), Expr::col(&alias, end_kind)],
                )
            })
            .collect(),
    );
    PhysicalPlan {
        source: PhysicalSource::Filter {
            predicate: Expr::conjoin(predicate).expect("hop has a deletion predicate"),
            input: Box::new(source),
        },
        outputs: vec![
            SelectExpr::col("e1", start),
            SelectExpr::col(&last, end),
            SelectExpr::col(kind_alias, RELATIONSHIP_KIND_COLUMN),
            SelectExpr::col(source_alias, SOURCE_ID_COLUMN),
            SelectExpr::col(source_alias, SOURCE_KIND_COLUMN),
            SelectExpr::col(source_alias, SOURCE_TAGS_COLUMN),
            SelectExpr::col(target_alias, TARGET_ID_COLUMN),
            SelectExpr::col(target_alias, TARGET_KIND_COLUMN),
            SelectExpr::col(target_alias, TARGET_TAGS_COLUMN),
            SelectExpr::new(path, PATH_NODES_COLUMN),
            SelectExpr::new(Expr::int(i64::from(depth)), DEPTH_COLUMN),
            SelectExpr::col("e1", DELETED_COLUMN),
            SelectExpr::col("e1", TRAVERSAL_PATH_COLUMN),
        ],
    }
}
