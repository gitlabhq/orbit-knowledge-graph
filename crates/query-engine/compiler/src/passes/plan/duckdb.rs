use std::collections::{HashMap, HashSet};

use ontology::constants::*;
use query_data_model::DuckDbDataModel;

use super::context::PlanningContext;
use super::physical::{BindingSource, ExecutionPlan, PhysicalSource};
use super::requirements::{Column, Predicate, node_outputs, node_predicates, property_filter};
use super::{HydrationCompileOptions, HydrationStrategy, QueryPlan};
use crate::input::QueryType;
use crate::{Input, QueryError, Result};

pub fn plan(
    input: &Input,
    model: &DuckDbDataModel,
    hydration_options: HydrationCompileOptions,
    _table_scans: &HashSet<String>,
) -> Result<QueryPlan> {
    let context = PlanningContext::new(input, model);
    match input.query_type {
        QueryType::Traversal | QueryType::Aggregation => traversal(context),
        QueryType::Neighbors => super::neighbors::plan_neighbors(context).map(|mut plan| {
            plan.operation.fused_table = None;
            QueryPlan::Neighbors(plan)
        }),
        QueryType::PathFinding => {
            super::pathfinding::plan_pathfinding(context).map(QueryPlan::PathFinding)
        }
        QueryType::Hydration => {
            super::hydration::plan_hydration(context, hydration_options).map(QueryPlan::Hydration)
        }
    }
}

fn traversal(mut context: PlanningContext<'_, DuckDbDataModel>) -> Result<QueryPlan> {
    let input = context.input;
    context.hops = super::edge_chain::build_hops(input, context.model)?;
    for (index, hop) in context.hops.iter_mut().enumerate() {
        hop.alias = format!("e{index}");
    }
    let mut nodes = super::edge_chain::build_node_plans(input, context.model);
    context.denormalized = super::denormalized_facts(input, context.model);
    let placement = super::conditions::place_conditions(
        input,
        context.model,
        &context.hops,
        &nodes,
        &context.denormalized,
    )?;
    context.bind_scan_conditions(&mut nodes, &placement)?;
    context.nodes = nodes;
    let aggregate = context.aggregate();
    for node in context.nodes.values_mut() {
        node.hydration = HydrationStrategy::Join;
        node.emit_select = !aggregate
            || crate::input::node_group_ids(&input.aggregation.group_by)
                .any(|alias| alias == node.alias);
    }
    let mut source: Option<PhysicalSource> = None;
    let mut endpoints = HashMap::<String, Column>::new();
    let mut outputs = Vec::new();
    for (index, hop) in context.hops.iter().enumerate() {
        let alias = format!("e{index}");
        let edge = if hop.max_hops > 1 {
            super::hops::multi_hop(hop, &alias, &context.nodes)
        } else {
            PhysicalSource::edge(hop, &alias, false)
                .filter(context.edge_predicates(&alias, hop, false))
        }
        .filter(
            hop.filters
                .iter()
                .map(|(property, filter)| property_filter(&alias, property, filter))
                .collect(),
        );
        let (start, end) = hop.direction.edge_columns();
        let current = [
            (&hop.from_node, Column::new(&alias, start)),
            (&hop.to_node, Column::new(&alias, end)),
        ];
        source = Some(match source {
            None => edge,
            Some(previous) => {
                let mut equalities = current.iter().filter_map(|(node, column)| {
                    endpoints
                        .get(*node)
                        .map(|previous| (previous.clone(), column.clone()))
                });
                let endpoints = equalities
                    .next()
                    .ok_or_else(|| QueryError::Lowering("disconnected traversal".into()))?;
                PhysicalSource::Join {
                    endpoints,
                    predicates: equalities
                        .map(|(left, right)| Predicate::ColumnsEqual { left, right })
                        .collect(),
                    left: Box::new(previous),
                    right: Box::new(edge),
                }
            }
        });
        for (node, column) in current {
            endpoints.entry(node.clone()).or_insert(column);
        }
        if !context.aggregate() {
            outputs.extend(super::requirements::edge_outputs(hop, &alias));
        }
    }
    let mut bindings = Vec::new();
    for input_node in &input.nodes {
        let node = &context.nodes[&input_node.id];
        let scan = PhysicalSource::Scan {
            table: node
                .table
                .clone()
                .ok_or_else(|| QueryError::Lowering("node table missing".into()))?,
            alias: node.alias.clone(),
            final_: false,
            relationship: None,
        }
        .filter(node_predicates(node));
        source = Some(match source {
            None => scan,
            Some(previous) => {
                let endpoint = endpoints.get(&node.alias).ok_or_else(|| {
                    QueryError::Lowering("node is not connected to an edge".into())
                })?;
                previous.inner_join(
                    scan,
                    (
                        Column::new(&node.alias, DEFAULT_PRIMARY_KEY),
                        endpoint.clone(),
                    ),
                )
            }
        });
        outputs.extend(node_outputs(node));
        let endpoint = endpoints.get(&node.alias);
        bindings.push(BindingSource {
            node: node.alias.clone(),
            alias: endpoint.map_or_else(|| node.alias.clone(), |column| column.source.clone()),
            column: endpoint
                .map_or_else(|| DEFAULT_PRIMARY_KEY.into(), |column| column.name.clone()),
            joined: true,
        });
    }
    let execution = ExecutionPlan {
        source: source
            .ok_or_else(|| QueryError::Lowering("query has no nodes".into()))?
            .filter(context.bind_join_conditions(&bindings, &placement)?),
        definitions: vec![],
        outputs,
        bindings,
    };
    Ok(context.finish_execution(execution))
}
