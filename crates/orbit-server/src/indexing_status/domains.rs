use std::collections::BTreeMap;

use query_data_model::catalog::sources::PipelineDescriptor;
use query_data_model::{Catalog, ClickHouse, Entity, GraphCatalog};

use super::phase::{Phase, combine_phases};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DomainStatus {
    pub name: String,
    pub phase: Phase,
    pub has_code_nodes: bool,
    pub entities: Vec<EntityStatus>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EntityStatus {
    pub name: String,
    pub phase: Option<Phase>,
}

pub fn get_domain_statuses(
    catalog: &Catalog<ClickHouse>,
    plan_phases: &[(&PipelineDescriptor, Phase)],
    source_unavailable: &[&PipelineDescriptor],
    code_phase: Option<Phase>,
) -> Vec<DomainStatus> {
    let graph = catalog.graph();
    let sources = catalog.model().sources();
    let indexed_by_code = |entity: &Entity| sources.entity_pipelines(entity.id).next().is_none();
    let mut domains = BTreeMap::<&str, Vec<&Entity>>::new();
    for entity in graph.entities() {
        if let Some(domain) = &entity.domain {
            domains.entry(domain).or_default().push(entity);
        }
    }
    domains
        .into_iter()
        .filter_map(|(domain, mut nodes)| {
            nodes.sort_by_key(|entity| entity.name.as_str());
            let has_code_nodes = nodes.iter().any(|entity| indexed_by_code(entity));

            let feeding_plans = plan_phases
                .iter()
                .filter(|(plan, _)| plan_belongs_to_domain(graph, plan, &nodes))
                .map(|(_, phase)| *phase);
            let code = code_phase.filter(|_| has_code_nodes);
            let silenced = source_unavailable
                .iter()
                .any(|plan| plan_belongs_to_domain(graph, plan, &nodes));
            let phase = match combine_phases(feeding_plans.chain(code)) {
                None if silenced => return None,
                phase => phase.unwrap_or(Phase::Unknown),
            };

            let entities = nodes
                .iter()
                .map(|entity| EntityStatus {
                    name: entity.name.clone(),
                    phase: if indexed_by_code(entity) {
                        code_phase
                    } else {
                        get_entity_phase(&entity.name, plan_phases)
                    },
                })
                .collect();

            Some(DomainStatus {
                name: domain.to_string(),
                phase,
                has_code_nodes,
                entities,
            })
        })
        .collect()
}

fn get_entity_phase(name: &str, plan_phases: &[(&PipelineDescriptor, Phase)]) -> Option<Phase> {
    let own_plans = plan_phases
        .iter()
        .filter(|(plan, _)| plan.entity == name)
        .map(|(_, phase)| *phase);
    combine_phases(own_plans)
}

fn plan_belongs_to_domain(
    graph: &GraphCatalog,
    plan: &PipelineDescriptor,
    domain: &[&Entity],
) -> bool {
    let in_domain = |kind: &str| domain.iter().any(|entity| entity.name == kind);
    if in_domain(&plan.entity) {
        return true;
    }

    // A node plan's foreign-key edges belong to the node's domain. Only plans that write no node
    // feed the domains of their edge endpoints.
    if graph.entity_id(&plan.entity).is_some() {
        return false;
    }

    plan.reindex_targets
        .iter()
        .filter_map(|kind| graph.relationship_id(kind))
        .flat_map(|relationship| &graph.relationship(relationship).variants)
        .map(|variant| graph.variant(*variant))
        .any(|variant| {
            in_domain(&graph.entity(variant.source).name)
                || in_domain(&graph.entity(variant.target).name)
        })
}
