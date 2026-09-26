use ontology::pipelines::PipelineDescriptor;
use ontology::{DomainInfo, NodeEntity, Ontology};

use super::phase::{Phase, fold_phases};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DomainStatus {
    pub name: String,
    pub phase: Phase,
    pub entities: Vec<EntityStatus>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EntityStatus {
    pub name: String,
    pub phase: Option<Phase>,
}

pub fn domain_statuses(
    ontology: &Ontology,
    plan_phases: &[(&PipelineDescriptor, Phase)],
    code_phase: Option<Phase>,
) -> Vec<DomainStatus> {
    ontology
        .domains()
        .map(|domain| {
            let nodes: Vec<&NodeEntity> = domain
                .node_names
                .iter()
                .filter_map(|name| ontology.get_node(name))
                .collect();
            let has_code_nodes = nodes.iter().any(|node| node.pipelines.is_empty());

            let feeding_plans = plan_phases
                .iter()
                .filter(|(plan, _)| plan_feeds_domain(ontology, plan, domain))
                .map(|(_, phase)| *phase);
            let code = code_phase.filter(|_| has_code_nodes);
            let phase = fold_phases(feeding_plans.chain(code)).unwrap_or(Phase::Unknown);

            let entities = nodes
                .iter()
                .map(|node| EntityStatus {
                    name: node.name.clone(),
                    phase: entity_phase(node, plan_phases, code_phase),
                })
                .collect();

            DomainStatus {
                name: domain.name.clone(),
                phase,
                entities,
            }
        })
        .collect()
}

fn entity_phase(
    node: &NodeEntity,
    plan_phases: &[(&PipelineDescriptor, Phase)],
    code_phase: Option<Phase>,
) -> Option<Phase> {
    if node.pipelines.is_empty() {
        return code_phase;
    }
    let own_plans = plan_phases
        .iter()
        .filter(|(plan, _)| plan.entity == node.name)
        .map(|(_, phase)| *phase);
    fold_phases(own_plans)
}

fn plan_feeds_domain(ontology: &Ontology, plan: &PipelineDescriptor, domain: &DomainInfo) -> bool {
    let in_domain = |kind: &str| domain.node_names.iter().any(|name| name == kind);
    if in_domain(&plan.entity) {
        return true;
    }

    let plan_writes_a_node = ontology.get_node(&plan.entity).is_some();
    if plan_writes_a_node {
        return false;
    }

    plan.reindex_targets
        .iter()
        .filter_map(|kind| ontology.get_edge(kind))
        .flatten()
        .any(|edge| in_domain(&edge.source_kind) || in_domain(&edge.target_kind))
}
