mod global;
mod namespace;

use super::plan::build_plans;
use ontology::{EtlConfig, Ontology};

pub use global::GlobalDispatcher;
pub use namespace::NamespaceDispatcher;

#[derive(Debug, Clone)]
pub struct EntityInfo {
    pub name: String,
    /// The source datalake table name (e.g. "siphon_ci_builds").
    /// None for entities with Query-based extraction (JOINs).
    pub source_table: Option<String>,
}

pub fn entity_info_by_scope(ontology: &Ontology) -> (Vec<EntityInfo>, Vec<EntityInfo>) {
    let plans = build_plans(ontology, 1, 1, &Default::default());

    let to_info = |plan: &super::plan::PipelinePlan| {
        let source_table = ontology
            .get_node(&plan.name)
            .and_then(|node| match &node.etl {
                Some(EtlConfig::Table { source, .. }) => Some(source.clone()),
                _ => None,
            });
        EntityInfo {
            name: plan.name.clone(),
            source_table,
        }
    };

    let global = plans.global.iter().map(to_info).collect();
    let namespaced = plans.namespaced.iter().map(to_info).collect();
    (global, namespaced)
}
