mod global;
mod namespace;

use super::plan::build_plans;
use ontology::Ontology;

pub use global::GlobalDispatcher;
pub use namespace::NamespaceDispatcher;

pub fn entity_names_by_scope(ontology: &Ontology) -> (Vec<String>, Vec<String>) {
    let plans = build_plans(ontology, 1, 1, &Default::default());
    let global = plans.global.iter().map(|p| p.name.clone()).collect();
    let namespaced = plans.namespaced.iter().map(|p| p.name.clone()).collect();
    (global, namespaced)
}
