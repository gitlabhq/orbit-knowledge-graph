use ontology::Ontology;

use super::ScopeItemCounts;
use crate::proto::{DomainItemCount, EntityItemCount, GetItemCountsResponse, NamespaceItemCounts};

pub fn build_item_counts_response(
    ontology: &Ontology,
    scope_counts: &[ScopeItemCounts],
) -> GetItemCountsResponse {
    GetItemCountsResponse {
        counts: scope_counts
            .iter()
            .map(|scope_counts| build_namespace_counts(ontology, scope_counts))
            .collect(),
    }
}

fn build_namespace_counts(
    ontology: &Ontology,
    scope_counts: &ScopeItemCounts,
) -> NamespaceItemCounts {
    let domains = ontology
        .domains()
        .filter_map(|domain| {
            let entities: Vec<EntityItemCount> = domain
                .node_names
                .iter()
                .filter_map(|name| {
                    let count = *scope_counts.counts.get(name)?;
                    Some(EntityItemCount {
                        name: name.clone(),
                        count,
                    })
                })
                .collect();
            (!entities.is_empty()).then(|| DomainItemCount {
                name: domain.name.clone(),
                entities,
            })
        })
        .collect();
    NamespaceItemCounts {
        traversal_path: scope_counts.scope.to_string(),
        domains,
    }
}
