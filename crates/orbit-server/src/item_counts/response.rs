use std::collections::HashMap;

use ontology::Ontology;

use super::ScopeItemCounts;
use crate::proto::{
    DomainItemCount, EntityItemCount, GetItemCountsRequest, GetItemCountsResponse,
    NamespaceItemCounts,
};

pub fn build_item_counts_response(
    ontology: &Ontology,
    request: &GetItemCountsRequest,
    counts: &[ScopeItemCounts],
) -> GetItemCountsResponse {
    let mut namespaces: Vec<NamespaceItemCounts> = counts
        .iter()
        .map(|scope_counts| NamespaceItemCounts {
            traversal_path: scope_counts.scope.as_str().to_string(),
            domains: build_domain_item_counts(ontology, &scope_counts.counts),
        })
        .collect();
    if request.traversal_paths.is_empty() {
        let domains = namespaces.pop().map(|namespace| namespace.domains);
        return GetItemCountsResponse {
            domains: domains.unwrap_or_default(),
            ..Default::default()
        };
    }
    GetItemCountsResponse {
        namespaces,
        ..Default::default()
    }
}

fn build_domain_item_counts(
    ontology: &Ontology,
    counts: &HashMap<String, i64>,
) -> Vec<DomainItemCount> {
    ontology
        .domains()
        .filter_map(|domain| {
            let entities: Vec<EntityItemCount> = domain
                .node_names
                .iter()
                .filter_map(|name| {
                    let count = *counts.get(name)?;
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
        .collect()
}
