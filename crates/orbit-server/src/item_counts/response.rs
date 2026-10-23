use std::collections::HashMap;

use ontology::Ontology;
use orbit_utils::traversal_path::TraversalPath;

use crate::proto::{DomainItemCount, EntityItemCount, GetItemCountsResponse, NamespaceItemCounts};

pub fn build_item_counts_response(
    ontology: &Ontology,
    scopes: &[TraversalPath],
    counts: &[HashMap<String, i64>],
) -> GetItemCountsResponse {
    let namespaces = scopes
        .iter()
        .zip(counts)
        .map(|(scope, counts)| NamespaceItemCounts {
            traversal_path: scope.as_str().to_string(),
            domains: build_domain_item_counts(ontology, counts),
        })
        .collect();
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
