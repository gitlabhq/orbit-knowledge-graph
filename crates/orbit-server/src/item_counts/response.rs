use std::collections::HashMap;

use query_data_model::Model;

use super::ScopeItemCounts;
use crate::proto::{
    DomainItemCount, EntityItemCount, GetItemCountsRequest, GetItemCountsResponse,
    NamespaceItemCounts,
};

pub fn build_item_counts_response(
    model: &Model,
    request: &GetItemCountsRequest,
    counts: &[ScopeItemCounts],
) -> GetItemCountsResponse {
    let mut namespaces: Vec<NamespaceItemCounts> = counts
        .iter()
        .map(|scope_counts| NamespaceItemCounts {
            traversal_path: scope_counts.scope.as_str().to_string(),
            domains: build_domain_item_counts(model, &scope_counts.counts),
        })
        .collect();
    let domains = match request.traversal_paths.is_empty() {
        true => namespaces.pop().map(|namespace| namespace.domains),
        false => None,
    };
    GetItemCountsResponse {
        domains: domains.unwrap_or_default(),
        namespaces,
    }
}

fn build_domain_item_counts(model: &Model, counts: &HashMap<String, i64>) -> Vec<DomainItemCount> {
    let graph = model.graph();
    graph
        .domains()
        .filter_map(|domain| {
            let entities: Vec<EntityItemCount> = graph
                .entities()
                .filter(|entity| entity.domain.as_deref() == Some(domain.name.as_str()))
                .filter_map(|entity| {
                    let count = *counts.get(&entity.name)?;
                    Some(EntityItemCount {
                        name: entity.name.clone(),
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
