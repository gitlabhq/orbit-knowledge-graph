use std::collections::HashMap;

use ontology::Ontology;

use crate::proto::{DomainItemCount, EntityItemCount, GetItemCountsResponse};

pub fn build_item_counts_response(
    ontology: &Ontology,
    counts: &HashMap<String, i64>,
) -> GetItemCountsResponse {
    let domains = ontology
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
        .collect();
    GetItemCountsResponse { domains }
}
