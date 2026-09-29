use std::collections::HashMap;

use query_data_model::QueryDataModel;

use super::{QueryScope, ScopeProof};
use crate::input::{Input, InputRelationship};

pub fn prepare(
    input: &Input,
    proofs: HashMap<String, ScopeProof>,
    model: &(impl QueryDataModel + ?Sized),
) -> QueryScope {
    QueryScope {
        relationships: input
            .relationships
            .iter()
            .map(|relationship| relationship_proof(input, relationship, &proofs, model))
            .collect(),
        nodes: proofs,
    }
}

fn relationship_proof(
    input: &Input,
    relationship: &InputRelationship,
    proofs: &HashMap<String, ScopeProof>,
    model: &(impl QueryDataModel + ?Sized),
) -> Option<ScopeProof> {
    let from_proof = proofs.get(&relationship.from);
    let to_proof = proofs.get(&relationship.to);
    if from_proof == to_proof {
        return from_proof.cloned();
    }

    let entity = |alias: &str| {
        input
            .nodes
            .iter()
            .find(|node| node.id == alias)?
            .entity
            .as_deref()
    };
    let from = entity(&relationship.from)?;
    let to = entity(&relationship.to)?;

    relationship.types.iter().find_map(|kind| {
        match model.variant_scope(kind, from, to) {
            Some(ontology::EdgeVariantScope::PruneToSource) => from_proof,
            Some(ontology::EdgeVariantScope::PruneToTarget) => to_proof,
            _ => None,
        }
        .cloned()
    })
}
