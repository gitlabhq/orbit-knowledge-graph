use ontology::Ontology;

use crate::input::{Direction, HopRange, InputRelationship};
use crate::{Input, QueryError, Result};

pub fn validate_relationships(input: &Input, ontology: &Ontology) -> Result<()> {
    input
        .relationships
        .iter()
        .try_for_each(|edge| check_direction(input, edge, ontology))
}

fn check_direction(input: &Input, edge: &InputRelationship, ontology: &Ontology) -> Result<()> {
    let entity = |id: &str| {
        input
            .nodes
            .iter()
            .find(|node| node.id == id)
            .and_then(|node| node.entity.as_deref())
    };
    let (Some(source), Some(target)) = (entity(&edge.from), entity(&edge.to)) else {
        return Ok(());
    };
    if edge.direction != Direction::Outgoing || edge.hops != HopRange::default() {
        return Ok(());
    }
    let (mut reversed, mut unconnected) = (Vec::new(), Vec::new());
    for kind in &edge.types {
        let variants: Vec<_> = ontology
            .get_edge(kind)
            .unwrap_or_default()
            .iter()
            .filter(|variant| !variant.source_kind.is_empty() && !variant.target_kind.is_empty())
            .collect();
        if variants.is_empty()
            || variants
                .iter()
                .any(|variant| variant.source_kind == source && variant.target_kind == target)
        {
            return Ok(());
        }
        if variants
            .iter()
            .any(|variant| variant.source_kind == target && variant.target_kind == source)
        {
            reversed.push(kind.as_str());
        } else {
            unconnected.push(kind.as_str());
        }
    }
    let (from, to) = (&edge.from, &edge.to);
    let unconnected = unconnected.join("|");
    Err(QueryError::Validation(if reversed.is_empty() {
        format!(
            "{unconnected} does not connect {source} and {target} in either direction; check CALL db.schema('{source}')"
        )
    } else {
        let reversed = reversed.join("|");
        let fix = format!(
            "{reversed} goes from {target} to {source}; reverse the arrow: ({to})-[:{reversed}]->({from})"
        );
        if unconnected.is_empty() {
            fix
        } else {
            format!("{fix}; {unconnected} does not connect {source} and {target}")
        }
    }))
}
