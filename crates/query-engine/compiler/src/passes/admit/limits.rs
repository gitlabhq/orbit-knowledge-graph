use std::collections::HashMap;

use crate::constants::internal_column_prefix;
use crate::error::{QueryError, Result};
use crate::input::{Input, PredicateTarget};

const MAX_PREDICATES: usize = 16;
const MAX_FILTERS_PER_NODE: usize = 10;
const MAX_FILTERS_PER_REL: usize = 5;
const MAX_FILTER_ENTRIES_PER_PROPERTY: usize = 10;

pub(super) fn predicates(input: &Input) -> Result<()> {
    let nodes: usize = input
        .nodes
        .iter()
        .map(|node| {
            node.filters.values().map(Vec::len).sum::<usize>()
                + usize::from(!node.node_ids.is_empty())
                + 2 * usize::from(node.id_range.is_some())
        })
        .sum();
    let relationships: usize = input
        .relationships
        .iter()
        .flat_map(|relationship| relationship.filters.values())
        .map(Vec::len)
        .sum();
    let count = input.conditions().count() + nodes + relationships;
    if count > MAX_PREDICATES {
        return Err(QueryError::Validation(format!(
            "Query has {count} predicates; the maximum is {MAX_PREDICATES} across the entire query. Remove redundant conditions or simplify the query."
        )));
    }
    Ok(())
}

pub(super) fn filters(input: &Input) -> Result<()> {
    let nodes = input.nodes.iter().map(|node| {
        let target = PredicateTarget::Node(node.id.clone());
        (target, &node.filters, MAX_FILTERS_PER_NODE)
    });
    let relationships = input.relationships.iter().enumerate().map(|(index, rel)| {
        let target = PredicateTarget::Relationship(index);
        (target, &rel.filters, MAX_FILTERS_PER_REL)
    });
    for (target, filters, maximum) in nodes.chain(relationships) {
        let mut counts: HashMap<&str, usize> = filters
            .iter()
            .map(|(property, filters)| (property.as_str(), filters.len()))
            .collect();
        for condition in input
            .conditions()
            .filter(|condition| condition.target == target && !condition.compares_aliases())
        {
            *counts.entry(&condition.property).or_default() += 1;
        }
        let owner = match &target {
            PredicateTarget::Node(alias) => format!("node \"{alias}\""),
            PredicateTarget::Relationship(index) => format!("relationship[{index}]"),
        };
        if counts.len() > maximum {
            return Err(QueryError::Validation(format!(
                "filter property count ({}) for {owner} must not exceed {maximum}",
                counts.len()
            )));
        }
        if let Some((property, count)) = counts
            .into_iter()
            .find(|(_, count)| *count > MAX_FILTER_ENTRIES_PER_PROPERTY)
        {
            return Err(QueryError::Validation(format!(
                "filter entry count ({count}) on property \"{property}\" for {owner} must not exceed {MAX_FILTER_ENTRIES_PER_PROPERTY}"
            )));
        }
    }
    Ok(())
}

pub(super) fn aliases(input: &Input) -> Result<()> {
    let prefix = internal_column_prefix();
    for (index, node) in input.nodes.iter().enumerate() {
        if format!("{}_", node.id).starts_with(prefix) {
            return Err(QueryError::Validation(format!(
                "identifier {:?} conflicts with reserved prefix {prefix:?}",
                node.id
            )));
        }
        for other in &input.nodes[index + 1..] {
            let (short, long) = match node.id.len() < other.id.len() {
                true => (&node.id, &other.id),
                false => (&other.id, &node.id),
            };
            if long.starts_with(&format!("{short}_")) {
                return Err(QueryError::Validation(format!(
                    "node ids \"{short}\" and \"{long}\" overlap; rename one so neither starts with the other followed by \"_\""
                )));
            }
        }
    }
    Ok(())
}
