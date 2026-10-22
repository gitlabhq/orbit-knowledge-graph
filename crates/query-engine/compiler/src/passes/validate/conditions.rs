use super::*;
use crate::query_limits::{MAX_FILTERS_PER_NODE, MAX_FILTERS_PER_REL};

impl<M: query_data_model::OrbitQueryModel> Validator<'_, M> {
    pub(super) fn check_predicate_shape(&self, input: &Input) -> Result<()> {
        if input
            .predicates
            .iter()
            .any(|root| root.depth() > MAX_PREDICATE_DEPTH)
        {
            return Err(QueryError::Validation(format!(
                "conditions must not nest deeper than {MAX_PREDICATE_DEPTH} levels"
            )));
        }
        for root in &input.predicates {
            match root.relationship_type() {
                Some((condition, _)) => self.check_relationship_type(condition)?,
                None if root.leaves().any(Condition::is_relationship_type) => {
                    return Err(QueryError::Validation(
                        "type(r) cannot be used inside a NOT group; write it as its own condition joined with AND, such as NOT type(r) = 'CLOSES'".into(),
                    ));
                }
                None => {}
            }
        }
        for predicate in input.conditions().filter(|c| !c.is_relationship_type()) {
            match &predicate.target {
                PredicateTarget::Node(_) => {
                    for (alias, property) in predicate.node_properties() {
                        let entity = predicate_entity(input, alias)?;
                        self.check_field(entity, property)?;
                    }
                }
                PredicateTarget::Relationship(index) => {
                    if input.relationships.get(*index).is_none() {
                        return Err(QueryError::ReferenceError(
                            "condition references an undefined relationship".into(),
                        ));
                    }
                }
            }
            check_filter_entries(&predicate.property, std::slice::from_ref(&predicate.filter))?;
        }
        Ok(())
    }

    fn check_relationship_type(&self, condition: &Condition) -> Result<()> {
        check_filter_entries("type", std::slice::from_ref(&condition.filter))?;
        let names = condition.relationship_type_names().ok_or_else(|| {
            QueryError::Validation(
                "type(r) supports =, <>, or IN with relationship type names, such as type(r) = 'CLOSES' or type(r) IN ['CLOSES', 'MENTIONS']".into(),
            )
        })?;
        if names
            .iter()
            .any(|name| !self.model.get().relationship_exists(name))
        {
            return Err(QueryError::Validation(
                "type() names a relationship type that the schema does not have; relationship type names are uppercase and case-sensitive, and CALL db.schema() lists them".into(),
            ));
        }
        Ok(())
    }

    pub(super) fn check_predicates(&self, input: &Input) -> Result<()> {
        let compares_nodes = input.conditions().any(Condition::compares_aliases);
        if compares_nodes && input.query_type != QueryType::Traversal {
            return Err(QueryError::Validation(
                "cross-node property comparisons are only supported in traversal queries".into(),
            ));
        }
        let shape = match input.query_type {
            QueryType::PathFinding => Some("shortest paths"),
            QueryType::Neighbors => Some("neighbors queries"),
            _ => None,
        };
        if let Some(shape) = shape
            && input.has_negation()
        {
            return Err(QueryError::Validation(format!(
                "{shape} support NOT only on =, <>, IS NULL, and IS NOT NULL conditions; use a traversal for other negated conditions"
            )));
        }
        for predicate in input
            .predicates
            .iter()
            .flat_map(|root| root.negated_leaves())
        {
            self.check_negated(input, predicate)?;
        }
        Ok(())
    }

    fn check_negated(&self, input: &Input, predicate: &Condition) -> Result<()> {
        for (alias, property) in predicate.node_properties() {
            let entity = predicate_entity(input, alias)?;
            if self.virtual_source(entity, property).is_some() {
                return Err(QueryError::Validation(format!(
                    "filter on \"{property}\" for {entity}: virtual columns cannot be used under NOT"
                )));
            }
            if self
                .property_id(entity, property)
                .is_some_and(|property| self.model.get().property_is_hidden(property))
            {
                return Err(QueryError::Validation(format!(
                    "filter on \"{property}\" for {entity}: this field cannot be used under NOT"
                )));
            }
        }
        Ok(())
    }

    pub(super) fn check_property_comparison(
        &self,
        input: &Input,
        lhs: (&str, &str),
        rhs: (&str, &str),
    ) -> Result<()> {
        let mut types = Vec::new();
        for (node_id, prop) in [lhs, rhs] {
            let entity = predicate_entity(input, node_id)?;
            self.check_field(entity, prop)?;
            if !self.model.get().property_is_filterable(entity, prop) {
                return Err(QueryError::AllowlistRejected(format!(
                    "filter on \"{prop}\" for {entity}: field is not filterable"
                )));
            }
            if self.virtual_source(entity, prop).is_some() {
                return Err(QueryError::Validation(format!(
                    "property comparison cannot reference virtual column \"{prop}\" on {entity}"
                )));
            }
            types.push(self.field_type(entity, prop));
        }
        if let [Some(lt), Some(rt)] = types[..]
            && lt != rt
        {
            return Err(QueryError::Validation(format!(
                "type mismatch in join predicate: {}.{} is {lt:?} but {}.{} is {rt:?}",
                lhs.0, lhs.1, rhs.0, rhs.1
            )));
        }
        Ok(())
    }
}

fn predicate_entity<'a>(input: &'a Input, alias: &str) -> Result<&'a str> {
    input.entity_of(alias).ok_or_else(|| {
        QueryError::ReferenceError(format!("condition references undefined node \"{alias}\""))
    })
}

pub(super) fn check_condition_counts(input: &Input) -> Result<()> {
    let nodes = input.nodes.iter().map(|node| {
        (
            PredicateTarget::Node(node.id.clone()),
            &node.filters,
            MAX_FILTERS_PER_NODE,
        )
    });
    let relationships = input.relationships.iter().enumerate().map(|(index, rel)| {
        (
            PredicateTarget::Relationship(index),
            &rel.filters,
            MAX_FILTERS_PER_REL,
        )
    });
    for (target, filters, maximum) in nodes.chain(relationships) {
        let mut counts: std::collections::HashMap<&str, usize> = filters
            .iter()
            .map(|(property, filters)| (property.as_str(), filters.len()))
            .collect();
        for predicate in input
            .conditions()
            .filter(|predicate| predicate.target == target && !predicate.compares_aliases())
        {
            *counts.entry(&predicate.property).or_default() += 1;
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

pub(super) fn check_condition_column(lhs: (&str, &str), filter: &InputFilter) -> Result<()> {
    let constant = lhs.1.is_empty()
        || filter
            .rhs_column
            .as_ref()
            .is_some_and(|(alias, property)| (alias.as_str(), property.as_str()) == lhs)
        || filter.op == Some(FilterOp::In)
            && filter
                .value
                .as_ref()
                .and_then(serde_json::Value::as_array)
                .is_none_or(Vec::is_empty);
    if constant {
        return Err(QueryError::Validation(CONDITION_COLUMN_RULE.into()));
    }
    Ok(())
}
