mod config;

use std::collections::HashMap;
use std::sync::LazyLock;

use config::{QueryContract, ValueType};
use minijinja::value::Value;
use minijinja::{Environment, UndefinedBehavior};
use strum::VariantNames;

use crate::input::{AggFunction, FilterOp, Input, PredicateTarget};
use crate::{QueryError, Result};

static CONTRACT: LazyLock<QueryContract> = LazyLock::new(|| {
    let contract: QueryContract = orbit_utils::yaml::from_str(include_str!(concat!(
        env!("CONFIG_DIR"),
        "/query_contract.yaml"
    )))
    .expect("config/query_contract.yaml");
    assert_eq!(
        contract
            .path_types
            .iter()
            .map(String::as_str)
            .collect::<Vec<_>>(),
        crate::input::PathType::VARIANTS
    );
    contract
});

struct ContractEvaluator {
    environment: Environment<'static>,
}

static EVALUATOR: LazyLock<ContractEvaluator> = LazyLock::new(|| {
    let mut environment = Environment::new();
    environment.set_undefined_behavior(UndefinedBehavior::Strict);
    environment.add_filter(
        "flatten",
        |values: Value| -> std::result::Result<Vec<Value>, minijinja::Error> {
            Ok(values
                .try_iter()?
                .flat_map(|value| value.try_iter().into_iter().flatten())
                .collect())
        },
    );
    environment.add_filter(
        "values",
        |value: Value| -> std::result::Result<Vec<Value>, minijinja::Error> {
            value.try_iter()?.map(|key| value.get_item(&key)).collect()
        },
    );
    environment.add_test("owner_above_limit", |owner: Value| {
        let property_count = owner
            .get_attr("property_count")
            .ok()
            .and_then(|value| usize::try_from(value).ok());
        let property_limit = owner
            .get_attr("property_limit")
            .ok()
            .and_then(|value| usize::try_from(value).ok());
        property_count
            .zip(property_limit)
            .is_some_and(|(count, limit)| count > limit)
    });
    environment.add_filter("first_above_limit", |values: Value, limit: usize| {
        values.try_iter().ok().and_then(|mut values| {
            values.find(|value| {
                value
                    .get_attr("count")
                    .ok()
                    .and_then(|count| usize::try_from(count).ok())
                    .is_some_and(|count| count > limit)
            })
        })
    });
    environment.add_filter("first_owner_above_limit", |values: Value| {
        values.try_iter().ok().and_then(|mut values| {
            values.find(|owner| {
                let property_count = owner
                    .get_attr("property_count")
                    .ok()
                    .and_then(|value| usize::try_from(value).ok());
                let property_limit = owner
                    .get_attr("property_limit")
                    .ok()
                    .and_then(|value| usize::try_from(value).ok());
                property_count
                    .zip(property_limit)
                    .is_some_and(|(count, limit)| count > limit)
            })
        })
    });
    ContractEvaluator { environment }
});

pub fn limits() -> &'static config::Limits {
    &CONTRACT.limits
}

pub fn validate_input(input: &Input) -> Result<()> {
    let context = input_context(input);
    for rule in &CONTRACT.input_rules {
        if EVALUATOR
            .environment
            .compile_expression(&rule.reject)
            .and_then(|expression| expression.eval(context.clone()))
            .unwrap_or_else(|error| panic!("contract rule {}: {error}", rule.code))
            .is_true()
        {
            let message = EVALUATOR
                .environment
                .render_str(&rule.message, context)
                .unwrap_or_else(|error| panic!("contract rule {}: {error}", rule.code));
            return Err(QueryError::Validation(message));
        }
    }
    Ok(())
}

pub fn filter_operator_accepts(op: FilterOp, data_type: ontology::DataType) -> bool {
    CONTRACT
        .filter_operators
        .get(op.as_ref())
        .is_some_and(|types| types.contains(&data_type.into()))
}

pub fn aggregation_accepts(function: AggFunction, data_type: ontology::DataType) -> bool {
    CONTRACT
        .aggregations
        .get(&function.to_string())
        .is_some_and(|types| types.contains(&data_type.into()))
}

pub fn ordering_accepts(data_type: ontology::DataType) -> bool {
    CONTRACT.ordering.contains(&data_type.into())
}

fn input_context(input: &Input) -> Value {
    [
        ("input", Value::from_serialize(input)),
        ("limits", Value::from_serialize(limits())),
        ("predicate_count", Value::from(predicate_count(input))),
        ("filter_owners", filter_owners(input)),
    ]
    .into_iter()
    .collect()
}

fn filter_owners(input: &Input) -> Value {
    let nodes = input.nodes.iter().map(|node| {
        (
            PredicateTarget::Node(node.id.clone()),
            &node.filters,
            limits().max_node_filters,
        )
    });
    let relationships = input
        .relationships
        .iter()
        .enumerate()
        .map(|(index, relationship)| {
            (
                PredicateTarget::Relationship(index),
                &relationship.filters,
                limits().max_relationship_filters,
            )
        });
    nodes
        .chain(relationships)
        .map(|(target, filters, maximum)| {
            let mut counts: HashMap<&str, usize> = filters
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
            let entries: Value = counts
                .iter()
                .map(|(property, count)| {
                    [
                        ("property", Value::from(*property)),
                        ("count", Value::from(*count)),
                    ]
                    .into_iter()
                    .collect::<Value>()
                })
                .collect::<Value>();
            [
                ("owner", Value::from(owner)),
                ("property_count", Value::from(counts.len())),
                ("property_limit", Value::from(maximum)),
                ("entries", entries),
            ]
            .into_iter()
            .collect::<Value>()
        })
        .collect::<Value>()
}

fn predicate_count(input: &Input) -> usize {
    input.conditions().count()
        + input
            .nodes
            .iter()
            .map(|node| {
                node.filters.values().map(Vec::len).sum::<usize>()
                    + usize::from(!node.node_ids.is_empty())
                    + 2 * usize::from(node.id_range.is_some())
            })
            .sum::<usize>()
        + input
            .relationships
            .iter()
            .flat_map(|relationship| relationship.filters.values())
            .map(Vec::len)
            .sum::<usize>()
}

impl From<ontology::DataType> for ValueType {
    fn from(data_type: ontology::DataType) -> Self {
        match data_type {
            ontology::DataType::String => Self::String,
            ontology::DataType::Int => Self::Integer,
            ontology::DataType::Float => Self::Number,
            ontology::DataType::Bool => Self::Boolean,
            ontology::DataType::Date => Self::Date,
            ontology::DataType::DateTime => Self::DateTime,
            ontology::DataType::Enum => Self::Enum,
            ontology::DataType::Uuid => Self::Uuid,
        }
    }
}
