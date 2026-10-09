use query_engine::compiler::input::{
    BooleanExpression, FilterOp, InputFilter, PredicateTarget, PropertyPredicate,
};
use serde::Deserialize;

#[derive(Debug, Deserialize)]
#[serde(untagged, deny_unknown_fields)]
pub enum PredicateSetup {
    Not {
        not: Box<Self>,
    },
    And {
        and: Vec<Self>,
    },
    Property {
        node: String,
        property: String,
        op: FilterOp,
        value: Option<serde_json::Value>,
        rhs_column: Option<(String, String)>,
    },
    Relationship {
        relationship: usize,
        property: String,
        op: FilterOp,
        value: Option<serde_json::Value>,
        rhs_column: Option<(String, String)>,
    },
}

impl PredicateSetup {
    pub fn expression(&self) -> BooleanExpression<PropertyPredicate> {
        let (target, property, op, value, rhs_column) = match self {
            Self::Not { not } => return BooleanExpression::Not(Box::new(not.expression())),
            Self::And { and } => {
                return BooleanExpression::And(and.iter().map(Self::expression).collect());
            }
            Self::Property {
                node,
                property,
                op,
                value,
                rhs_column,
            } => (
                PredicateTarget::Node(node.clone()),
                property,
                op,
                value,
                rhs_column,
            ),
            Self::Relationship {
                relationship,
                property,
                op,
                value,
                rhs_column,
            } => (
                PredicateTarget::Relationship(*relationship),
                property,
                op,
                value,
                rhs_column,
            ),
        };
        BooleanExpression::Leaf(PropertyPredicate {
            target,
            property: property.clone(),
            filter: InputFilter {
                op: Some(*op),
                value: value.clone(),
                rhs_column: rhs_column.clone(),
            },
        })
    }
}
