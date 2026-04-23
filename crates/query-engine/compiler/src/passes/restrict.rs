//! Field-level access restriction.
//!
//! Strips `admin_only` columns from column selections and rejects filters,
//! ordering, and aggregations on `admin_only` fields when the caller is not
//! an instance administrator. Also rejects aggregation queries whose node set
//! contains no `traversal_path`-scoped entity, since aggregation bypasses the
//! Rails redaction layer that protects other query types. Runs after
//! normalization (columns are expanded) and before lowering.

use crate::error::{QueryError, Result};
use crate::input::{ColumnSelection, Input, QueryType};
use crate::types::SecurityContext;
use ontology::Ontology;

fn entity_of<'a>(input: &'a Input, node_id: &str) -> Option<&'a str> {
    input
        .nodes
        .iter()
        .find(|n| n.id == node_id)
        .and_then(|n| n.entity.as_deref())
}

pub fn restrict(
    input: &mut Input,
    ontology: &Ontology,
    security_ctx: &SecurityContext,
) -> Result<()> {
    if security_ctx.admin {
        return Ok(());
    }

    if matches!(input.query_type, QueryType::Aggregation) {
        let any_scoped = input.nodes.iter().any(|n| {
            n.entity
                .as_deref()
                .and_then(|e| ontology.get_node(e))
                .is_some_and(|ne| ne.has_traversal_path)
        });
        if !any_scoped {
            return Err(QueryError::Validation(
                "aggregation requires at least one node scoped by traversal_path \
                 (e.g. Group, Project, Note); aggregating on globally-scoped entities \
                 such as User alone is not permitted"
                    .into(),
            ));
        }
    }

    for node in &mut input.nodes {
        let Some(entity) = node.entity.as_deref() else {
            continue;
        };

        for prop in node.filters.keys() {
            if ontology.is_admin_only(entity, prop) {
                return Err(QueryError::Validation(format!(
                    "filter on \"{prop}\" for {entity}: field requires administrator access"
                )));
            }
        }

        if let Some(ColumnSelection::All) = &node.columns {
            return Err(QueryError::PipelineInvariant(
                "RestrictPass requires expanded columns; normalization must run first".into(),
            ));
        }

        if let Some(ColumnSelection::List(cols)) = &mut node.columns {
            cols.retain(|col_name| !ontology.is_admin_only(entity, col_name));
        }
    }

    if let Some(ob) = &input.order_by
        && let Some(entity) = entity_of(input, &ob.node)
        && ontology.is_admin_only(entity, &ob.property)
    {
        return Err(QueryError::Validation(format!(
            "order_by on \"{}\" for {entity}: field requires administrator access",
            ob.property
        )));
    }

    for agg in &input.aggregations {
        let (Some(prop), Some(target)) = (&agg.property, &agg.target) else {
            continue;
        };
        let Some(entity) = entity_of(input, target) else {
            continue;
        };
        if ontology.is_admin_only(entity, prop) {
            return Err(QueryError::Validation(format!(
                "aggregation on \"{prop}\" for {entity}: field requires administrator access"
            )));
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::input::{
        AggFunction, FilterOp, InputAggregation, InputFilter, InputNode, InputOrderBy,
        OrderDirection, QueryType,
    };
    use ontology::DataType;
    use serde_json::Value;
    use std::collections::HashMap;

    fn ontology() -> Ontology {
        Ontology::new()
            .with_nodes(["User", "Group"])
            .with_fields(
                "User",
                [
                    ("username", DataType::String),
                    ("is_admin", DataType::Bool),
                    ("is_auditor", DataType::Bool),
                    ("state", DataType::String),
                ],
            )
            .with_fields("Group", [("traversal_path", DataType::String)])
            .modify_field("User", "is_admin", |f| f.admin_only = true)
            .unwrap()
            .modify_field("User", "is_auditor", |f| f.admin_only = true)
            .unwrap()
    }

    fn scoped_node() -> InputNode {
        InputNode {
            id: "_g".into(),
            entity: Some("Group".into()),
            ..Default::default()
        }
    }

    fn non_admin_ctx() -> SecurityContext {
        SecurityContext::new(1, vec!["1/".into()]).unwrap()
    }

    fn admin_ctx() -> SecurityContext {
        SecurityContext::new(1, vec!["1/".into()])
            .unwrap()
            .with_role(true, None)
    }

    fn input_with_columns(cols: Vec<&str>) -> Input {
        Input {
            query_type: QueryType::Search,
            nodes: vec![InputNode {
                id: "_u".into(),
                entity: Some("User".into()),
                columns: Some(ColumnSelection::List(
                    cols.into_iter().map(String::from).collect(),
                )),
                ..Default::default()
            }],
            ..Input::default()
        }
    }

    fn input_with_filter(field: &str, value: Value) -> Input {
        let mut filters = HashMap::new();
        filters.insert(
            field.to_string(),
            InputFilter {
                op: Some(FilterOp::Eq),
                value: Some(value),
            },
        );
        Input {
            query_type: QueryType::Search,
            nodes: vec![InputNode {
                id: "_u".into(),
                entity: Some("User".into()),
                columns: Some(ColumnSelection::List(vec!["username".into()])),
                filters,
                ..Default::default()
            }],
            ..Input::default()
        }
    }

    #[test]
    fn admin_bypasses_all_restrictions() {
        let ont = ontology();
        let ctx = admin_ctx();
        let mut input = input_with_columns(vec!["username", "is_admin", "is_auditor"]);
        restrict(&mut input, &ont, &ctx).unwrap();
        let cols = match &input.nodes[0].columns {
            Some(ColumnSelection::List(c)) => c.clone(),
            _ => panic!("expected List"),
        };
        assert_eq!(cols, vec!["username", "is_admin", "is_auditor"]);
    }

    #[test]
    fn admin_can_filter_on_admin_only_fields() {
        let ont = ontology();
        let ctx = admin_ctx();
        let mut input = input_with_filter("is_admin", Value::Bool(true));
        assert!(restrict(&mut input, &ont, &ctx).is_ok());
    }

    #[test]
    fn non_admin_strips_admin_only_columns() {
        let ont = ontology();
        let ctx = non_admin_ctx();
        let mut input = input_with_columns(vec!["username", "is_admin", "state", "is_auditor"]);
        restrict(&mut input, &ont, &ctx).unwrap();
        let cols = match &input.nodes[0].columns {
            Some(ColumnSelection::List(c)) => c.clone(),
            _ => panic!("expected List"),
        };
        assert_eq!(cols, vec!["username", "state"]);
    }

    #[test]
    fn non_admin_preserves_non_admin_only_columns() {
        let ont = ontology();
        let ctx = non_admin_ctx();
        let mut input = input_with_columns(vec!["username", "state"]);
        restrict(&mut input, &ont, &ctx).unwrap();
        let cols = match &input.nodes[0].columns {
            Some(ColumnSelection::List(c)) => c.clone(),
            _ => panic!("expected List"),
        };
        assert_eq!(cols, vec!["username", "state"]);
    }

    #[test]
    fn non_admin_rejects_filter_on_admin_only_field() {
        let ont = ontology();
        let ctx = non_admin_ctx();
        let mut input = input_with_filter("is_admin", Value::Bool(true));
        let err = restrict(&mut input, &ont, &ctx).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("is_admin"),
            "error should name the field: {msg}"
        );
        assert!(
            msg.contains("administrator"),
            "error should mention admin access: {msg}"
        );
    }

    #[test]
    fn non_admin_accepts_filter_on_normal_field() {
        let ont = ontology();
        let ctx = non_admin_ctx();
        let mut input = input_with_filter("username", Value::String("alice".into()));
        assert!(restrict(&mut input, &ont, &ctx).is_ok());
    }

    #[test]
    fn non_admin_node_without_entity_is_skipped() {
        let ont = ontology();
        let ctx = non_admin_ctx();
        let mut input = Input {
            query_type: QueryType::Search,
            nodes: vec![InputNode {
                id: "_u".into(),
                entity: None,
                columns: Some(ColumnSelection::List(vec!["is_admin".into()])),
                ..Default::default()
            }],
            ..Input::default()
        };
        assert!(restrict(&mut input, &ont, &ctx).is_ok());
    }

    fn input_with_order_by(property: &str) -> Input {
        Input {
            query_type: QueryType::Search,
            nodes: vec![InputNode {
                id: "_u".into(),
                entity: Some("User".into()),
                columns: Some(ColumnSelection::List(vec!["username".into()])),
                ..Default::default()
            }],
            order_by: Some(InputOrderBy {
                node: "_u".into(),
                property: property.into(),
                direction: OrderDirection::Desc,
            }),
            ..Input::default()
        }
    }

    fn input_with_aggregation(function: AggFunction, property: Option<&str>) -> Input {
        Input {
            query_type: QueryType::Aggregation,
            nodes: vec![
                InputNode {
                    id: "_u".into(),
                    entity: Some("User".into()),
                    columns: Some(ColumnSelection::List(vec!["username".into()])),
                    ..Default::default()
                },
                scoped_node(),
            ],
            aggregations: vec![InputAggregation {
                function,
                target: Some("_u".into()),
                group_by: None,
                property: property.map(String::from),
                alias: Some("_agg".into()),
            }],
            ..Input::default()
        }
    }

    fn user_only_aggregation(function: AggFunction, property: Option<&str>) -> Input {
        Input {
            query_type: QueryType::Aggregation,
            nodes: vec![InputNode {
                id: "_u".into(),
                entity: Some("User".into()),
                columns: Some(ColumnSelection::List(vec!["username".into()])),
                ..Default::default()
            }],
            aggregations: vec![InputAggregation {
                function,
                target: Some("_u".into()),
                group_by: None,
                property: property.map(String::from),
                alias: Some("_agg".into()),
            }],
            ..Input::default()
        }
    }

    #[test]
    fn non_admin_rejects_order_by_on_admin_only_field() {
        let ont = ontology();
        let ctx = non_admin_ctx();
        let mut input = input_with_order_by("is_admin");
        let err = restrict(&mut input, &ont, &ctx).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("is_admin"),
            "error should name the field: {msg}"
        );
        assert!(
            msg.contains("order_by"),
            "error should mention order_by: {msg}"
        );
        assert!(
            msg.contains("administrator"),
            "error should mention admin access: {msg}"
        );
    }

    #[test]
    fn non_admin_accepts_order_by_on_normal_field() {
        let ont = ontology();
        let ctx = non_admin_ctx();
        let mut input = input_with_order_by("username");
        assert!(restrict(&mut input, &ont, &ctx).is_ok());
    }

    #[test]
    fn admin_can_order_by_admin_only_field() {
        let ont = ontology();
        let ctx = admin_ctx();
        let mut input = input_with_order_by("is_admin");
        assert!(restrict(&mut input, &ont, &ctx).is_ok());
    }

    #[test]
    fn non_admin_rejects_aggregation_on_admin_only_field() {
        let ont = ontology();
        let ctx = non_admin_ctx();
        let mut input = input_with_aggregation(AggFunction::Max, Some("is_admin"));
        let err = restrict(&mut input, &ont, &ctx).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("is_admin"),
            "error should name the field: {msg}"
        );
        assert!(
            msg.contains("aggregation"),
            "error should mention aggregation: {msg}"
        );
        assert!(
            msg.contains("administrator"),
            "error should mention admin access: {msg}"
        );
    }

    #[test]
    fn non_admin_rejects_count_aggregation_on_admin_only_field() {
        let ont = ontology();
        let ctx = non_admin_ctx();
        let mut input = input_with_aggregation(AggFunction::Count, Some("is_auditor"));
        assert!(restrict(&mut input, &ont, &ctx).is_err());
    }

    #[test]
    fn non_admin_accepts_aggregation_on_normal_field() {
        let ont = ontology();
        let ctx = non_admin_ctx();
        let mut input = input_with_aggregation(AggFunction::Max, Some("username"));
        assert!(restrict(&mut input, &ont, &ctx).is_ok());
    }

    #[test]
    fn non_admin_accepts_propertyless_aggregation() {
        let ont = ontology();
        let ctx = non_admin_ctx();
        let mut input = input_with_aggregation(AggFunction::Count, None);
        assert!(restrict(&mut input, &ont, &ctx).is_ok());
    }

    #[test]
    fn admin_can_aggregate_admin_only_field() {
        let ont = ontology();
        let ctx = admin_ctx();
        let mut input = input_with_aggregation(AggFunction::Max, Some("is_admin"));
        assert!(restrict(&mut input, &ont, &ctx).is_ok());
    }

    #[test]
    fn non_admin_rejects_aggregation_on_user_only() {
        let ont = ontology();
        let ctx = non_admin_ctx();
        let mut input = user_only_aggregation(AggFunction::Count, None);
        let err = restrict(&mut input, &ont, &ctx).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("traversal_path"),
            "error should reference traversal_path scoping: {msg}"
        );
        assert!(
            msg.contains("aggregation"),
            "error should mention aggregation: {msg}"
        );
    }

    #[test]
    fn non_admin_rejects_aggregation_on_user_only_with_email_filter() {
        let ont = ontology();
        let ctx = non_admin_ctx();
        let mut input = user_only_aggregation(AggFunction::Count, None);
        input.nodes[0].filters.insert(
            "username".into(),
            InputFilter {
                op: Some(FilterOp::Eq),
                value: Some(Value::String("bob".into())),
            },
        );
        let err = restrict(&mut input, &ont, &ctx).unwrap_err();
        assert!(err.to_string().contains("traversal_path"));
    }

    #[test]
    fn non_admin_accepts_aggregation_when_scoped_node_present() {
        let ont = ontology();
        let ctx = non_admin_ctx();
        let mut input = input_with_aggregation(AggFunction::Count, None);
        assert!(restrict(&mut input, &ont, &ctx).is_ok());
    }

    #[test]
    fn admin_bypasses_user_only_aggregation_guard() {
        let ont = ontology();
        let ctx = admin_ctx();
        let mut input = user_only_aggregation(AggFunction::Count, None);
        assert!(
            restrict(&mut input, &ont, &ctx).is_ok(),
            "admin should bypass traversal_path scoping guard"
        );
    }

    #[test]
    fn non_admin_search_on_user_is_not_rejected() {
        let ont = ontology();
        let ctx = non_admin_ctx();
        let mut input = input_with_filter("username", Value::String("bob".into()));
        assert!(
            restrict(&mut input, &ont, &ctx).is_ok(),
            "search queries are redacted by the Rails layer and must not be blocked here"
        );
    }

    #[test]
    fn unexpanded_wildcard_fails_closed() {
        let ont = ontology();
        let ctx = non_admin_ctx();
        let mut input = Input {
            query_type: QueryType::Search,
            nodes: vec![InputNode {
                id: "_u".into(),
                entity: Some("User".into()),
                columns: Some(ColumnSelection::All),
                ..Default::default()
            }],
            ..Input::default()
        };
        let err = restrict(&mut input, &ont, &ctx).unwrap_err();
        assert!(
            err.to_string().contains("normalization"),
            "should reference normalization: {err}"
        );
    }
}
