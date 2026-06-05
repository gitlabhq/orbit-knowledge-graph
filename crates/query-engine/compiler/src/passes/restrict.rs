//! Field-level access restriction.
//!
//! Strips `admin_only` columns from column selections and rejects filters,
//! ordering, and aggregations on `admin_only` fields when the caller is not
//! an instance administrator. Also rejects aggregation queries in which a
//! globally-scoped node (no `traversal_path` column) is not reachable through
//! `relationships` or `path` from a `traversal_path`-scoped node, since
//! aggregation bypasses the Rails redaction layer that protects other query
//! types. Runs after normalization (columns are expanded) and before lowering.

use crate::constants::TRAVERSAL_PATH_COLUMN;
use crate::error::{QueryError, Result};
use crate::input::{ColumnSelection, FilterOp, Input, InputFilter, QueryType};
use crate::types::{DEFAULT_PATH_ACCESS_LEVEL, SecurityContext};
use ontology::Ontology;
use std::collections::HashSet;

fn entity_of<'a>(input: &'a Input, node_id: &str) -> Option<&'a str> {
    input
        .nodes
        .iter()
        .find(|n| n.id == node_id)
        .and_then(|n| n.entity.as_deref())
}

fn enforce_aggregation_scope(input: &Input, ontology: &Ontology) -> Result<()> {
    let is_scoped = |entity: &str| {
        ontology
            .get_node(entity)
            .is_some_and(|n| n.has_traversal_path)
    };

    let mut reachable: HashSet<&str> = input
        .nodes
        .iter()
        .filter(|n| n.entity.as_deref().is_some_and(is_scoped))
        .map(|n| n.id.as_str())
        .collect();

    if reachable.is_empty() {
        return Err(QueryError::Restrict(
            "aggregation requires at least one node scoped by traversal_path \
             (e.g. Group, Project, Note); aggregating on globally-scoped entities \
             such as User alone is not permitted"
                .into(),
        ));
    }

    let edges: Vec<(&str, &str)> = input
        .relationships
        .iter()
        .map(|r| (r.from.as_str(), r.to.as_str()))
        .chain(input.path.iter().map(|p| (p.from.as_str(), p.to.as_str())))
        .collect();

    // Flood-fill until no new node is reached.
    loop {
        let before = reachable.len();
        for &(a, b) in &edges {
            if reachable.contains(a) {
                reachable.insert(b);
            }
            if reachable.contains(b) {
                reachable.insert(a);
            }
        }
        if reachable.len() == before {
            break;
        }
    }

    if let Some(orphan) = input
        .nodes
        .iter()
        .find(|n| !reachable.contains(n.id.as_str()))
    {
        return Err(QueryError::Restrict(format!(
            "aggregation node \"{}\" is globally-scoped and must be connected to a \
             traversal_path-scoped node via \"relationships\" or \"path\"",
            orphan.id
        )));
    }

    Ok(())
}

fn enforce_traversal_path_filters(
    input: &Input,
    ontology: &Ontology,
    security_ctx: &SecurityContext,
) -> Result<()> {
    for node in &input.nodes {
        let (Some(entity), Some(tp_filters)) = (
            node.entity.as_deref(),
            node.filters.get(TRAVERSAL_PATH_COLUMN),
        ) else {
            continue;
        };
        let Some(ont_node) = ontology.get_node(entity) else {
            continue;
        };
        // Entities without a redaction role use the normal traversal-path floor:
        // Rails only sends Reporter+ paths, and stricter entities override this.
        let min_role = ont_node
            .redaction
            .as_ref()
            .map(|r| r.required_role.as_access_level())
            .unwrap_or(DEFAULT_PATH_ACCESS_LEVEL);
        let eligible_paths = security_ctx.paths_at_least(min_role);
        for tp_filter in tp_filters {
            validate_traversal_path_filter_scope(
                &format!("filter on \"{TRAVERSAL_PATH_COLUMN}\" for {entity}"),
                tp_filter,
                &eligible_paths,
            )?;
        }
    }

    for (i, rel) in input.relationships.iter().enumerate() {
        let Some(tp_filters) = rel.filters.get(TRAVERSAL_PATH_COLUMN) else {
            continue;
        };
        let eligible_paths = security_ctx.paths_at_least(DEFAULT_PATH_ACCESS_LEVEL);
        for tp_filter in tp_filters {
            validate_traversal_path_filter_scope(
                &format!("relationship[{i}] filter on \"{TRAVERSAL_PATH_COLUMN}\""),
                tp_filter,
                &eligible_paths,
            )?;
        }
    }

    Ok(())
}

fn validate_traversal_path_filter_scope(
    label: &str,
    traversal_path_filter: &InputFilter,
    eligible_paths: &[&str],
) -> Result<()> {
    for path in traversal_path_values(label, traversal_path_filter)? {
        validate_traversal_path_within_scope(label, path, eligible_paths)?;
    }
    Ok(())
}

fn invalid_traversal_path_filter_invariant(label: &str) -> QueryError {
    QueryError::PipelineInvariant(format!(
        "{label}: invalid traversal_path filter reached RestrictPass"
    ))
}

fn traversal_path_values<'a>(
    label: &str,
    traversal_path_filter: &'a InputFilter,
) -> Result<Vec<&'a str>> {
    match traversal_path_filter.op.unwrap_or(FilterOp::Eq) {
        FilterOp::Eq | FilterOp::StartsWith => traversal_path_filter
            .value
            .as_ref()
            .and_then(|v| v.as_str())
            .map(|path| vec![path])
            .ok_or_else(|| invalid_traversal_path_filter_invariant(label)),
        FilterOp::In => {
            let paths = traversal_path_filter
                .value
                .as_ref()
                .and_then(|v| v.as_array())
                .ok_or_else(|| invalid_traversal_path_filter_invariant(label))?;
            paths
                .iter()
                .map(|v| {
                    v.as_str()
                        .ok_or_else(|| invalid_traversal_path_filter_invariant(label))
                })
                .collect()
        }
        _ => Err(invalid_traversal_path_filter_invariant(label)),
    }
}

fn validate_traversal_path_within_scope(
    label: &str,
    path: &str,
    eligible_paths: &[&str],
) -> Result<()> {
    if eligible_paths
        .iter()
        .any(|authorized| path.starts_with(authorized))
    {
        return Ok(());
    }

    Err(QueryError::Authorization(format!(
        "{label}: path is not within an authorized traversal_path scope for this entity"
    )))
}

/// Confine edge scans to a tight `traversal_path` prefix when the traversal is
/// pinned to a project/group. An edge row's `traversal_path` is its source
/// entity's, so an edge whose two endpoints both resolve to the same scope can
/// only hold rows under that scope; scoping it is lossless and restores the
/// edge PK prefix that the broad org-wide authorization filter erases (#601941).
///
/// The endpoint prefixes come from the ontology's scope-annotation taint walk
/// ([`Ontology::propagate_scope_prefixes`]) seeded with the prefixes the path
/// resolver already attached to `scope_prefixes`. The node-table scans are
/// scoped separately via `scope_prefixes` in the security pass; this stamps the
/// edges the lowerer emits.
fn stamp_edge_scope_prefixes(
    input: &mut Input,
    ontology: &Ontology,
    security_ctx: &SecurityContext,
) {
    if security_ctx.scope_prefixes.is_empty() {
        return;
    }

    let node_prefix = {
        let edges = crate::scope::scope_edges(input);
        ontology.propagate_scope_prefixes(&edges, &security_ctx.scope_prefixes)
    };

    // Scope an edge only when both endpoints share one prefix: then the edge's
    // traversal_path (its source side) is under that prefix regardless of which
    // endpoint storage treats as the source.
    for rel in &mut input.relationships {
        if let (Some(pf), Some(pt)) = (node_prefix.get(&rel.from), node_prefix.get(&rel.to))
            && pf == pt
        {
            rel.scope_prefix = Some(pf.clone());
        }
    }
}

pub fn restrict(
    input: &mut Input,
    ontology: &Ontology,
    security_ctx: &SecurityContext,
) -> Result<()> {
    enforce_traversal_path_filters(input, ontology, security_ctx)?;
    stamp_edge_scope_prefixes(input, ontology, security_ctx);

    if security_ctx.admin {
        return Ok(());
    }

    if matches!(input.query_type, QueryType::Aggregation) {
        enforce_aggregation_scope(input, ontology)?;
    }

    for node in &mut input.nodes {
        let Some(entity) = node.entity.as_deref() else {
            continue;
        };

        for prop in node.filters.keys() {
            if ontology.is_admin_only(entity, prop) {
                return Err(QueryError::Restrict(format!(
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
        return Err(QueryError::Restrict(format!(
            "order_by on \"{}\" for {entity}: field requires administrator access",
            ob.property
        )));
    }

    for agg in &input.aggregation.metrics {
        let (Some(prop), Some(target)) = (&agg.property, &agg.target) else {
            continue;
        };
        let Some(entity) = entity_of(input, target) else {
            continue;
        };
        if ontology.is_admin_only(entity, prop) {
            return Err(QueryError::Restrict(format!(
                "aggregation on \"{prop}\" for {entity}: field requires administrator access"
            )));
        }
    }

    for group in crate::input::property_groups(&input.aggregation.group_by) {
        let (node, property, _) = group;
        let Some(entity) = entity_of(input, node) else {
            continue;
        };
        if ontology.is_admin_only(entity, property) {
            return Err(QueryError::Restrict(format!(
                "group_by on \"{}\" for {entity}: field requires administrator access",
                property
            )));
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::input::{
        AggFunction, FilterOp, InputAggregation, InputAggregationMetric, InputFilter,
        InputGroupByKey, InputNode, InputOrderBy, OrderDirection, QueryType,
    };

    use ontology::{DataType, RequiredRole};
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

    fn rel(from: &str, to: &str) -> crate::input::InputRelationship {
        crate::input::InputRelationship {
            types: vec!["MEMBER_OF".into()],
            from: from.into(),
            to: to.into(),
            min_hops: 1,
            max_hops: 1,
            direction: crate::input::Direction::Outgoing,
            filters: std::collections::HashMap::new(),
            fk_column: None,
            scope_prefix: None,
        }
    }

    use crate::testkit::{admin_ctx, non_admin_ctx};

    fn input_with_columns(cols: Vec<&str>) -> Input {
        Input {
            query_type: QueryType::Traversal,
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
            vec![InputFilter {
                op: Some(FilterOp::Eq),
                value: Some(value),
                ..Default::default()
            }],
        );
        Input {
            query_type: QueryType::Traversal,
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

    fn input_with_traversal_path_filter(entity: &str, filter: InputFilter) -> Input {
        let mut filters = HashMap::new();
        filters.insert(TRAVERSAL_PATH_COLUMN.to_string(), vec![filter]);
        Input {
            query_type: QueryType::Traversal,
            nodes: vec![InputNode {
                id: "_n".into(),
                entity: Some(entity.into()),
                filters,
                ..Default::default()
            }],
            ..Input::default()
        }
    }

    fn traversal_path_filter(op: FilterOp, value: Value) -> InputFilter {
        InputFilter {
            op: Some(op),
            value: Some(value),
            ..Default::default()
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
    fn non_admin_accepts_traversal_path_filter_inside_scope() {
        let ont = ontology();
        let ctx = SecurityContext::new(1, vec!["1/100/".into()]).unwrap();
        let mut input = input_with_traversal_path_filter(
            "Group",
            traversal_path_filter(FilterOp::StartsWith, Value::String("1/100/200/".into())),
        );

        assert!(restrict(&mut input, &ont, &ctx).is_ok());
    }

    #[test]
    fn non_admin_rejects_traversal_path_filter_above_scope() {
        let ont = ontology();
        let ctx = SecurityContext::new(1, vec!["1/100/".into()]).unwrap();
        let mut input = input_with_traversal_path_filter(
            "Group",
            traversal_path_filter(FilterOp::StartsWith, Value::String("1/".into())),
        );

        let err = restrict(&mut input, &ont, &ctx).unwrap_err();
        assert!(
            matches!(err, QueryError::Authorization(_)),
            "scope rejection should be an authorization error, got: {err:?}"
        );
        assert!(
            err.is_client_safe(),
            "traversal_path scope errors should be safe for clients, got: {err:?}"
        );
        assert!(
            err.to_string().contains("authorized traversal_path scope"),
            "error should reject paths outside the JWT scope, got: {err}"
        );
    }

    #[test]
    fn non_admin_rejects_traversal_path_filter_below_entity_role() {
        let ont = Ontology::new()
            .with_nodes(["Vulnerability"])
            .with_fields("Vulnerability", [("traversal_path", DataType::String)])
            .with_redaction("Vulnerability", "vulnerabilities", "id")
            .with_redaction_role("Vulnerability", RequiredRole::SecurityManager);
        let ctx = SecurityContext::new_with_roles(
            1,
            vec![crate::TraversalPath::new(
                "1/100/",
                DEFAULT_PATH_ACCESS_LEVEL,
            )],
        )
        .unwrap();
        let mut input = input_with_traversal_path_filter(
            "Vulnerability",
            traversal_path_filter(FilterOp::Eq, Value::String("1/100/".into())),
        );

        let err = restrict(&mut input, &ont, &ctx).unwrap_err();
        assert!(
            matches!(err, QueryError::Authorization(_)),
            "role-scope rejection should be an authorization error, got: {err:?}"
        );
        assert!(
            err.is_client_safe(),
            "traversal_path role-scope errors should be safe for clients, got: {err:?}"
        );
        assert!(
            err.to_string().contains("authorized traversal_path scope"),
            "Reporter paths must not satisfy SecurityManager traversal_path filters, got: {err}"
        );
    }

    #[test]
    fn non_admin_rejects_relationship_traversal_path_filter_outside_scope() {
        let ont = ontology();
        let ctx = SecurityContext::new(1, vec!["1/100/".into()]).unwrap();
        let mut input = Input {
            query_type: QueryType::Traversal,
            nodes: vec![
                scoped_node(),
                InputNode {
                    id: "_h".into(),
                    entity: Some("Group".into()),
                    ..Default::default()
                },
            ],
            relationships: vec![crate::input::InputRelationship {
                filters: HashMap::from([(
                    TRAVERSAL_PATH_COLUMN.to_string(),
                    vec![traversal_path_filter(
                        FilterOp::Eq,
                        Value::String("2/".into()),
                    )],
                )]),
                ..rel("_g", "_h")
            }],
            ..Input::default()
        };

        let err = restrict(&mut input, &ont, &ctx).unwrap_err();
        assert!(
            matches!(err, QueryError::Authorization(_)),
            "relationship scope rejection should be an authorization error, got: {err:?}"
        );
        assert!(
            err.is_client_safe(),
            "relationship traversal_path scope errors should be safe for clients, got: {err:?}"
        );
        assert!(
            err.to_string().contains("authorized traversal_path scope"),
            "relationship traversal_path filters should be scoped too, got: {err}"
        );
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
        assert!(
            matches!(err, QueryError::Restrict(_)),
            "admin-only field rejection should be a restrict error, got: {err:?}"
        );
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
            query_type: QueryType::Traversal,
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
            query_type: QueryType::Traversal,
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
            relationships: vec![rel("_u", "_g")],
            aggregation: InputAggregation {
                metrics: vec![InputAggregationMetric {
                    function,
                    target: Some("_u".into()),
                    property: property.map(String::from),
                    alias: Some("_agg".into()),
                }],
                ..Default::default()
            },
            ..Input::default()
        }
    }

    fn input_with_property_group(property: &str) -> Input {
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
            relationships: vec![rel("_u", "_g")],
            aggregation: InputAggregation {
                group_by: vec![InputGroupByKey::Property {
                    node: "_u".into(),
                    property: property.into(),
                    alias: None,
                    transform: None,
                }],
                metrics: vec![InputAggregationMetric {
                    function: AggFunction::Count,
                    target: Some("_u".into()),
                    property: None,
                    alias: Some("_agg".into()),
                }],
                ..Default::default()
            },
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
            aggregation: InputAggregation {
                metrics: vec![InputAggregationMetric {
                    function,
                    target: Some("_u".into()),
                    property: property.map(String::from),
                    alias: Some("_agg".into()),
                }],
                ..Default::default()
            },
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
        assert!(
            matches!(err, QueryError::Restrict(_)),
            "admin-only aggregation rejection should be a restrict error, got: {err:?}"
        );
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
    fn non_admin_rejects_group_by_property_on_admin_only_field() {
        let ont = ontology();
        let ctx = non_admin_ctx();
        let mut input = input_with_property_group("is_admin");
        let err = restrict(&mut input, &ont, &ctx).unwrap_err();
        assert!(
            matches!(err, QueryError::Restrict(_)),
            "admin-only group_by rejection should be a restrict error, got: {err:?}"
        );
        assert!(
            err.to_string().contains("group_by") && err.to_string().contains("administrator"),
            "expected admin_only group_by rejection, got: {err}"
        );
    }

    #[test]
    fn admin_can_group_by_property_on_admin_only_field() {
        let ont = ontology();
        let ctx = admin_ctx();
        let mut input = input_with_property_group("is_admin");
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
            vec![InputFilter {
                op: Some(FilterOp::Eq),
                value: Some(Value::String("bob".into())),
                ..Default::default()
            }],
        );
        let err = restrict(&mut input, &ont, &ctx).unwrap_err();
        assert!(err.to_string().contains("traversal_path"));
    }

    #[test]
    fn non_admin_accepts_aggregation_when_scoped_node_present() {
        let ont = ontology();
        let ctx = non_admin_ctx();
        let mut input = input_with_aggregation(AggFunction::Count, None);
        input.relationships.push(rel("_u", "_g"));
        assert!(restrict(&mut input, &ont, &ctx).is_ok());
    }

    #[test]
    fn non_admin_rejects_aggregation_with_disconnected_scoped_node() {
        let ont = ontology();
        let ctx = non_admin_ctx();
        let mut input = input_with_aggregation(AggFunction::Count, None);
        // Drop the helper's default relationship so User and Group are declared
        // but not connected. The old declaration-based check accepted this;
        // the reachability check must reject it.
        input.relationships.clear();
        let err = restrict(&mut input, &ont, &ctx).unwrap_err();
        assert!(
            matches!(err, QueryError::Restrict(_)),
            "aggregation reachability rejection should be a restrict error, got: {err:?}"
        );
        let msg = err.to_string();
        assert!(
            msg.contains("globally-scoped") && msg.contains("relationships"),
            "error should reference the reachability requirement, got: {msg}"
        );
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
            query_type: QueryType::Traversal,
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

    // ── Real-ontology coverage for User admin-only columns ────────────────
    //
    // The synthetic ontology above only declares is_admin/is_auditor as
    // admin_only. These tests load the embedded production ontology so any
    // future drift in user.yaml is caught here, in addition to the pin test
    // in the ontology crate.

    const USER_ADMIN_ONLY_COLUMNS: &[&str] = &[
        "email",
        "first_name",
        "last_name",
        "preferred_language",
        "private_profile",
        "is_external",
        "is_admin",
        "is_auditor",
        "updated_at",
    ];

    fn user_filter_input(field: &str, value: Value) -> Input {
        let mut filters = HashMap::new();
        filters.insert(
            field.to_string(),
            vec![InputFilter {
                op: Some(FilterOp::Eq),
                value: Some(value),
                ..Default::default()
            }],
        );
        Input {
            query_type: QueryType::Traversal,
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

    fn user_order_by_input(field: &str) -> Input {
        Input {
            query_type: QueryType::Traversal,
            nodes: vec![InputNode {
                id: "_u".into(),
                entity: Some("User".into()),
                columns: Some(ColumnSelection::List(vec!["username".into()])),
                ..Default::default()
            }],
            order_by: Some(InputOrderBy {
                node: "_u".into(),
                property: field.into(),
                direction: OrderDirection::Desc,
            }),
            ..Input::default()
        }
    }

    fn sample_value(field: &str) -> Value {
        match field {
            "private_profile" | "is_external" | "is_admin" | "is_auditor" => Value::Bool(true),
            "updated_at" => Value::String("2026-01-01T00:00:00Z".into()),
            _ => Value::String("x".into()),
        }
    }

    #[test]
    fn real_ontology_non_admin_strips_user_admin_only_columns() {
        let ont = ontology::Ontology::load_embedded().expect("embedded ontology loads");
        let ctx = non_admin_ctx();
        let mut input = Input {
            query_type: QueryType::Traversal,
            nodes: vec![InputNode {
                id: "_u".into(),
                entity: Some("User".into()),
                columns: Some(ColumnSelection::List(
                    USER_ADMIN_ONLY_COLUMNS
                        .iter()
                        .copied()
                        .chain(["username", "name", "public_email"])
                        .map(String::from)
                        .collect(),
                )),
                ..Default::default()
            }],
            ..Input::default()
        };
        restrict(&mut input, &ont, &ctx).expect("restrict pass succeeds for non-admin select");
        let cols = match &input.nodes[0].columns {
            Some(ColumnSelection::List(c)) => c.clone(),
            _ => panic!("expected List"),
        };
        for forbidden in USER_ADMIN_ONLY_COLUMNS {
            assert!(
                !cols.contains(&forbidden.to_string()),
                "non-admin column selection must not retain {forbidden}: {cols:?}"
            );
        }
        assert!(cols.contains(&"username".to_string()));
        assert!(cols.contains(&"public_email".to_string()));
    }

    #[test]
    fn real_ontology_non_admin_rejects_filter_on_each_user_admin_only_column() {
        let ont = ontology::Ontology::load_embedded().expect("embedded ontology loads");
        let ctx = non_admin_ctx();
        for field in USER_ADMIN_ONLY_COLUMNS {
            let mut input = user_filter_input(field, sample_value(field));
            let err = restrict(&mut input, &ont, &ctx).expect_err(field);
            let msg = err.to_string();
            assert!(
                msg.contains(field) && msg.contains("administrator"),
                "expected admin-required rejection for filter on User.{field}, got: {msg}"
            );
        }
    }

    #[test]
    fn real_ontology_non_admin_rejects_order_by_on_each_user_admin_only_column() {
        let ont = ontology::Ontology::load_embedded().expect("embedded ontology loads");
        let ctx = non_admin_ctx();
        for field in USER_ADMIN_ONLY_COLUMNS {
            let mut input = user_order_by_input(field);
            let err = restrict(&mut input, &ont, &ctx).expect_err(field);
            let msg = err.to_string();
            assert!(
                msg.contains(field) && msg.contains("administrator"),
                "expected admin-required rejection for order_by on User.{field}, got: {msg}"
            );
        }
    }

    #[test]
    fn real_ontology_admin_keeps_all_user_admin_only_columns() {
        let ont = ontology::Ontology::load_embedded().expect("embedded ontology loads");
        let ctx = admin_ctx();
        let mut input = Input {
            query_type: QueryType::Traversal,
            nodes: vec![InputNode {
                id: "_u".into(),
                entity: Some("User".into()),
                columns: Some(ColumnSelection::List(
                    USER_ADMIN_ONLY_COLUMNS
                        .iter()
                        .map(|c| c.to_string())
                        .collect(),
                )),
                ..Default::default()
            }],
            ..Input::default()
        };
        restrict(&mut input, &ont, &ctx).expect("admin restrict succeeds");
        let cols = match &input.nodes[0].columns {
            Some(ColumnSelection::List(c)) => c.clone(),
            _ => panic!("expected List"),
        };
        for col in USER_ADMIN_ONLY_COLUMNS {
            assert!(
                cols.contains(&col.to_string()),
                "admin must retain User.{col} in selection"
            );
        }
    }

    #[test]
    fn real_ontology_admin_can_filter_each_user_admin_only_column() {
        let ont = ontology::Ontology::load_embedded().expect("embedded ontology loads");
        let ctx = admin_ctx();
        for field in USER_ADMIN_ONLY_COLUMNS {
            let mut input = user_filter_input(field, sample_value(field));
            assert!(
                restrict(&mut input, &ont, &ctx).is_ok(),
                "admin must be allowed to filter on User.{field}"
            );
        }
    }
}
