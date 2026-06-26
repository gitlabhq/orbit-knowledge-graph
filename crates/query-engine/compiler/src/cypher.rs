//! Cypher front-end (DSL v2 spike).
//!
//! Parses a Cypher string with `lance-graph` and lowers the resulting AST into
//! the compiler's [`Input`] IR, which then flows through the same pipeline as
//! the JSON DSL (validate_input → normalize → … → security → codegen). Only a
//! constrained traversal subset is supported; anything the engine cannot
//! represent is rejected here with a [`QueryError::Cypher`] rather than parsed
//! into a query we would have to refuse later.
//!
//! Supported: `MATCH` node/relationship patterns (including variable-length
//! `*min..max`), inline `{prop: value}` equality, `WHERE` with `=`, `<`, `<=`,
//! `>`, `>=`, `IN`, `CONTAINS`, `STARTS WITH`, `ENDS WITH`, `IS [NOT] NULL`
//! combined with `AND`, `RETURN` of whole nodes or `node.property` columns,
//! `ORDER BY`, and `LIMIT`.

use std::collections::HashMap;

use lance_graph::ast::{
    BooleanExpression, ComparisonOperator, CypherQuery, GraphPattern, MatchClause, NodePattern,
    PathPattern, PropertyValue, ReadingClause, RelationshipDirection, RelationshipPattern,
    ReturnItem, SortDirection, ValueExpression,
};
use lance_graph::parser::parse_cypher_query;
use ontology::Ontology;
use serde_json::Value;

use crate::error::{QueryError, Result};
use crate::input::{
    ColumnSelection, Direction, FilterOp, Input, InputFilter, InputNode, InputOrderBy,
    InputRelationship, OrderDirection,
};

/// The engine caps multi-hop expansion at 3; reject variable-length patterns
/// that exceed it during translation so the error names the Cypher construct.
const MAX_HOPS: u32 = 3;

fn err(msg: impl Into<String>) -> QueryError {
    QueryError::Cypher(msg.into())
}

/// Parse a Cypher query string and lower it to an [`Input`].
///
/// The `ontology` is accepted for symmetry with the JSON path and to leave room
/// for label/relationship resolution; reference legality is enforced uniformly
/// by the `validate_input` phase that runs after this returns.
pub fn parse_to_input(cypher: &str, _ontology: &Ontology) -> Result<Input> {
    let ast = parse_cypher_query(cypher).map_err(|e| err(format!("parse error: {e}")))?;
    translate(ast)
}

fn translate(ast: CypherQuery) -> Result<Input> {
    reject_out_of_scope(&ast)?;

    let mut builder = Builder::default();
    for clause in &ast.reading_clauses {
        match clause {
            ReadingClause::Match(m) => builder.add_match(m)?,
            ReadingClause::Unwind(_) => {
                return Err(err("UNWIND is not supported"));
            }
        }
    }

    if let Some(where_clause) = &ast.where_clause {
        builder.apply_where(&where_clause.expression)?;
    }

    builder.apply_return(&ast.return_clause.items)?;

    let mut input = builder.into_input()?;

    if let Some(order) = &ast.order_by {
        input.order_by = Some(translate_order_by(order)?);
    }
    if let Some(limit) = ast.limit {
        input.limit = u32::try_from(limit).map_err(|_| err("LIMIT is too large"))?;
    }

    Ok(input)
}

fn reject_out_of_scope(ast: &CypherQuery) -> Result<()> {
    if ast.with_clause.is_some()
        || !ast.post_with_reading_clauses.is_empty()
        || ast.post_with_where_clause.is_some()
    {
        return Err(err("WITH clauses are not supported"));
    }
    if ast.skip.is_some() {
        return Err(err("SKIP is not supported"));
    }
    Ok(())
}

/// Accumulates nodes (deduplicated by variable, preserving first-seen order)
/// and relationships while walking the MATCH patterns.
#[derive(Default)]
struct Builder {
    order: Vec<String>,
    nodes: HashMap<String, InputNode>,
    relationships: Vec<InputRelationship>,
}

impl Builder {
    fn add_match(&mut self, m: &MatchClause) -> Result<()> {
        for pattern in &m.patterns {
            match pattern {
                GraphPattern::Node(node) => {
                    self.add_node(node)?;
                }
                GraphPattern::Path(path) => self.add_path(path)?,
            }
        }
        Ok(())
    }

    fn add_path(&mut self, path: &PathPattern) -> Result<()> {
        let mut from = self.add_node(&path.start_node)?;
        for segment in &path.segments {
            let to = self.add_node(&segment.end_node)?;
            self.relationships
                .push(translate_relationship(&segment.relationship, &from, &to)?);
            from = to;
        }
        Ok(())
    }

    /// Registers a node pattern and returns its variable name. Repeated
    /// variables (e.g. `(a)-[]->(b), (b)-[]->(c)`) merge labels and inline
    /// filters into the first-seen entry.
    fn add_node(&mut self, node: &NodePattern) -> Result<String> {
        let var = node
            .variable
            .clone()
            .ok_or_else(|| err("every node must be named, e.g. (u:User)"))?;

        let entity = match node.labels.as_slice() {
            [label] => label.clone(),
            [] => {
                return Err(err(format!(
                    "node `{var}` must have a label, e.g. (u:User)"
                )));
            }
            _ => {
                return Err(err(format!(
                    "node `{var}` has multiple labels; one is required"
                )));
            }
        };

        let inline = inline_filters(&node.properties)?;

        if !self.nodes.contains_key(&var) {
            self.order.push(var.clone());
            self.nodes.insert(
                var.clone(),
                InputNode {
                    id: var.clone(),
                    ..Default::default()
                },
            );
        }
        let entry = self.nodes.get_mut(&var).expect("node was just inserted");

        match &entry.entity {
            None => entry.entity = Some(entity),
            Some(existing) if *existing != entity => {
                return Err(err(format!(
                    "node `{var}` is declared as both `{existing}` and `{entity}`"
                )));
            }
            Some(_) => {}
        }
        for (prop, filters) in inline {
            entry.filters.entry(prop).or_default().extend(filters);
        }

        Ok(var)
    }

    fn apply_where(&mut self, expr: &BooleanExpression) -> Result<()> {
        for (var, prop, filter) in flatten_where(expr)? {
            let node = self
                .nodes
                .get_mut(&var)
                .ok_or_else(|| err(format!("WHERE references unknown node `{var}`")))?;
            node.filters.entry(prop).or_default().push(filter);
        }
        Ok(())
    }

    fn apply_return(&mut self, items: &[ReturnItem]) -> Result<()> {
        let mut columns: HashMap<String, Vec<String>> = HashMap::new();
        for item in items {
            match &item.expression {
                ValueExpression::Variable(var) => {
                    if !self.nodes.contains_key(var) {
                        return Err(err(format!("RETURN references unknown node `{var}`")));
                    }
                    columns.entry(var.clone()).or_default();
                }
                ValueExpression::Property(prop) => {
                    if !self.nodes.contains_key(&prop.variable) {
                        return Err(err(format!(
                            "RETURN references unknown node `{}`",
                            prop.variable
                        )));
                    }
                    columns
                        .entry(prop.variable.clone())
                        .or_default()
                        .push(prop.property.clone());
                }
                ValueExpression::AggregateFunction { .. } => {
                    return Err(err("aggregations in RETURN are not supported yet"));
                }
                _ => return Err(err("unsupported RETURN expression")),
            }
        }

        for (var, cols) in columns {
            if let Some(node) = self.nodes.get_mut(&var)
                && !cols.is_empty()
            {
                node.columns = Some(ColumnSelection::List(cols));
            }
        }
        Ok(())
    }

    fn into_input(mut self) -> Result<Input> {
        if self.order.is_empty() {
            return Err(err("query must MATCH at least one node"));
        }
        let nodes = self
            .order
            .iter()
            .map(|var| {
                self.nodes
                    .remove(var)
                    .expect("every ordered var was inserted")
            })
            .collect();
        Ok(Input {
            nodes,
            relationships: self.relationships,
            ..Default::default()
        })
    }
}

fn translate_relationship(
    rel: &RelationshipPattern,
    from: &str,
    to: &str,
) -> Result<InputRelationship> {
    if rel.types.is_empty() {
        return Err(err(
            "relationship type is required, e.g. -[:DEFINES]-> (wildcards not supported)",
        ));
    }
    let (min_hops, max_hops) = match &rel.length {
        None => (1, 1),
        Some(range) => {
            let min = range.min.unwrap_or(1);
            let max = range.max.ok_or_else(|| {
                err("unbounded variable-length paths are not supported; give a max, e.g. *1..3")
            })?;
            if max > MAX_HOPS {
                return Err(err(format!(
                    "variable-length paths are capped at {MAX_HOPS} hops"
                )));
            }
            (min, max)
        }
    };

    Ok(InputRelationship {
        types: rel.types.clone(),
        from: from.to_string(),
        to: to.to_string(),
        min_hops,
        max_hops,
        direction: translate_direction(&rel.direction),
        filters: HashMap::new(),
        fk_column: None,
        scope_prefix: None,
        scope_preserving: false,
    })
}

fn translate_direction(direction: &RelationshipDirection) -> Direction {
    match direction {
        RelationshipDirection::Outgoing => Direction::Outgoing,
        RelationshipDirection::Incoming => Direction::Incoming,
        RelationshipDirection::Undirected => Direction::Both,
    }
}

/// Lowers inline `{prop: value}` map entries into equality filters.
fn inline_filters(
    props: &HashMap<String, PropertyValue>,
) -> Result<HashMap<String, Vec<InputFilter>>> {
    let mut out: HashMap<String, Vec<InputFilter>> = HashMap::new();
    for (prop, value) in props {
        let filter = InputFilter {
            op: Some(FilterOp::Eq),
            value: Some(property_value(value)?),
            ..Default::default()
        };
        out.entry(prop.clone()).or_default().push(filter);
    }
    Ok(out)
}

/// Flattens an AND-chain of predicates into `(node_var, property, filter)`.
/// `OR`/`NOT` and predicates that aren't anchored on a single `node.property`
/// are rejected, matching the JSON DSL's AND-only filter model.
fn flatten_where(expr: &BooleanExpression) -> Result<Vec<(String, String, InputFilter)>> {
    match expr {
        BooleanExpression::And(left, right) => {
            let mut out = flatten_where(left)?;
            out.extend(flatten_where(right)?);
            Ok(out)
        }
        BooleanExpression::Or(_, _) => Err(err("OR in WHERE is not supported")),
        BooleanExpression::Not(_) => Err(err("NOT in WHERE is not supported")),
        BooleanExpression::Comparison {
            left,
            operator,
            right,
        } => {
            let (var, prop) = property_ref(left)?;
            let filter = InputFilter {
                op: Some(comparison_op(operator)?),
                value: Some(literal(right)?),
                ..Default::default()
            };
            Ok(vec![(var, prop, filter)])
        }
        BooleanExpression::In { expression, list } => {
            let (var, prop) = property_ref(expression)?;
            let values = list.iter().map(literal).collect::<Result<Vec<_>>>()?;
            let filter = InputFilter {
                op: Some(FilterOp::In),
                value: Some(Value::Array(values)),
                ..Default::default()
            };
            Ok(vec![(var, prop, filter)])
        }
        BooleanExpression::Contains {
            expression,
            substring,
        } => string_filter(expression, FilterOp::Contains, substring),
        BooleanExpression::StartsWith { expression, prefix } => {
            string_filter(expression, FilterOp::StartsWith, prefix)
        }
        BooleanExpression::EndsWith { expression, suffix } => {
            string_filter(expression, FilterOp::EndsWith, suffix)
        }
        BooleanExpression::IsNull(expression) => null_filter(expression, FilterOp::IsNull),
        BooleanExpression::IsNotNull(expression) => null_filter(expression, FilterOp::IsNotNull),
        BooleanExpression::Like { .. } | BooleanExpression::ILike { .. } => Err(err(
            "LIKE is not supported; use CONTAINS/STARTS WITH/ENDS WITH",
        )),
        BooleanExpression::Exists(_) => Err(err("EXISTS is not supported")),
    }
}

fn string_filter(
    expression: &ValueExpression,
    op: FilterOp,
    operand: &str,
) -> Result<Vec<(String, String, InputFilter)>> {
    let (var, prop) = property_ref(expression)?;
    let filter = InputFilter {
        op: Some(op),
        value: Some(Value::String(operand.to_string())),
        ..Default::default()
    };
    Ok(vec![(var, prop, filter)])
}

fn null_filter(
    expression: &ValueExpression,
    op: FilterOp,
) -> Result<Vec<(String, String, InputFilter)>> {
    let (var, prop) = property_ref(expression)?;
    let filter = InputFilter {
        op: Some(op),
        value: None,
        ..Default::default()
    };
    Ok(vec![(var, prop, filter)])
}

fn comparison_op(op: &ComparisonOperator) -> Result<FilterOp> {
    Ok(match op {
        ComparisonOperator::Equal => FilterOp::Eq,
        ComparisonOperator::LessThan => FilterOp::Lt,
        ComparisonOperator::LessThanOrEqual => FilterOp::Lte,
        ComparisonOperator::GreaterThan => FilterOp::Gt,
        ComparisonOperator::GreaterThanOrEqual => FilterOp::Gte,
        ComparisonOperator::NotEqual => return Err(err("`<>` (not-equal) is not supported")),
    })
}

fn property_ref(expr: &ValueExpression) -> Result<(String, String)> {
    match expr {
        ValueExpression::Property(prop) => Ok((prop.variable.clone(), prop.property.clone())),
        _ => Err(err(
            "filters must compare a node property, e.g. u.state = 'active'",
        )),
    }
}

fn literal(expr: &ValueExpression) -> Result<Value> {
    match expr {
        ValueExpression::Literal(value) => property_value(value),
        _ => Err(err("filter values must be literals")),
    }
}

fn property_value(value: &PropertyValue) -> Result<Value> {
    Ok(match value {
        PropertyValue::String(s) => Value::String(s.clone()),
        PropertyValue::Integer(i) => Value::from(*i),
        PropertyValue::Float(f) => Value::from(*f),
        PropertyValue::Boolean(b) => Value::Bool(*b),
        PropertyValue::Null => Value::Null,
        PropertyValue::Parameter(_) => return Err(err("query parameters are not supported")),
        PropertyValue::Property(_) => {
            return Err(err("property-to-property comparison is not supported"));
        }
    })
}

fn translate_order_by(order: &lance_graph::ast::OrderByClause) -> Result<InputOrderBy> {
    let [item] = order.items.as_slice() else {
        return Err(err("ORDER BY supports exactly one key"));
    };
    let (node, property) = property_ref(&item.expression)?;
    Ok(InputOrderBy {
        node,
        property,
        direction: match item.direction {
            SortDirection::Ascending => OrderDirection::Asc,
            SortDirection::Descending => OrderDirection::Desc,
        },
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::input::QueryType;
    use std::sync::LazyLock;

    static ONTOLOGY: LazyLock<Ontology> =
        LazyLock::new(|| Ontology::load_embedded().expect("ontology must load"));

    fn input(cypher: &str) -> Input {
        parse_to_input(cypher, &ONTOLOGY).expect("should translate")
    }

    fn filter(node: &InputNode, prop: &str) -> InputFilter {
        node.filters
            .get(prop)
            .and_then(|f| f.first())
            .cloned()
            .unwrap_or_else(|| panic!("missing filter on `{prop}`"))
    }

    #[test]
    fn single_node_with_equality_filter() {
        let input = input("MATCH (u:User) WHERE u.state = 'active' RETURN u");

        assert_eq!(input.query_type, QueryType::Traversal);
        assert_eq!(input.nodes.len(), 1);
        assert_eq!(input.nodes[0].id, "u");
        assert_eq!(input.nodes[0].entity.as_deref(), Some("User"));
        assert_eq!(filter(&input.nodes[0], "state").op, Some(FilterOp::Eq));
        assert_eq!(
            filter(&input.nodes[0], "state").value,
            Some(Value::String("active".into()))
        );
    }

    #[test]
    fn inline_property_map_becomes_equality_filter() {
        let input = input("MATCH (u:User {state: 'active'}) RETURN u");
        assert_eq!(filter(&input.nodes[0], "state").op, Some(FilterOp::Eq));
    }

    #[test]
    fn two_node_traversal_maps_relationship_and_filters() {
        let input = input(
            "MATCH (f:File)-[:DEFINES]->(d:Definition) \
             WHERE f.path ENDS WITH '.rb' AND d.name STARTS WITH 'process' \
             RETURN f, d LIMIT 20",
        );

        assert_eq!(input.nodes.len(), 2);
        assert_eq!(input.nodes[0].id, "f");
        assert_eq!(input.nodes[1].id, "d");
        assert_eq!(input.limit, 20);

        assert_eq!(input.relationships.len(), 1);
        let rel = &input.relationships[0];
        assert_eq!(rel.types, vec!["DEFINES".to_string()]);
        assert_eq!(rel.from, "f");
        assert_eq!(rel.to, "d");
        assert_eq!(rel.direction, Direction::Outgoing);
        assert_eq!(rel.min_hops, 1);
        assert_eq!(rel.max_hops, 1);

        assert_eq!(filter(&input.nodes[0], "path").op, Some(FilterOp::EndsWith));
        assert_eq!(
            filter(&input.nodes[1], "name").op,
            Some(FilterOp::StartsWith)
        );
    }

    #[test]
    fn incoming_arrow_maps_to_incoming_direction() {
        let input = input("MATCH (u:User)<-[:AUTHORED]-(mr:MergeRequest) RETURN u, mr");
        assert_eq!(input.relationships[0].direction, Direction::Incoming);
        assert_eq!(input.relationships[0].from, "u");
        assert_eq!(input.relationships[0].to, "mr");
    }

    #[test]
    fn variable_length_maps_to_hop_range() {
        let input = input("MATCH (g:Group)-[:CONTAINS*1..3]->(p:Project) RETURN g, p");
        assert_eq!(input.relationships[0].min_hops, 1);
        assert_eq!(input.relationships[0].max_hops, 3);
    }

    #[test]
    fn return_properties_select_columns() {
        let input = input("MATCH (u:User) RETURN u.id, u.username");
        match input.nodes[0].columns.as_ref().expect("columns set") {
            ColumnSelection::List(cols) => assert_eq!(cols, &["id", "username"]),
            other => panic!("expected column list, got {other:?}"),
        }
    }

    #[test]
    fn order_by_and_limit() {
        let input = input("MATCH (u:User) RETURN u ORDER BY u.created_at DESC LIMIT 5");
        let order = input.order_by.expect("order_by set");
        assert_eq!(order.node, "u");
        assert_eq!(order.property, "created_at");
        assert_eq!(order.direction, OrderDirection::Desc);
        assert_eq!(input.limit, 5);
    }

    #[test]
    fn in_list_maps_to_in_filter() {
        let input =
            input("MATCH (v:Vulnerability) WHERE v.report_type IN ['sast', 'dast'] RETURN v");
        let f = filter(&input.nodes[0], "report_type");
        assert_eq!(f.op, Some(FilterOp::In));
        assert_eq!(
            f.value,
            Some(Value::Array(vec!["sast".into(), "dast".into()]))
        );
    }

    #[test]
    fn rejects_unsupported_constructs() {
        for (cypher, needle) in [
            (
                "MATCH (u:User) WHERE u.state = 'a' OR u.state = 'b' RETURN u",
                "OR",
            ),
            (
                "MATCH (g:Group)-[:CONTAINS*]->(p:Project) RETURN g, p",
                "unbounded",
            ),
            (
                "MATCH (g:Group)-[:CONTAINS*1..9]->(p:Project) RETURN g, p",
                "capped",
            ),
            ("MATCH (u) RETURN u", "label"),
        ] {
            let e = parse_to_input(cypher, &ONTOLOGY).expect_err("should reject");
            let msg = e.to_string();
            assert!(
                msg.contains(needle),
                "error for `{cypher}` should mention `{needle}`, got: {msg}"
            );
        }
    }

    #[test]
    fn compiles_through_pipeline_to_scoped_sql() {
        let ctx = crate::testkit::non_admin_ctx();
        let compiled = crate::compile(
            crate::QueryInput::Cypher(
                "MATCH (f:File)-[:DEFINES]->(d:Definition) WHERE d.name STARTS WITH 'process' RETURN f, d LIMIT 20"
                    .into(),
            ),
            &ONTOLOGY,
            &ctx,
        )
        .expect("cypher should compile to SQL");

        let sql = compiled.base.render();
        assert!(
            sql.contains("startsWith"),
            "tenant-scoping predicate must be injected, got:\n{sql}"
        );
    }

    #[test]
    fn unknown_entity_rejected_by_validation() {
        let ctx = crate::testkit::non_admin_ctx();
        let err = crate::compile(
            crate::QueryInput::Cypher("MATCH (x:NotARealEntity) RETURN x".into()),
            &ONTOLOGY,
            &ctx,
        )
        .expect_err("unknown entity must be rejected");
        assert!(err.is_client_safe(), "got: {err:?}");
    }
}
