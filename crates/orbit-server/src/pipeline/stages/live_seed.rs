//! The live-seed stage: starts a query from rows a provider's live search returns,
//! then lets the stored query traverse from them into the indexed graph.
//!
//! It runs before compilation. When the parsed query has a searchable alias with a
//! pushable `WHERE` (pushdown shape) or reaches an indexed, constrained alias through
//! an inbound relationship (inbound shape), it runs the search engine, removes the
//! live predicate by dropping the searched alias, pins each of that alias's graph
//! neighbours to the ids its seed edges point at, and rewrites the query to the
//! remaining pattern as JSON-DSL so the normal security-applied path compiles it.
//! The searched nodes and their edges are stashed as a [`GraphSeed`] for the output
//! formatter to splice back in. With no search trigger the stage leaves the query
//! untouched, so ordinary queries are byte-for-byte unchanged. A search timeout or
//! provider error fails the query; it never yields a partial result.

use std::collections::BTreeMap;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use clickhouse_client::ArrowClickHouseClient;
use compiler::input::{Direction, InputFilter, InputRelationship};
use compiler::{FilterOp, Frontend, Input, InputNode, SecurityContext};
use extensions::live::LiveValue;
use extensions::search::{
    Caller, FieldPredicate, InboundEdge, InboundTarget, PredicateOp, SearchCatalog, SearchError,
    SearchSpec, SeedRow, plan_pushdown, run_inbound_search, run_search, target_ids,
};
use orbit_arrow::ColumnValue;
use query_data_model::ClickHouseDataModel;
use query_engine::compiler;
use query_engine::pipeline::{
    PipelineError, PipelineObserver, PipelineStage, QueryPipelineContext,
};
use query_engine::shared::{GraphSeed, SeedGraphEdge, SeedGraphNode};
use query_engine::types::QueryResult;
use serde_json::{Value, json};

use super::live_enrich::{LiveBudget, LiveRuntime};
use crate::auth::Claims;

#[derive(Clone)]
pub struct LiveSeedStage;

impl PipelineStage for LiveSeedStage {
    type Input = ();
    type Output = ();

    async fn execute(
        &self,
        ctx: &mut QueryPipelineContext,
        obs: &mut dyn PipelineObserver,
    ) -> Result<Self::Output, PipelineError> {
        let Some(prepared) = self.prepare(ctx) else {
            return Ok(());
        };
        let Some(outcome) = run_seed(&prepared)
            .await
            .inspect_err(|e| obs.record_error(e))?
        else {
            return Ok(());
        };
        ctx.phases.remove::<compiler::gql::ParsedQuery>();
        ctx.query_json = outcome.query_json;
        ctx.frontend = Frontend::JsonDsl;
        ctx.phases.insert(outcome.seed);
        Ok(())
    }
}

/// Everything `run_seed` needs, cloned out of the pipeline context so the later
/// mutation of `ctx` does not conflict with the borrows the detection read.
struct Prepared {
    runtime: LiveRuntime,
    data_model: Arc<ClickHouseDataModel>,
    client: Arc<ArrowClickHouseClient>,
    security: SecurityContext,
    user_id: i64,
    budget: Duration,
    input: Input,
    plan: SeedPlan,
}

struct SeedOutcome {
    query_json: String,
    seed: GraphSeed,
}

/// One graph neighbour of the searched alias in the query pattern: the adjacent
/// alias, its entity, and the relationship that connects them.
#[derive(Clone, Debug)]
struct Neighbor {
    alias: String,
    entity: String,
    relationship: String,
}

#[derive(Debug)]
enum Seeding {
    /// The searched alias starts the pattern; its `WHERE` pushes into the search.
    Pushdown { filters: BTreeMap<String, String> },
    /// The searched alias is reached from an indexed, constrained anchor.
    Inbound {
        anchor: Neighbor,
        cited_by_property: String,
        extra_filters: BTreeMap<String, String>,
    },
}

struct SeedPlan {
    seed_alias: String,
    seed_entity: String,
    seeding: Seeding,
    neighbors: Vec<Neighbor>,
}

impl LiveSeedStage {
    fn prepare(&self, ctx: &QueryPipelineContext) -> Option<Prepared> {
        if ctx.frontend != Frontend::Gql {
            return None;
        }
        let runtime = ctx.server_extensions.get::<LiveRuntime>()?.clone();
        if runtime.search.is_empty() {
            return None;
        }
        let data_model = ctx
            .server_extensions
            .get::<Arc<ClickHouseDataModel>>()?
            .clone();
        let client = ctx
            .server_extensions
            .get::<Arc<ArrowClickHouseClient>>()?
            .clone();
        let security = ctx.security_context.clone()?;

        let input = compiler::parse_gql_input(&ctx.query_json, &data_model).ok()?;
        let plan = detect(&input, &runtime.search)?;

        let user_id = ctx
            .server_extensions
            .get::<Claims>()
            .map(|claims| claims.user_id as i64)
            .unwrap_or(security.org_id);
        let budget = ctx
            .server_extensions
            .get::<LiveBudget>()
            .copied()
            .unwrap_or_default()
            .0;

        Some(Prepared {
            runtime,
            data_model,
            client,
            security,
            user_id,
            budget,
            input,
            plan,
        })
    }
}

/// Decides whether a query is a seedable search, and in which shape. Returns `None`
/// for an ordinary query (which the pipeline then runs unchanged).
fn detect(input: &Input, search: &SearchCatalog) -> Option<SeedPlan> {
    let seed_alias = sole_searched_alias(input, search)?;
    let seed_entity = input.entity_of(&seed_alias)?.to_string();
    let spec = search.get(&seed_entity)?;
    let neighbors = neighbors_of(input, &seed_alias);

    let pushdown = pushdown_filters(input, &seed_alias, spec);
    if !pushdown.is_empty() {
        return Some(SeedPlan {
            seed_alias,
            seed_entity,
            seeding: Seeding::Pushdown { filters: pushdown },
            neighbors,
        });
    }

    let anchor = neighbors.iter().find(|neighbor| {
        search.get(&neighbor.entity).is_none()
            && spec.inbound_mapping(&neighbor.relationship).is_some()
            && search.cited_by(&neighbor.entity).is_some()
            && input
                .node(&neighbor.alias)
                .is_some_and(|node| !node.filters.is_empty())
    })?;
    let cited_by_property = search.cited_by(&anchor.entity)?.to_string();
    Some(SeedPlan {
        seed_alias,
        seed_entity,
        seeding: Seeding::Inbound {
            anchor: anchor.clone(),
            cited_by_property,
            extra_filters: BTreeMap::new(),
        },
        neighbors,
    })
}

/// Runs the search and rewrites the query. `None` leaves the query to the stored
/// path: an inbound anchor set larger than one search may fan out to is answered
/// from the index alone, where the searched entity's stored rows still match.
async fn run_seed(prepared: &Prepared) -> Result<Option<SeedOutcome>, PipelineError> {
    let spec = prepared
        .runtime
        .search
        .get(&prepared.plan.seed_entity)
        .ok_or_else(|| PipelineError::custom("searched entity left the catalog"))?;
    let provider = prepared
        .runtime
        .providers
        .get(&spec.package)
        .ok_or_else(|| {
            PipelineError::ContentResolution(format!(
                "live search provider for {} is unavailable",
                spec.package
            ))
        })?;
    let caller = Caller {
        scope_paths: &scope_paths(&prepared.security),
        user_id: prepared.user_id,
    };

    let mut anchor_nodes: Vec<SeedGraphNode> = Vec::new();
    let (rows, anchor_ids) = match &prepared.plan.seeding {
        Seeding::Pushdown { filters } => {
            let deadline = Duration::from_millis(spec.timeout_ms).min(prepared.budget);
            let rows = run_search(provider.as_ref(), spec, caller, filters.clone(), deadline)
                .await
                .map_err(search_error)?;
            (rows, None)
        }
        Seeding::Inbound {
            anchor,
            cited_by_property,
            extra_filters,
        } => {
            let resolved = resolve_anchors(prepared, anchor, cited_by_property).await?;
            if resolved.len() > spec.max_rows as usize {
                return Ok(None);
            }
            let anchor_ids: Vec<i64> = resolved.iter().map(|a| a.target.id).collect();
            anchor_nodes = resolved.iter().map(|a| a.node.clone()).collect();
            let targets = resolved.into_iter().map(|a| a.target).collect();
            let matches = run_inbound_search(
                provider.as_ref(),
                spec,
                InboundEdge {
                    relationship: &anchor.relationship,
                    target_entity: &anchor.entity,
                },
                targets,
                extra_filters,
                caller,
                prepared.budget,
            )
            .await
            .map_err(search_error)?;
            let rows = matches
                .into_iter()
                .flat_map(|m| m.rows)
                .fold(Vec::new(), dedup_rows_by_id);
            (rows, Some(anchor_ids))
        }
    };

    let pins = neighbor_pins(&prepared.plan, &rows, anchor_ids.as_deref());
    let mut seed = build_seed(
        &prepared.plan,
        &rows,
        &pins,
        keeps_anchor(&prepared.input, &prepared.plan),
    );
    seed.anchors = anchor_nodes;
    let seed_ids: Vec<i64> = rows.iter().map(|row| row.id).collect();
    let query_json = build_phase_b(&prepared.input, &prepared.plan, &pins, &seed_ids);
    Ok(Some(SeedOutcome { query_json, seed }))
}

/// The pinned id set for each neighbour of the searched alias: the inbound anchor
/// keeps the ids the indexed part resolved; every other neighbour is pinned to the
/// ids the seed rows' edges point at.
fn neighbor_pins(
    plan: &SeedPlan,
    rows: &[SeedRow],
    anchor_ids: Option<&[i64]>,
) -> BTreeMap<String, Vec<i64>> {
    let anchor_alias = match &plan.seeding {
        Seeding::Inbound { anchor, .. } => Some(anchor.alias.as_str()),
        Seeding::Pushdown { .. } => None,
    };
    let mut pins = BTreeMap::new();
    for neighbor in &plan.neighbors {
        // An anchor is pinned only to the ids a kept row cites, so an anchor no
        // message reached does not come back on its own.
        let ids = if Some(neighbor.alias.as_str()) == anchor_alias {
            let cited = target_ids(rows, &neighbor.relationship, &neighbor.entity);
            let mut ids = anchor_ids.unwrap_or_default().to_vec();
            ids.retain(|id| cited.contains(id));
            ids
        } else {
            target_ids(rows, &neighbor.relationship, &neighbor.entity)
        };
        pins.insert(neighbor.alias.clone(), ids);
    }
    pins
}

/// `anchor_in_stored_query` says the stored query still returns the inbound anchor,
/// so a seed row must reach an anchor that survived it, like any other hop.
fn build_seed(
    plan: &SeedPlan,
    rows: &[SeedRow],
    pins: &BTreeMap<String, Vec<i64>>,
    anchor_in_stored_query: bool,
) -> GraphSeed {
    let nodes = rows
        .iter()
        .map(|row| SeedGraphNode {
            entity_type: plan.seed_entity.clone(),
            id: row.id,
            properties: live_properties_to_json(&row.properties),
        })
        .collect();

    let mut edges = Vec::new();
    for row in rows {
        for neighbor in &plan.neighbors {
            let pinned = pins.get(&neighbor.alias).map(Vec::as_slice).unwrap_or(&[]);
            for edge in &row.edges {
                if edge.relationship == neighbor.relationship
                    && edge.target_entity == neighbor.entity
                    && pinned.contains(&edge.target_id)
                {
                    edges.push(SeedGraphEdge {
                        from_entity: plan.seed_entity.clone(),
                        from_id: row.id,
                        to_entity: neighbor.entity.clone(),
                        to_id: edge.target_id,
                        edge_type: neighbor.relationship.clone(),
                    });
                }
            }
        }
    }
    // An anchor served from the seed is already satisfied: the search kept only
    // rows citing it.
    let anchor_alias = match &plan.seeding {
        Seeding::Inbound { anchor, .. } if !anchor_in_stored_query => Some(anchor.alias.as_str()),
        _ => None,
    };
    let required_hops = plan
        .neighbors
        .iter()
        .filter(|neighbor| Some(neighbor.alias.as_str()) != anchor_alias)
        .map(|neighbor| (neighbor.relationship.clone(), neighbor.entity.clone()))
        .collect();
    GraphSeed {
        nodes,
        edges,
        anchors: Vec::new(),
        required_hops,
    }
}

/// Rewrites the pattern into JSON-DSL without the aliases the seed already produced
/// (the searched alias, and the inbound anchor which the indexed part resolved):
/// every remaining node keeps its columns, a neighbour is pinned to its seed ids,
/// and relationships that touched a removed alias are dropped.
///
/// When the seed produced every alias, the stored query would have no node, which
/// the query schema rejects. It then keeps one alias, pinned: the inbound anchor to
/// its resolved ids, or the searched alias to the seed row ids.
fn build_phase_b(
    input: &Input,
    plan: &SeedPlan,
    pins: &BTreeMap<String, Vec<i64>>,
    seed_ids: &[i64],
) -> String {
    let mut removed = removed_aliases(input, plan);
    let mut pins = pins.clone();
    if input.nodes.iter().all(|node| removed.contains(&node.id)) {
        let kept = match &plan.seeding {
            Seeding::Inbound { anchor, .. } => anchor.alias.clone(),
            Seeding::Pushdown { .. } => {
                pins.insert(plan.seed_alias.clone(), seed_ids.to_vec());
                plan.seed_alias.clone()
            }
        };
        removed.retain(|alias| *alias != kept);
    }
    let nodes: Vec<Value> = input
        .nodes
        .iter()
        .filter(|node| !removed.contains(&node.id))
        .map(|node| phase_b_node(node, &pins))
        .collect();

    let relationships: Vec<Value> = input
        .relationships
        .iter()
        .filter(|rel| !removed.contains(&rel.from) && !removed.contains(&rel.to))
        .map(relationship_to_json)
        .collect();

    // The query schema forbids a `relationships` key on a single-node (table-scan)
    // query, so it is omitted rather than sent empty.
    let mut query = json!({
        "query_type": "traversal",
        "nodes": nodes,
        "limit": input.limit,
    });
    if !relationships.is_empty() {
        query["relationships"] = Value::Array(relationships);
    }
    query.to_string()
}

/// The aliases the seed already produced, so the stored query omits them: always the
/// searched alias, plus the inbound anchor unless the stored query still needs it.
fn removed_aliases(input: &Input, plan: &SeedPlan) -> Vec<String> {
    let mut removed = vec![plan.seed_alias.clone()];
    if let Seeding::Inbound { anchor, .. } = &plan.seeding
        && !keeps_anchor(input, plan)
    {
        removed.push(anchor.alias.clone());
    }
    removed
}

/// The stored query keeps the inbound anchor, pinned, when more of the pattern hangs
/// off it (in (m)-[:CITES]->(i)-[:CITES]->(g), g is constrained only through i), or
/// when nothing else would be left to query.
fn keeps_anchor(input: &Input, plan: &SeedPlan) -> bool {
    let Seeding::Inbound { anchor, .. } = &plan.seeding else {
        return false;
    };
    anchor_component(input, &plan.seed_alias, &anchor.alias).len() > 1 || input.nodes.len() <= 2
}

/// The aliases reachable from the anchor without passing through the searched alias:
/// the stored part of the pattern that constrains the anchor.
fn anchor_component(input: &Input, seed_alias: &str, anchor_alias: &str) -> Vec<String> {
    let mut component = vec![anchor_alias.to_string()];
    let mut next = 0;
    while next < component.len() {
        let alias = component[next].clone();
        next += 1;
        for rel in &input.relationships {
            let other = if rel.from == alias {
                &rel.to
            } else if rel.to == alias {
                &rel.from
            } else {
                continue;
            };
            if other != seed_alias && !component.contains(other) {
                component.push(other.clone());
            }
        }
    }
    component
}

fn phase_b_node(node: &InputNode, pins: &BTreeMap<String, Vec<i64>>) -> Value {
    let mut json = json!({ "id": node.id, "columns": "*" });
    if let Some(entity) = &node.entity {
        json["entity"] = json!(entity);
    }
    match pins.get(&node.id) {
        // An empty pin means no seed edge reached this neighbour, so the query must
        // return nothing; a sentinel id no real node carries forces an empty result.
        Some(ids) if ids.is_empty() => json["node_ids"] = json!([i64::MIN]),
        Some(ids) => json["node_ids"] = json!(ids),
        None => {
            if let Some(filters) = filters_to_json(&node.filters) {
                json["filters"] = filters;
            }
        }
    }
    json
}

fn relationship_to_json(rel: &InputRelationship) -> Value {
    json!({
        "type": rel.types,
        "from": rel.from,
        "to": rel.to,
        "direction": direction_name(rel.direction),
    })
}

/// The stored rows read to find the distinct anchors: a path can repeat an anchor,
/// so this is well above the search fan-out limit.
const ANCHOR_SCAN_ROWS: u32 = 1000;

/// One indexed anchor the inbound part resolved: its search target (id plus the
/// `cited_by` value that fills `$cited_by`) and its full node for the result graph.
struct AnchorResolved {
    target: InboundTarget,
    node: SeedGraphNode,
}

/// Runs the indexed part for the anchor alias and reads each anchor's id, `cited_by`
/// value, and full properties. The caller falls back to the stored path when there
/// are more anchors than one search may fan out to.
async fn resolve_anchors(
    prepared: &Prepared,
    anchor: &Neighbor,
    cited_by_property: &str,
) -> Result<Vec<AnchorResolved>, PipelineError> {
    // The anchors are resolved with the stored part of the pattern that hangs off
    // them, so only anchors that can complete the path fan out to a search.
    let input = &prepared.input;
    let component = anchor_component(input, &prepared.plan.seed_alias, &anchor.alias);
    let nodes: Vec<Value> = input
        .nodes
        .iter()
        .filter(|node| component.contains(&node.id))
        .map(|node| phase_b_node(node, &BTreeMap::new()))
        .collect();
    let relationships: Vec<Value> = input
        .relationships
        .iter()
        .filter(|rel| component.contains(&rel.from) && component.contains(&rel.to))
        .map(relationship_to_json)
        .collect();
    let mut anchor_query = json!({
        "query_type": "traversal",
        "nodes": nodes,
        "limit": ANCHOR_SCAN_ROWS,
    });
    if !relationships.is_empty() {
        anchor_query["relationships"] = Value::Array(relationships);
    }
    let anchor_query = anchor_query.to_string();

    let compiled = compiler::compile_model(
        &anchor_query,
        Frontend::JsonDsl,
        &prepared.data_model,
        &prepared.security,
    )
    .map_err(|e| PipelineError::Compile {
        client_safe: false,
        reason: e.rejection_reason(),
        message: e.to_string(),
    })?;

    let mut query = prepared.client.query(&compiled.base.sql);
    for (key, param) in &compiled.base.params {
        query = ArrowClickHouseClient::bind_param(query, key, &param.value, &param.data_type);
    }
    let (batches, _) = query
        .fetch_arrow_with_summary()
        .await
        .map_err(|e| PipelineError::Execution(e.to_string()))?;

    let result = QueryResult::from_batches(&batches, &compiled.base.result_context)
        .map_err(|e| PipelineError::Execution(e.to_string()))?;
    let Some(anchor_result_node) = compiled
        .base
        .result_context
        .nodes()
        .find(|node| node.alias == anchor.alias)
    else {
        return Ok(Vec::new());
    };

    let mut anchors = Vec::new();
    for row in result.rows() {
        let Some(id) = row.get_public_id(anchor_result_node) else {
            continue;
        };
        if anchors
            .iter()
            .any(|known: &AnchorResolved| known.target.id == id)
        {
            continue;
        }
        let properties = row.entity_properties(&anchor_result_node.alias);
        let cited_by_value = properties
            .get(cited_by_property)
            .map(column_value_to_string)
            .unwrap_or_default();
        anchors.push(AnchorResolved {
            target: InboundTarget { id, cited_by_value },
            node: SeedGraphNode {
                entity_type: anchor.entity.clone(),
                id,
                properties: column_properties_to_json(&properties),
            },
        });
    }
    Ok(anchors)
}

fn sole_searched_alias(input: &Input, search: &SearchCatalog) -> Option<String> {
    let mut searched = input.nodes.iter().filter(|node| {
        node.entity
            .as_deref()
            .is_some_and(|entity| search.get(entity).is_some())
    });
    let first = searched.next()?;
    searched.next().is_none().then(|| first.id.clone())
}

fn neighbors_of(input: &Input, alias: &str) -> Vec<Neighbor> {
    let mut neighbors = Vec::new();
    for rel in &input.relationships {
        let other = if rel.from == alias {
            Some(&rel.to)
        } else if rel.to == alias {
            Some(&rel.from)
        } else {
            None
        };
        let (Some(other), Some(relationship)) = (other, rel.types.first()) else {
            continue;
        };
        if let Some(entity) = input.entity_of(other) {
            neighbors.push(Neighbor {
                alias: other.clone(),
                entity: entity.to_string(),
                relationship: relationship.clone(),
            });
        }
    }
    neighbors
}

fn pushdown_filters(input: &Input, alias: &str, spec: &SearchSpec) -> BTreeMap<String, String> {
    let Some(node) = input.node(alias) else {
        return BTreeMap::new();
    };
    let predicates = search_predicates(node, spec);
    plan_pushdown(spec, &predicates).filters
}

fn search_predicates(node: &InputNode, spec: &SearchSpec) -> Vec<FieldPredicate> {
    let mut predicates = Vec::new();
    for (property, filters) in &node.filters {
        if !spec.fields.contains_key(property) {
            continue;
        }
        for filter in filters {
            let (Some(op), Some(value)) = (filter.op.and_then(predicate_op), filter_value(filter))
            else {
                continue;
            };
            predicates.push(FieldPredicate {
                field: property.clone(),
                op,
                value,
            });
        }
    }
    predicates
}

fn predicate_op(op: FilterOp) -> Option<PredicateOp> {
    match op {
        FilterOp::Contains => Some(PredicateOp::Contains),
        FilterOp::Eq => Some(PredicateOp::Equals),
        FilterOp::Gt => Some(PredicateOp::Gt),
        FilterOp::Gte => Some(PredicateOp::Gte),
        FilterOp::Lt => Some(PredicateOp::Lt),
        FilterOp::Lte => Some(PredicateOp::Lte),
        _ => None,
    }
}

fn filter_value(filter: &InputFilter) -> Option<String> {
    value_to_string(filter.value.as_ref()?)
}

fn filters_to_json(filters: &HashMap<String, Vec<InputFilter>>) -> Option<Value> {
    if filters.is_empty() {
        return None;
    }
    let mut map = serde_json::Map::new();
    for (property, entries) in filters {
        let mut ops = serde_json::Map::new();
        for filter in entries {
            let (Some(op), Some(value)) = (filter.op, filter.value.clone()) else {
                continue;
            };
            ops.insert(filter_op_name(op).to_string(), value);
        }
        if !ops.is_empty() {
            map.insert(property.clone(), Value::Object(ops));
        }
    }
    (!map.is_empty()).then_some(Value::Object(map))
}

fn filter_op_name(op: FilterOp) -> &'static str {
    match op {
        FilterOp::Eq => "eq",
        FilterOp::Ne => "ne",
        FilterOp::Gt => "gt",
        FilterOp::Lt => "lt",
        FilterOp::Gte => "gte",
        FilterOp::Lte => "lte",
        FilterOp::In => "in",
        FilterOp::Contains => "contains",
        FilterOp::StartsWith => "starts_with",
        FilterOp::EndsWith => "ends_with",
        FilterOp::IsNull => "is_null",
        FilterOp::IsNotNull => "is_not_null",
        FilterOp::TokenMatch => "token_match",
        FilterOp::AllTokens => "all_tokens",
        FilterOp::AnyTokens => "any_tokens",
    }
}

fn direction_name(direction: Direction) -> &'static str {
    match direction {
        Direction::Outgoing => "outgoing",
        Direction::Incoming => "incoming",
        Direction::Both => "both",
    }
}

fn dedup_rows_by_id(mut acc: Vec<SeedRow>, row: SeedRow) -> Vec<SeedRow> {
    if !acc.iter().any(|existing| existing.id == row.id) {
        acc.push(row);
    }
    acc
}

fn scope_paths(security: &SecurityContext) -> Vec<String> {
    security
        .traversal_paths
        .iter()
        .map(|path| path.path.to_string())
        .collect()
}

fn live_properties_to_json(
    properties: &HashMap<String, LiveValue>,
) -> serde_json::Map<String, Value> {
    properties
        .iter()
        .map(|(name, value)| (name.clone(), live_value_to_json(value)))
        .collect()
}

fn live_value_to_json(value: &LiveValue) -> Value {
    match value {
        LiveValue::String(v) => json!(v),
        LiveValue::Int(v) | LiveValue::TimestampUnixMs(v) => json!(v),
        LiveValue::Float(v) => json!(v),
        LiveValue::Bool(v) => json!(v),
    }
}

fn value_to_string(value: &Value) -> Option<String> {
    match value {
        Value::String(s) => Some(s.clone()),
        Value::Number(n) => Some(n.to_string()),
        Value::Bool(b) => Some(b.to_string()),
        _ => None,
    }
}

/// Converts an anchor's resolved columns to JSON for its result node, dropping `id`
/// (the result node carries it separately) and any internal columns.
fn column_properties_to_json(
    properties: &HashMap<String, ColumnValue>,
) -> serde_json::Map<String, Value> {
    properties
        .iter()
        .filter(|(name, _)| name.as_str() != "id" && !name.starts_with('_'))
        .map(|(name, value)| (name.clone(), column_value_to_json(value)))
        .collect()
}

fn column_value_to_json(value: &ColumnValue) -> Value {
    match value {
        ColumnValue::String(v) => json!(v),
        ColumnValue::Int64(v) => json!(v),
        ColumnValue::Float64(v) => json!(v),
        ColumnValue::Bool(v) => json!(v),
        ColumnValue::Null => Value::Null,
    }
}

fn column_value_to_string(value: &ColumnValue) -> String {
    match value {
        ColumnValue::String(v) => v.clone(),
        ColumnValue::Int64(v) => v.to_string(),
        ColumnValue::Float64(v) => v.to_string(),
        ColumnValue::Bool(v) => v.to_string(),
        ColumnValue::Null => String::new(),
    }
}

fn search_error(error: SearchError) -> PipelineError {
    PipelineError::ContentResolution(error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    const MESSAGES: &str =
        include_str!("../../../../extensions/tests/fixtures/example.messages.yaml");

    fn catalog() -> SearchCatalog {
        let package = extensions::package::Package::parse(
            MESSAGES,
            extensions::package::short_digest(MESSAGES),
        )
        .expect("fixture parses");
        SearchCatalog::from_packages(&[package])
    }

    fn data_model() -> Arc<ClickHouseDataModel> {
        let package = extensions::package::Package::parse(
            MESSAGES,
            extensions::package::short_digest(MESSAGES),
        )
        .expect("fixture parses");
        let ontology =
            extensions::loader::load_merged_ontology(orbit_versions::VERSIONS.schema, &[package])
                .expect("ontology loads");
        Arc::new(ClickHouseDataModel::derive(Arc::new(ontology)).expect("model derives"))
    }

    fn plan_for(query: &str) -> SeedPlan {
        let input = compiler::parse_gql_input(query, &data_model()).expect("parses");
        detect(&input, &catalog()).expect("a search plan is detected")
    }

    #[test]
    fn detects_the_pushdown_shape_and_builds_the_search_filter() {
        let plan = plan_for(
            "MATCH (m:SlackMessage)-[:CITES]->(i:Incident)-[:CITES]->(g:GitLabRef) \
             WHERE m.text CONTAINS 'deploy' RETURN m, i, g LIMIT 20",
        );
        assert_eq!(plan.seed_alias, "m");
        assert_eq!(plan.seed_entity, "SlackMessage");
        match &plan.seeding {
            Seeding::Pushdown { filters } => {
                assert_eq!(filters.get("text").map(String::as_str), Some("deploy"))
            }
            other => panic!("expected pushdown, got {other:?}"),
        }
        assert!(
            plan.neighbors
                .iter()
                .any(|n| n.alias == "i" && n.entity == "Incident")
        );
    }

    #[test]
    fn pushdown_rewrite_removes_the_seed_alias_and_pins_its_neighbour() {
        let input = compiler::parse_gql_input(
            "MATCH (m:SlackMessage)-[:CITES]->(i:Incident)-[:CITES]->(g:GitLabRef) \
             WHERE m.text CONTAINS 'deploy' RETURN m, i, g LIMIT 20",
            &data_model(),
        )
        .expect("parses");
        let plan = detect(&input, &catalog()).expect("plan");
        let pins = BTreeMap::from([("i".to_string(), vec![10, 20])]);
        let phase_b: Value =
            serde_json::from_str(&build_phase_b(&input, &plan, &pins, &[])).unwrap();

        let ids: Vec<&str> = phase_b["nodes"]
            .as_array()
            .unwrap()
            .iter()
            .map(|n| n["id"].as_str().unwrap())
            .collect();
        assert_eq!(ids, vec!["i", "g"], "the seed alias m is gone");
        let incident = phase_b["nodes"]
            .as_array()
            .unwrap()
            .iter()
            .find(|n| n["id"] == "i")
            .unwrap();
        assert_eq!(incident["node_ids"], json!([10, 20]));
        assert_eq!(
            phase_b["relationships"].as_array().unwrap().len(),
            1,
            "only the i->g hop survives"
        );
    }

    #[test]
    fn detects_the_inbound_shape_from_an_indexed_anchor() {
        let plan = plan_for(
            "MATCH (i:Incident)<-[:CITES]-(m:SlackMessage)-[:CITES]->(g:GitLabRef) \
             WHERE i.reference = 'INC-77' RETURN i, m, g LIMIT 20",
        );
        assert_eq!(plan.seed_alias, "m");
        match &plan.seeding {
            Seeding::Inbound {
                anchor,
                cited_by_property,
                ..
            } => {
                assert_eq!(anchor.entity, "Incident");
                assert_eq!(anchor.relationship, "CITES");
                assert_eq!(cited_by_property, "reference");
            }
            other => panic!("expected inbound, got {other:?}"),
        }
    }

    #[test]
    fn a_query_with_no_searchable_alias_is_not_a_seed() {
        let input = compiler::parse_gql_input(
            "MATCH (i:Incident)-[:CITES]->(g:GitLabRef) WHERE i.reference = 'INC-1' RETURN i, g LIMIT 20",
            &data_model(),
        )
        .expect("parses");
        assert!(detect(&input, &catalog()).is_none());
    }

    #[test]
    fn a_two_hop_inbound_query_keeps_the_anchor_pinned() {
        let input = compiler::parse_gql_input(
            "MATCH (i:Incident)<-[:CITES]-(m:SlackMessage) WHERE i.reference = 'INC-77' \
             RETURN i, m LIMIT 20",
            &data_model(),
        )
        .expect("parses");
        let plan = detect(&input, &catalog()).expect("plan");
        let pins = BTreeMap::from([("i".to_string(), vec![10])]);
        let phase_b: Value =
            serde_json::from_str(&build_phase_b(&input, &plan, &pins, &[101])).unwrap();

        let nodes = phase_b["nodes"].as_array().unwrap();
        assert_eq!(nodes.len(), 1, "the stored query keeps one node");
        assert_eq!(nodes[0]["id"], "i");
        assert_eq!(nodes[0]["node_ids"], json!([10]));
        assert!(phase_b.get("relationships").is_none());
    }

    #[test]
    fn a_search_only_query_pins_the_searched_alias_to_the_seed_rows() {
        let input = compiler::parse_gql_input(
            "MATCH (m:SlackMessage) WHERE m.text CONTAINS 'deploy' RETURN m LIMIT 20",
            &data_model(),
        )
        .expect("parses");
        let plan = detect(&input, &catalog()).expect("plan");
        let phase_b: Value =
            serde_json::from_str(&build_phase_b(&input, &plan, &BTreeMap::new(), &[101, 102]))
                .unwrap();

        let nodes = phase_b["nodes"].as_array().unwrap();
        assert_eq!(nodes.len(), 1);
        assert_eq!(nodes[0]["node_ids"], json!([101, 102]));
        assert!(
            nodes[0].get("filters").is_none(),
            "the live predicate is not compiled"
        );
    }

    #[test]
    fn a_pushdown_seed_requires_every_hop_but_an_inbound_seed_skips_its_anchor() {
        let pushdown = plan_for(
            "MATCH (m:SlackMessage)-[:CITES]->(i:Incident) WHERE m.text CONTAINS 'deploy' RETURN m, i LIMIT 20",
        );
        let seed = build_seed(&pushdown, &[], &BTreeMap::new(), false);
        assert_eq!(
            seed.required_hops,
            vec![("CITES".to_string(), "Incident".to_string())]
        );

        let inbound = plan_for(
            "MATCH (i:Incident)<-[:CITES]-(m:SlackMessage)-[:CITES]->(g:GitLabRef) \
             WHERE i.reference = 'INC-77' RETURN i, m, g LIMIT 20",
        );
        let seed = build_seed(&inbound, &[], &BTreeMap::new(), false);
        assert_eq!(
            seed.required_hops,
            vec![("CITES".to_string(), "GitLabRef".to_string())]
        );
    }

    #[test]
    fn an_anchor_with_onward_hops_stays_pinned_and_becomes_a_required_hop() {
        let input = compiler::parse_gql_input(
            "MATCH (m:SlackMessage)-[:CITES]->(i:Incident)-[:CITES]->(g:GitLabRef) \
             WHERE i.created_at >= '2026-11-01' RETURN i, m, g LIMIT 20",
            &data_model(),
        )
        .expect("parses");
        let plan = detect(&input, &catalog()).expect("plan");
        assert!(matches!(plan.seeding, Seeding::Inbound { .. }));
        assert_eq!(anchor_component(&input, "m", "i"), vec!["i", "g"]);

        let pins = BTreeMap::from([("i".to_string(), vec![10])]);
        let phase_b: Value =
            serde_json::from_str(&build_phase_b(&input, &plan, &pins, &[])).unwrap();
        let ids: Vec<&str> = phase_b["nodes"]
            .as_array()
            .unwrap()
            .iter()
            .map(|n| n["id"].as_str().unwrap())
            .collect();
        assert_eq!(
            ids,
            vec!["i", "g"],
            "g stays constrained through the pinned anchor"
        );
        assert_eq!(phase_b["relationships"].as_array().unwrap().len(), 1);

        let seed = build_seed(&plan, &[], &pins, keeps_anchor(&input, &plan));
        assert_eq!(
            seed.required_hops,
            vec![("CITES".to_string(), "Incident".to_string())]
        );
    }
}
