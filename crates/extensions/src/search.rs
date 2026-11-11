//! Searchable live entities: starting a graph query from rows a provider's live
//! search returns, then traversing from those rows into the indexed graph.
//!
//! The entity's searchable text lives at the provider, not in ClickHouse. A query
//! predicate on a search field pushes down into one `Live` search call (see the
//! filter encoding on [`plan_pushdown`]); the call streams back rows carrying each
//! row's graph `id`, its properties, and its outgoing [`SeedEdge`]s. Orbit then
//! constrains the next hop with [`target_ids`] and runs the rest in ClickHouse.
//!
//! Two shapes use the same machinery. The start-of-pattern shape seeds `m` first
//! ([`run_search`]). The inbound shape ([`run_inbound_search`]) runs the indexed
//! anchor first, then one search per anchor to find the non-indexed rows that
//! point at it. Unlike live-property enrich, a failed or slow search is a hard
//! error: a search answer is never silently partial.

use std::collections::BTreeMap;
use std::collections::HashMap;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use futures::stream::{FuturesUnordered, StreamExt};

use crate::live::{KeyStatus, LiveProvider, LiveValue, key_status};
use crate::package::{Package, SearchMatch};
use crate::proto::{LiveRequest, live_response::Kind};

/// Every searchable entity of every loaded package, keyed by entity name.
#[derive(Debug, Clone, Default)]
pub struct SearchCatalog {
    specs: BTreeMap<String, SearchSpec>,
    cited_by: BTreeMap<String, String>,
}

/// One entity's search contract: the operation that returns rows, how each field's
/// predicate pushes down, and how to find rows that point at an indexed target.
#[derive(Debug, Clone)]
pub struct SearchSpec {
    pub package: String,
    pub entity: String,
    pub operation: String,
    pub fields: BTreeMap<String, SearchMatch>,
    /// relationship -> (search field -> placeholder). The only v0 placeholder is
    /// `$cited_by`, filled with the target node's `cited_by` value at query time.
    pub inbound: BTreeMap<String, BTreeMap<String, String>>,
    pub max_rows: u32,
    pub timeout_ms: u64,
}

/// One row a search returned: its graph id, the key tuple (may be empty), the
/// resolved properties, and the outgoing edges Orbit traverses from.
#[derive(Debug, Clone, PartialEq)]
pub struct SeedRow {
    pub id: i64,
    pub key: Vec<String>,
    pub properties: HashMap<String, LiveValue>,
    pub edges: Vec<SeedEdge>,
}

/// One outgoing edge of a [`SeedRow`]: the relationship, the target entity, and
/// the target's graph id.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SeedEdge {
    pub relationship: String,
    pub target_entity: String,
    pub target_id: i64,
}

/// The security principal of a search: the caller's scope paths and user id, which
/// every search call forwards to the provider unchanged.
#[derive(Debug, Clone, Copy)]
pub struct Caller<'a> {
    pub scope_paths: &'a [String],
    pub user_id: i64,
}

/// The inbound edge to follow back from an indexed anchor to the searched entity:
/// the relationship and the anchor's entity type.
#[derive(Debug, Clone, Copy)]
pub struct InboundEdge<'a> {
    pub relationship: &'a str,
    pub target_entity: &'a str,
}

/// A query predicate on a search field, already reduced to a field name, an
/// operator, and a string value (ISO-8601 for a datetime range bound).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FieldPredicate {
    pub field: String,
    pub op: PredicateOp,
    pub value: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PredicateOp {
    Contains,
    Equals,
    Gt,
    Gte,
    Lt,
    Lte,
}

/// The filters to push into one search call, and which input predicates they
/// consumed. A consumed predicate is removed from the stored query.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Pushdown {
    pub filters: BTreeMap<String, String>,
    pub consumed: Vec<usize>,
}

/// One indexed anchor of an inbound search: its graph id and its `cited_by` value
/// (the entity's citation key, as a string), which fills the `$cited_by` filter.
#[derive(Debug, Clone)]
pub struct InboundTarget {
    pub id: i64,
    pub cited_by_value: String,
}

/// The rows one inbound anchor matched: the kept rows that carry an edge back to
/// the anchor, so the `m -> anchor` edges can be assembled from them.
#[derive(Debug, Clone)]
pub struct InboundMatch {
    pub target_id: i64,
    pub rows: Vec<SeedRow>,
}

#[derive(Debug, thiserror::Error)]
pub enum SearchError {
    #[error("search {operation} timed out before the provider finished streaming rows")]
    Timeout { operation: String },
    #[error("search {operation} failed: {source}")]
    Rpc {
        operation: String,
        source: tonic::Status,
    },
    #[error("search {operation} returned status {status} from the provider: {message}")]
    Provider {
        operation: String,
        status: &'static str,
        message: String,
    },
    #[error(
        "inbound search {operation}: {targets} indexed anchors exceed max_rows {max_rows}; narrow the query"
    )]
    TooManyTargets {
        operation: String,
        targets: usize,
        max_rows: u32,
    },
    #[error("unsupported search query: {0}")]
    Unsupported(String),
}

impl SearchCatalog {
    pub fn from_packages(packages: &[Package]) -> Self {
        let mut specs = BTreeMap::new();
        let mut cited_by = BTreeMap::new();
        for package in packages {
            for (entity, def) in &package.document.graph.entities {
                if let Some(property) = &def.cited_by {
                    cited_by.insert(entity.clone(), property.clone());
                }
            }
            let Some(live) = &package.document.live else {
                continue;
            };
            for (entity, live_entity) in &live.entities {
                let Some(search) = &live_entity.search else {
                    continue;
                };
                specs.insert(
                    entity.clone(),
                    SearchSpec {
                        package: package.name().to_string(),
                        entity: entity.clone(),
                        operation: search.operation.clone(),
                        fields: search.fields.clone(),
                        inbound: search.inbound.clone(),
                        max_rows: search.max_rows,
                        timeout_ms: search.timeout_ms,
                    },
                );
            }
        }
        Self { specs, cited_by }
    }

    pub fn is_empty(&self) -> bool {
        self.specs.is_empty()
    }

    pub fn get(&self, entity: &str) -> Option<&SearchSpec> {
        self.specs.get(entity)
    }

    /// The `cited_by` property of an entity, if it declares one. The planner reads
    /// it to pull an inbound anchor's citation value from the indexed part.
    pub fn cited_by(&self, entity: &str) -> Option<&str> {
        self.cited_by.get(entity).map(String::as_str)
    }

    /// Each searchable entity mapped to its fields and their declared match. The
    /// loader reads it to mark those properties searchable in the graph schema.
    pub fn matches_by_entity(&self) -> BTreeMap<String, BTreeMap<String, SearchMatch>> {
        self.specs
            .iter()
            .map(|(entity, spec)| (entity.clone(), spec.fields.clone()))
            .collect()
    }
}

impl SearchSpec {
    /// The `(search field -> placeholder)` map for one inbound relationship.
    pub fn inbound_mapping(&self, relationship: &str) -> Option<&BTreeMap<String, String>> {
        self.inbound.get(relationship)
    }
}

/// Turns predicates on an entity's search fields into push-down filters. A
/// predicate is consumed only when its operator matches the field's declared
/// match (`CONTAINS` for `contains`, `=` for `equals`, a `<`/`>` comparison for
/// `range`); others are left in place. Range bounds encode as `<field>_from` and
/// `<field>_to`, every other match as the bare `<field>`.
pub fn plan_pushdown(spec: &SearchSpec, predicates: &[FieldPredicate]) -> Pushdown {
    let mut pushdown = Pushdown::default();
    for (index, predicate) in predicates.iter().enumerate() {
        let Some(declared) = spec.fields.get(&predicate.field) else {
            continue;
        };
        let key = match (declared, predicate.op) {
            (SearchMatch::Contains, PredicateOp::Contains)
            | (SearchMatch::Equals, PredicateOp::Equals) => predicate.field.clone(),
            (SearchMatch::Range, PredicateOp::Gt | PredicateOp::Gte) => {
                format!("{}_from", predicate.field)
            }
            (SearchMatch::Range, PredicateOp::Lt | PredicateOp::Lte) => {
                format!("{}_to", predicate.field)
            }
            _ => continue,
        };
        pushdown.filters.insert(key, predicate.value.clone());
        pushdown.consumed.push(index);
    }
    pushdown
}

/// Runs one search call and collects its rows. The provider streams rows until it
/// closes the stream; a non-`OK` status, a transport error, or a missed deadline
/// is a hard error, so a search result is never silently partial.
pub async fn run_search(
    provider: &dyn LiveProvider,
    spec: &SearchSpec,
    caller: Caller<'_>,
    filters: BTreeMap<String, String>,
    deadline: Duration,
) -> Result<Vec<SeedRow>, SearchError> {
    let request = LiveRequest {
        package: spec.package.clone(),
        operation: spec.operation.clone(),
        scope_paths: caller.scope_paths.to_vec(),
        user_id: caller.user_id,
        keys: Vec::new(),
        filters: filters.into_iter().collect(),
        deadline_unix_ms: unix_ms_from_now(deadline),
        max_rows: spec.max_rows,
    };

    let stream = provider
        .live(request)
        .await
        .map_err(|source| SearchError::Rpc {
            operation: spec.operation.clone(),
            source,
        })?;
    collect_rows(stream, &spec.operation, deadline).await
}

/// The distinct target ids of the rows' edges for one relationship and target
/// entity, in first-seen order. This is the id set that constrains the next hop.
pub fn target_ids(rows: &[SeedRow], relationship: &str, target_entity: &str) -> Vec<i64> {
    let mut ids = Vec::new();
    for row in rows {
        for edge in &row.edges {
            if edge.relationship == relationship
                && edge.target_entity == target_entity
                && !ids.contains(&edge.target_id)
            {
                ids.push(edge.target_id);
            }
        }
    }
    ids
}

/// Finds the non-indexed rows that point at each indexed anchor. One search per
/// anchor runs in parallel under the live budget, its filters substituting the
/// anchor's `cited_by` value for each `$cited_by` field (ANDed with `extra_filters`
/// from the WHERE); only rows that carry an edge `(relationship, target_entity,
/// anchor.id)` are kept.
pub async fn run_inbound_search(
    provider: &dyn LiveProvider,
    spec: &SearchSpec,
    edge: InboundEdge<'_>,
    targets: Vec<InboundTarget>,
    extra_filters: &BTreeMap<String, String>,
    caller: Caller<'_>,
    total_budget: Duration,
) -> Result<Vec<InboundMatch>, SearchError> {
    if targets.len() > spec.max_rows as usize {
        return Err(SearchError::TooManyTargets {
            operation: spec.operation.clone(),
            targets: targets.len(),
            max_rows: spec.max_rows,
        });
    }
    let mapping = spec.inbound_mapping(edge.relationship).ok_or_else(|| {
        SearchError::Unsupported(format!(
            "{} has no inbound search for {}",
            spec.entity, edge.relationship
        ))
    })?;

    let started = Instant::now();
    let mut searches = FuturesUnordered::new();
    for target in targets {
        let filters = inbound_filters(mapping, &target.cited_by_value, extra_filters);
        let remaining = total_budget.saturating_sub(started.elapsed());
        let deadline = Duration::from_millis(spec.timeout_ms).min(remaining);
        searches.push(search_one_anchor(
            provider, spec, edge, target.id, filters, caller, deadline,
        ));
    }

    let mut matches = Vec::new();
    while let Some(result) = searches.next().await {
        matches.push(result?);
    }
    Ok(matches)
}

/// Builds one anchor's search filters: each `$cited_by` field resolves to the
/// anchor's citation value, ANDed with the WHERE filters.
fn inbound_filters(
    mapping: &BTreeMap<String, String>,
    cited_by_value: &str,
    extra_filters: &BTreeMap<String, String>,
) -> BTreeMap<String, String> {
    let mut filters = extra_filters.clone();
    for (search_field, placeholder) in mapping {
        if placeholder == crate::package::CITED_BY_PLACEHOLDER {
            filters.insert(search_field.clone(), cited_by_value.to_string());
        }
    }
    filters
}

async fn search_one_anchor(
    provider: &dyn LiveProvider,
    spec: &SearchSpec,
    edge: InboundEdge<'_>,
    target_id: i64,
    filters: BTreeMap<String, String>,
    caller: Caller<'_>,
    deadline: Duration,
) -> Result<InboundMatch, SearchError> {
    let rows = run_search(provider, spec, caller, filters, deadline).await?;
    let kept = rows
        .into_iter()
        .filter(|row| {
            row.edges.iter().any(|row_edge| {
                row_edge.relationship == edge.relationship
                    && row_edge.target_entity == edge.target_entity
                    && row_edge.target_id == target_id
            })
        })
        .collect();
    Ok(InboundMatch {
        target_id,
        rows: kept,
    })
}

async fn collect_rows(
    stream: crate::live::LiveStream,
    operation: &str,
    deadline: Duration,
) -> Result<Vec<SeedRow>, SearchError> {
    let drain = async {
        let mut rows = Vec::new();
        tokio::pin!(stream);
        while let Some(item) = stream.next().await {
            let response = item.map_err(|source| SearchError::Rpc {
                operation: operation.to_string(),
                source,
            })?;
            match response.kind {
                Some(Kind::Row(row)) => rows.push(seed_row(row)),
                Some(Kind::Status(status)) => {
                    let resolved = key_status(status.status);
                    if resolved != KeyStatus::Ok {
                        return Err(SearchError::Provider {
                            operation: operation.to_string(),
                            status: resolved.as_str(),
                            message: status.message,
                        });
                    }
                }
                None => {}
            }
        }
        Ok(rows)
    };

    match tokio::time::timeout(deadline, drain).await {
        Err(_) => Err(SearchError::Timeout {
            operation: operation.to_string(),
        }),
        Ok(result) => result,
    }
}

fn seed_row(row: crate::proto::Row) -> SeedRow {
    let properties = row
        .properties
        .into_iter()
        .filter_map(|(name, value)| LiveValue::from_proto(value).map(|mapped| (name, mapped)))
        .collect();
    let edges = row
        .edges
        .into_iter()
        .map(|edge| SeedEdge {
            relationship: edge.relationship,
            target_entity: edge.target_entity,
            target_id: edge.target_id,
        })
        .collect();
    SeedRow {
        id: row.id,
        key: row.key.map(|k| k.values).unwrap_or_default(),
        properties,
        edges,
    }
}

fn unix_ms_from_now(deadline: Duration) -> i64 {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    (now + deadline).as_millis() as i64
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::package::{
        DEFAULT_SEARCH_MAX_ROWS, DEFAULT_SEARCH_TIMEOUT_MS, Package, short_digest,
    };

    const MESSAGES: &str = include_str!("../tests/fixtures/example.messages.yaml");

    fn messages_catalog() -> SearchCatalog {
        let package = Package::parse(MESSAGES, short_digest(MESSAGES)).expect("fixture parses");
        SearchCatalog::from_packages(&[package])
    }

    fn slack_spec() -> SearchSpec {
        messages_catalog()
            .get("SlackMessage")
            .expect("SlackMessage is searchable")
            .clone()
    }

    fn predicate(field: &str, op: PredicateOp, value: &str) -> FieldPredicate {
        FieldPredicate {
            field: field.into(),
            op,
            value: value.into(),
        }
    }

    fn edge(relationship: &str, target_entity: &str, target_id: i64) -> SeedEdge {
        SeedEdge {
            relationship: relationship.into(),
            target_entity: target_entity.into(),
            target_id,
        }
    }

    fn row(id: i64, edges: Vec<SeedEdge>) -> SeedRow {
        SeedRow {
            id,
            key: Vec::new(),
            properties: HashMap::new(),
            edges,
        }
    }

    #[test]
    fn catalog_reads_the_search_block_with_fields_inbound_and_defaults() {
        let catalog = messages_catalog();
        let spec = catalog.get("SlackMessage").expect("searchable");
        assert_eq!(spec.package, "example.messages");
        assert_eq!(spec.operation, "messages.search");
        assert_eq!(spec.fields["text"], SearchMatch::Contains);
        assert_eq!(spec.fields["sent_at"], SearchMatch::Range);
        assert_eq!(
            spec.inbound_mapping("CITES"),
            Some(&BTreeMap::from([(
                "text".to_string(),
                "$cited_by".to_string()
            )]))
        );
        assert_eq!(spec.max_rows, 20);
        assert_eq!(catalog.cited_by("Incident"), Some("reference"));
        assert_eq!(catalog.cited_by("GitLabRef"), Some("url"));
        assert_eq!(catalog.cited_by("SlackMessage"), None);
    }

    #[test]
    fn search_defaults_apply_when_the_block_omits_limits() {
        let yaml = MESSAGES
            .replace("        max_rows: 20\n", "")
            .replace("        timeout_ms: 5000\n", "");
        let package = Package::parse(&yaml, "d".into()).expect("parses without limits");
        let catalog = SearchCatalog::from_packages(&[package]);
        let spec = catalog.get("SlackMessage").expect("searchable");
        assert_eq!(spec.max_rows, DEFAULT_SEARCH_MAX_ROWS);
        assert_eq!(spec.timeout_ms, DEFAULT_SEARCH_TIMEOUT_MS);
    }

    #[test]
    fn contains_and_equals_predicates_push_down_under_their_field_name() {
        let spec = slack_spec();
        let predicates = vec![
            predicate("text", PredicateOp::Contains, "deploy"),
            predicate("author_name", PredicateOp::Equals, "alice"),
        ];
        let pushdown = plan_pushdown(&spec, &predicates);
        assert_eq!(pushdown.filters["text"], "deploy");
        assert_eq!(pushdown.filters["author_name"], "alice");
        assert_eq!(pushdown.consumed, vec![0, 1]);
    }

    #[test]
    fn a_range_predicate_pair_pushes_down_as_from_and_to_bounds() {
        let spec = slack_spec();
        let predicates = vec![
            predicate("sent_at", PredicateOp::Gte, "2026-01-01T00:00:00Z"),
            predicate("sent_at", PredicateOp::Lt, "2026-02-01T00:00:00Z"),
        ];
        let pushdown = plan_pushdown(&spec, &predicates);
        assert_eq!(pushdown.filters["sent_at_from"], "2026-01-01T00:00:00Z");
        assert_eq!(pushdown.filters["sent_at_to"], "2026-02-01T00:00:00Z");
        assert_eq!(pushdown.consumed, vec![0, 1]);
    }

    #[test]
    fn a_predicate_whose_operator_mismatches_the_declared_match_is_left_in_sql() {
        let spec = slack_spec();
        // text declares `contains`, so an equality on it is not a push-down.
        let predicates = vec![
            predicate("text", PredicateOp::Equals, "deploy"),
            predicate("unknown", PredicateOp::Contains, "x"),
        ];
        let pushdown = plan_pushdown(&spec, &predicates);
        assert!(pushdown.filters.is_empty());
        assert!(pushdown.consumed.is_empty());
    }

    #[test]
    fn target_ids_follow_one_relationship_and_target_in_first_seen_order() {
        let rows = vec![
            row(
                1,
                vec![
                    edge("CITES", "Incident", 10),
                    edge("CITES", "GitLabRef", 99),
                ],
            ),
            row(
                2,
                vec![edge("CITES", "Incident", 20), edge("CITES", "Incident", 10)],
            ),
        ];
        assert_eq!(target_ids(&rows, "CITES", "Incident"), vec![10, 20]);
        assert_eq!(target_ids(&rows, "CITES", "GitLabRef"), vec![99]);
    }

    #[test]
    fn inbound_filters_substitute_the_cited_by_value_and_keep_extra() {
        let mapping = BTreeMap::from([("text".to_string(), "$cited_by".to_string())]);
        let extra = BTreeMap::from([("channel_name".to_string(), "incidents".to_string())]);
        let filters = inbound_filters(&mapping, "INC-15808", &extra);
        assert_eq!(filters["text"], "INC-15808");
        assert_eq!(filters["channel_name"], "incidents");
    }
}
