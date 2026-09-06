use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;

use arrow::array::{BooleanArray, Int64Array, StringArray};
use clickhouse_client::ArrowClickHouseClient;
use futures::StreamExt;
use ontology::{Ontology, TokenBoundary};
use orbit_utils::arrow::ArrowUtils;
use orbit_utils::traversal_path::TraversalPath;
use query_engine::compiler::{Input, SecurityContext, TokenScope};
use query_engine::types::ResourceCheck;
use tokio::sync::mpsc;
use tonic::{Status, Streaming};
use tracing::info;

use crate::auth::Claims;
use crate::pipeline::correlation;
use crate::proto::ExecuteQueryMessage;
use crate::redaction::RedactionService;

const BATCH_SIZE: usize = 1000;

pub fn query_entities(input: &Input, ontology: &Ontology) -> HashSet<String> {
    use query_engine::compiler::input::Direction;
    let entity_for = |alias: &str| {
        input
            .nodes
            .iter()
            .find(|node| node.id == alias)
            .and_then(|node| node.entity.as_deref())
    };
    let mut entities: HashSet<String> = input
        .nodes
        .iter()
        .filter_map(|n| n.entity.clone())
        .collect();
    if let Some(neighbors) = &input.neighbors {
        entities.extend(reachable_entities(
            ontology,
            entities.clone(),
            &neighbors.rel_types,
            neighbors.direction,
            1,
        ));
    }
    if let Some(path) = &input.path {
        entities.extend(reachable_entities(
            ontology,
            entities.clone(),
            &path.rel_types,
            Direction::Both,
            path.max_depth,
        ));
    }
    for relationship in &input.relationships {
        if relationship.hops.max == 1
            && entity_for(&relationship.from).is_some()
            && entity_for(&relationship.to).is_some()
        {
            continue;
        }
        let (seed, direction) = match (entity_for(&relationship.from), entity_for(&relationship.to))
        {
            (Some(entity), _) => (HashSet::from([entity.to_string()]), relationship.direction),
            (_, Some(entity)) => (
                HashSet::from([entity.to_string()]),
                match relationship.direction {
                    Direction::Outgoing => Direction::Incoming,
                    Direction::Incoming => Direction::Outgoing,
                    Direction::Both => Direction::Both,
                },
            ),
            _ => (
                ontology
                    .edges()
                    .filter(|edge| {
                        relationship.types.is_empty()
                            || relationship.types.contains(&edge.relationship_kind)
                    })
                    .flat_map(|edge| [edge.source_kind.clone(), edge.target_kind.clone()])
                    .collect(),
                relationship.direction,
            ),
        };
        entities.extend(reachable_entities(
            ontology,
            seed,
            &relationship.types,
            direction,
            relationship.hops.max,
        ));
    }
    entities
}

fn reachable_entities(
    ontology: &Ontology,
    mut entities: HashSet<String>,
    relationships: &[String],
    direction: query_engine::compiler::input::Direction,
    depth: u32,
) -> HashSet<String> {
    use query_engine::compiler::input::Direction;
    let mut frontier = entities.clone();
    for _ in 0..depth {
        let mut next = HashSet::new();
        for edge in ontology.edges().filter(|edge| {
            relationships.is_empty() || relationships.contains(&edge.relationship_kind)
        }) {
            if direction != Direction::Incoming && frontier.contains(&edge.source_kind) {
                next.insert(edge.target_kind.clone());
            }
            if direction != Direction::Outgoing && frontier.contains(&edge.target_kind) {
                next.insert(edge.source_kind.clone());
            }
        }
        next.retain(|entity| !entities.contains(entity));
        if next.is_empty() {
            break;
        }
        entities.extend(next.iter().cloned());
        frontier = next;
    }
    entities
}

pub fn narrow_query_scope(input: &Input, ontology: &Ontology, security: &mut SecurityContext) {
    if input.path.is_some()
        || input.neighbors.is_some()
        || input.relationships.iter().any(|r| r.hops.max > 1)
        || input.nodes.iter().any(|node| node.entity.is_none())
    {
        return;
    }
    let prefixes = input
        .nodes
        .iter()
        .filter(|node| {
            node.entity
                .as_deref()
                .and_then(|entity| ontology.get_node(entity))
                .is_some_and(|node| node.has_traversal_path)
        })
        .map(|node| security.scope_prefixes.get(&node.id).cloned())
        .collect::<Option<Vec<_>>>();
    let Some(prefixes) = prefixes.filter(|p| !p.is_empty()) else {
        return;
    };
    narrow_to_prefixes(security, &prefixes);
}

pub fn narrow_to_prefixes(security: &mut SecurityContext, prefixes: &[TraversalPath]) {
    security.traversal_paths = security
        .traversal_paths
        .iter()
        .flat_map(|authorized| {
            prefixes.iter().filter_map(move |prefix| {
                let path = if prefix.is_descendant_of(&authorized.path) {
                    prefix
                } else if authorized.path.is_descendant_of(prefix) {
                    &authorized.path
                } else {
                    return None;
                };
                Some(query_engine::compiler::AuthorizedPath::with_access_levels(
                    path.clone(),
                    authorized.access_levels.clone(),
                ))
            })
        })
        .collect();
}

pub async fn authorize(
    claims: &Claims,
    security: &mut SecurityContext,
    ontology: &Ontology,
    client: &Arc<ArrowClickHouseClient>,
    entities: &HashSet<String>,
    tx: &mpsc::Sender<Result<ExecuteQueryMessage, Status>>,
    stream: &mut Streaming<ExecuteQueryMessage>,
) -> Result<HashSet<TraversalPath>, Status> {
    if !claims.token_authorization_required {
        return Ok(HashSet::new());
    }
    let mut project_paths = HashSet::new();
    let mut scoped: BTreeMap<String, Vec<String>> = BTreeMap::new();
    let mut scopes = HashMap::new();
    for entity in entities {
        let Some(node) = ontology.get_node(entity) else {
            continue;
        };
        let Some(auth) = &node.redaction else {
            scopes.insert(entity.clone(), TokenScope::Denied);
            continue;
        };
        match auth.token_boundary {
            TokenBoundary::Namespace if node.has_traversal_path => {
                scoped
                    .entry(auth.permission().to_string())
                    .or_default()
                    .push(entity.clone());
            }
            TokenBoundary::User if node.global => {
                let check = ResourceCheck {
                    resource_type: auth.resource_type.clone(),
                    ability: auth.permission().to_string(),
                    permission: auth.permission().to_string(),
                    ids: vec![claims.user_id as i64],
                };
                let allowed = check_ids(check, tx, stream).await?;
                scopes.insert(
                    entity.clone(),
                    if allowed.contains(&(claims.user_id as i64)) {
                        TokenScope::All
                    } else {
                        TokenScope::Denied
                    },
                );
            }
            TokenBoundary::Resource if node.global => {
                let sql = format!(
                    "SELECT id FROM {} FINAL WHERE _deleted = false",
                    node.destination_table
                );
                let mut batches = client
                    .query(&sql)
                    .with_setting("query_id", correlation::query_id("token-resources"))
                    .with_setting(
                        "log_comment",
                        correlation::log_comment(Some("token:resources")),
                    )
                    .fetch_arrow_streamed(Some(BATCH_SIZE as u64))
                    .await
                    .map_err(database_error)?;
                let mut allowed = Vec::new();
                while let Some(batch) = batches.next().await {
                    let batch = batch.map_err(database_error)?;
                    let ids = ArrowUtils::get_column_by_name::<Int64Array>(&batch, "id")
                        .ok_or_else(|| Status::internal("token resource query returned no IDs"))?;
                    let ids: Vec<i64> = ids.iter().flatten().collect();
                    for chunk in ids.chunks(BATCH_SIZE) {
                        let check = ResourceCheck {
                            resource_type: auth.resource_type.clone(),
                            ability: auth.permission().to_string(),
                            permission: auth.permission().to_string(),
                            ids: chunk.to_vec(),
                        };
                        allowed.extend(check_ids(check, tx, stream).await?);
                    }
                }
                scopes.insert(entity.clone(), TokenScope::Resources(Arc::new(allowed)));
            }
            _ => {
                scopes.insert(entity.clone(), TokenScope::Denied);
            }
        }
    }
    if !scoped.is_empty() {
        let tables: Vec<&str> = ["Group", "Project"]
            .into_iter()
            .filter_map(|name| ontology.get_node(name))
            .map(|n| n.destination_table.as_str())
            .collect();
        if tables.len() != 2 {
            return Err(Status::internal(
                "token authorization requires namespace catalogs",
            ));
        }
        let sql = catalog_sql(&tables);
        let paths: Vec<&str> = security
            .traversal_paths
            .iter()
            .map(|p| p.path.as_str())
            .collect();
        let mut batches = client
            .query(&sql)
            .param("paths", paths)
            .with_setting("query_id", correlation::query_id("token-namespaces"))
            .with_setting(
                "log_comment",
                correlation::log_comment(Some("token:namespaces")),
            )
            .fetch_arrow_streamed(Some(BATCH_SIZE as u64))
            .await
            .map_err(database_error)?;
        let mut accepted: HashMap<String, Vec<TraversalPath>> = HashMap::new();
        let mut candidate_count = 0;
        let mut callback_count = 0;
        while let Some(batch) = batches.next().await {
            let batch = batch.map_err(database_error)?;
            let paths = ArrowUtils::get_column_by_name::<StringArray>(&batch, "traversal_path")
                .ok_or_else(|| Status::internal("token catalog query returned no paths"))?;
            let is_project = ArrowUtils::get_column_by_name::<BooleanArray>(&batch, "is_project")
                .ok_or_else(|| {
                Status::internal("token catalog query returned no boundary types")
            })?;
            let candidates: Vec<(i64, TraversalPath)> = paths
                .iter()
                .enumerate()
                .filter_map(|(row, value)| {
                    let candidate = namespace_path(value?)?;
                    if is_project.value(row) {
                        project_paths.insert(candidate.1.clone());
                    }
                    Some(candidate)
                })
                .collect();
            candidate_count += candidates.len();
            for chunk in candidates.chunks(BATCH_SIZE) {
                let ids: Vec<i64> = chunk
                    .iter()
                    .map(|(id, _)| *id)
                    .collect::<HashSet<_>>()
                    .into_iter()
                    .collect();
                let checks: Vec<ResourceCheck> = scoped
                    .keys()
                    .map(|permission| ResourceCheck {
                        resource_type: "namespace".into(),
                        ability: permission.clone(),
                        permission: permission.clone(),
                        ids: ids.clone(),
                    })
                    .collect();
                let output =
                    RedactionService::request_authorization(&checks, true, true, tx, stream)
                        .await
                        .map_err(|error| error.into_status())?;
                callback_count += 1;
                for permission in scoped.keys() {
                    let decision = output
                        .authorizations
                        .iter()
                        .find(|a| a.resource_type == "namespace" && a.ability == *permission);
                    accepted.entry(permission.clone()).or_default().extend(
                        chunk
                            .iter()
                            .filter(|(id, _)| {
                                decision.is_some_and(|a| a.authorized.get(id) == Some(&true))
                            })
                            .map(|(_, path)| path.clone()),
                    );
                }
            }
        }
        info!(
            candidate_count,
            callback_count,
            permission_count = scoped.len(),
            "Token namespace authorization completed"
        );
        for (permission, entities) in scoped {
            let paths = Arc::new(accepted.remove(&permission).unwrap_or_default());
            for entity in entities {
                scopes.insert(entity, TokenScope::Namespaces(paths.clone()));
            }
        }
    }
    security.token_scopes = Some(scopes);
    Ok(project_paths)
}

fn catalog_sql(tables: &[&str]) -> String {
    let arms = tables.iter().enumerate().map(|(index, table)| format!(
        "SELECT traversal_path, {} AS is_project FROM {table} FINAL WHERE _deleted = false AND arrayExists(p -> startsWith(traversal_path, p), {{paths:Array(String)}})", if index == 1 {"true"} else {"false"}
    )).collect::<Vec<_>>().join(" UNION ALL ");
    format!("SELECT DISTINCT traversal_path, is_project FROM ({arms})")
}

fn namespace_path(value: &str) -> Option<(i64, TraversalPath)> {
    let path = TraversalPath::new_unchecked(value);
    path.validate().ok()?;
    let mut segments = value.trim_end_matches('/').split('/');
    segments.next()?;
    let id = segments.next_back()?.parse::<i64>().ok()?;
    (id > 0).then_some((id, path))
}

async fn check_ids(
    check: ResourceCheck,
    tx: &mpsc::Sender<Result<ExecuteQueryMessage, Status>>,
    stream: &mut Streaming<ExecuteQueryMessage>,
) -> Result<HashSet<i64>, Status> {
    let output = RedactionService::request_authorization(
        std::slice::from_ref(&check),
        true,
        true,
        tx,
        stream,
    )
    .await
    .map_err(|error| error.into_status())?;
    let allowed = output
        .authorizations
        .iter()
        .find(|a| a.resource_type == check.resource_type && a.ability == check.ability);
    Ok(check
        .ids
        .into_iter()
        .filter(|id| allowed.is_some_and(|a| a.authorized.get(id) == Some(&true)))
        .collect())
}

fn database_error(error: clickhouse_client::ClickHouseError) -> Status {
    Status::internal(format!("token boundary query failed: {error}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn group_neighbors_exclude_unrelated_code_components() {
        let ontology = Ontology::load_embedded().unwrap();
        let input = query_engine::compiler::validate_normalize(r#"{"query_type":"neighbors","nodes":[{"id":"g","entity":"Group","node_ids":[1]}],"neighbors":{"direction":"outgoing","rel_types":["CONTAINS"]}}"#, &ontology).unwrap();
        let entities = query_entities(&input, &ontology);
        assert_eq!(entities, HashSet::from(["Group".into(), "Project".into()]));
    }

    #[test]
    fn namespace_leaf_uses_full_validated_path() {
        assert_eq!(namespace_path("1/23/456/").unwrap().0, 456);
        for path in ["1/", "1/0/", "1/3 OR 1/", "1/-2/", "1//2/"] {
            assert!(namespace_path(path).is_none(), "{path}");
        }
    }

    #[test]
    fn catalog_reads_only_live_scoped_boundaries() {
        let sql = catalog_sql(&["gl_group", "gl_project"]);
        assert_eq!(sql.matches(" FINAL ").count(), 2);
        assert_eq!(sql.matches("_deleted = false").count(), 2);
        assert_eq!(sql.matches("{paths:Array(String)}").count(), 2);
        assert!(sql.contains("SELECT DISTINCT traversal_path, is_project"));
    }

    #[test]
    fn narrowing_retains_roles_and_all_cross_namespace_inputs() {
        use query_engine::compiler::AuthorizedPath;
        let mut security = SecurityContext::new_with_roles(
            1,
            vec![
                AuthorizedPath::new("1/", 20),
                AuthorizedPath::new("1/10/11/", 30),
            ],
        )
        .unwrap();
        narrow_to_prefixes(&mut security, &["1/10/".into(), "1/20/".into()]);
        assert_eq!(
            security.paths_at_least(30),
            vec![&TraversalPath::from("1/10/11/")]
        );
        assert!(
            security
                .paths_at_least(20)
                .contains(&&TraversalPath::from("1/20/"))
        );
    }
}
