use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use arrow::array::{Array, Int64Array, StringArray};
use clickhouse_client::ArrowClickHouseClient;
use gkg_server_config::PathResolverConfig;
use gkg_utils::arrow::ArrowUtils;
use moka::future::Cache;
use ontology::{Ontology, TraversalPathKind, TraversalPathLookup};
use query_engine::compiler::{PathResolutionKey, PathScopeId, is_valid_traversal_path};
use tracing::{debug, warn};

const DICT_DEFAULT: &str = "0/";

pub struct PathResolver {
    client: Arc<ArrowClickHouseClient>,
    lookups: Vec<TraversalPathLookup>,
    dict_available: bool,
    cache: Cache<PathResolutionKey, Option<String>>,
}

impl PathResolver {
    pub async fn new(
        client: Arc<ArrowClickHouseClient>,
        ontology: &Ontology,
        cfg: &PathResolverConfig,
    ) -> Self {
        let lookups = ontology.traversal_path_lookups().to_vec();
        let dicts: Vec<&str> = lookups
            .iter()
            .filter_map(|l| l.dictionary.as_deref())
            .collect();

        let dict_available = probe_dict_available(&client, &dicts).await;
        if !dict_available {
            warn!(
                ?dicts,
                "traversal-path dictionaries unavailable; using argMax fallback"
            );
        }

        let cache = Cache::builder()
            .max_capacity(cfg.cache_capacity)
            .time_to_live(Duration::from_secs(cfg.cache_ttl_secs))
            .build();

        Self {
            client,
            lookups,
            dict_available,
            cache,
        }
    }

    pub async fn resolve_batch(
        &self,
        keys: &[PathResolutionKey],
    ) -> HashMap<PathResolutionKey, Option<String>> {
        let mut resolved = HashMap::new();
        let mut groups: HashMap<(String, TraversalPathKind), Vec<PathResolutionKey>> =
            HashMap::new();

        for key in keys {
            if resolved.contains_key(key) {
                continue;
            }
            if let Some(cached) = self.cache.get(key).await {
                resolved.insert(key.clone(), cached);
                continue;
            }
            groups
                .entry((key.entity.clone(), key.kind))
                .or_default()
                .push(key.clone());
        }

        for ((entity, kind), group) in groups {
            let Some(spec) = self.lookup_spec(&entity, kind) else {
                continue;
            };
            let found = match self.resolve_group(spec, &group).await {
                Ok(found) => found,
                Err(e) => {
                    warn!(?entity, ?kind, error = %e, "batch path resolution failed");
                    HashMap::new()
                }
            };
            for key in group {
                let value = normalize_path(found.get(&key.value).cloned());
                self.cache.insert(key.clone(), value.clone()).await;
                resolved.insert(key, value);
            }
        }

        resolved
    }

    fn lookup_spec(&self, entity: &str, kind: TraversalPathKind) -> Option<&TraversalPathLookup> {
        self.lookups
            .iter()
            .find(|l| l.entity == entity && l.kind == kind)
    }

    async fn resolve_group(
        &self,
        spec: &TraversalPathLookup,
        group: &[PathResolutionKey],
    ) -> Result<HashMap<PathScopeId, String>, clickhouse_client::ClickHouseError> {
        match (self.dict_available, &spec.dictionary) {
            (true, Some(dict)) => self.dict_group(dict, group).await,
            _ => self.argmax_group(spec, group).await,
        }
    }

    async fn dict_group(
        &self,
        dict: &str,
        group: &[PathResolutionKey],
    ) -> Result<HashMap<PathScopeId, String>, clickhouse_client::ClickHouseError> {
        let ids: Vec<i64> = group
            .iter()
            .filter_map(|k| match k.value {
                PathScopeId::Numeric(id) if id > 0 => Some(id),
                _ => None,
            })
            .collect();
        if ids.is_empty() {
            return Ok(HashMap::new());
        }
        let sql = format!(
            "SELECT arrayJoin({{ids:Array(Int64)}}) AS k, \
             dictGetOrDefault('{dict}', 'traversal_path', toInt64(k), '{DICT_DEFAULT}') AS p"
        );
        let batches = self
            .client
            .query(&sql)
            .param("ids", ids)
            .fetch_arrow()
            .await?;
        Ok(key_paths::<Int64Array>(&batches, |k, i| {
            PathScopeId::Numeric(k.value(i))
        }))
    }

    async fn argmax_group(
        &self,
        spec: &TraversalPathLookup,
        group: &[PathResolutionKey],
    ) -> Result<HashMap<PathScopeId, String>, clickhouse_client::ClickHouseError> {
        let table = &spec.source_table;
        let key_col = &spec.key_column;
        let base = format!(
            "SELECT {key_col} AS k, argMax(traversal_path, _version) AS p FROM {table} \
             WHERE {key_col} IN "
        );
        let tail = format!(" GROUP BY {key_col} HAVING argMax(_deleted, _version) = false");

        match spec.kind {
            TraversalPathKind::Id => {
                let ids: Vec<i64> = group
                    .iter()
                    .filter_map(|k| match k.value {
                        PathScopeId::Numeric(id) if id > 0 => Some(id),
                        _ => None,
                    })
                    .collect();
                if ids.is_empty() {
                    return Ok(HashMap::new());
                }
                let sql = format!("{base}{{ids:Array(Int64)}}{tail}");
                let batches = self
                    .client
                    .query(&sql)
                    .param("ids", ids)
                    .fetch_arrow()
                    .await?;
                Ok(key_paths::<Int64Array>(&batches, |k, i| {
                    PathScopeId::Numeric(k.value(i))
                }))
            }
            TraversalPathKind::FullPath => {
                let vals: Vec<String> = group
                    .iter()
                    .filter_map(|k| match &k.value {
                        PathScopeId::Text(s) => Some(s.clone()),
                        _ => None,
                    })
                    .collect();
                if vals.is_empty() {
                    return Ok(HashMap::new());
                }
                let sql = format!("{base}{{vals:Array(String)}}{tail}");
                let batches = self
                    .client
                    .query(&sql)
                    .param("vals", vals)
                    .fetch_arrow()
                    .await?;
                Ok(key_paths::<StringArray>(&batches, |k, i| {
                    PathScopeId::Text(k.value(i).to_string())
                }))
            }
        }
    }
}

fn normalize_path(path: Option<String>) -> Option<String> {
    match path {
        Some(p) if p != DICT_DEFAULT && is_valid_traversal_path(&p) => Some(p),
        _ => None,
    }
}

fn key_paths<K: Array + 'static>(
    batches: &[arrow::record_batch::RecordBatch],
    key: impl Fn(&K, usize) -> PathScopeId,
) -> HashMap<PathScopeId, String> {
    let mut out = HashMap::new();
    for batch in batches {
        let (Some(keys), Some(paths)) = (
            ArrowUtils::get_column_by_name::<K>(batch, "k"),
            ArrowUtils::get_column_by_name::<StringArray>(batch, "p"),
        ) else {
            continue;
        };
        for i in 0..batch.num_rows() {
            if keys.is_null(i) || paths.is_null(i) {
                continue;
            }
            out.insert(key(keys, i), paths.value(i).to_string());
        }
    }
    out
}

fn first_string(batches: &[arrow::record_batch::RecordBatch]) -> Option<String> {
    let batch = batches.first()?;
    let col = ArrowUtils::get_column_by_name::<StringArray>(batch, "p")?;
    if batch.num_rows() == 0 || col.is_null(0) {
        return None;
    }
    Some(col.value(0).to_string())
}

async fn probe_dict_available(client: &ArrowClickHouseClient, dicts: &[&str]) -> bool {
    if dicts.is_empty() {
        return false;
    }
    let list = dicts
        .iter()
        .map(|d| format!("'{d}'"))
        .collect::<Vec<_>>()
        .join(", ");
    let sql = format!(
        "SELECT name AS p FROM system.dictionaries \
         WHERE database = currentDatabase() AND name IN ({list}) LIMIT 1"
    );
    match client.query(&sql).fetch_arrow().await {
        Ok(batches) => first_string(&batches).is_some(),
        Err(e) => {
            debug!(error = %e, "system.dictionaries probe failed; assuming unavailable");
            false
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalize_accepts_valid_and_rejects_malformed() {
        assert_eq!(
            normalize_path(Some("1/2/".to_string())),
            Some("1/2/".to_string())
        );
        assert_eq!(normalize_path(Some("0/".to_string())), None);
        assert_eq!(normalize_path(Some("1/22/../../foo".to_string())), None);
        assert_eq!(normalize_path(Some(String::new())), None);
        assert_eq!(normalize_path(None), None);
    }
}
