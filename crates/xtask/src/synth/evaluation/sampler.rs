//! Parameter sampling from ClickHouse.
//!
//! Samples valid IDs and values from the database to use as query parameters.

use anyhow::Result;
use clickhouse::Row;
use clickhouse_client::ArrowClickHouseClient;
use ontology::Ontology;
use ontology::constants::{DEFAULT_PRIMARY_KEY, TRAVERSAL_PATH_COLUMN};
use rand::RngExt;
use rand::seq::IndexedRandom;
use serde::Deserialize;
use std::collections::HashMap;

/// Samples valid parameter values from the database.
pub struct ParameterSampler {
    client: ArrowClickHouseClient,
    /// Cached samples: entity_type -> list of sampled IDs
    cache: HashMap<String, Vec<i64>>,
    sample_size: usize,
}

#[derive(Debug, Row, Deserialize)]
struct IdRow {
    id: i64,
}

#[derive(Debug, Row, Deserialize)]
struct TraversalRow {
    traversal_path: String,
}

impl ParameterSampler {
    pub fn new(client: ArrowClickHouseClient, sample_size: usize) -> Self {
        Self {
            client,
            cache: HashMap::new(),
            sample_size,
        }
    }

    /// Sample IDs for a given entity type.
    pub async fn sample_ids(&mut self, entity: &str, ontology: &Ontology) -> Result<Vec<i64>> {
        // Return cached values if available
        if let Some(ids) = self.cache.get(entity) {
            return Ok(ids.clone());
        }

        let table_name = ontology.table_name(entity)?;

        // Use SAMPLE or ORDER BY RAND() with LIMIT for random sampling
        // ClickHouse's cityHash64(rand()) is efficient for random ordering
        let query = format!(
            "SELECT {pk} FROM {} ORDER BY cityHash64(rand()) LIMIT {}",
            table_name,
            self.sample_size,
            pk = DEFAULT_PRIMARY_KEY
        );

        let ids: Vec<i64> = self
            .client
            .inner()
            .query(&query)
            .fetch_all::<IdRow>()
            .await?
            .into_iter()
            .map(|r| r.id)
            .collect();

        self.cache.insert(entity.to_string(), ids.clone());
        Ok(ids)
    }

    /// Get a random ID for an entity type.
    pub async fn random_id(&mut self, entity: &str, ontology: &Ontology) -> Result<Option<i64>> {
        let ids = self.sample_ids(entity, ontology).await?;
        if ids.is_empty() {
            return Ok(None);
        }
        let mut rng = rand::rng();
        let idx = rng.random_range(0..ids.len());
        Ok(Some(ids[idx]))
    }

    /// Get multiple random IDs for an entity type.
    pub async fn random_ids(
        &mut self,
        entity: &str,
        count: usize,
        ontology: &Ontology,
    ) -> Result<Vec<i64>> {
        let ids = self.sample_ids(entity, ontology).await?;
        if ids.is_empty() {
            return Ok(vec![]);
        }

        let mut rng = rand::rng();

        let count = count.min(ids.len());
        Ok(ids.sample(&mut rng, count).copied().collect())
    }

    /// Get multiple random IDs for an entity type within a specific traversal path.
    ///
    /// This ensures sampled IDs exist within the security context's scope,
    /// preventing empty results from path mismatches.
    pub async fn random_ids_in_path(
        &self,
        entity: &str,
        count: usize,
        traversal_path: &str,
        ontology: &Ontology,
    ) -> Result<Vec<i64>> {
        let table_name = ontology.table_name(entity)?;

        // Sample IDs that exist within the given traversal path
        let escaped = traversal_path.replace('\'', "''");
        let query = format!(
            "SELECT {pk} FROM {} WHERE startsWith({tp}, '{}') \
             ORDER BY cityHash64(rand()) LIMIT {}",
            table_name,
            escaped,
            count,
            pk = DEFAULT_PRIMARY_KEY,
            tp = TRAVERSAL_PATH_COLUMN
        );

        let ids: Vec<i64> = self
            .client
            .inner()
            .query(&query)
            .fetch_all::<IdRow>()
            .await?
            .into_iter()
            .map(|r| r.id)
            .collect();

        Ok(ids)
    }

    /// Get multiple random IDs for an entity type within a specific organization.
    ///
    /// This is used as a fallback when path-scoped sampling returns no results,
    /// ensuring we still sample from the correct organization.
    pub async fn random_ids_in_org(
        &self,
        entity: &str,
        count: usize,
        org_id: i64,
        ontology: &Ontology,
    ) -> Result<Vec<i64>> {
        let table_name = ontology.table_name(entity)?;

        // Sample IDs where traversal_path starts with "org_id/"
        let query = format!(
            "SELECT {pk} FROM {} WHERE startsWith({tp}, '{}/') \
             ORDER BY cityHash64(rand()) LIMIT {}",
            table_name,
            org_id,
            count,
            pk = DEFAULT_PRIMARY_KEY,
            tp = TRAVERSAL_PATH_COLUMN
        );

        let ids: Vec<i64> = self
            .client
            .inner()
            .query(&query)
            .fetch_all::<IdRow>()
            .await?
            .into_iter()
            .map(|r| r.id)
            .collect();

        Ok(ids)
    }

    /// Sample valid values for string enum fields (e.g., state, status).
    pub async fn sample_enum_values(
        &mut self,
        entity: &str,
        field: &str,
        ontology: &Ontology,
    ) -> Result<Vec<String>> {
        let table_name = ontology.table_name(entity)?;

        let query = format!("SELECT DISTINCT {} FROM {} LIMIT 100", field, table_name);

        #[derive(Debug, Row, Deserialize)]
        struct StringRow {
            #[serde(rename = "0")]
            value: String,
        }

        let values: Vec<String> = self
            .client
            .inner()
            .query(&query)
            .fetch_all::<StringRow>()
            .await
            .unwrap_or_default()
            .into_iter()
            .map(|r| r.value)
            .collect();

        Ok(values)
    }

    /// Sample traversal paths from the namespace entity table.
    /// Returns (org_id, traversal_path) pairs.
    /// The first segment of traversal_path is the org_id.
    pub async fn sample_traversal_paths(
        &self,
        namespace_entity: &str,
        ontology: &Ontology,
    ) -> Result<Vec<(i64, String)>> {
        let table_name = ontology.table_name(namespace_entity)?;
        let query = format!(
            "SELECT {tp} FROM {} \
             WHERE {tp} != '' \
             ORDER BY cityHash64(rand()) LIMIT {}",
            table_name,
            self.sample_size,
            tp = TRAVERSAL_PATH_COLUMN
        );

        let rows: Vec<(i64, String)> = self
            .client
            .inner()
            .query(&query)
            .fetch_all::<TraversalRow>()
            .await?
            .into_iter()
            .filter_map(|r| {
                // First segment is org_id
                let org_id: i64 = gkg_utils::traversal_path::org_id(&r.traversal_path)?;
                // Append trailing slash for SecurityContext format
                let path = if r.traversal_path.ends_with('/') {
                    r.traversal_path
                } else {
                    format!("{}/", r.traversal_path)
                };
                Some((org_id, path))
            })
            .collect();

        Ok(rows)
    }

    /// Pre-warm the cache for all entity types in the ontology.
    pub async fn warm_cache(&mut self, ontology: &Ontology) -> Result<()> {
        for node in ontology.nodes() {
            if let Err(e) = self.sample_ids(&node.name, ontology).await {
                tracing::warn!("Failed to sample IDs for {}: {}", node.name, e);
            }
        }
        Ok(())
    }

    /// Get cache statistics.
    pub fn cache_stats(&self) -> HashMap<String, usize> {
        self.cache
            .iter()
            .map(|(k, v)| (k.clone(), v.len()))
            .collect()
    }

    /// Clear the cache.
    pub fn clear_cache(&mut self) {
        self.cache.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sampler_creation() {
        let client = ArrowClickHouseClient::new(
            "http://localhost:8123",
            "default",
            "default",
            None,
            &std::collections::HashMap::new(),
        );
        let sampler = ParameterSampler::new(client, 100);
        assert_eq!(sampler.sample_size, 100);
        assert!(sampler.cache.is_empty());
    }
}
