//! Parameter sampling from ClickHouse.
//!
//! Samples valid IDs and values from the database to use as query parameters.

use anyhow::Result;
use clickhouse::{Client, Row};
use ontology::Ontology;
use serde::Deserialize;
use std::collections::HashMap;

/// Samples valid parameter values from the database.
pub struct ParameterSampler {
    client: Client,
    /// Cached samples: entity_type -> list of sampled IDs
    cache: HashMap<String, Vec<i64>>,
    sample_size: usize,
}

#[derive(Debug, Row, Deserialize)]
struct IdRow {
    id: i64,
}

impl ParameterSampler {
    pub fn new(clickhouse_url: &str, sample_size: usize) -> Self {
        let client = Client::default().with_url(clickhouse_url);
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
            "SELECT id FROM {} ORDER BY cityHash64(rand()) LIMIT {}",
            table_name, self.sample_size
        );

        let ids: Vec<i64> = self
            .client
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
        use fake::rand::Rng;
        let mut rng = fake::rand::thread_rng();
        let idx = rng.gen_range(0..ids.len());
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

        use fake::rand::seq::SliceRandom;
        let mut rng = fake::rand::thread_rng();

        let count = count.min(ids.len());
        Ok(ids.choose_multiple(&mut rng, count).copied().collect())
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
            .query(&query)
            .fetch_all::<StringRow>()
            .await
            .unwrap_or_default()
            .into_iter()
            .map(|r| r.value)
            .collect();

        Ok(values)
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
        let sampler = ParameterSampler::new("http://localhost:8123", 100);
        assert_eq!(sampler.sample_size, 100);
        assert!(sampler.cache.is_empty());
    }
}
