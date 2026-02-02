//! Extract redaction data from query results using ontology configuration.

use std::collections::{HashMap, HashSet};

use ontology::Ontology;

use super::ResourceCheck;
use super::query_result::{QueryResult, RedactableNodes};

pub struct RedactionExtractor<'a> {
    ontology: &'a Ontology,
}

impl<'a> RedactionExtractor<'a> {
    pub fn new(ontology: &'a Ontology) -> Self {
        Self { ontology }
    }

    pub fn extract(&self, result: &QueryResult) -> (RedactableNodes, Vec<ResourceCheck>) {
        let nodes = result.extract_redactable_nodes();
        let resource_checks = self.build_resource_checks(result);
        (nodes, resource_checks)
    }

    pub fn entity_to_resource_map(&self) -> HashMap<&str, &str> {
        let mut map = HashMap::new();
        for node in self.ontology.nodes() {
            if let Some(config) = &node.redaction {
                map.insert(node.name.as_str(), config.resource_type.as_str());
            }
        }
        map
    }

    /// Returns a mapping from entity type to its id_column for authorization lookups.
    /// Entities with id_column != "id" need special handling during authorization.
    pub fn entity_to_id_column_map(&self) -> HashMap<&str, &str> {
        let mut map = HashMap::new();
        for node in self.ontology.nodes() {
            if let Some(config) = &node.redaction {
                map.insert(node.name.as_str(), config.id_column.as_str());
            }
        }
        map
    }

    /// Build resource checks using the correct id_column from each entity's redaction config.
    ///
    /// For entities where id_column == "id" (like Project, User), we use the node's ID.
    /// For entities where id_column != "id" (like Definition with project_id), we extract
    /// the value from that column instead.
    fn build_resource_checks(&self, result: &QueryResult) -> Vec<ResourceCheck> {
        // Collect resource IDs grouped by (resource_type, ability)
        let mut resource_ids: HashMap<(&str, &str), HashSet<i64>> = HashMap::new();

        for row in result.iter() {
            // Check each node alias in the result
            for alias in result.node_aliases() {
                let Some(node_ref) = row.node_ref(alias) else {
                    continue;
                };

                let Some(config) = self.ontology.get_redaction_config(&node_ref.entity_type) else {
                    continue;
                };

                let auth_id = if config.id_column == "id" {
                    node_ref.id
                } else {
                    let column_name = format!("{}_{}", alias, config.id_column);
                    row.get(&column_name)
                        .and_then(|v| v.as_i64())
                        .or_else(|| row.get(&config.id_column).and_then(|v| v.as_i64()))
                        .unwrap_or(node_ref.id)
                };

                resource_ids
                    .entry((config.resource_type.as_str(), config.ability.as_str()))
                    .or_default()
                    .insert(auth_id);
            }

            // Also check dynamic nodes (path finding, neighbors)
            for node_ref in row.dynamic_nodes() {
                let Some(config) = self.ontology.get_redaction_config(&node_ref.entity_type) else {
                    continue;
                };

                // For dynamic nodes, we only have the node ID available
                // If id_column != "id", we cannot extract the correct value
                // This is a limitation - dynamic nodes with indirect authorization
                // will use their own ID, which may fail authorization
                resource_ids
                    .entry((config.resource_type.as_str(), config.ability.as_str()))
                    .or_default()
                    .insert(node_ref.id);
            }
        }

        resource_ids
            .into_iter()
            .map(|((resource_type, ability), ids)| ResourceCheck {
                resource_type: resource_type.to_string(),
                ids: ids.into_iter().collect(),
                ability: ability.to_string(),
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Array, Int64Array, StringArray};
    use arrow::datatypes::{Field, Schema};
    use arrow::record_batch::RecordBatch;
    use query_engine::ResultContext;
    use std::sync::Arc;

    fn make_batch(columns: Vec<(&str, Arc<dyn Array>)>) -> RecordBatch {
        let fields: Vec<Field> = columns
            .iter()
            .map(|(name, arr)| Field::new(*name, arr.data_type().clone(), true))
            .collect();
        let schema = Arc::new(Schema::new(fields));
        let arrays: Vec<Arc<dyn Array>> = columns.into_iter().map(|(_, arr)| arr).collect();
        RecordBatch::try_new(schema, arrays).unwrap()
    }

    #[test]
    fn extracts_nodes_from_result() {
        let ontology = Ontology::load_embedded().unwrap();
        let extractor = RedactionExtractor::new(&ontology);

        let mut ctx = ResultContext::new();
        ctx.add_node("p", "Project");

        let batch = make_batch(vec![
            ("_gkg_p_id", Arc::new(Int64Array::from(vec![1, 2, 3]))),
            (
                "_gkg_p_type",
                Arc::new(StringArray::from(vec!["Project", "Project", "Project"])),
            ),
        ]);
        let result = QueryResult::from_batches(&[batch], &ctx);

        let (nodes, checks) = extractor.extract(&result);

        assert_eq!(nodes.len(), 3);
        assert_eq!(checks.len(), 1);
        assert_eq!(checks[0].resource_type, "projects");
    }

    #[test]
    fn extracts_multiple_entity_types() {
        let ontology = Ontology::load_embedded().unwrap();
        let extractor = RedactionExtractor::new(&ontology);

        let mut ctx = ResultContext::new();
        ctx.add_node("u", "User");
        ctx.add_node("p", "Project");

        let batch = make_batch(vec![
            ("_gkg_u_id", Arc::new(Int64Array::from(vec![10, 20]))),
            (
                "_gkg_u_type",
                Arc::new(StringArray::from(vec!["User", "User"])),
            ),
            ("_gkg_p_id", Arc::new(Int64Array::from(vec![100, 200]))),
            (
                "_gkg_p_type",
                Arc::new(StringArray::from(vec!["Project", "Project"])),
            ),
        ]);
        let result = QueryResult::from_batches(&[batch], &ctx);

        let (nodes, checks) = extractor.extract(&result);

        assert_eq!(nodes.len(), 4);
        let check_types: Vec<&str> = checks.iter().map(|c| c.resource_type.as_str()).collect();
        assert!(check_types.contains(&"users"));
        assert!(check_types.contains(&"projects"));
    }

    #[test]
    fn entity_to_resource_map_contains_configured_entities() {
        let ontology = Ontology::load_embedded().unwrap();
        let extractor = RedactionExtractor::new(&ontology);
        let map = extractor.entity_to_resource_map();

        assert_eq!(map.get("Project"), Some(&"projects"));
        assert_eq!(map.get("User"), Some(&"users"));
        assert_eq!(map.get("Group"), Some(&"groups"));
    }

    #[test]
    fn skips_entities_without_redaction_config() {
        let ontology = Ontology::new().with_nodes(["TestNode"]);
        let extractor = RedactionExtractor::new(&ontology);

        let mut ctx = ResultContext::new();
        ctx.add_node("t", "TestNode");

        let batch = make_batch(vec![
            ("_gkg_t_id", Arc::new(Int64Array::from(vec![1, 2]))),
            (
                "_gkg_t_type",
                Arc::new(StringArray::from(vec!["TestNode", "TestNode"])),
            ),
        ]);
        let result = QueryResult::from_batches(&[batch], &ctx);

        let (nodes, checks) = extractor.extract(&result);

        assert_eq!(nodes.len(), 2);
        assert!(checks.is_empty());
    }
}
