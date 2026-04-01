use crate::analysis::types::{ConsolidatedRelationship, DefinitionNode, DefinitionType, FqnType};
use crate::graph::RelationshipType;
use crate::parsing::processor::FileProcessingResult;
use internment::ArcIntern;
use parser_core::utils::Range;
use std::collections::HashMap;

pub fn process_definitions(
    file_result: &FileProcessingResult,
    relative_file_path: &str,
    definition_map: &mut HashMap<(String, String), (DefinitionNode, FqnType)>,
    relationships: &mut Vec<ConsolidatedRelationship>,
) {
    if let Some(defs) = file_result.definitions.iter_dsl() {
        for definition in defs {
            let fqn = FqnType::Dsl(definition.fqn.clone());
            let path = ArcIntern::new(relative_file_path.to_string());
            let definition_node = DefinitionNode::new(
                fqn.clone(),
                DefinitionType::Dsl(definition.definition_type.clone()),
                definition.range,
                path.clone(),
            );

            let key = (fqn.to_string(), relative_file_path.to_string());

            if definition_map.contains_key(&key) {
                log::warn!(
                    "Duplicate definition found for DSL language: {} in file {}",
                    definition.name,
                    relative_file_path
                );
                continue;
            }

            definition_map.insert(key, (definition_node, fqn));

            let mut relationship =
                ConsolidatedRelationship::file_to_definition(path.clone(), path.clone());
            relationship.relationship_type = RelationshipType::FileDefines;
            relationship.source_range = ArcIntern::new(Range::empty());
            relationship.target_range = ArcIntern::new(definition.range);
            relationships.push(relationship);
        }
    }
}

pub fn add_definition_relationships(
    definition_map: &HashMap<(String, String), (DefinitionNode, FqnType)>,
    relationships: &mut Vec<ConsolidatedRelationship>,
) {
    let dsl_definitions: Vec<_> = definition_map
        .values()
        .filter_map(|(node, fqn_type)| {
            if let FqnType::Dsl(fqn) = fqn_type {
                Some((node, fqn))
            } else {
                None
            }
        })
        .collect();

    for (node, fqn) in &dsl_definitions {
        if fqn.len() <= 1 {
            continue;
        }

        let parent_parts = fqn.parts[..fqn.len() - 1].to_vec();
        let parent_fqn_string = parent_parts.join(".");

        if let Some((parent_node, _)) = dsl_definitions
            .iter()
            .find(|(def_node, _)| def_node.fqn.to_string() == parent_fqn_string)
        {
            let mut relationship = ConsolidatedRelationship::definition_to_definition(
                parent_node.file_path.clone(),
                node.file_path.clone(),
            );
            relationship.relationship_type = RelationshipType::ClassToMethod;
            relationship.source_range = ArcIntern::new(parent_node.range);
            relationship.target_range = ArcIntern::new(node.range);
            relationships.push(relationship);
        }
    }
}
