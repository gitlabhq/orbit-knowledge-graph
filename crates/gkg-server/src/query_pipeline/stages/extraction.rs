use std::collections::HashMap;
use std::sync::Arc;

use ontology::Ontology;

use crate::redaction::RedactionExtractor;

use super::super::types::{ExecutionOutput, ExtractionOutput, RedactionPlan};

pub struct ExtractionStage {
    ontology: Arc<Ontology>,
}

impl ExtractionStage {
    pub fn new(ontology: Arc<Ontology>) -> Self {
        Self { ontology }
    }

    pub fn execute(&self, input: ExecutionOutput) -> ExtractionOutput {
        let query_result =
            crate::redaction::QueryResult::from_batches(&input.batches, &input.result_context);
        let extractor = RedactionExtractor::new(&self.ontology);

        let entity_to_resource_map: HashMap<String, String> = extractor
            .entity_to_resource_map()
            .into_iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        let (_, resources_to_check) = extractor.extract(&query_result);

        let redaction_plan = RedactionPlan {
            resources_to_check,
            entity_to_resource_map,
        };

        ExtractionOutput {
            query_result,
            result_context: input.result_context,
            redaction_plan,
            generated_sql: input.generated_sql,
        }
    }
}
