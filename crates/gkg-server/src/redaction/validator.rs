//! Arrow schema validation against ResultContext.

use arrow::datatypes::{DataType, Schema};
use ontology::Ontology;
use query_engine::ResultContext;

#[derive(Debug)]
pub struct SchemaValidationError {
    pub missing_columns: Vec<String>,
    pub type_mismatches: Vec<TypeMismatch>,
    pub unknown_entities: Vec<String>,
}

#[derive(Debug)]
pub struct TypeMismatch {
    pub column: String,
    pub expected: DataType,
    pub actual: DataType,
}

impl std::fmt::Display for SchemaValidationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "schema validation failed: missing={:?}, mismatches={:?}, unknown={:?}",
            self.missing_columns,
            self.type_mismatches
                .iter()
                .map(|t| &t.column)
                .collect::<Vec<_>>(),
            self.unknown_entities
        )
    }
}

impl std::error::Error for SchemaValidationError {}

pub struct SchemaValidator<'a> {
    ontology: &'a Ontology,
}

impl<'a> SchemaValidator<'a> {
    pub fn new(ontology: &'a Ontology) -> Self {
        Self { ontology }
    }

    pub fn validate(
        &self,
        arrow_schema: &Schema,
        result_context: &ResultContext,
    ) -> Result<(), SchemaValidationError> {
        let mut missing_columns = Vec::new();
        let mut type_mismatches = Vec::new();
        let mut unknown_entities = Vec::new();

        for node in result_context.nodes() {
            match arrow_schema.column_with_name(&node.id_column) {
                Some((_, field)) => {
                    if !matches!(field.data_type(), DataType::Int64) {
                        type_mismatches.push(TypeMismatch {
                            column: node.id_column.clone(),
                            expected: DataType::Int64,
                            actual: field.data_type().clone(),
                        });
                    }
                }
                None => missing_columns.push(node.id_column.clone()),
            }

            match arrow_schema.column_with_name(&node.type_column) {
                Some((_, field)) => {
                    if !matches!(field.data_type(), DataType::Utf8) {
                        type_mismatches.push(TypeMismatch {
                            column: node.type_column.clone(),
                            expected: DataType::Utf8,
                            actual: field.data_type().clone(),
                        });
                    }
                }
                None => missing_columns.push(node.type_column.clone()),
            }

            if !self.ontology.requires_redaction(&node.entity_type) {
                unknown_entities.push(node.entity_type.clone());
            }
        }

        if missing_columns.is_empty() && type_mismatches.is_empty() && unknown_entities.is_empty() {
            Ok(())
        } else {
            Err(SchemaValidationError {
                missing_columns,
                type_mismatches,
                unknown_entities,
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::Field;

    fn test_ontology() -> Ontology {
        Ontology::load_embedded().unwrap()
    }

    fn make_schema(fields: Vec<(&str, DataType)>) -> Schema {
        Schema::new(
            fields
                .into_iter()
                .map(|(name, dtype)| Field::new(name, dtype, true))
                .collect::<Vec<_>>(),
        )
    }

    #[test]
    fn valid_schema_passes() {
        let ontology = test_ontology();
        let validator = SchemaValidator::new(&ontology);

        let mut ctx = ResultContext::new();
        ctx.add_node("u", "User");
        ctx.add_node("p", "Project");

        let schema = make_schema(vec![
            ("_gkg_u_id", DataType::Int64),
            ("_gkg_u_type", DataType::Utf8),
            ("_gkg_p_id", DataType::Int64),
            ("_gkg_p_type", DataType::Utf8),
        ]);

        assert!(validator.validate(&schema, &ctx).is_ok());
    }

    #[test]
    fn missing_column_fails() {
        let ontology = test_ontology();
        let validator = SchemaValidator::new(&ontology);

        let mut ctx = ResultContext::new();
        ctx.add_node("u", "User");

        let schema = make_schema(vec![("_gkg_u_id", DataType::Int64)]);

        let err = validator.validate(&schema, &ctx).unwrap_err();
        assert!(err.missing_columns.contains(&"_gkg_u_type".to_string()));
    }

    #[test]
    fn wrong_type_fails() {
        let ontology = test_ontology();
        let validator = SchemaValidator::new(&ontology);

        let mut ctx = ResultContext::new();
        ctx.add_node("u", "User");

        let schema = make_schema(vec![
            ("_gkg_u_id", DataType::Utf8),
            ("_gkg_u_type", DataType::Utf8),
        ]);

        let err = validator.validate(&schema, &ctx).unwrap_err();
        assert_eq!(err.type_mismatches.len(), 1);
        assert_eq!(err.type_mismatches[0].column, "_gkg_u_id");
    }

    #[test]
    fn unknown_entity_fails() {
        let ontology = Ontology::new().with_nodes(["TestNode"]);
        let validator = SchemaValidator::new(&ontology);

        let mut ctx = ResultContext::new();
        ctx.add_node("t", "TestNode");

        let schema = make_schema(vec![
            ("_gkg_t_id", DataType::Int64),
            ("_gkg_t_type", DataType::Utf8),
        ]);

        let err = validator.validate(&schema, &ctx).unwrap_err();
        assert!(err.unknown_entities.contains(&"TestNode".to_string()));
    }

    #[test]
    fn empty_context_passes() {
        let ontology = test_ontology();
        let validator = SchemaValidator::new(&ontology);
        let ctx = ResultContext::new();
        let schema = make_schema(vec![]);

        assert!(validator.validate(&schema, &ctx).is_ok());
    }
}
