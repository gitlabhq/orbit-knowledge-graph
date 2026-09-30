use crate::error::Result;
use crate::input::Input;
use crate::passes::{cursor, validate};
use ontology::Ontology;

pub fn parse(json: &str, ontology: &Ontology) -> Result<(Input, u64)> {
    let value: serde_json::Value = serde_json::from_str(json)?;
    validate::collect_schema_errors(validate::base_validator(), &value)?;
    let schema = ontology
        .derive_json_schema(validate::BASE_SCHEMA_JSON)
        .map_err(|error| {
            crate::error::QueryError::Validation(format!("failed to derive schema: {error}"))
        })?;
    let validator = jsonschema::validator_for(&schema).map_err(|error| {
        crate::error::QueryError::Validation(format!("invalid derived schema: {error}"))
    })?;
    validate::collect_schema_errors(&validator, &value).map_err(|error| match error {
        crate::error::QueryError::Validation(message) => {
            crate::error::QueryError::AllowlistRejected(message)
        }
        other => other,
    })?;
    let query_hash = cursor::canonical_hash(&value);
    Ok((serde_json::from_value(value)?, query_hash))
}
