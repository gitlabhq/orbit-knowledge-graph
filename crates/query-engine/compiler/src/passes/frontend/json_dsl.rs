use crate::error::Result;
use crate::input::Input;
use crate::passes::{cursor, validate};
use ontology::Ontology;

pub fn parse(json: &str, ontology: &Ontology) -> Result<Input> {
    let validator = validate::Validator::new(ontology);
    let value = validator.check_json(json)?;
    validator.check_ontology(&value)?;
    let query_hash = cursor::canonical_hash(&value);
    let mut input: Input = serde_json::from_value(value)?;
    input.compiler.query_hash = query_hash;
    Ok(input)
}
