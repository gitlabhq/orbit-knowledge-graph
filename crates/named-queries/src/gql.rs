use std::sync::{Arc, Mutex};

use minijinja::{Environment, Error, ErrorKind, UndefinedBehavior};
use serde_json::Value;

use crate::query::{Lookup, Slot};

type Encoder = fn(&Value) -> Result<String, String>;

const FUNCTIONS: [(&str, Slot, Encoder); 4] = [
    ("param", Slot::Param, encode_literal),
    ("identifier", Slot::Param, encode_identifier),
    ("integer", Slot::Param, encode_integer),
    ("binding", Slot::Binding, encode_literal),
];

pub(crate) fn render(template: &str, lookup: Lookup) -> Result<(String, Lookup), String> {
    let mut environment = Environment::new();
    environment.set_undefined_behavior(UndefinedBehavior::Strict);
    let shared = Arc::new(Mutex::new(lookup));
    for (function, slot, encode) in FUNCTIONS {
        let shared = Arc::clone(&shared);
        environment.add_function(function, move |name: String| -> Result<String, Error> {
            let value = shared
                .lock()
                .expect("lookup lock poisoned")
                .resolve(slot, &name)
                .map_err(template_error)?;
            encode(&value).map_err(template_error)
        });
    }
    let rendered = environment
        .template_from_str(template)
        .and_then(|template| template.render(()))
        .map_err(|error| error.to_string());
    drop(environment);
    let rendered = rendered?;
    let lookup = Arc::try_unwrap(shared)
        .expect("template functions are dropped with the environment")
        .into_inner()
        .expect("lookup lock poisoned");
    Ok((rendered, lookup))
}

fn template_error(message: String) -> Error {
    Error::new(ErrorKind::InvalidOperation, message)
}

fn encode_identifier(value: &Value) -> Result<String, String> {
    let name = value
        .as_str()
        .ok_or_else(|| "identifier must be a string".to_string())?;
    let mut chars = name.chars();
    if !chars
        .next()
        .is_some_and(|c| c.is_ascii_alphabetic() || c == '_')
        || !chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
    {
        return Err("identifier must match [A-Za-z_][A-Za-z0-9_]*".to_string());
    }
    Ok(format!("`{name}`"))
}

fn encode_integer(value: &Value) -> Result<String, String> {
    let integer = match value {
        Value::String(text) if text.bytes().all(|c| c.is_ascii_digit()) => text.parse::<i64>().ok(),
        _ => value.as_i64(),
    }
    .filter(|integer| *integer >= 0)
    .ok_or_else(|| "integer must be a non-negative decimal integer within Int64".to_string())?;
    Ok(integer.to_string())
}

fn encode_literal(value: &Value) -> Result<String, String> {
    match value {
        Value::String(_) | Value::Bool(_) => Ok(value.to_string()),
        Value::Number(number) if number.is_i64() => Ok(value.to_string()),
        Value::Array(items) => {
            let values = items
                .iter()
                .map(encode_literal)
                .collect::<Result<Vec<_>, _>>()?;
            Ok(format!("[{}]", values.join(",")))
        }
        _ => Err("expected a string, Int64, boolean, or array literal".to_string()),
    }
}
