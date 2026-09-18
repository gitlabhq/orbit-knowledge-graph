use serde_json::{Map, Value};

use crate::query::{Lookup, Slot};

const BINDING_KEY: &str = "$binding";
const PARAM_KEY: &str = "$param";
const PARAM_KEY_PREFIX: &str = "$param:";

pub(crate) fn render(template: &Value, mut lookup: Lookup) -> Result<(String, Lookup), String> {
    let mut rendered = template.clone();
    substitute(&mut rendered, &mut lookup)?;
    Ok((rendered.to_string(), lookup))
}

fn substitute(value: &mut Value, lookup: &mut Lookup) -> Result<(), String> {
    match value {
        Value::Object(map) => {
            if let Some(slot) = placeholder_slot(map) {
                *value = resolve_placeholder(slot, map, lookup)?;
            } else {
                let mut rekeyed = Map::with_capacity(map.len());
                for (key, mut nested) in std::mem::take(map) {
                    substitute(&mut nested, lookup)?;
                    let key = resolve_key(&key, lookup)?;
                    if rekeyed.contains_key(&key) {
                        return Err(format!(
                            "parameter key `{key}` collides with another key in the same object"
                        ));
                    }
                    rekeyed.insert(key, nested);
                }
                *map = rekeyed;
            }
        }
        Value::Array(items) => {
            for item in items {
                substitute(item, lookup)?;
            }
        }
        _ => {}
    }
    Ok(())
}

fn placeholder_slot(map: &Map<String, Value>) -> Option<Slot> {
    if map.contains_key(BINDING_KEY) {
        Some(Slot::Binding)
    } else if map.contains_key(PARAM_KEY) {
        Some(Slot::Param)
    } else {
        None
    }
}

fn resolve_placeholder(
    slot: Slot,
    map: &Map<String, Value>,
    lookup: &mut Lookup,
) -> Result<Value, String> {
    let key = match slot {
        Slot::Binding => BINDING_KEY,
        Slot::Param => PARAM_KEY,
    };
    if map.len() != 1 {
        return Err(format!("a {key} object must have no other keys"));
    }
    let name = map[key]
        .as_str()
        .ok_or_else(|| format!("{key} value must be a string"))?;
    lookup.resolve(slot, name)
}

fn resolve_key(key: &str, lookup: &mut Lookup) -> Result<String, String> {
    let Some(name) = key.strip_prefix(PARAM_KEY_PREFIX) else {
        return Ok(key.to_string());
    };
    match lookup.resolve(Slot::Param, name)? {
        Value::String(s) if s.is_empty() || s.starts_with('$') => Err(format!(
            "parameter `{name}` is used as an object key so it must not be empty or start with `$`"
        )),
        Value::String(s) => Ok(s),
        other => Err(format!(
            "parameter `{name}` is used as an object key so it must be a string, got {other}"
        )),
    }
}
