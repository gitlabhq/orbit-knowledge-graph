//! Generic, invertible JSON operations behind assistant specs: a marker-owned array merge
//! (hook entries) and a string registration (plugin lists). Both preserve everything they do
//! not own, and removal prunes containers they emptied.

use anyhow::{Result, anyhow};
use serde_json::{Value, json};

pub(super) fn contains_marker(value: &Value, marker: &str) -> bool {
    match value {
        Value::String(s) => s.contains(marker),
        Value::Array(items) => items.iter().any(|item| contains_marker(item, marker)),
        Value::Object(map) => map.values().any(|item| contains_marker(item, marker)),
        _ => false,
    }
}

pub(super) fn merge_owned(
    root: &mut Value,
    path: &[String],
    marker: &str,
    entries: &[Value],
) -> Result<()> {
    let target = ensure_array_at(root, path)?;
    target.retain(|entry| !contains_marker(entry, marker));
    target.extend(entries.iter().cloned());
    Ok(())
}

pub(super) fn remove_owned(root: &mut Value, path: &[String], marker: &str) -> bool {
    retain_and_prune(root, path, &|entry| !contains_marker(entry, marker))
}

pub(super) fn register(root: &mut Value, path: &[String], value: &str) -> Result<bool> {
    let target = ensure_array_at(root, path)?;
    if target.iter().any(|entry| entry.as_str() == Some(value)) {
        return Ok(false);
    }
    target.push(json!(value));
    Ok(true)
}

pub(super) fn deregister(root: &mut Value, path: &[String], value: &str) -> bool {
    retain_and_prune(root, path, &|entry| entry.as_str() != Some(value))
}

fn ensure_array_at<'a>(root: &'a mut Value, path: &[String]) -> Result<&'a mut Vec<Value>> {
    let (last, parents) = path.split_last().expect("spec paths are non-empty");
    let mut current = root;
    for (depth, segment) in parents.iter().enumerate() {
        if !current.is_object() {
            return Err(type_conflict(path, depth, "an object", current));
        }
        current = current
            .as_object_mut()
            .expect("checked above")
            .entry(segment.clone())
            .or_insert_with(|| json!({}));
    }
    if !current.is_object() {
        return Err(type_conflict(path, parents.len(), "an object", current));
    }
    let target = current
        .as_object_mut()
        .expect("checked above")
        .entry(last.clone())
        .or_insert_with(|| json!([]));
    if !target.is_array() {
        return Err(type_conflict(path, path.len(), "an array", target));
    }
    Ok(target.as_array_mut().expect("checked above"))
}

fn type_conflict(path: &[String], depth: usize, expected: &str, found: &Value) -> anyhow::Error {
    let location = match path[..depth].join(".") {
        location if location.is_empty() => "the file root".to_string(),
        location => format!("\"{location}\""),
    };
    anyhow!(
        "expected {expected} at {location}, found {}; fix or remove it and re-run",
        type_name(found)
    )
}

fn type_name(value: &Value) -> &'static str {
    match value {
        Value::Null => "null",
        Value::Bool(_) => "a boolean",
        Value::Number(_) => "a number",
        Value::String(_) => "a string",
        Value::Array(_) => "an array",
        Value::Object(_) => "an object",
    }
}

fn retain_and_prune(value: &mut Value, path: &[String], keep: &dyn Fn(&Value) -> bool) -> bool {
    let Some((first, rest)) = path.split_first() else {
        return false;
    };
    let Some(map) = value.as_object_mut() else {
        return false;
    };
    let Some(child) = map.get_mut(first) else {
        return false;
    };

    let changed = if rest.is_empty() {
        match child.as_array_mut() {
            Some(entries) => {
                let before = entries.len();
                entries.retain(keep);
                entries.len() != before
            }
            None => false,
        }
    } else {
        retain_and_prune(child, rest, keep)
    };

    let now_empty = match map.get(first) {
        Some(Value::Array(entries)) => entries.is_empty(),
        Some(Value::Object(children)) => children.is_empty(),
        _ => false,
    };
    if changed && now_empty {
        map.remove(first);
    }
    changed
}

#[cfg(test)]
mod tests {
    use super::*;

    fn hook_path() -> Vec<String> {
        vec!["hooks".into(), "PreToolUse".into()]
    }

    fn orbit_entry() -> Value {
        json!({"matcher": "Read", "hooks": [{"type": "command", "command": "orbit hook-guard read"}]})
    }

    fn foreign_entry() -> Value {
        json!({"matcher": "Bash", "hooks": [{"type": "command", "command": "other guard"}]})
    }

    #[test]
    fn merge_is_idempotent_and_preserves_foreign_entries() {
        let mut root = json!({
            "permissions": {"allow": ["Bash"]},
            "hooks": {"PreToolUse": [foreign_entry()]}
        });
        merge_owned(
            &mut root,
            &hook_path(),
            "orbit hook-guard",
            &[orbit_entry()],
        )
        .unwrap();
        merge_owned(
            &mut root,
            &hook_path(),
            "orbit hook-guard",
            &[orbit_entry()],
        )
        .unwrap();

        assert_eq!(root["permissions"]["allow"][0], "Bash");
        let entries = root["hooks"]["PreToolUse"].as_array().unwrap();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0], foreign_entry());
    }

    #[test]
    fn merge_creates_missing_structure() {
        let mut root = json!({});
        merge_owned(
            &mut root,
            &hook_path(),
            "orbit hook-guard",
            &[orbit_entry()],
        )
        .unwrap();
        assert_eq!(root["hooks"]["PreToolUse"].as_array().unwrap().len(), 1);
    }

    #[test]
    fn remove_owned_prunes_emptied_containers() {
        let mut root = json!({});
        merge_owned(
            &mut root,
            &hook_path(),
            "orbit hook-guard",
            &[orbit_entry()],
        )
        .unwrap();
        assert!(remove_owned(&mut root, &hook_path(), "orbit hook-guard"));
        assert_eq!(root, json!({}));
    }

    #[test]
    fn remove_owned_keeps_foreign_entries_and_containers() {
        let mut root = json!({"hooks": {"PreToolUse": [foreign_entry(), orbit_entry()]}});
        assert!(remove_owned(&mut root, &hook_path(), "orbit hook-guard"));
        assert_eq!(root["hooks"]["PreToolUse"], json!([foreign_entry()]));

        assert!(!remove_owned(&mut root, &hook_path(), "orbit hook-guard"));
    }

    #[test]
    fn register_and_deregister_roundtrip() {
        let plugin_path = vec!["plugin".to_string()];
        let mut root = json!({"theme": "dark"});

        assert!(register(&mut root, &plugin_path, "orbit.js").unwrap());
        assert!(!register(&mut root, &plugin_path, "orbit.js").unwrap());
        assert_eq!(root["plugin"], json!(["orbit.js"]));

        assert!(deregister(&mut root, &plugin_path, "orbit.js"));
        assert_eq!(root, json!({"theme": "dark"}));
    }

    #[test]
    fn deregister_keeps_other_plugins() {
        let plugin_path = vec!["plugin".to_string()];
        let mut root = json!({"plugin": ["other.js", "orbit.js"]});
        assert!(deregister(&mut root, &plugin_path, "orbit.js"));
        assert_eq!(root["plugin"], json!(["other.js"]));
    }

    #[test]
    fn conflicting_types_error_and_leave_the_value_untouched() {
        let mut string_container = json!({"hooks": "a-string"});
        let err = merge_owned(
            &mut string_container,
            &hook_path(),
            "orbit hook-guard",
            &[orbit_entry()],
        )
        .unwrap_err();
        assert!(err.to_string().contains("\"hooks\""), "{err}");
        assert_eq!(string_container, json!({"hooks": "a-string"}));

        let mut object_where_array_expected = json!({"hooks": {"PreToolUse": {"foo": 1}}});
        let err = merge_owned(
            &mut object_where_array_expected,
            &hook_path(),
            "orbit hook-guard",
            &[orbit_entry()],
        )
        .unwrap_err();
        assert!(err.to_string().contains("an array"), "{err}");
        assert_eq!(
            object_where_array_expected,
            json!({"hooks": {"PreToolUse": {"foo": 1}}})
        );

        let mut plugin_string = json!({"plugin": "other.js"});
        assert!(register(&mut plugin_string, &["plugin".to_string()], "orbit.js").is_err());
        assert_eq!(plugin_string, json!({"plugin": "other.js"}));
    }

    #[test]
    fn missing_path_is_a_no_op() {
        let mut root = json!({"unrelated": true});
        assert!(!remove_owned(&mut root, &hook_path(), "orbit hook-guard"));
        assert_eq!(root, json!({"unrelated": true}));
    }
}
