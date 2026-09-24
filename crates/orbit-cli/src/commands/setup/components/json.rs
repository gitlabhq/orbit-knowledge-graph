use std::path::Path;

use anyhow::{Context, Result, anyhow, bail};
use jsonc_parser::cst::{CstArray, CstInputValue, CstObject, CstRootNode};
use jsonc_parser::{CollectOptions, CommentCollectionStrategy, ParseOptions};
use serde_json::{Map, Value, json};

use super::{Report, drop_backup_when_restored, remove_file_and_empty_parents, write_file};
use crate::commands::setup::Target;
use crate::commands::setup::spec::{Agent, McpFormat};

pub(super) fn read_agent_object(path: &Path, agent: Agent) -> Result<Value> {
    read_object(
        path,
        matches!(
            agent.mcp.as_ref().map(|entry| entry.format),
            Some(McpFormat::Opencode)
        ),
    )
}

pub(super) fn read_mcp_object(path: &Path, format: McpFormat) -> Result<Value> {
    read_object(path, matches!(format, McpFormat::Opencode))
}

fn read_object(path: &Path, jsonc: bool) -> Result<Value> {
    match std::fs::read_to_string(path) {
        Ok(raw) => {
            let value: Value = if jsonc {
                jsonc_parser::parse_to_serde_value(&raw, &Default::default())
                    .map_err(anyhow::Error::from)
            } else {
                serde_json::from_str(&raw).map_err(anyhow::Error::from)
            }
            .with_context(|| {
                format!(
                    "{} is not valid JSON; fix or remove it and re-run",
                    path.display()
                )
            })?;
            if !value.is_object() {
                bail!("{} is not a JSON object", path.display());
            }
            Ok(value)
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(json!({})),
        Err(e) => Err(e).with_context(|| format!("failed to read {}", path.display())),
    }
}

pub(super) fn render(path: &Path, value: &Value) -> Result<String> {
    let Some(raw) = std::fs::read_to_string(path).ok() else {
        return render_fresh(value);
    };
    let Ok(root) = CstRootNode::parse(&raw, &ParseOptions::default()) else {
        return render_fresh(value);
    };
    let (Some(object), Some(desired)) = (root.object_value(), value.as_object()) else {
        return render_fresh(value);
    };
    sync_object(&object, desired);
    let patched = root.to_string();
    let reparsed: Value = jsonc_parser::parse_to_serde_value(&patched, &Default::default())
        .with_context(|| format!("failed to update {} in place", path.display()))?;
    if reparsed != *value {
        bail!("failed to update {} in place", path.display());
    }
    Ok(patched)
}

fn render_fresh(value: &Value) -> Result<String> {
    let raw = serde_json::to_string_pretty(value).context("failed to serialize JSON")?;
    Ok(raw + "\n")
}

fn sync_object(object: &CstObject, desired: &Map<String, Value>) {
    let mut present = Vec::new();
    for prop in object.properties() {
        let Some(name) = prop.name().and_then(|name| name.decoded_value().ok()) else {
            continue;
        };
        let Some(wanted) = desired.get(&name) else {
            prop.remove();
            continue;
        };
        present.push(name);
        let current = prop.value().and_then(|node| node.to_serde_value());
        if current.as_ref() == Some(wanted) {
            continue;
        }
        match (prop.object_value(), prop.array_value(), wanted) {
            (Some(child), _, Value::Object(map)) => sync_object(&child, map),
            (_, Some(child), Value::Array(items)) => sync_array(&child, items),
            _ => prop.set_value(cst_input(wanted)),
        }
    }
    for (name, wanted) in desired {
        if !present.contains(name) {
            object.append(name, cst_input(wanted));
        }
    }
}

fn sync_array(array: &CstArray, desired: &[Value]) {
    let mut kept = 0;
    for element in array.elements() {
        if desired.get(kept) == element.to_serde_value().as_ref() {
            kept += 1;
        } else {
            element.remove();
        }
    }
    for wanted in &desired[kept..] {
        array.append(cst_input(wanted));
    }
}

fn cst_input(value: &Value) -> CstInputValue {
    match value {
        Value::Null => CstInputValue::Null,
        Value::Bool(flag) => CstInputValue::Bool(*flag),
        Value::Number(number) => CstInputValue::Number(number.to_string()),
        Value::String(text) => CstInputValue::String(text.clone()),
        Value::Array(items) => CstInputValue::Array(items.iter().map(cst_input).collect()),
        Value::Object(map) => CstInputValue::Object(
            map.iter()
                .map(|(name, item)| (name.clone(), cst_input(item)))
                .collect(),
        ),
    }
}

fn has_comments(path: &Path) -> bool {
    let Ok(raw) = std::fs::read_to_string(path) else {
        return false;
    };
    let options = CollectOptions {
        comments: CommentCollectionStrategy::Separate,
        tokens: false,
    };
    jsonc_parser::parse_to_ast(&raw, &options, &ParseOptions::default())
        .ok()
        .and_then(|parsed| parsed.comments)
        .is_some_and(|comments| !comments.is_empty())
}

pub(super) fn write_object(path: &Path, value: &Value) -> Result<()> {
    write_file(path, render(path, value)?)
}

pub(super) fn write_or_delete_when_empty(
    path: &Path,
    root: &Value,
    target: &Target,
    label: &str,
    report: &mut Report,
) -> Result<()> {
    if root.as_object().is_some_and(|map| map.is_empty()) && !has_comments(path) {
        remove_file_and_empty_parents(path, target)?;
        report.note(label, "removed (was orbit-only)");
    } else {
        write_object(path, root)?;
        report.note(label, "orbit entries removed");
        drop_backup_when_restored(path, label, report)?;
    }
    Ok(())
}

pub(in crate::commands::setup) fn contains_marker(value: &Value, marker: &str) -> bool {
    match value {
        Value::String(s) => s.contains(marker),
        Value::Array(items) => items.iter().any(|item| contains_marker(item, marker)),
        Value::Object(map) => map.values().any(|item| contains_marker(item, marker)),
        _ => false,
    }
}

pub(super) fn replace_marked_entries(
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

pub(super) fn remove_marked_entries(root: &mut Value, path: &[String], marker: &str) -> bool {
    retain_and_prune(root, path, &|entry| !contains_marker(entry, marker))
}

pub(super) fn append_unique(root: &mut Value, path: &[String], value: &str) -> Result<bool> {
    let target = ensure_array_at(root, path)?;
    if target.iter().any(|entry| entry.as_str() == Some(value)) {
        return Ok(false);
    }
    target.push(json!(value));
    Ok(true)
}

pub(super) fn remove_value(root: &mut Value, path: &[String], value: &str) -> bool {
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
