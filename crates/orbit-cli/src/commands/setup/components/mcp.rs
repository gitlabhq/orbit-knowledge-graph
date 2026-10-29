use std::path::Path;

use anyhow::Result;
use serde_json::Value;
use toml_edit::{DocumentMut, Item};

use super::json;
use super::{Report, drop_backup_when_restored, remove_file_and_empty_parents, write_file};
use crate::commands::setup::Target;
use crate::commands::setup::spec::{Agent, DIRECT_LAUNCHER, McpFormat};

const SERVER_NAME: &str = "orbit";
const RETIRED_SUBCOMMAND: [&str; 2] = ["mcp", "serve"];

pub(super) fn retired_label(agent: Agent, target: &Target) -> Result<Option<String>> {
    let Some(entry) = &agent.retired.mcp else {
        return Ok(None);
    };
    let (path, label) = target.resolve(&entry.file)?;
    let holds_server = match entry.format {
        McpFormat::Codex => toml_without_server(&path).is_some(),
        McpFormat::Claude | McpFormat::Opencode => {
            json_without_server(&path, entry.format).is_some()
        }
    };
    Ok(holds_server.then_some(label))
}

pub(super) fn remove_retired(agents: &[Agent], target: &Target, report: &mut Report) -> Result<()> {
    for entry in agents.iter().filter_map(|agent| agent.retired.mcp.as_ref()) {
        let (path, label) = target.resolve(&entry.file)?;
        match entry.format {
            McpFormat::Codex => {
                if let Some(document) = toml_without_server(&path) {
                    write_toml_or_delete_when_empty(&path, &document, target, &label, report)?;
                }
            }
            McpFormat::Claude | McpFormat::Opencode => {
                if let Some(root) = json_without_server(&path, entry.format) {
                    json::write_or_delete_when_empty(&path, &root, target, &label, report)?;
                }
            }
        }
    }
    Ok(())
}

fn servers_key(format: McpFormat) -> &'static str {
    match format {
        McpFormat::Claude => "mcpServers",
        McpFormat::Opencode => "mcp",
        McpFormat::Codex => "mcp_servers",
    }
}

fn launches_retired_server(words: &[&str]) -> bool {
    words.contains(&DIRECT_LAUNCHER) && words.ends_with(&RETIRED_SUBCOMMAND)
}

fn json_without_server(path: &Path, format: McpFormat) -> Option<Value> {
    let key = servers_key(format);
    let mut root = json::read_object(path).ok()?;
    let map = root.as_object_mut()?;
    let servers = map.get_mut(key)?.as_object_mut()?;
    let entry = servers.get(SERVER_NAME)?;
    let words: Vec<&str> = ["command", "args"]
        .into_iter()
        .filter_map(|field| entry.get(field))
        .flat_map(|value| match value {
            Value::Array(items) => items.iter().filter_map(Value::as_str).collect(),
            other => other.as_str().into_iter().collect::<Vec<_>>(),
        })
        .collect();
    if !launches_retired_server(&words) {
        return None;
    }
    servers.remove(SERVER_NAME);
    if servers.is_empty() {
        map.remove(key);
    }
    Some(root)
}

fn toml_without_server(path: &Path) -> Option<DocumentMut> {
    let key = servers_key(McpFormat::Codex);
    let mut document: DocumentMut = std::fs::read_to_string(path).ok()?.parse().ok()?;
    let servers = document.get_mut(key).and_then(Item::as_table_mut)?;
    let entry = servers.get(SERVER_NAME)?;
    let args = entry.get("args").and_then(Item::as_array);
    let words: Vec<&str> = entry
        .get("command")
        .and_then(Item::as_str)
        .into_iter()
        .chain(
            args.into_iter()
                .flat_map(|args| args.iter().filter_map(|arg| arg.as_str())),
        )
        .collect();
    if !launches_retired_server(&words) {
        return None;
    }
    servers.remove(SERVER_NAME);
    if servers.is_empty() {
        document.remove(key);
    }
    Some(document)
}

fn write_toml_or_delete_when_empty(
    path: &Path,
    document: &DocumentMut,
    target: &Target,
    label: &str,
    report: &mut Report,
) -> Result<()> {
    let text = document.to_string();
    if text.trim().is_empty() {
        remove_file_and_empty_parents(path, target)?;
        report.note(label, "removed (was orbit-only)");
    } else {
        write_file(path, text)?;
        report.note(label, "orbit entries removed");
        drop_backup_when_restored(path, label, report)?;
    }
    Ok(())
}
