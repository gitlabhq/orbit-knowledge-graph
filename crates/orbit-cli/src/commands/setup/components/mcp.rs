use std::path::Path;

use anyhow::{Context, Result, bail};
use serde_json::{Value, json};
use toml_edit::{Array, DocumentMut, Item, Table, value};

use super::json;
use super::{
    Installer, Report, backup_once, drop_backup_when_restored, remove_file_and_empty_parents,
    write_file,
};
use crate::commands::setup::Target;
use crate::commands::setup::spec::{self, Agent, DIRECT_LAUNCHER, McpFormat};

pub(super) struct McpServer;

impl Installer for McpServer {
    fn plan(&self, agent: Agent, target: &Target) -> Result<Vec<String>> {
        agent
            .mcp
            .iter()
            .map(|entry| target.resolve(&entry.file).map(|(_, label)| label))
            .collect()
    }

    fn install(&self, agents: &[Agent], target: &Target, report: &mut Report) -> Result<()> {
        let server = spec::mcp_server();
        for entry in agents.iter().filter_map(|agent| agent.mcp.as_ref()) {
            let (path, label) = target.resolve(&entry.file)?;
            match entry.format {
                McpFormat::Codex => install_toml(&path, &label, &server, report)?,
                McpFormat::Claude | McpFormat::Opencode => {
                    install_json(&path, &label, entry.format, &server, report)?
                }
            }
        }
        Ok(())
    }

    fn remove(&self, agents: &[Agent], target: &Target, report: &mut Report) -> Result<()> {
        let name = spec::mcp_server().name;
        for entry in agents.iter().filter_map(|agent| agent.mcp.as_ref()) {
            let (path, label) = target.resolve(&entry.file)?;
            if !path.exists() {
                continue;
            }
            match entry.format {
                McpFormat::Codex => remove_toml(&path, &label, name, target, report)?,
                McpFormat::Claude | McpFormat::Opencode => {
                    remove_json(&path, &label, entry.format, name, target, report)?
                }
            }
        }
        Ok(())
    }
}

fn servers_key(format: McpFormat) -> &'static str {
    match format {
        McpFormat::Claude => "mcpServers",
        McpFormat::Opencode => "mcp",
        McpFormat::Codex => "mcp_servers",
    }
}

fn server_json_entry(format: McpFormat, server: &spec::McpServer) -> Value {
    match format {
        McpFormat::Opencode => {
            let command: Vec<&str> = std::iter::once(server.command.as_str())
                .chain(server.args.iter().map(String::as_str))
                .collect();
            json!({"type": "local", "command": command, "enabled": true})
        }
        McpFormat::Claude | McpFormat::Codex => {
            json!({"type": "stdio", "command": server.command, "args": server.args})
        }
    }
}

fn install_json(
    path: &Path,
    label: &str,
    format: McpFormat,
    server: &spec::McpServer,
    report: &mut Report,
) -> Result<()> {
    let key = servers_key(format);
    let entry = server_json_entry(format, server);
    refuse_commented_sibling(path, key, server.name, &entry)?;

    let mut root = json::read_object(path)?;
    let container = root
        .as_object_mut()
        .expect("read_object returns objects")
        .entry(key)
        .or_insert_with(|| json!({}));
    let Some(servers) = container.as_object_mut() else {
        bail!(
            "expected an object at \"{key}\" in {}; fix or remove it and re-run",
            path.display()
        );
    };
    servers.insert(server.name.to_string(), entry);

    if path.exists() {
        backup_once(path, label, report)?;
    }
    json::write_object(path, &root)?;
    report.note(label, format!("mcp server {} registered", server.name));
    Ok(())
}

fn refuse_commented_sibling(path: &Path, key: &str, name: &str, entry: &Value) -> Result<()> {
    let sibling = path.with_extension("jsonc");
    if path.exists() || !sibling.exists() {
        return Ok(());
    }
    let snippet = serde_json::to_string_pretty(&json!({key: {name: entry}}))?;
    bail!(
        "{} may contain comments that a rewrite would drop; add this entry by hand:\n{snippet}",
        sibling.display()
    );
}

fn remove_json(
    path: &Path,
    label: &str,
    format: McpFormat,
    name: &str,
    target: &Target,
    report: &mut Report,
) -> Result<()> {
    let key = servers_key(format);
    let mut root = json::read_object(path)?;
    let map = root.as_object_mut().expect("read_object returns objects");
    let Some(servers) = map.get_mut(key).and_then(Value::as_object_mut) else {
        return Ok(());
    };
    let owned = servers
        .get(name)
        .is_some_and(|entry| json::contains_marker(entry, DIRECT_LAUNCHER));
    if !owned {
        return Ok(());
    }

    servers.remove(name);
    if servers.is_empty() {
        map.remove(key);
    }
    json::write_or_delete_when_empty(path, &root, target, label, report)
}

fn install_toml(
    path: &Path,
    label: &str,
    server: &spec::McpServer,
    report: &mut Report,
) -> Result<()> {
    let mut document = read_toml(path)?;
    let key = servers_key(McpFormat::Codex);
    let container = document.entry(key).or_insert_with(|| {
        let mut table = Table::new();
        table.set_implicit(true);
        Item::Table(table)
    });
    let Some(servers) = container.as_table_mut() else {
        bail!(
            "expected a table at \"{key}\" in {}; fix or remove it and re-run",
            path.display()
        );
    };

    let mut entry = Table::new();
    entry["command"] = value(server.command.as_str());
    entry["args"] = value(server.args.iter().map(String::as_str).collect::<Array>());
    servers.insert(server.name, Item::Table(entry));

    if path.exists() {
        backup_once(path, label, report)?;
    }
    write_file(path, document.to_string())?;
    report.note(label, format!("mcp server {} registered", server.name));
    Ok(())
}

fn remove_toml(
    path: &Path,
    label: &str,
    name: &str,
    target: &Target,
    report: &mut Report,
) -> Result<()> {
    let mut document = read_toml(path)?;
    let key = servers_key(McpFormat::Codex);
    let Some(servers) = document.get_mut(key).and_then(Item::as_table_mut) else {
        return Ok(());
    };
    let owned = servers
        .get(name)
        .is_some_and(|entry| entry.to_string().contains(DIRECT_LAUNCHER));
    if !owned {
        return Ok(());
    }

    servers.remove(name);
    if servers.is_empty() {
        document.remove(key);
    }
    if document.to_string().trim().is_empty() {
        remove_file_and_empty_parents(path, target)?;
        report.note(label, "removed (was orbit-only)");
    } else {
        write_file(path, document.to_string())?;
        report.note(label, "orbit entries removed");
        drop_backup_when_restored(path, label, report)?;
    }
    Ok(())
}

fn read_toml(path: &Path) -> Result<DocumentMut> {
    match std::fs::read_to_string(path) {
        Ok(raw) => raw.parse().with_context(|| {
            format!(
                "{} is not valid TOML; fix or remove it and re-run",
                path.display()
            )
        }),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(DocumentMut::new()),
        Err(e) => Err(e).with_context(|| format!("failed to read {}", path.display())),
    }
}
