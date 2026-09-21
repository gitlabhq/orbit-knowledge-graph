use std::path::Path;

use anyhow::{Context, Result, bail};
use serde_json::{Value, json};
use toml_edit::{Array, DocumentMut, Item, Table, value};

use super::spec::{DIRECT_LAUNCHER, McpFormat, McpServer};
use super::{json_config, json_ops};

pub(super) fn install(
    path: &Path,
    label: &str,
    format: McpFormat,
    server: &McpServer,
) -> Result<()> {
    match format {
        McpFormat::Codex => install_toml(path, label, server),
        McpFormat::Claude | McpFormat::Opencode => install_json(path, label, format, server),
    }
}

pub(super) fn remove(path: &Path, label: &str, format: McpFormat, name: &str) -> Result<()> {
    if !path.exists() {
        return Ok(());
    }
    match format {
        McpFormat::Codex => remove_toml(path, label, name),
        McpFormat::Claude | McpFormat::Opencode => remove_json(path, label, format, name),
    }
}

fn container_key(format: McpFormat) -> &'static str {
    match format {
        McpFormat::Claude => "mcpServers",
        McpFormat::Opencode => "mcp",
        McpFormat::Codex => "mcp_servers",
    }
}

fn json_entry(format: McpFormat, server: &McpServer) -> Value {
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

fn install_json(path: &Path, label: &str, format: McpFormat, server: &McpServer) -> Result<()> {
    let key = container_key(format);
    let entry = json_entry(format, server);
    refuse_commented_sibling(path, key, server.name, &entry)?;

    let mut root = json_config::read_object(path)?;
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
        super::backup_once(path, label)?;
    }
    json_config::write_object(path, &root)?;
    println!("  {label}  ->  mcp server {} registered", server.name);
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

fn remove_json(path: &Path, label: &str, format: McpFormat, name: &str) -> Result<()> {
    let key = container_key(format);
    let mut root = json_config::read_object(path)?;
    let map = root.as_object_mut().expect("read_object returns objects");
    let Some(servers) = map.get_mut(key).and_then(Value::as_object_mut) else {
        return Ok(());
    };
    let owned = servers
        .get(name)
        .is_some_and(|entry| json_ops::contains_marker(entry, DIRECT_LAUNCHER));
    if !owned {
        return Ok(());
    }

    servers.remove(name);
    if servers.is_empty() {
        map.remove(key);
    }
    super::write_or_delete_when_empty(path, &root, label)
}

fn install_toml(path: &Path, label: &str, server: &McpServer) -> Result<()> {
    let mut document = read_toml(path)?;
    let key = container_key(McpFormat::Codex);
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
        super::backup_once(path, label)?;
    }
    write_toml(path, &document)?;
    println!("  {label}  ->  mcp server {} registered", server.name);
    Ok(())
}

fn remove_toml(path: &Path, label: &str, name: &str) -> Result<()> {
    let mut document = read_toml(path)?;
    let key = container_key(McpFormat::Codex);
    let Some(servers) = document.get_mut(key).and_then(Item::as_table_mut) else {
        return Ok(());
    };
    let owned = servers
        .get(name)
        .and_then(|entry| entry.get("command"))
        .and_then(Item::as_str)
        .is_some_and(|command| command.contains(DIRECT_LAUNCHER));
    if !owned {
        return Ok(());
    }

    servers.remove(name);
    if servers.is_empty() {
        document.remove(key);
    }
    if document.to_string().trim().is_empty() {
        std::fs::remove_file(path)
            .with_context(|| format!("failed to remove {}", path.display()))?;
        println!("  {label}  ->  removed (was orbit-only)");
    } else {
        write_toml(path, &document)?;
        println!("  {label}  ->  orbit entries removed");
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

fn write_toml(path: &Path, document: &DocumentMut) -> Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("failed to create {}", parent.display()))?;
    }
    std::fs::write(path, document.to_string())
        .with_context(|| format!("failed to write {}", path.display()))
}
