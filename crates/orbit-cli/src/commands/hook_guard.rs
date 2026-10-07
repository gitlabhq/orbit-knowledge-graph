//! Hidden `orbit hook-guard` — the Claude Code PreToolUse guard installed by `orbit setup`.
//! Reads a tool call from stdin and nudges the agent to use `orbit grep` instead of grep or
//! rg. Fails open: on any error it prints nothing and exits 0, never blocking a tool call.
//! `read` is accepted for installs that still register it, and never nudges.

use std::io::Read;
use std::path::Path;

use clap::ValueEnum;
use serde_json::{Value, json};

use crate::commands::setup::spec;
use crate::workspace;

#[derive(ValueEnum, Clone, Copy, Debug)]
pub(crate) enum Kind {
    Search,
    Read,
}

const SEARCH_COMMANDS: &[&str] = &["ack", "ag", "egrep", "fgrep", "grep", "rg", "ripgrep"];

const COMMAND_WRAPPERS: &[&str] = &[
    "command", "env", "git", "nice", "nohup", "sudo", "time", "xargs",
];

pub(crate) fn run(kind: Kind) {
    let mut input = String::new();
    if std::io::stdin().read_to_string(&mut input).is_err() {
        return;
    }
    let Ok(call) = serde_json::from_str::<Value>(&input) else {
        return;
    };
    if !local_graph_exists() {
        return;
    }
    if let Some(response) = respond(kind, &call) {
        println!("{response}");
    }
}

fn respond(kind: Kind, call: &Value) -> Option<Value> {
    (matches!(kind, Kind::Search) && should_nudge(call)).then(|| {
        json!({
            "hookSpecificOutput": {
                "hookEventName": "PreToolUse",
                "additionalContext": spec::search_nudge_text(),
            }
        })
    })
}

fn local_graph_exists() -> bool {
    workspace::resolve_db_path(None)
        .map(|path| path.is_file())
        .unwrap_or(false)
}

fn should_nudge(call: &Value) -> bool {
    let tool_input = call.get("tool_input").unwrap_or(call);
    let cwd = Path::new(call.get("cwd").and_then(Value::as_str).unwrap_or("."));
    let field = |name: &str| tool_input.get(name).and_then(Value::as_str).unwrap_or("");
    let command = field("command");
    if command.is_empty() {
        return call.get("tool_name").and_then(Value::as_str) != Some("Glob")
            && !field("pattern").is_empty()
            && !cwd.join(field("path")).is_file();
    }
    command
        .split([';', '&', '\n', '(', ')', '`'])
        .flat_map(|pipeline| pipeline.split('|').enumerate())
        .any(|(position, segment)| searches_tree(segment, position > 0, cwd))
}

fn searches_tree(segment: &str, piped: bool, cwd: &Path) -> bool {
    let mut tokens = segment
        .split_whitespace()
        .map(|t| t.trim_matches(['"', '\'']));
    let mut via_git = false;
    let name = loop {
        match tokens.next() {
            None => return false,
            Some(t) if t.starts_with('-') || t.contains('=') => continue,
            Some(t) => {
                let name = t.rsplit('/').next().unwrap_or(t);
                if !COMMAND_WRAPPERS.contains(&name) {
                    break name;
                }
                via_git |= matches!(name, "git" | "xargs");
            }
        }
    };
    if !SEARCH_COMMANDS.contains(&name) {
        return false;
    }
    let rest: Vec<&str> = tokens.collect();
    let recursive = rest.iter().any(|t| {
        *t == "--recursive"
            || (t.starts_with('-') && !t.starts_with("--") && t.contains(['r', 'R']))
    });
    let paths: Vec<&str> = rest
        .iter()
        .filter(|t| !t.starts_with('-'))
        .skip(1)
        .copied()
        .collect();
    let greps_tree = matches!(name, "rg" | "ripgrep" | "ag" | "ack") && paths.is_empty() && !piped;
    via_git || recursive || greps_tree || paths.iter().any(|p| cwd.join(p).is_dir())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn grep_family_and_the_grep_tool_nudge() {
        for command in [
            "rg -n foo src/",
            "grep -r foo .",
            "grep -n foo src",
            "sudo rg foo",
            "xargs -n1 grep foo",
            "/usr/bin/rg foo",
            "git grep foo",
            "rg foo src",
            "RUST_LOG=debug rg foo",
        ] {
            let call =
                json!({"cwd": env!("CARGO_MANIFEST_DIR"), "tool_input": {"command": command}});
            assert!(respond(Kind::Search, &call).is_some(), "{command}");
        }
        let tool = json!({"tool_name": "Grep", "tool_input": {"pattern": "fn main"}});
        assert!(respond(Kind::Search, &tool).is_some());
        let file = json!({"cwd": env!("CARGO_MANIFEST_DIR"), "tool_name": "Grep", "tool_input": {"pattern": "rand", "path": "Cargo.toml"}});
        assert!(respond(Kind::Search, &file).is_none());
    }

    #[test]
    fn reads_listings_and_other_commands_never_nudge() {
        for command in [
            "cat src/main.rs",
            "sed -n '1,40p' app/models/user.rb",
            "find . -name '*.rs'",
            "ls -la",
            "cargo build",
            "git log --grep=foo",
            "cat x.txt | grep foo",
            "grep -n rand Cargo.toml crates/x/Cargo.toml",
            "rg -n rand Cargo.toml",
        ] {
            let call =
                json!({"cwd": env!("CARGO_MANIFEST_DIR"), "tool_input": {"command": command}});
            assert!(respond(Kind::Search, &call).is_none(), "{command}");
        }
        let glob = json!({"tool_name": "Glob", "tool_input": {"pattern": "*.rs"}});
        assert!(respond(Kind::Search, &glob).is_none());
        let read = json!({"tool_input": {"file_path": "/repo/src/main.rs"}});
        assert!(respond(Kind::Read, &read).is_none());
    }
}
