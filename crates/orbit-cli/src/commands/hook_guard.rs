//! Hidden `orbit hook-guard` — the Claude Code PreToolUse guard installed by `orbit setup`.
//! Reads a tool call from stdin and nudges the agent to use `orbit grep` instead of grep or
//! rg. Fails open: on any error it prints nothing and exits 0, never blocking a tool call.
//! `read` is accepted for installs that still register it, and never nudges.

use std::io::Read;

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
    let command = tool_input
        .get("command")
        .and_then(Value::as_str)
        .unwrap_or("");
    let is_grep_tool = command.is_empty()
        && call.get("tool_name").and_then(Value::as_str) != Some("Glob")
        && tool_input
            .get("pattern")
            .and_then(Value::as_str)
            .is_some_and(|p| !p.is_empty());
    is_grep_tool
        || command
            .split(['|', ';', '&', '\n', '(', ')', '`'])
            .any(segment_invokes_search)
}

fn segment_invokes_search(segment: &str) -> bool {
    for token in segment.split_whitespace() {
        if token.starts_with('-') || token.contains('=') {
            continue;
        }
        let name = token.rsplit('/').next().unwrap_or(token);
        if COMMAND_WRAPPERS.contains(&name) {
            continue;
        }
        return SEARCH_COMMANDS.contains(&name);
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn grep_family_and_the_grep_tool_nudge() {
        for command in [
            "rg -n foo src/",
            "grep -r foo .",
            "sudo rg foo",
            "xargs -n1 grep foo",
            "/usr/bin/rg foo",
            "git grep foo",
            "cat x.txt | grep foo",
            "RUST_LOG=debug rg foo",
        ] {
            let call = json!({"tool_input": {"command": command}});
            assert!(respond(Kind::Search, &call).is_some(), "{command}");
        }
        let tool = json!({"tool_name": "Grep", "tool_input": {"pattern": "fn main"}});
        assert!(respond(Kind::Search, &tool).is_some());
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
        ] {
            let call = json!({"tool_input": {"command": command}});
            assert!(respond(Kind::Search, &call).is_none(), "{command}");
        }
        let glob = json!({"tool_name": "Glob", "tool_input": {"pattern": "*.rs"}});
        assert!(respond(Kind::Search, &glob).is_none());
        let read = json!({"tool_input": {"file_path": "/repo/src/main.rs"}});
        assert!(respond(Kind::Read, &read).is_none());
    }
}
