//! Hidden `orbit hook-guard` — the Claude Code PreToolUse guard installed by `orbit setup`.
//! Reads a tool call from stdin and nudges the agent to query the graph before grepping or
//! reading source. Fails open: on any error it prints nothing and exits 0, never blocking a
//! tool call.

use std::io::Read;
use std::path::{Path, PathBuf};

use clap::ValueEnum;
use serde_json::{Value, json};

use crate::commands::setup::spec;
use crate::workspace;

#[derive(ValueEnum, Clone, Copy, Debug)]
pub(crate) enum Kind {
    Search,
    Read,
}

const SEARCH_COMMANDS: &[&str] = &[
    "ack", "ag", "egrep", "fd", "fgrep", "find", "grep", "rg", "ripgrep",
];

const CONTENT_SEARCH_COMMANDS: &[&str] = &["ack", "ag", "egrep", "fgrep", "grep", "rg", "ripgrep"];

const READ_COMMANDS: &[&str] = &["bat", "cat", "head", "less", "more", "sed", "tail"];

const COMMAND_WRAPPERS: &[&str] = &[
    "command", "env", "git", "nice", "nohup", "sudo", "time", "xargs",
];

const SOURCE_EXTS: &[&str] = &[
    "py", "js", "cjs", "mjs", "ts", "tsx", "jsx", "vue", "svelte", "go", "rs", "java", "rb", "c",
    "h", "cpp", "hpp", "cc", "cs", "kt", "kts", "swift", "php", "scala", "lua", "sh", "pl",
];

const GRAPH_FIRST_ENV: &str = "ORBIT_GRAPH_FIRST";

pub(crate) fn run(kind: Kind, graph_first: bool) {
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
    let graph_first =
        graph_first_enabled(graph_first, std::env::var(GRAPH_FIRST_ENV).ok().as_deref())
            .then(|| {
                Some(GraphFirst {
                    sessions: session_dir()?,
                    project_dir: std::env::var("CLAUDE_PROJECT_DIR")
                        .ok()
                        .filter(|dir| !dir.is_empty()),
                })
            })
            .flatten();
    if let Some(response) = respond(kind, &call, graph_first.as_ref()) {
        println!("{response}");
    }
}

struct GraphFirst {
    sessions: PathBuf,
    project_dir: Option<String>,
}

fn respond(kind: Kind, call: &Value, graph_first: Option<&GraphFirst>) -> Option<Value> {
    let session = graph_first.zip(session_id(call));
    if let Some((graph_first, id)) = &session
        && matches!(kind, Kind::Search)
        && invokes_orbit(command_of(call))
    {
        claim_session(&graph_first.sessions, id);
        return None;
    }
    if !should_nudge(kind, call) {
        return None;
    }
    if let Some((graph_first, id)) = &session
        && should_block(kind, call, graph_first)
        && claim_session(&graph_first.sessions, id)
    {
        return Some(json!({
            "hookSpecificOutput": {
                "hookEventName": "PreToolUse",
                "permissionDecision": "deny",
                "permissionDecisionReason": spec::graph_first_deny_text(),
            }
        }));
    }
    Some(json!({
        "hookSpecificOutput": {
            "hookEventName": "PreToolUse",
            "additionalContext": nudge_text(kind),
        }
    }))
}

fn graph_first_enabled(installed: bool, env: Option<&str>) -> bool {
    match env
        .map(|value| value.trim().to_ascii_lowercase())
        .as_deref()
    {
        Some("1" | "true" | "on" | "yes") => true,
        Some("0" | "false" | "off" | "no") => false,
        _ => installed,
    }
}

fn session_dir() -> Option<PathBuf> {
    workspace::Workspace::default_root()
        .ok()
        .map(|root| root.join("hook-sessions"))
}

fn session_id(call: &Value) -> Option<String> {
    let raw = call.get("session_id").and_then(Value::as_str)?;
    let id: String = raw
        .chars()
        .map(
            |c| match c.is_ascii_alphanumeric() || c == '-' || c == '_' {
                true => c,
                false => '_',
            },
        )
        .take(64)
        .collect();
    (!id.is_empty()).then_some(id)
}

fn claim_session(dir: &Path, id: &str) -> bool {
    if std::fs::create_dir_all(dir).is_err() {
        return false;
    }
    std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(dir.join(id))
        .is_ok()
}

fn command_of(call: &Value) -> &str {
    call.get("tool_input")
        .unwrap_or(call)
        .get("command")
        .and_then(Value::as_str)
        .unwrap_or("")
}

fn invokes_orbit(command: &str) -> bool {
    command
        .split(['|', ';', '&', '\n', '(', ')', '`'])
        .any(|segment| {
            let mut words = segment.split_whitespace().filter(|t| !t.contains('='));
            match words.next().map(basename) {
                Some("orbit") => true,
                Some("glab") => words.next() == Some("orbit"),
                _ => false,
            }
        })
}

fn should_block(kind: Kind, call: &Value, graph_first: &GraphFirst) -> bool {
    let tool_input = call.get("tool_input").unwrap_or(call);
    let path = |key: &str| tool_input.get(key).and_then(Value::as_str).unwrap_or("");
    match kind {
        Kind::Search => match call.get("tool_name").and_then(Value::as_str) {
            Some("Glob") => false,
            _ => {
                let command = command_of(call);
                let is_pattern_tool = command.is_empty() && !path("pattern").is_empty();
                (is_pattern_tool && in_project(path("path"), call, graph_first))
                    || invokes_search(command, CONTENT_SEARCH_COMMANDS)
                    || reads_source(command)
            }
        },
        Kind::Read => in_project(path("file_path"), call, graph_first),
    }
}

fn in_project(path: &str, call: &Value, graph_first: &GraphFirst) -> bool {
    let path = Path::new(path);
    if path.as_os_str().is_empty() || path.is_relative() {
        return true;
    }
    graph_first
        .project_dir
        .as_deref()
        .or_else(|| call.get("cwd").and_then(Value::as_str))
        .is_none_or(|root| path.starts_with(root))
}

fn local_graph_exists() -> bool {
    workspace::resolve_db_path(None)
        .map(|path| path.is_file())
        .unwrap_or(false)
}

fn nudge_text(kind: Kind) -> &'static str {
    match kind {
        Kind::Search => spec::search_nudge_text(),
        Kind::Read => spec::read_nudge_text(),
    }
}

fn should_nudge(kind: Kind, call: &Value) -> bool {
    let tool_input = call.get("tool_input").unwrap_or(call);
    match kind {
        Kind::Search => {
            let command = tool_input
                .get("command")
                .and_then(Value::as_str)
                .unwrap_or("");
            let is_pattern_tool = command.is_empty()
                && tool_input
                    .get("pattern")
                    .and_then(Value::as_str)
                    .is_some_and(|p| !p.is_empty());
            is_pattern_tool || invokes_search(command, SEARCH_COMMANDS) || reads_source(command)
        }
        Kind::Read => {
            let path = tool_input
                .get("file_path")
                .and_then(Value::as_str)
                .unwrap_or("");
            is_source_path(path)
        }
    }
}

fn invokes_search(command: &str, commands: &[&str]) -> bool {
    command
        .split(['|', ';', '&', '\n', '(', ')', '`'])
        .any(|segment| segment_invokes_search(segment, commands))
}

fn segment_invokes_search(segment: &str, commands: &[&str]) -> bool {
    for token in segment.split_whitespace() {
        if token.starts_with('-') || token.contains('=') {
            continue;
        }
        let name = basename(token);
        if COMMAND_WRAPPERS.contains(&name) {
            continue;
        }
        return commands.contains(&name);
    }
    false
}

fn reads_source(command: &str) -> bool {
    command
        .split(['|', ';', '&', '\n', '(', ')', '`'])
        .any(|segment| {
            let mut tokens = segment.split_whitespace().filter(|t| !t.starts_with('-'));
            let is_reader = tokens
                .find(|t| !COMMAND_WRAPPERS.contains(&basename(t)))
                .is_some_and(|t| READ_COMMANDS.contains(&basename(t)));
            is_reader && tokens.any(is_source_path)
        })
}

fn basename(token: &str) -> &str {
    token.rsplit('/').next().unwrap_or(token)
}

fn is_source_path(path: &str) -> bool {
    let normalized = path.replace('\\', "/");
    let name = normalized.rsplit('/').next().unwrap_or("");
    match name.rsplit_once('.') {
        Some((stem, ext)) if !stem.is_empty() => {
            SOURCE_EXTS.contains(&ext.to_ascii_lowercase().as_str())
        }
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn grep_tool_pattern_nudges() {
        let call = json!({"tool_input": {"pattern": "fn main"}});
        assert!(should_nudge(Kind::Search, &call));
    }

    #[test]
    fn bash_search_commands_nudge() {
        for command in [
            "rg -n foo src/",
            "grep -r foo .",
            "find . -name '*.rs'",
            "sudo rg foo",
            "xargs -n1 grep foo",
            "/usr/bin/rg foo",
            "git grep foo",
            "cat x.txt | grep foo",
            "RUST_LOG=debug rg foo",
        ] {
            let call = json!({"tool_input": {"command": command}});
            assert!(should_nudge(Kind::Search, &call), "{command}");
        }
    }

    #[test]
    fn bash_source_reads_nudge() {
        for command in [
            "cat src/main.rs",
            "head -50 crates/foo/src/lib.rs",
            "sed -n '1,40p' app/models/user.rb",
            "cd repo && cat lib/x.py",
        ] {
            let call = json!({"tool_input": {"command": command}});
            assert!(should_nudge(Kind::Search, &call), "{command}");
        }
    }

    #[test]
    fn non_search_bash_does_not_nudge() {
        for command in [
            "cargo build",
            "ls -la",
            "git status",
            "git tag -a v1.0",
            "docker tag img repo/img",
            "npm run build --flag foo",
            "git log --grep=foo",
            "echo storage",
            "cat README.md",
            "cat Cargo.toml",
            "tail -f server.log",
        ] {
            let call = json!({"tool_input": {"command": command}});
            assert!(!should_nudge(Kind::Search, &call), "{command}");
        }
    }

    #[test]
    fn source_reads_nudge_but_docs_do_not() {
        let source = json!({"tool_input": {"file_path": "/repo/src/main.rs"}});
        assert!(should_nudge(Kind::Read, &source));

        for path in [
            "/repo/README.md",
            "/repo/config.yaml",
            "/repo/.env",
            "/repo/Cargo.toml",
        ] {
            let call = json!({"tool_input": {"file_path": path}});
            assert!(!should_nudge(Kind::Read, &call), "{path}");
        }
    }

    #[test]
    fn dotfiles_are_not_source() {
        assert!(!is_source_path("/repo/.rs"));
        assert!(!is_source_path(""));
        assert!(is_source_path("C:\\repo\\src\\main.RS"));
    }

    #[test]
    fn missing_tool_input_falls_back_to_root() {
        let call = json!({"pattern": "foo"});
        assert!(should_nudge(Kind::Search, &call));
    }

    fn decide(kind: Kind, call: &Value, graph_first: Option<&GraphFirst>) -> &'static str {
        match respond(kind, call, graph_first) {
            None => "none",
            Some(out) if out["hookSpecificOutput"]["permissionDecision"] == "deny" => "deny",
            Some(_) => "nudge",
        }
    }

    #[test]
    fn graph_first_blocks_once_per_session_unless_orbit_ran_first() {
        let dir = tempfile::tempdir().unwrap();
        let graph_first = GraphFirst {
            sessions: dir.path().to_path_buf(),
            project_dir: Some("/repo".to_string()),
        };
        let read =
            |id: &str, path: &str| json!({"session_id": id, "tool_input": {"file_path": path}});
        let bash = |id: &str, cmd: &str| json!({"session_id": id, "tool_input": {"command": cmd}});
        let glob =
            json!({"session_id": "c", "tool_name": "Glob", "tool_input": {"pattern": "*.rs"}});
        for (kind, call, expected) in [
            (Kind::Read, read("a", "/repo/src/main.rs"), "deny"),
            (Kind::Read, read("a", "/repo/src/main.rs"), "nudge"),
            (Kind::Search, bash("b", "cd repo && rg foo"), "deny"),
            (Kind::Search, glob, "nudge"),
            (Kind::Search, bash("c", "find . -name '*.rs'"), "nudge"),
            (Kind::Search, bash("d", "glab orbit grep foo"), "none"),
            (Kind::Read, read("d", "/repo/src/main.rs"), "nudge"),
            (Kind::Read, read("e", "/elsewhere/lib.rs"), "nudge"),
            (
                Kind::Read,
                json!({"tool_input": {"file_path": "/repo/a.rs"}}),
                "nudge",
            ),
        ] {
            assert_eq!(decide(kind, &call, Some(&graph_first)), expected, "{call}");
        }
        assert_eq!(decide(Kind::Read, &read("f", "/repo/a.rs"), None), "nudge");
        assert_eq!(
            session_id(&json!({"session_id": "../x"})).as_deref(),
            Some("___x")
        );
    }

    #[test]
    fn graph_first_env_overrides_the_installed_flag() {
        assert!(graph_first_enabled(false, Some("1")) && !graph_first_enabled(true, Some("off")));
        assert!(graph_first_enabled(true, None) && graph_first_enabled(true, Some("junk")));
    }

    #[test]
    fn nudge_text_names_the_launcher_verbs() {
        assert!(nudge_text(Kind::Search).contains("`orbit grep"));
        assert!(nudge_text(Kind::Read).contains("`orbit context"));
        assert!(spec::graph_first_deny_text().contains("`orbit grep"));
    }
}
