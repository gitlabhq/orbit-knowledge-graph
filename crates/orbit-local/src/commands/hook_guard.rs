//! Hidden `orbit hook-guard` — the PreToolUse guard that `orbit setup`
//! installs for Claude Code. Reads the tool-call JSON from stdin and emits an
//! `additionalContext` nudge steering the agent to query the graph before
//! grepping or reading raw source files. The nudge wording follows the mode
//! the guard was installed with (local `orbit sql` vs `glab orbit remote`).
//! Fails open: any error prints nothing and exits 0, so a tool call is never
//! blocked.

use std::io::Read;

use clap::ValueEnum;
use serde_json::{Value, json};

use crate::commands::setup::spec::{self, Mode};
use crate::workspace;

#[derive(ValueEnum, Clone, Copy, Debug)]
pub(crate) enum Kind {
    Search,
    Read,
}

const SEARCH_COMMANDS: &[&str] = &[
    "ack", "ag", "egrep", "fd", "fgrep", "find", "grep", "rg", "ripgrep",
];

/// Commands that run the command following them, so a search can hide behind
/// one (`sudo rg …`, `xargs grep …`, `git grep …`).
const COMMAND_WRAPPERS: &[&str] = &[
    "command", "env", "git", "nice", "nohup", "sudo", "time", "xargs",
];

/// Code extensions the graph indexes; reads of anything else (docs, config,
/// data) never nudge.
const SOURCE_EXTS: &[&str] = &[
    "py", "js", "cjs", "mjs", "ts", "tsx", "jsx", "vue", "svelte", "go", "rs", "java", "rb", "c",
    "h", "cpp", "hpp", "cc", "cs", "kt", "kts", "swift", "php", "scala", "lua", "sh", "pl",
];

pub(crate) fn run(kind: Kind, mode: Mode) {
    let mut input = String::new();
    if std::io::stdin().read_to_string(&mut input).is_err() {
        return;
    }
    let Ok(call) = serde_json::from_str::<Value>(&input) else {
        return;
    };
    // Remote availability was verified when setup wrote the hook; only the
    // local graph can disappear (database deleted) after install.
    if mode == Mode::Local && !local_graph_exists() {
        return;
    }
    if should_nudge(kind, &call) {
        println!(
            "{}",
            json!({
                "hookSpecificOutput": {
                    "hookEventName": "PreToolUse",
                    "additionalContext": nudge_text(kind, mode),
                }
            })
        );
    }
}

fn local_graph_exists() -> bool {
    workspace::resolve_db_path(None)
        .map(|path| path.is_file())
        .unwrap_or(false)
}

fn nudge_text(kind: Kind, mode: Mode) -> &'static str {
    match kind {
        Kind::Search => spec::nudge_search(mode),
        Kind::Read => spec::nudge_read(mode),
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
            // Grep and Glob carry `pattern` and no `command`; a pattern-based
            // call is a content search by definition. Bash only nudges on
            // search-looking commands.
            let is_pattern_tool = command.is_empty()
                && tool_input
                    .get("pattern")
                    .and_then(Value::as_str)
                    .is_some_and(|p| !p.is_empty());
            is_pattern_tool || invokes_search(command)
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

/// Whether any pipeline segment of a shell command *runs* a search tool.
/// Matching whole commands rather than substrings keeps `git tag` and
/// `npm run build --flag foo` from reading as searches.
fn invokes_search(command: &str) -> bool {
    command
        .split(['|', ';', '&', '\n', '(', ')', '`'])
        .any(segment_invokes_search)
}

fn segment_invokes_search(segment: &str) -> bool {
    for token in segment.split_whitespace() {
        if token.starts_with('-') || token.contains('=') {
            continue;
        }
        let name = basename(token);
        if COMMAND_WRAPPERS.contains(&name) {
            continue;
        }
        return SEARCH_COMMANDS.contains(&name);
    }
    false
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

    #[test]
    fn nudge_text_follows_the_mode() {
        assert!(nudge_text(Kind::Search, Mode::Local).contains("orbit sql"));
        assert!(nudge_text(Kind::Search, Mode::Remote).contains("glab orbit remote"));
        assert!(nudge_text(Kind::Read, Mode::Remote).contains("glab orbit remote"));
    }
}
