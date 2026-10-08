//! Hidden `orbit hook-guard` — the Claude Code PreToolUse guard installed by `orbit setup`.
//! Reads a tool call from stdin and nudges the agent to use `orbit grep` instead of grep or
//! rg. Fails open: on any error it prints nothing and exits 0, never blocking a tool call.
//! `read` is accepted for installs that still register it, and never nudges.

use std::io::Read;
use std::path::{Path, PathBuf};

use clap::ValueEnum;
use serde_json::{Value, json};

use crate::commands::setup::spec;

#[derive(ValueEnum, Clone, Copy, Debug)]
pub(crate) enum Kind {
    Search,
    Read,
}

const SEARCH_COMMANDS: &[&str] = &["ack", "ag", "egrep", "fgrep", "grep", "rg", "ripgrep"];

const COMMAND_WRAPPERS: &[&str] = &[
    "command", "env", "git", "nice", "nohup", "sudo", "time", "xargs",
];

const WRAPPER_VALUE_FLAGS: &[&str] = &[
    "-C", "-E", "-I", "-L", "-P", "-a", "-c", "-d", "-g", "-n", "-s", "-u",
];

const SHELLS: &[&str] = &["bash", "sh", "zsh"];

const PATTERN_FLAGS: &[&str] = &["-e", "-f", "--regexp", "--file"];

const INFO_FLAGS: &[&str] = &["--files", "--type-list", "--version", "-V", "--help"];

/// Short flags whose value is the next word, and the long ones, per tool family.
const RG_VALUE_SHORT: &str = "ABCEMTdefgjmrt";
const GREP_VALUE_SHORT: &str = "ABCDdefm";
const VALUE_LONG: &[&str] = &[
    "--after-context",
    "--before-context",
    "--colors",
    "--context",
    "--context-separator",
    "--devices",
    "--directories",
    "--encoding",
    "--exclude",
    "--exclude-dir",
    "--file",
    "--glob",
    "--iglob",
    "--ignore-dir",
    "--include",
    "--label",
    "--max-columns",
    "--max-count",
    "--max-depth",
    "--max-filesize",
    "--pre",
    "--pre-glob",
    "--regexp",
    "--replace",
    "--sort",
    "--sortr",
    "--threads",
    "--type",
    "--type-add",
    "--type-not",
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

/// Checks the graph's default location without creating or migrating the data directory.
fn local_graph_exists() -> bool {
    let root = match std::env::var("ORBIT_DATA_DIR") {
        Ok(dir) if !dir.is_empty() => PathBuf::from(dir),
        _ => match dirs::home_dir() {
            Some(home) => home.join(".gitlab").join("orbit"),
            None => return false,
        },
    };
    root.join("graph.duckdb").is_file()
}

/// The enclosing git work tree, found without running git.
fn work_tree(cwd: &Path) -> Option<PathBuf> {
    let cwd = dunce::canonicalize(cwd).ok()?;
    cwd.ancestors()
        .find(|dir| dir.join(".git").exists())
        .map(Path::to_path_buf)
}

fn should_nudge(call: &Value) -> bool {
    let tool_input = call.get("tool_input").unwrap_or(call);
    let cwd = Path::new(call.get("cwd").and_then(Value::as_str).unwrap_or("."));
    let Some(root) = work_tree(cwd) else {
        return false;
    };
    let field = |name: &str| tool_input.get(name).and_then(Value::as_str).unwrap_or("");
    let command = field("command");
    if command.is_empty() {
        let path = field("path");
        return call.get("tool_name").and_then(Value::as_str) != Some("Glob")
            && !field("pattern").is_empty()
            && (path.is_empty() || tree_dir(cwd, &root, path));
    }
    command_searches_tree(command, cwd, &root)
}

fn command_searches_tree(command: &str, cwd: &Path, root: &Path) -> bool {
    segments(command)
        .iter()
        .any(|(words, piped)| searches_tree(words, *piped, cwd, root))
}

fn tree_dir(cwd: &Path, root: &Path, path: &str) -> bool {
    dunce::canonicalize(cwd.join(path)).is_ok_and(|full| full.is_dir() && full.starts_with(root))
}

/// Splits a shell command into simple commands, each as its words and whether its input is piped.
/// Quotes and escapes are honored; redirections and their targets are dropped.
fn segments(command: &str) -> Vec<(Vec<String>, bool)> {
    let mut segments = Vec::new();
    let (mut words, mut word, mut started) = (Vec::new(), String::new(), false);
    let (mut quote, mut piped) = (None, false);
    let mut chars = command.chars().peekable();
    let finish = |words: &mut Vec<String>, word: &mut String, started: &mut bool| {
        if *started {
            words.push(std::mem::take(word));
            *started = false;
        }
    };
    while let Some(c) = chars.next() {
        match (quote, c) {
            (Some(q), c) if c == q => quote = None,
            (Some('"'), '\\') => word.extend(chars.next()),
            (Some(_), c) => word.push(c),
            (None, '\'' | '"') => (quote, started) = (Some(c), true),
            (None, '\\') => {
                word.extend(chars.next());
                started = true;
            }
            (None, '>' | '<') => {
                if !word.is_empty() && word.chars().all(|d| d.is_ascii_digit()) {
                    word.clear();
                    started = false;
                } else {
                    finish(&mut words, &mut word, &mut started);
                }
                let mut duplicates = false;
                while let Some(&next) = chars.peek().filter(|n| matches!(n, '>' | '<' | '&')) {
                    duplicates |= next == '&';
                    chars.next();
                }
                match duplicates {
                    true => while chars.next_if(|n| n.is_ascii_digit() || *n == '-').is_some() {},
                    false => {
                        while chars.next_if(|n| n.is_whitespace() && *n != '\n').is_some() {}
                        while chars
                            .next_if(|n| !n.is_whitespace() && !"|;&<>()`".contains(*n))
                            .is_some()
                        {}
                    }
                }
            }
            (None, '|' | ';' | '&' | '\n' | '(' | ')' | '`') => {
                finish(&mut words, &mut word, &mut started);
                segments.push((std::mem::take(&mut words), piped));
                let doubled = matches!(c, '|' | '&') && chars.next_if_eq(&c).is_some();
                piped = c == '|' && !doubled;
            }
            (None, c) if c.is_whitespace() => finish(&mut words, &mut word, &mut started),
            (None, c) => {
                word.push(c);
                started = true;
            }
        }
    }
    finish(&mut words, &mut word, &mut started);
    segments.push((words, piped));
    segments.retain(|(words, _)| !words.is_empty());
    segments
}

fn program(word: &str) -> &str {
    let name = word.rsplit(['/', '\\']).next().unwrap_or(word);
    name.strip_suffix(".exe").unwrap_or(name)
}

fn searches_tree(words: &[String], piped: bool, cwd: &Path, root: &Path) -> bool {
    let mut words = words.iter().map(String::as_str);
    let mut via_git = false;
    let mut in_wrapper = false;
    let name = loop {
        match words.next() {
            None => return false,
            Some(w) if in_wrapper && WRAPPER_VALUE_FLAGS.contains(&w) => {
                words.next();
            }
            Some(w) if w.starts_with('-') || w.contains('=') => continue,
            Some(w) => {
                let name = program(w);
                if !COMMAND_WRAPPERS.contains(&name) {
                    break name;
                }
                in_wrapper = true;
                via_git |= matches!(name, "git" | "xargs");
            }
        }
    };
    let rest: Vec<&str> = words.collect();
    if SHELLS.contains(&name) {
        return rest
            .iter()
            .position(|w| w.starts_with('-') && !w.starts_with("--") && w.ends_with('c'))
            .and_then(|at| rest.get(at + 1))
            .is_some_and(|script| command_searches_tree(script, cwd, root));
    }
    if !SEARCH_COMMANDS.contains(&name) {
        return false;
    }
    let ripgrep_like = matches!(name, "rg" | "ripgrep" | "ag" | "ack");
    let value_short = match ripgrep_like {
        true => RG_VALUE_SHORT,
        false => GREP_VALUE_SHORT,
    };
    let (mut positionals, mut recursive, mut pattern_flag) = (Vec::new(), false, false);
    let mut rest = rest.into_iter();
    while let Some(word) = rest.next() {
        if word == "--" {
            positionals.extend(rest.by_ref());
            break;
        }
        if INFO_FLAGS.contains(&word) || (ripgrep_like && word == "-h") {
            return false;
        }
        if let Some(long) = word.strip_prefix("--") {
            let flag = long.split('=').next().unwrap_or(long);
            recursive |= matches!(flag, "recursive" | "dereference-recursive")
                || word == "--directories=recurse";
            pattern_flag |= PATTERN_FLAGS.contains(&format!("--{flag}").as_str());
            if !long.contains('=') && VALUE_LONG.contains(&word) {
                let value = rest.next();
                recursive |= word == "--directories" && value == Some("recurse");
            }
            continue;
        }
        if let Some(cluster) = word.strip_prefix('-').filter(|c| !c.is_empty()) {
            let value_at = cluster.find(|c: char| value_short.contains(c));
            let flags = &cluster[..value_at.map_or(cluster.len(), |at| at + 1)];
            recursive |= !ripgrep_like && flags.contains(['r', 'R']);
            pattern_flag |= flags.ends_with(['e', 'f']) && value_at.is_some();
            if value_at.is_some_and(|at| at + 1 == cluster.len()) {
                rest.next();
            }
            continue;
        }
        positionals.push(word);
    }
    let paths = match pattern_flag {
        true => positionals.as_slice(),
        false => positionals.get(1..).unwrap_or_default(),
    };
    let greps_tree = ripgrep_like && paths.is_empty() && !piped;
    let greps_here = recursive && paths.is_empty();
    via_git || greps_tree || greps_here || paths.iter().any(|p| tree_dir(cwd, root, p))
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
            "rg \"fn main\"",
            "rg -t rust foo",
            "rg -g '*.rs' foo",
            "rg -A 3 foo",
            "rg -nA3 foo",
            "rg foo 2>/dev/null",
            "rg foo 2>&1 | head",
            "rg -e foo",
            "grep -nE 'a|b' src/",
            "grep -rn foo",
            "sudo -u me rg foo",
            "git -C . grep foo",
            "bash -lc \"rg foo\"",
            "rg.exe foo",
            "cd src && rg foo",
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
            "rg --files",
            "rg --version",
            "rg foo /usr/include",
            "rg -trust foo Cargo.toml",
            "rg -e foo Cargo.toml",
            "grep -n 'struct src' Cargo.toml",
            "git commit -m \"x; rg foo\"",
            "grep --color foo Cargo.toml",
        ] {
            let call =
                json!({"cwd": env!("CARGO_MANIFEST_DIR"), "tool_input": {"command": command}});
            assert!(respond(Kind::Search, &call).is_none(), "{command}");
        }
        let outside = json!({"cwd": "/", "tool_input": {"command": "rg foo"}});
        assert!(respond(Kind::Search, &outside).is_none());
        let elsewhere = json!({"cwd": env!("CARGO_MANIFEST_DIR"), "tool_name": "Grep", "tool_input": {"pattern": "x", "path": "/usr"}});
        assert!(respond(Kind::Search, &elsewhere).is_none());
        let glob = json!({"tool_name": "Glob", "tool_input": {"pattern": "*.rs"}});
        assert!(respond(Kind::Search, &glob).is_none());
        let read = json!({"tool_input": {"file_path": "/repo/src/main.rs"}});
        assert!(respond(Kind::Read, &read).is_none());
    }
}
