//! Hidden `orbit hook-guard` — the Claude Code PreToolUse guard installed by `orbit setup`.
//! Reads a tool call from stdin and nudges the agent to query the graph before grepping or
//! reading source. Fails open: on any error it prints nothing and exits 0, never blocking a
//! tool call.

use std::io::Read;

use clap::ValueEnum;
use serde_json::{Value, json};

use crate::commands::{setup::spec, shell_quote};
use crate::workspace;

#[derive(ValueEnum, Clone, Copy, Debug)]
pub(crate) enum Kind {
    Search,
    Read,
}

const SEARCH_COMMANDS: &[&str] = &[
    "ack", "ag", "egrep", "fd", "fgrep", "find", "grep", "rg", "ripgrep",
];

const READ_COMMANDS: &[&str] = &["bat", "cat", "head", "less", "more", "sed", "tail"];

const COMMAND_WRAPPERS: &[&str] = &[
    "command", "env", "git", "nice", "nohup", "sudo", "time", "xargs",
];

const SOURCE_EXTS: &[&str] = &[
    "py", "js", "cjs", "mjs", "ts", "tsx", "jsx", "vue", "svelte", "go", "rs", "java", "rb", "c",
    "h", "cpp", "hpp", "cc", "cs", "kt", "kts", "swift", "php", "scala", "lua", "sh", "pl",
];

pub(crate) fn run(kind: Kind) {
    let mut input = String::new();
    if std::io::stdin().read_to_string(&mut input).is_err() {
        return;
    }
    let Ok(call) = serde_json::from_str::<Value>(&input) else {
        return;
    };
    if !workspace::resolve_db_path(None).is_ok_and(|path| path.is_file()) {
        return;
    }
    if let Some(nudge) = resolve(kind, &call) {
        println!(
            "{}",
            json!({
                "hookSpecificOutput": {
                    "hookEventName": "PreToolUse",
                    "additionalContext": nudge.text(),
                }
            })
        );
    }
}

#[derive(Debug, PartialEq, Eq)]
enum Nudge {
    Search,
    Read { path: Option<String> },
}

impl Nudge {
    fn text(&self) -> String {
        match self {
            Self::Search => spec::nudge_search().to_string(),
            Self::Read { path: Some(path) } => {
                let Ok(path) = workspace::absolutize(path.into()) else {
                    return spec::nudge_read().to_string();
                };
                let quoted = shell_quote(&path.to_string_lossy());
                spec::nudge_read().replace("--file <path>", &format!("--file={quoted}"))
            }
            Self::Read { path: None } => spec::nudge_read().to_string(),
        }
    }
}

fn resolve(kind: Kind, call: &Value) -> Option<Nudge> {
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
            if is_pattern_tool {
                return Some(Nudge::Search);
            }
            source_read_nudge(command).or_else(|| invokes_search(command).then_some(Nudge::Search))
        }
        Kind::Read => {
            let path = tool_input
                .get("file_path")
                .and_then(Value::as_str)
                .unwrap_or("");
            is_source_path(path).then(|| Nudge::Read {
                path: Some(path.to_string()),
            })
        }
    }
}

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

fn source_read_nudge(command: &str) -> Option<Nudge> {
    let direct_reader = command
        .split_whitespace()
        .next()
        .is_some_and(|t| READ_COMMANDS.contains(&basename(t)));
    let ambiguous = !direct_reader
        || command.split_whitespace().any(|t| matches!(t, "--" | "-"))
        || command.contains([
            '\'', '"', '\\', '$', '`', '(', ')', '|', ';', '&', '\n', '<', '>', '*', '?', '[', ']',
            '{', '}', '~', '#',
        ]);
    command
        .split([';', '&', '\n'])
        .filter(|statement| !invokes_search(statement))
        .flat_map(|statement| statement.split(['|', '(', ')', '`']))
        .find_map(|segment| {
            let mut tokens = segment.split_whitespace().filter(|t| !t.starts_with('-'));
            let is_reader = tokens
                .find(|t| !COMMAND_WRAPPERS.contains(&basename(t)))
                .is_some_and(|t| READ_COMMANDS.contains(&basename(t)));
            if !is_reader {
                return None;
            }
            let operands: Vec<&str> = tokens.collect();
            let path = operands
                .iter()
                .map(|t| t.trim_matches(['\'', '"']))
                .find(|t| is_source_path(t))?;
            Some(Nudge::Read {
                path: (!ambiguous && operands.len() == 1).then(|| path.to_string()),
            })
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
        assert_eq!(resolve(Kind::Search, &call), Some(Nudge::Search));
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
            assert_eq!(
                resolve(Kind::Search, &call),
                Some(Nudge::Search),
                "{command}"
            );
        }
    }

    #[test]
    fn bash_source_reads_get_the_read_nudge_with_the_path() {
        for (command, path) in [
            ("cat src/main.rs", "src/main.rs"),
            ("head -50 crates/foo/src/lib.rs", "crates/foo/src/lib.rs"),
        ] {
            let call = json!({"tool_input": {"command": command}});
            let expected = Nudge::Read {
                path: Some(path.to_string()),
            };
            assert_eq!(resolve(Kind::Search, &call), Some(expected), "{command}");
        }
    }

    #[test]
    fn ambiguous_source_reads_keep_generic_read_guidance() {
        for command in [
            "cat 'lib/file with spaces.py'",
            "cat \"lib/file with spaces.py\"",
            "cat 'lib/it'\\''s file.py'",
            "cat lib/file\\ with\\ spaces.py",
            "cd repo && cat lib/x.py",
            "cd repo; cat lib/x.py",
            "env --chdir=repo cat src/main.rs",
            "sudo --chdir=repo cat src/main.rs",
            "sed -n '1,40p' app/models/user.rb",
            "cat src/main.rs src/lib.rs",
            "cat -- src/main.rs -other.rs",
            "cat src/main.rs -",
            "cat README.md src/main.rs",
            "cat lib/some file.rs",
            "cat $ROOT/src/main.rs",
        ] {
            let call = json!({"tool_input": {"command": command}});
            let nudge = resolve(Kind::Search, &call).unwrap();
            assert_eq!(nudge, Nudge::Read { path: None }, "{command}");
            assert_eq!(nudge.text(), spec::nudge_read(), "{command}");
        }
    }

    #[test]
    fn concrete_read_paths_are_shell_quoted() {
        let dir = tempfile::tempdir().unwrap();
        for (name, escaped) in [
            ("my file.rs", "my file.rs"),
            ("it's.rs", "it'\\''s.rs"),
            ("$x.rs", "$x.rs"),
            ("-source.rs", "-source.rs"),
        ] {
            let path = dir.path().join(name);
            std::fs::write(&path, "fn example() {}\n").unwrap();
            let call = json!({"tool_input": {"file_path": path}});
            let text = resolve(Kind::Read, &call).unwrap().text();
            let quoted = format!("'{}/{escaped}'", dir.path().display());
            assert!(text.contains(&format!("context --file={quoted}")), "{text}");
            assert!(!text.contains("<path>"), "{text}");
        }
    }

    #[test]
    fn independent_source_read_wins_over_manifest_search() {
        for command in [
            "cat router.rs; ls src; grep rand Cargo.toml",
            "grep rand Cargo.toml; ls src; cat router.rs",
            "cat router.rs && grep rand Cargo.toml",
            "cat router.rs\nls src\ngrep rand Cargo.toml",
            "cd src; cat router.rs; grep rand ../Cargo.toml",
            "cat 'my router.rs'; grep rand Cargo.toml",
            "cat -- router.rs; grep rand Cargo.toml",
            "cat router.rs -; grep rand Cargo.toml",
            "cat router.rs | rg handler; cat app.rs",
        ] {
            let call = json!({"tool_input": {"command": command}});
            assert_eq!(
                resolve(Kind::Search, &call),
                Some(Nudge::Read { path: None }),
                "{command}"
            );
        }
    }

    #[test]
    fn piped_read_into_search_is_a_search() {
        for command in [
            "cat src/main.rs | grep foo",
            "cat 'my file.rs' | rg foo",
            "cat router.rs | rg pattern; ls src; grep rand Cargo.toml",
            "cat -- router.rs | rg pattern",
            "cat router.rs - | rg pattern",
            "cd src && cat router.rs | rg pattern",
        ] {
            let call = json!({"tool_input": {"command": command}});
            assert_eq!(resolve(Kind::Search, &call), Some(Nudge::Search));
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
            assert_eq!(resolve(Kind::Search, &call), None, "{command}");
        }
    }

    #[test]
    fn source_reads_nudge_but_docs_do_not() {
        let source = json!({"tool_input": {"file_path": "/repo/src/main.rs"}});
        let expected = Nudge::Read {
            path: Some("/repo/src/main.rs".to_string()),
        };
        assert_eq!(resolve(Kind::Read, &source), Some(expected));

        for path in [
            "/repo/README.md",
            "/repo/config.yaml",
            "/repo/.env",
            "/repo/Cargo.toml",
        ] {
            let call = json!({"tool_input": {"file_path": path}});
            assert_eq!(resolve(Kind::Read, &call), None, "{path}");
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
        assert_eq!(resolve(Kind::Search, &call), Some(Nudge::Search));
    }

    #[test]
    fn nudge_text_names_the_launcher_verbs() {
        assert!(Nudge::Search.text().contains("`orbit grep"));
        let cwd = std::env::current_dir().unwrap();
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src/main.rs");
        let relative = path.strip_prefix(&cwd).unwrap().to_string_lossy();
        let read = Nudge::Read {
            path: Some(relative.to_string()),
        }
        .text();
        assert!(path.is_file());
        assert!(
            read.contains(&format!("`orbit context --file='{}'`", path.display())),
            "{read}"
        );
        assert!(!read.contains("<path>"), "{read}");
    }
}
