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

const ESCAPED_METACHARS: [(&str, &str); 3] = [("\\|", "\u{1}"), ("\\(", "\u{2}"), ("\\)", "\u{3}")];

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
    Usage { term: String },
    InFile { term: String, path: String },
    Read { path: Option<String> },
}

impl Nudge {
    fn text(&self) -> String {
        match self {
            Self::Search => spec::nudge_search().to_string(),
            Self::Usage { term } => spec::nudge_usage().replace("{{term}}", term),
            Self::InFile { term, path } => {
                let path = workspace::absolutize(path.into())
                    .map(|p| p.to_string_lossy().into_owned())
                    .unwrap_or_else(|_| path.clone());
                spec::nudge_in_file()
                    .replace("{{term}}", term)
                    .replace("{{path}}", &shell_quote(&path))
            }
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
            let pattern = tool_input.get("pattern").and_then(Value::as_str);
            if command.is_empty() && pattern.is_some_and(|p| !p.is_empty()) {
                let path = tool_input.get("path").and_then(Value::as_str);
                let pattern = mask_escapes(pattern.unwrap_or(""));
                return Some(search_nudge(&pattern, path.into_iter()));
            }
            let command = mask_escapes(command);
            source_read_nudge(&command).or_else(|| command_search_nudge(&command))
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

fn mask_escapes(text: &str) -> String {
    ESCAPED_METACHARS
        .iter()
        .fold(text.to_string(), |acc, (escaped, mask)| {
            acc.replace(escaped, mask)
        })
}

fn invokes_search(command: &str) -> bool {
    search_segments(command).next().is_some()
}

fn search_segments(command: &str) -> impl Iterator<Item = &str> {
    command
        .split(['|', ';', '&', '\n', '(', ')', '`'])
        .filter(|segment| search_operands(segment).is_some())
}

fn search_operands(segment: &str) -> Option<Vec<&str>> {
    let mut tokens = segment.split_whitespace();
    let name = tokens
        .by_ref()
        .filter(|t| !t.starts_with('-') && !t.contains('='))
        .map(basename)
        .find(|name| !COMMAND_WRAPPERS.contains(name))?;
    SEARCH_COMMANDS.contains(&name).then(|| {
        let mut operands = Vec::new();
        let mut explicit_pattern = None;
        let mut tokens = tokens.peekable();
        while let Some(token) = tokens.next() {
            if matches!(token, "-e" | "--regexp") {
                explicit_pattern = tokens.next();
            } else if !token.starts_with('-') {
                operands.push(token);
            }
        }
        if let Some(pattern) = explicit_pattern {
            operands.insert(0, pattern);
        }
        operands
    })
}

fn command_search_nudge(command: &str) -> Option<Nudge> {
    let segment = search_segments(command).next()?;
    let operands = search_operands(segment)?;
    let mut operands = operands
        .into_iter()
        .skip_while(|t| t.chars().all(|c| c.is_ascii_digit()));
    let pattern = operands.next().unwrap_or("");
    Some(search_nudge(pattern, operands))
}

fn search_nudge<'a>(pattern: &str, targets: impl Iterator<Item = &'a str>) -> Nudge {
    let targets: Vec<&str> = targets.map(|t| t.trim_matches(['\'', '"'])).collect();
    let Some(term) = identifier_term(pattern) else {
        return Nudge::Search;
    };
    match targets.as_slice() {
        [path] if is_source_path(path) && !path.contains(['*', '?', '[', '{']) => Nudge::InFile {
            term,
            path: path.to_string(),
        },
        _ => Nudge::Usage { term },
    }
}

fn identifier_term(pattern: &str) -> Option<String> {
    let quote = pattern.chars().next().filter(|c| matches!(c, '\'' | '"'));
    if quote.is_some_and(|q| pattern.len() == 1 || !pattern.ends_with(q)) {
        return None;
    }
    let is_identifier_char = |c: char| c.is_ascii_alphanumeric() || matches!(c, '_' | '.' | ':');
    pattern
        .trim_matches(['\'', '"'])
        .split(['|', '\u{1}'])
        .map(strip_regex_anchors)
        .find(|candidate| {
            candidate
                .chars()
                .next()
                .is_some_and(|c| c.is_ascii_alphabetic() || c == '_')
                && candidate.chars().all(is_identifier_char)
        })
        .map(str::to_string)
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

fn strip_regex_anchors(alternative: &str) -> &str {
    let mut candidate = alternative;
    loop {
        let trimmed = ["^", "\\b", "\\<", "(", "\u{2}"]
            .iter()
            .fold(candidate, |acc, anchor| {
                acc.strip_prefix(anchor).unwrap_or(acc)
            });
        let trimmed = ["$", "\\b", "\\>", ")", "\u{3}", "(", "\u{2}"]
            .iter()
            .fold(trimmed, |acc, anchor| {
                acc.strip_suffix(anchor).unwrap_or(acc)
            });
        if trimmed == candidate {
            return candidate;
        }
        candidate = trimmed;
    }
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

    fn usage(term: &str) -> Option<Nudge> {
        Some(Nudge::Usage {
            term: term.to_string(),
        })
    }

    #[test]
    fn grep_tool_pattern_nudges() {
        let call = json!({"tool_input": {"pattern": "fn main"}});
        assert_eq!(resolve(Kind::Search, &call), Some(Nudge::Search));
        let call = json!({"tool_input": {"pattern": "NewSource", "path": "/repo"}});
        assert_eq!(resolve(Kind::Search, &call), usage("NewSource"));
        let call =
            json!({"tool_input": {"pattern": "WithInsecureTLS\\(", "path": "/repo/internal"}});
        assert_eq!(resolve(Kind::Search, &call), usage("WithInsecureTLS"));
        let call =
            json!({"tool_input": {"pattern": "Git", "path": "/repo/internal/config/storage.go"}});
        assert_eq!(
            resolve(Kind::Search, &call),
            Some(Nudge::InFile {
                term: "Git".into(),
                path: "/repo/internal/config/storage.go".into()
            })
        );
    }

    #[test]
    fn bash_search_commands_nudge() {
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
            assert_eq!(resolve(Kind::Search, &call), usage("foo"), "{command}");
        }
        for command in [
            "find . -name '*.rs'",
            "grep -rn 'TODO: fix' src/",
            "rg '^\\s*$' src/",
            "grep -rn 42 src/",
            "grep -rn 'a.*b' src/",
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
    fn usage_searches_name_the_first_identifier_alternative() {
        for (command, term) in [
            (
                "grep -rn \"git.NewSource\\|git.WithAuth\\|git.WithRef\" /app --include=*.go | grep -v _test.go",
                "git.NewSource",
            ),
            (
                "grep -rln \"insecure_skip_tls\\|ca_cert\" /app --include=*.go -i",
                "insecure_skip_tls",
            ),
            ("rg -n 'WithInsecureTLS\\(' internal/", "WithInsecureTLS"),
            ("grep -n -e Source -A 3 -r internal/storage", "Source"),
            ("rg -A 3 Source internal/storage", "Source"),
            (
                "grep -rn \"TLS\\|CA\\|insecure\" -ri /app/internal/storage/fs/git/*_test.go",
                "TLS",
            ),
        ] {
            let call = json!({"tool_input": {"command": command}});
            assert_eq!(resolve(Kind::Search, &call), usage(term), "{command}");
        }
    }

    #[test]
    fn single_source_file_searches_name_the_file() {
        for (command, term, path) in [
            (
                "grep -n \"Git\" /app/internal/config/storage.go | head -80",
                "Git",
                "/app/internal/config/storage.go",
            ),
            (
                "grep -n 'func\\|type' internal/cmd/grpc.go",
                "func",
                "internal/cmd/grpc.go",
            ),
            ("rg NewSource source.go", "NewSource", "source.go"),
        ] {
            let call = json!({"tool_input": {"command": command}});
            let expected = Nudge::InFile {
                term: term.into(),
                path: path.into(),
            };
            assert_eq!(resolve(Kind::Search, &call), Some(expected), "{command}");
        }
        let call = json!({"tool_input": {"command": "grep -n Git go.mod"}});
        assert_eq!(resolve(Kind::Search, &call), usage("Git"));
    }

    #[test]
    fn specific_nudges_render_term_and_quoted_path() {
        let text = Nudge::Usage {
            term: "git.NewSource".into(),
        }
        .text();
        assert!(
            text.contains("`orbit context \"git.NewSource\" --related`"),
            "{text}"
        );
        assert!(text.contains("`orbit grep \"git.NewSource\"`"), "{text}");
        assert!(!text.contains("{{"), "{text}");
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("it's.go");
        std::fs::write(&path, "package x\n").unwrap();
        let text = Nudge::InFile {
            term: "Git".into(),
            path: path.to_string_lossy().into_owned(),
        }
        .text();
        let quoted = format!("'{}/it'\\''s.go'", dir.path().display());
        assert!(text.contains(&format!("--file={quoted}")), "{text}");
        assert!(
            text.contains(&format!("`orbit grep \"Git\" --path {quoted}`")),
            "{text}"
        );
        assert!(!text.contains("{{"), "{text}");
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
            assert!(
                matches!(resolve(Kind::Search, &call), Some(Nudge::Usage { .. })),
                "{command}"
            );
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
        assert_eq!(resolve(Kind::Search, &call), usage("foo"));
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
