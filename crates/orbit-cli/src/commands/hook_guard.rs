//! Hidden `orbit hook-guard` — the Claude Code PreToolUse guard installed by `orbit setup`.
//! Reads a tool call from stdin and nudges the agent to query the graph before grepping or
//! reading source. Fails open: on any error it prints nothing and exits 0, never blocking a
//! tool call.

use std::io::Read;
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime};

use clap::ValueEnum;
use serde_json::{Value, json};

use crate::commands::setup::spec;
use crate::{telemetry, workspace};

#[derive(ValueEnum, Clone, Copy, Debug)]
pub(crate) enum Kind {
    Search,
    Read,
}

const SEARCH_COMMANDS: &[&str] = &[
    "ack", "ag", "egrep", "fd", "fgrep", "find", "grep", "rg", "ripgrep",
];

const FILE_SEARCH_COMMANDS: &[&str] = &["fd", "find"];
const READ_COMMANDS: &[&str] = &["bat", "cat", "head", "less", "more", "sed", "tail"];

const COMMAND_WRAPPERS: &[&str] = &[
    "command", "env", "git", "nice", "nohup", "sudo", "time", "xargs",
];

const SOURCE_EXTS: &[&str] = &[
    "py", "js", "cjs", "mjs", "ts", "tsx", "jsx", "vue", "svelte", "go", "rs", "java", "rb", "c",
    "h", "cpp", "hpp", "cc", "cs", "kt", "kts", "swift", "php", "scala", "lua", "sh", "pl",
];

const GRAPH_FIRST_ENV: &str = "ORBIT_GRAPH_FIRST";

const SESSION_TTL: Duration = Duration::from_secs(2 * 24 * 60 * 60);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Event {
    Deny,
    OrbitUsed,
}

impl Event {
    fn action(self) -> &'static str {
        match self {
            Event::Deny => "graph_first_deny",
            Event::OrbitUsed => "graph_first_orbit_used",
        }
    }
}

pub(crate) fn run(
    kind: Kind,
    graph_first: bool,
    tracker: Option<&orbit_analytics::SnowplowAnalyticsTracker>,
    coding_agent: Option<&str>,
) {
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
    let (response, event) = respond(kind, &call, graph_first.as_ref());
    if let Some(response) = response {
        println!("{response}");
    }
    if let (Some(tracker), Some(event)) = (tracker, event) {
        telemetry::emit_hook_guard_event(tracker, event.action(), coding_agent);
    }
}

struct GraphFirst {
    sessions: PathBuf,
    project_dir: Option<String>,
}

fn respond(
    kind: Kind,
    call: &Value,
    graph_first: Option<&GraphFirst>,
) -> (Option<Value>, Option<Event>) {
    let session = graph_first.zip(session_id(call));
    let mut event = None;
    if runs_orbit(call)
        && let Some((graph_first, id)) = &session
        && claim_session(&graph_first.sessions, id)
    {
        event = Some(Event::OrbitUsed);
    }
    if session
        .as_ref()
        .is_some_and(|(graph_first, id)| graph_first.sessions.join(id).exists())
        || !should_nudge(kind, call)
    {
        return (None, event);
    }
    if let Some((graph_first, id)) = &session
        && should_block(kind, call, graph_first)
        && claim_session(&graph_first.sessions, id)
    {
        let deny = json!({
            "hookSpecificOutput": {
                "hookEventName": "PreToolUse",
                "permissionDecision": "deny",
                "permissionDecisionReason": spec::graph_first_deny_text(),
            }
        });
        return (Some(deny), Some(Event::Deny));
    }
    if graph_first.is_some() {
        return (None, event);
    }
    let nudge = json!({
        "hookSpecificOutput": {
            "hookEventName": "PreToolUse",
            "additionalContext": nudge_text(kind),
        }
    });
    (Some(nudge), event)
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
    let base = dirs::runtime_dir()
        .or_else(dirs::cache_dir)
        .unwrap_or_else(std::env::temp_dir);
    Some(base.join("orbit-hook-sessions"))
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
    let claimed = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(dir.join(id))
        .is_ok();
    if claimed {
        prune_sessions(dir, SystemTime::now());
    }
    claimed
}

fn prune_sessions(dir: &Path, now: SystemTime) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let stale = entry
            .metadata()
            .and_then(|meta| meta.modified())
            .ok()
            .and_then(|modified| now.duration_since(modified).ok())
            .is_some_and(|age| age > SESSION_TTL);
        if stale {
            let _ = std::fs::remove_file(entry.path());
        }
    }
}

fn command_of(call: &Value) -> &str {
    call.get("tool_input")
        .unwrap_or(call)
        .get("command")
        .and_then(Value::as_str)
        .unwrap_or("")
}

fn runs_orbit(call: &Value) -> bool {
    let tool = call.get("tool_name").and_then(Value::as_str).unwrap_or("");
    tool.starts_with("mcp__orbit__") || invokes_orbit(command_of(call))
}

fn invokes_orbit(command: &str) -> bool {
    command
        .split(['|', ';', '&', '\n', '(', ')', '`'])
        .any(|segment| {
            let mut words = segment
                .split_whitespace()
                .filter(|t| !t.starts_with('-') && !t.contains('='))
                .map(basename)
                .skip_while(|word| COMMAND_WRAPPERS.contains(word));
            match words.next() {
                Some("orbit") => true,
                Some("glab") => words.next() == Some("orbit"),
                _ => false,
            }
        })
}

fn should_block(kind: Kind, call: &Value, graph_first: &GraphFirst) -> bool {
    if matches!(kind, Kind::Read) {
        return false;
    }
    let cwd = call.get("cwd").and_then(Value::as_str);
    let Some(root) = graph_first.project_dir.as_deref().or(cwd).map(Path::new) else {
        return false;
    };
    let cwd = cwd.map(Path::new).unwrap_or(root);
    if !cwd.starts_with(root) {
        return false;
    }
    let command = command_of(call);
    if !command.is_empty() {
        let paths: Vec<_> = command_paths(command, cwd).collect();
        if paths.iter().any(|path| !path.starts_with(root)) {
            return false;
        }
        if invokes_file_search(command) {
            return true;
        }
        return invokes_search(command) && !paths.iter().any(|path| path.is_file());
    }
    if call.get("tool_name").and_then(Value::as_str) == Some("Glob") {
        return true;
    }
    let path = call
        .get("tool_input")
        .unwrap_or(call)
        .get("path")
        .and_then(Value::as_str)
        .unwrap_or("");
    if path.is_empty() {
        return true;
    }
    let path = Path::new(path);
    let path = if path.is_absolute() {
        path.to_path_buf()
    } else {
        cwd.join(path)
    };
    dunce::canonicalize(path).map_or(true, |path| path.starts_with(root) && !path.is_file())
}

fn command_paths<'a>(command: &'a str, cwd: &'a Path) -> impl Iterator<Item = PathBuf> + 'a {
    let home = dirs::home_dir();
    command
        .split(|c: char| c.is_whitespace() || "|;&()`<>=".contains(c))
        .map(|token| token.trim_matches(['\'', '"']))
        .filter(|token| !token.starts_with('-'))
        .filter_map(move |token| {
            let path = match token.strip_prefix("~/") {
                Some(rest) => home.as_ref()?.join(rest),
                None if token.starts_with('/') => PathBuf::from(token),
                None => cwd.join(token),
            };
            (!path.starts_with("/dev") && path.exists()).then_some(path)
        })
}

fn invokes_file_search(command: &str) -> bool {
    command
        .split(['|', ';', '&', '\n', '(', ')', '`'])
        .any(|segment| segment_invokes(segment, FILE_SEARCH_COMMANDS))
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
            is_pattern_tool || invokes_search(command) || reads_source(command)
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

fn invokes_search(command: &str) -> bool {
    command
        .split(['|', ';', '&', '\n', '(', ')', '`'])
        .any(|segment| segment_invokes(segment, SEARCH_COMMANDS))
}

fn segment_invokes(segment: &str, commands: &[&str]) -> bool {
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
        match respond(kind, call, graph_first).0 {
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
        let mcp = json!({"session_id": "h", "tool_name": "mcp__orbit__run_sql", "tool_input": {}});
        for (kind, call, expected) in [
            (Kind::Read, read("a", "/repo/src/main.rs"), "none"),
            (Kind::Search, bash("b", "rg foo"), "deny"),
            (Kind::Read, read("b", "/repo/src/main.rs"), "none"),
            (Kind::Search, glob, "deny"),
            (Kind::Search, bash("c2", "find . -name '*.rs'"), "deny"),
            (Kind::Search, bash("d", "glab orbit grep foo"), "none"),
            (Kind::Read, read("d", "/repo/src/main.rs"), "none"),
            (Kind::Search, bash("g", "time orbit grep foo"), "none"),
            (Kind::Search, mcp, "none"),
            (Kind::Read, read("h", "/repo/src/main.rs"), "none"),
            (Kind::Read, read("e", "/elsewhere/lib.rs"), "none"),
            (
                Kind::Search,
                bash("i", "orbit context Foo && grep bar src/"),
                "none",
            ),
            (Kind::Search, bash("j", "rg foo /etc"), "none"),
            (Kind::Search, bash("k", "rg foo src 2>/dev/null"), "deny"),
            (
                Kind::Read,
                json!({"tool_input": {"file_path": "/repo/a.rs"}}),
                "none",
            ),
        ] {
            assert_eq!(decide(kind, &call, Some(&graph_first)), expected, "{call}");
        }
        assert_eq!(decide(Kind::Read, &read("f", "/repo/a.rs"), None), "nudge");
        let mixed = bash("f", "orbit context Foo && grep bar src/");
        assert_eq!(decide(Kind::Search, &mixed, None), "nudge");
        let event = |kind, call| respond(kind, &call, Some(&graph_first)).1;
        assert_eq!(
            event(Kind::Search, bash("m", "orbit grep x")),
            Some(Event::OrbitUsed)
        );
        assert_eq!(event(Kind::Search, bash("n", "rg x")), Some(Event::Deny));
        assert_eq!(event(Kind::Search, bash("n", "rg x")), None);
        prune_sessions(dir.path(), SystemTime::now() + SESSION_TTL * 2);
        assert!(std::fs::read_dir(dir.path()).unwrap().next().is_none());
        assert_eq!(
            session_id(&json!({"session_id": "../x"})).as_deref(),
            Some("___x")
        );
    }

    #[test]
    fn graph_first_allows_known_files_and_blocks_broad_searches() {
        let root = tempfile::tempdir().unwrap();
        let root_path = dunce::canonicalize(root.path()).unwrap();
        let src = root_path.join("src");
        std::fs::create_dir(&src).unwrap();
        let file = src.join("main.rs");
        std::fs::write(&file, "fn main() {}\n").unwrap();
        let graph_first = GraphFirst {
            sessions: root_path.join("sessions"),
            project_dir: Some(root_path.display().to_string()),
        };
        let bash = |id: &str, command: &str| json!({"session_id": id, "cwd": root_path, "tool_input": {"command": command}});
        assert_eq!(
            decide(
                Kind::Search,
                &bash(
                    "known",
                    &format!("cat {}; grep main {}", file.display(), file.display())
                ),
                Some(&graph_first),
            ),
            "none"
        );
        assert_eq!(
            decide(
                Kind::Search,
                &bash("known", "rg main src"),
                Some(&graph_first)
            ),
            "deny"
        );
        assert_eq!(
            decide(
                Kind::Search,
                &bash("find", "find src -name '*.rs'"),
                Some(&graph_first)
            ),
            "deny"
        );
        let native_file = json!({
            "session_id": "native-file", "tool_name": "Grep", "cwd": root_path,
            "tool_input": {"pattern": "main", "path": file},
        });
        let native_dir = json!({
            "session_id": "native-dir", "tool_name": "Grep", "cwd": root_path,
            "tool_input": {"pattern": "main", "path": src},
        });
        assert_eq!(
            decide(Kind::Search, &native_file, Some(&graph_first)),
            "none"
        );
        assert_eq!(
            decide(Kind::Search, &native_dir, Some(&graph_first)),
            "deny"
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
