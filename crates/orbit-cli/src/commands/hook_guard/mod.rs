//! Hidden `orbit hook-guard`: the agent hook installed by `orbit setup`. It reads a Claude
//! Code-shaped hook call from stdin; Codex sends that shape natively, and the OpenCode and Pi
//! adapters translate to it. In an indexed repository it nudges the agent toward the graph once
//! per search pattern per session and when it rereads unchanged source. Graph-first mode blocks
//! the first search or source read of a session that has not used Orbit. The `session` kind
//! returns session-start context for agents whose hooks cannot block. Fails open: on any error
//! it prints nothing.

mod session;
mod shell;
mod target;

use std::cell::OnceCell;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::time::SystemTime;

use clap::ValueEnum;
use serde_json::{Value, json};

use self::session::{GRAPH_MARKER, Session, marker};
use self::target::Target;
use crate::commands::setup::spec;
use crate::{telemetry, workspace};

#[derive(ValueEnum, Clone, Copy, Debug)]
pub(crate) enum Kind {
    Search,
    Read,
    Session,
}

const GRAPH_FIRST_ENV: &str = "ORBIT_GRAPH_FIRST";

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

enum Decision {
    Deny,
    Nudge(&'static str),
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
    let graph_exists = workspace::resolve_db_path(None).is_ok_and(|path| path.is_file());
    let Some(cwd) = call_cwd(&call).filter(|_| graph_exists) else {
        return;
    };
    let context = Context {
        cwd,
        session: Session::for_call(&call, SystemTime::now()),
        graph_first: graph_first_enabled(
            graph_first,
            std::env::var(GRAPH_FIRST_ENV).ok().as_deref(),
        ),
    };
    let (response, event) = respond(kind, &call, &context, &Local);
    if let Some(response) = response {
        println!("{response}");
    }
    if let (Some(tracker), Some(event)) = (tracker, event) {
        telemetry::emit_hook_guard_event(tracker, event.action(), coding_agent);
    }
}

struct Context {
    cwd: PathBuf,
    session: Option<Session>,
    graph_first: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Index {
    Indexed,
    Missing,
    Unknown,
}

trait Probe {
    fn root(&self, cwd: &Path) -> Option<PathBuf>;
    fn index(&self, root: &Path) -> Index;
}

struct Local;

impl Probe for Local {
    fn root(&self, cwd: &Path) -> Option<PathBuf> {
        workspace::git_toplevel(cwd).ok()
    }

    fn index(&self, root: &Path) -> Index {
        manifest_index(root).unwrap_or(Index::Unknown)
    }
}

fn manifest_index(root: &Path) -> anyhow::Result<Index> {
    let parent = workspace::git_info(root)?.parent_repo_path;
    let db = workspace::resolve_db_path(None)?;
    let client = duckdb_client::DuckDbClient::open_read_only(&db)?;
    let rows = client.query_arrow_json(
        "SELECT COUNT(*) AS n FROM _orbit_manifest \
         WHERE CAST(status AS VARCHAR) = 'indexed' AND (repo_path = ?1 OR parent_repo_path = ?2)",
        &[
            root.to_string_lossy().into(),
            parent.to_string_lossy().into(),
        ],
    )?;
    Ok(match duckdb_client::scalar_i64(&rows) {
        0 => Index::Missing,
        _ => Index::Indexed,
    })
}

fn respond(
    kind: Kind,
    call: &Value,
    context: &Context,
    probe: &impl Probe,
) -> (Option<Value>, Option<Event>) {
    if let Kind::Session = kind {
        let text = session_start(context, probe);
        return (text.map(|text| with_context("SessionStart", text)), None);
    }
    let mut event = None;
    if target::runs_orbit(call)
        && let Some(session) = &context.session
    {
        if session.claim(GRAPH_MARKER) && context.graph_first {
            event = Some(Event::OrbitUsed);
        }
        for term in target::orbit_grep_terms(call) {
            session.claim(&marker(&format!("s:{term}")));
        }
    }
    match decide(kind, call, context, probe) {
        Some(Decision::Deny) => (
            Some(json!({
                "hookSpecificOutput": {
                    "hookEventName": "PreToolUse",
                    "permissionDecision": "deny",
                    "permissionDecisionReason": spec::graph_first_deny_text(),
                }
            })),
            Some(Event::Deny),
        ),
        Some(Decision::Nudge(text)) => (Some(with_context("PreToolUse", text)), event),
        None => (None, event),
    }
}

fn session_start(context: &Context, probe: &impl Probe) -> Option<&'static str> {
    let root = probe.root(&context.cwd)?;
    (probe.index(&root) == Index::Indexed).then(|| spec::session_start_text(context.graph_first))
}

fn decide(kind: Kind, call: &Value, context: &Context, probe: &impl Probe) -> Option<Decision> {
    let candidates = target::candidates(kind, call);
    if candidates.is_empty() {
        return None;
    }
    let root = probe.root(&context.cwd)?;
    let target = target::first_target(candidates, &context.cwd, &root)?;
    let cached = OnceCell::new();
    let index = || *cached.get_or_init(|| probe.index(&root));
    let session = context.session.as_ref();
    if context.graph_first
        && let Some(session) = session
        && !session.has(GRAPH_MARKER)
        && index() == Index::Indexed
        && session.claim(GRAPH_MARKER)
    {
        return Some(Decision::Deny);
    }
    let (nudge, text) = match &target {
        Target::Search(key) => (
            session.is_none_or(|session| session.claim(&marker(key))),
            spec::search_nudge_text(),
        ),
        Target::Read { key, stamp } => (
            session.is_some_and(|s| s.swap(&marker(key), stamp).as_deref() == Some(stamp)),
            spec::read_nudge_text(),
        ),
    };
    (nudge && index() != Index::Missing).then_some(Decision::Nudge(text))
}

fn with_context(event: &str, text: &str) -> Value {
    json!({"hookSpecificOutput": {"hookEventName": event, "additionalContext": text}})
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

fn call_cwd(call: &Value) -> Option<PathBuf> {
    let project = std::env::var("CLAUDE_PROJECT_DIR").ok();
    let cwd = call.get("cwd").and_then(Value::as_str);
    [cwd, project.as_deref()]
        .into_iter()
        .flatten()
        .find(|dir| !dir.is_empty())
        .map(PathBuf::from)
        .or_else(|| std::env::current_dir().ok())
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Fixed(PathBuf, Index);

    impl Probe for Fixed {
        fn root(&self, cwd: &Path) -> Option<PathBuf> {
            cwd.starts_with(&self.0).then(|| self.0.clone())
        }

        fn index(&self, _: &Path) -> Index {
            self.1
        }
    }

    fn call(spec: &str) -> (Kind, Value) {
        let (tool, rest) = spec.split_once(' ').unwrap_or((spec, ""));
        let (kind, tool, input) = match tool {
            "Read" => (Kind::Read, tool, json!({"file_path": rest})),
            "Session" => (Kind::Session, tool, json!({})),
            "Grep" | "Glob" => (Kind::Search, tool, json!({"pattern": rest})),
            mcp if mcp.starts_with("mcp__") => (Kind::Search, tool, json!({})),
            _ => (Kind::Search, "Bash", json!({"command": spec})),
        };
        (kind, json!({"tool_name": tool, "tool_input": input}))
    }

    #[test]
    fn decisions() {
        let (dir, sessions) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
        let root = dunce::canonicalize(dir.path()).unwrap();
        for file in "src/main.rs src/lib.rs README.md docs/a.md node_modules/a.js".split(' ') {
            std::fs::create_dir_all(root.join(file).parent().unwrap()).unwrap();
            std::fs::write(root.join(file), "").unwrap();
        }
        let check = |id: &str, spec: &str, index| {
            let (name, now) = (id.trim_start_matches('!'), SystemTime::now());
            let context = Context {
                cwd: root.clone(),
                session: Session::open(sessions.path(), name, now).filter(|_| id != "-"),
                graph_first: id.starts_with('!'),
            };
            let (kind, call) = call(spec);
            let (out, event) = respond(kind, &call, &context, &Fixed(root.clone(), index));
            let decision = out.map_or("none", |out| match &out["hookSpecificOutput"] {
                out if out["permissionDecision"] == "deny" => "deny",
                out if out["additionalContext"] == spec::read_nudge_text() => "read",
                out if out["hookEventName"] == "SessionStart" => "start",
                _ => "search",
            });
            assert_eq!(decision == "deny", event == Some(Event::Deny), "{spec}");
            match event {
                Some(Event::OrbitUsed) => "used",
                _ => decision,
            }
        };
        for (id, spec, expected) in [
            ("-", "rg -n foo src/", "search"),
            ("-", "timeout 5 git grep foo", "search"),
            ("-", "rg foo 2>&1 | head -20", "search"),
            ("-", "echo $(rg foo src)", "search"),
            ("-", "find src -name '*.rs' | xargs grep foo", "search"),
            ("-", "Grep Foo", "search"),
            ("-", "Glob **/*.rs", "search"),
            ("-", "cargo test 2>&1 | rg FAILED", "none"),
            ("-", "echo 'x | rg foo'", "none"),
            ("-", "rg -v foo src", "none"),
            ("-", "rg 'fn\\s+\\w+' src", "none"),
            ("-", "rg foo src/main.rs", "none"),
            ("-", "rg foo docs node_modules /etc", "none"),
            ("-", "rg -g '*.md' foo", "none"),
            ("-", "python3 - <<'EOF'\nrg foo src\nEOF", "none"),
            ("-", "sed -i 's/a/b/' src/main.rs", "none"),
            ("s", "rg Foo src", "search"),
            ("s", "rg -n foo .", "none"),
            ("s", "orbit grep 'Baz|rate limit'", "none"),
            ("s", "rg 'rate limit'", "none"),
            ("s", "orbit context Foo && grep bar src/", "search"),
            ("r", "Read src/main.rs", "none"),
            ("r", "Read src/main.rs", "read"),
            ("r", "head -5 src/lib.rs", "none"),
            ("r", "head -5 src/lib.rs", "read"),
            ("!a", "Read src/main.rs", "deny"),
            ("!a", "Read src/main.rs", "none"),
            ("!a", "Read src/main.rs", "read"),
            ("!b", "cd src && rg foo", "deny"),
            ("!b", "rg foo", "search"),
            ("!c", "Glob *.rs", "deny"),
            ("!d", "glab orbit grep foo", "used"),
            ("!d", "rg foo", "none"),
            ("!d", "rg bar", "search"),
            ("!e", "mcp__orbit__run_sql", "used"),
            ("!f", "rg foo /etc", "none"),
            ("!f", "rg foo src 2>/dev/null", "deny"),
            ("!g", "Session", "start"),
            ("-", "cd src && rg foo main.rs", "none"),
            ("-", "cargo test |& rg FAILED", "none"),
            ("-", "rg foo \"$HOME/x\" ~", "none"),
            ("-", "find -L /tmp -name '*.rs'", "none"),
            ("-", "fd -e yaml", "none"),
            ("!k", "cd /tmp && rg handler", "none"),
            ("!l", "orbit sql - <<'S'\nx\nS", "used"),
            ("!l", "rg Foo src", "search"),
        ] {
            assert_eq!(check(id, spec, Index::Indexed), expected, "{id} {spec}");
        }
        assert_eq!(check("-", "rg foo", Index::Missing), "none");
        assert_eq!(check("!h", "rg foo", Index::Unknown), "search");
        assert_eq!(check("!h", "Session", Index::Unknown), "none");
        assert!(graph_first_enabled(false, Some("1")) && !graph_first_enabled(true, Some("off")));
    }
}
