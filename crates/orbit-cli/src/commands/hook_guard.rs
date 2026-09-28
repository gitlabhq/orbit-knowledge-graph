//! Hidden `orbit hook-guard`: the agent hook installed by `orbit setup`. It reads a
//! Claude Code-shaped hook call from stdin; Codex sends that shape natively, and the OpenCode and
//! Pi adapters translate to it. Inside an indexed repository it nudges the agent toward the graph
//! once per search pattern per session and when it rereads unchanged source. Graph-first mode
//! blocks the first search or source read of a session that has not used Orbit. The `session`
//! kind returns session-start context for agents whose hooks cannot block. Fails open: on any
//! error it prints nothing and exits 0.

use std::cell::OnceCell;
use std::hash::{Hash, Hasher};
use std::io::Read;
use std::path::{Component, Path, PathBuf};
use std::time::UNIX_EPOCH;

use clap::ValueEnum;
use serde_json::{Value, json};

use crate::commands::setup::spec;
use crate::workspace;

#[derive(ValueEnum, Clone, Copy, Debug)]
pub(crate) enum Kind {
    Search,
    Read,
    Session,
}

const CONTENT_SEARCH_COMMANDS: &[&str] = &[
    "ack", "ag", "egrep", "fgrep", "grep", "rg", "ripgrep", "ugrep",
];

const READ_COMMANDS: &[&str] = &["bat", "cat", "head", "less", "more", "sed", "tail"];

const COMMAND_WRAPPERS: &[&str] = &[
    "command", "env", "nice", "nohup", "sudo", "time", "timeout", "xargs",
];

const SOURCE_EXTS: &[&str] = &[
    "py", "js", "cjs", "mjs", "ts", "tsx", "jsx", "vue", "svelte", "go", "rs", "java", "rb", "c",
    "h", "cpp", "hpp", "cc", "cs", "kt", "kts", "swift", "php", "scala", "lua", "sh", "pl",
];

const VENDORED_DIRS: &[&str] = &["node_modules", "target", "vendor", "dist", "build", ".git"];

const DOC_DIRS: &[&str] = &["doc", "docs", "documentation", ".github", ".gitlab"];

const NON_CODE_TYPES: &[&str] = &[
    "config", "css", "csv", "docker", "html", "json", "lock", "make", "markdown", "md", "sql",
    "toml", "txt", "xml", "yaml", "yml",
];

const SEARCH_VALUE_FLAGS: &[&str] = &[
    "--after-context",
    "--before-context",
    "--color",
    "--colors",
    "--context",
    "--exclude",
    "--exclude-dir",
    "--file",
    "--glob",
    "--iglob",
    "--include",
    "--max-columns",
    "--max-count",
    "--max-depth",
    "--regexp",
    "--sort",
    "--sortr",
    "--threads",
    "--type",
    "--type-not",
];

const SEARCH_SHORT_VALUE_FLAGS: &str = "ABCMTdefgjmt";

const FD_VALUE_FLAGS: &[&str] = &[
    "-E",
    "-S",
    "-d",
    "-e",
    "-t",
    "--exclude",
    "--extension",
    "--max-depth",
    "--size",
    "--type",
];

const FIND_NAME_TESTS: &[&str] = &[
    "-iname",
    "-ipath",
    "-iwholename",
    "-name",
    "-path",
    "-wholename",
];

const ORBIT_GREP_VALUE_FLAGS: &[&str] = &[
    "-F", "--db", "--format", "--kind", "--limit", "--path", "--repo",
];

const GRAPH_FIRST_ENV: &str = "ORBIT_GRAPH_FIRST";

const GRAPH_MARKER: &str = "graph";

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
    let Some(cwd) = call_cwd(&call) else {
        return;
    };
    let context = Context {
        cwd,
        session: session_id(&call).and_then(|id| Session::open(&session_dir(), &id)),
        graph_first: graph_first_enabled(
            graph_first,
            std::env::var(GRAPH_FIRST_ENV).ok().as_deref(),
        ),
    };
    if let Some(response) = respond(kind, &call, &context, &Local) {
        println!("{response}");
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

fn respond(kind: Kind, call: &Value, context: &Context, probe: &impl Probe) -> Option<Value> {
    if let Kind::Session = kind {
        let root = probe.root(&context.cwd)?;
        return (probe.index(&root) == Index::Indexed).then(|| {
            json!({
                "hookSpecificOutput": {
                    "hookEventName": "SessionStart",
                    "additionalContext": spec::session_start_text(context.graph_first),
                }
            })
        });
    }
    let session = context.session.as_ref();
    if runs_orbit(call) {
        if let Some(session) = session {
            session.claim(GRAPH_MARKER);
            for term in orbit_grep_terms(command_of(call)) {
                session.claim(&marker(&format!("s:{term}")));
            }
        }
        return None;
    }
    let candidates = candidates(kind, call);
    if candidates.is_empty() {
        return None;
    }
    let root = probe.root(&context.cwd)?;
    let scope = Scope {
        cwd: &context.cwd,
        root: &root,
    };
    let event = candidates
        .into_iter()
        .find_map(|candidate| scope.admit(candidate))?;
    let cached = OnceCell::new();
    let index = || *cached.get_or_init(|| probe.index(&root));
    if context.graph_first
        && let Some(session) = session
        && !session.has(GRAPH_MARKER)
        && index() == Index::Indexed
        && session.claim(GRAPH_MARKER)
    {
        return Some(json!({
            "hookSpecificOutput": {
                "hookEventName": "PreToolUse",
                "permissionDecision": "deny",
                "permissionDecisionReason": spec::graph_first_deny_text(),
            }
        }));
    }
    let (nudge, text) = match &event {
        Event::Search(key) => (
            session.is_none_or(|session| session.claim(&marker(key))),
            spec::search_nudge_text(),
        ),
        Event::Read { key, stamp } => (
            session.is_some_and(|session| {
                session.swap(&marker(key), stamp).as_deref() == Some(stamp.as_str())
            }),
            spec::read_nudge_text(),
        ),
    };
    (nudge && index() != Index::Missing).then(|| {
        json!({
            "hookSpecificOutput": {
                "hookEventName": "PreToolUse",
                "additionalContext": text,
            }
        })
    })
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
    call.get("cwd")
        .and_then(Value::as_str)
        .filter(|dir| !dir.is_empty())
        .map(PathBuf::from)
        .or_else(|| {
            std::env::var("CLAUDE_PROJECT_DIR")
                .ok()
                .filter(|dir| !dir.is_empty())
                .map(PathBuf::from)
        })
        .or_else(|| std::env::current_dir().ok())
}

fn session_dir() -> PathBuf {
    dirs::runtime_dir()
        .unwrap_or_else(std::env::temp_dir)
        .join("orbit-hook-sessions")
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

struct Session {
    dir: PathBuf,
}

impl Session {
    fn open(base: &Path, id: &str) -> Option<Self> {
        let dir = base.join(id);
        std::fs::create_dir_all(&dir).ok()?;
        Some(Self { dir })
    }

    fn has(&self, name: &str) -> bool {
        self.dir.join(name).exists()
    }

    fn claim(&self, name: &str) -> bool {
        std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(self.dir.join(name))
            .is_ok()
    }

    fn swap(&self, name: &str, value: &str) -> Option<String> {
        let path = self.dir.join(name);
        let previous = std::fs::read_to_string(&path).ok();
        std::fs::write(&path, value).ok()?;
        previous
    }
}

fn marker(key: &str) -> String {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    key.hash(&mut hasher);
    format!("m-{:016x}", hasher.finish())
}

fn command_of(call: &Value) -> &str {
    tool_input(call)
        .get("command")
        .and_then(Value::as_str)
        .unwrap_or("")
}

fn tool_input(call: &Value) -> &Value {
    call.get("tool_input").unwrap_or(call)
}

fn input_str<'a>(call: &'a Value, key: &str) -> &'a str {
    tool_input(call)
        .get(key)
        .and_then(Value::as_str)
        .unwrap_or("")
}

fn runs_orbit(call: &Value) -> bool {
    let tool = call.get("tool_name").and_then(Value::as_str).unwrap_or("");
    tool.starts_with("mcp__orbit__") || !orbit_invocations(command_of(call)).is_empty()
}

fn orbit_invocations(command: &str) -> Vec<Vec<String>> {
    split_shell(command)
        .unwrap_or_default()
        .into_iter()
        .flatten()
        .filter_map(|stage| {
            let (_, words) = strip_wrappers(&stage);
            let rest = match words {
                [first, rest @ ..] if basename(first) == "orbit" => rest,
                [first, second, rest @ ..] if basename(first) == "glab" && second == "orbit" => {
                    rest
                }
                _ => return None,
            };
            Some(rest.to_vec())
        })
        .collect()
}

fn orbit_grep_terms(command: &str) -> Vec<String> {
    let mut terms = Vec::new();
    for args in orbit_invocations(command) {
        let args = match args.first().map(String::as_str) {
            Some("local" | "remote") => &args[1..],
            _ => &args[..],
        };
        let Some(("grep", rest)) = args.split_first().map(|(verb, rest)| (verb.as_str(), rest))
        else {
            continue;
        };
        let mut words = rest.iter();
        while let Some(word) = words.next() {
            if word.starts_with('-') {
                if ORBIT_GREP_VALUE_FLAGS.contains(&word.as_str()) {
                    words.next();
                }
                continue;
            }
            let query = word.to_lowercase();
            for alternative in query.split('|').map(str::trim).filter(|a| !a.is_empty()) {
                terms.extend(alternative.split_whitespace().map(str::to_string));
                terms.push(alternative.to_string());
            }
            terms.push(query);
            break;
        }
    }
    terms
}

enum Candidate {
    Content {
        pattern: String,
        paths: Vec<String>,
        globs: Vec<String>,
    },
    Files {
        patterns: Vec<String>,
        paths: Vec<String>,
    },
    Read {
        key: String,
        paths: Vec<String>,
    },
}

enum Event {
    Search(String),
    Read { key: String, stamp: String },
}

fn candidates(kind: Kind, call: &Value) -> Vec<Candidate> {
    let tool = call.get("tool_name").and_then(Value::as_str).unwrap_or("");
    match kind {
        Kind::Session => Vec::new(),
        Kind::Read => {
            let path = input_str(call, "file_path");
            let input = tool_input(call);
            let key = format!(
                "{path}|{}|{}",
                input.get("offset").unwrap_or(&Value::Null),
                input.get("limit").unwrap_or(&Value::Null)
            );
            vec![Candidate::Read {
                key,
                paths: vec![path.to_string()],
            }]
        }
        Kind::Search => {
            let command = command_of(call);
            if !command.is_empty() {
                return bash_candidates(command);
            }
            let pattern = input_str(call, "pattern");
            if pattern.is_empty() {
                return Vec::new();
            }
            let paths: Vec<String> = Some(input_str(call, "path"))
                .filter(|path| !path.is_empty())
                .map(str::to_string)
                .into_iter()
                .collect();
            if tool == "Glob" {
                return vec![Candidate::Files {
                    patterns: vec![pattern.to_string()],
                    paths,
                }];
            }
            if NON_CODE_TYPES.contains(&input_str(call, "type")) {
                return Vec::new();
            }
            vec![Candidate::Content {
                pattern: pattern.to_string(),
                paths,
                globs: Some(input_str(call, "glob"))
                    .filter(|glob| !glob.is_empty())
                    .map(str::to_string)
                    .into_iter()
                    .collect(),
            }]
        }
    }
}

fn bash_candidates(command: &str) -> Vec<Candidate> {
    split_shell(command)
        .unwrap_or_default()
        .iter()
        .filter_map(|stages| {
            stages
                .iter()
                .enumerate()
                .find_map(|(index, stage)| stage_candidate(index, stage))
        })
        .collect()
}

fn stage_candidate(index: usize, stage: &[String]) -> Option<Candidate> {
    let (via_xargs, words) = strip_wrappers(stage);
    if index > 0 && !via_xargs {
        return None;
    }
    let (name, args) = words.split_first()?;
    match basename(name) {
        "git" if args.first().is_some_and(|verb| verb == "grep") => content_search(&args[1..]),
        "find" => Some(find_search(args)),
        "fd" | "fdfind" => Some(fd_search(args)),
        name if CONTENT_SEARCH_COMMANDS.contains(&name) => content_search(args),
        "sed"
            if args
                .iter()
                .any(|arg| arg.starts_with("-i") || arg.starts_with("--in-place")) =>
        {
            None
        }
        name if READ_COMMANDS.contains(&name) => Some(Candidate::Read {
            key: words.join(" "),
            paths: args
                .iter()
                .filter(|arg| !arg.starts_with('-'))
                .cloned()
                .collect(),
        }),
        _ => None,
    }
}

fn strip_wrappers(words: &[String]) -> (bool, &[String]) {
    let mut via_xargs = false;
    let mut in_wrapper = false;
    let mut start = 0;
    for word in words {
        let name = basename(word);
        if COMMAND_WRAPPERS.contains(&name) {
            via_xargs |= name == "xargs";
            in_wrapper = true;
        } else if !is_assignment(word)
            && !(in_wrapper && (word.starts_with('-') || is_duration(word)))
        {
            break;
        }
        start += 1;
    }
    (via_xargs, &words[start..])
}

fn is_assignment(word: &str) -> bool {
    word.split_once('=').is_some_and(|(name, _)| {
        name.chars()
            .next()
            .is_some_and(|c| c.is_ascii_alphabetic() || c == '_')
            && name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
    })
}

fn is_duration(word: &str) -> bool {
    let digits = word.trim_end_matches(['s', 'm', 'h', 'd']);
    !digits.is_empty() && digits.chars().all(|c| c.is_ascii_digit() || c == '.')
}

fn content_search(args: &[String]) -> Option<Candidate> {
    let mut pattern = None;
    let mut positional = Vec::new();
    let mut globs = Vec::new();
    let mut lists_files = false;
    let mut words = args.iter();
    while let Some(word) = words.next() {
        if word == "--" {
            positional.extend(words.by_ref().cloned());
            break;
        }
        let (flag, value) = if let Some(long) = word.strip_prefix("--") {
            match long {
                "invert-match" => return None,
                "files" => {
                    lists_files = true;
                    continue;
                }
                _ => {}
            }
            match word.split_once('=') {
                Some((flag, value)) => (flag.to_string(), Some(value.to_string())),
                None if SEARCH_VALUE_FLAGS.contains(&word.as_str()) => {
                    (word.clone(), words.next().cloned())
                }
                None => continue,
            }
        } else if let Some(short) = word.strip_prefix('-').filter(|short| !short.is_empty()) {
            if short.contains('v') && !short.contains(|c| SEARCH_SHORT_VALUE_FLAGS.contains(c)) {
                return None;
            }
            let Some(at) = short.find(|c| SEARCH_SHORT_VALUE_FLAGS.contains(c)) else {
                continue;
            };
            if short[..at].contains('v') {
                return None;
            }
            let rest = &short[at + 1..];
            let value = match rest.is_empty() {
                true => words.next().cloned(),
                false => Some(rest.to_string()),
            };
            (format!("-{}", &short[at..at + 1]), value)
        } else {
            positional.push(word.clone());
            continue;
        };
        let Some(value) = value else {
            continue;
        };
        match flag.as_str() {
            "-e" | "--regexp" => pattern = pattern.or(Some(value)),
            "-g" | "--glob" | "--iglob" | "--include" => globs.push(value),
            "-t" | "--type" if NON_CODE_TYPES.contains(&value.as_str()) => return None,
            _ => {}
        }
    }
    if lists_files {
        return Some(Candidate::Files {
            patterns: globs,
            paths: positional,
        });
    }
    let (pattern, paths) = match pattern {
        Some(pattern) => (pattern, positional),
        None if !positional.is_empty() => {
            let pattern = positional.remove(0);
            (pattern, positional)
        }
        None => return None,
    };
    Some(Candidate::Content {
        pattern,
        paths,
        globs,
    })
}

fn find_search(args: &[String]) -> Candidate {
    let mut paths = Vec::new();
    let mut patterns = Vec::new();
    let mut in_expression = false;
    let mut words = args.iter();
    while let Some(word) = words.next() {
        if word.starts_with('-') || word == "(" || word == "!" {
            in_expression = true;
            if FIND_NAME_TESTS.contains(&word.as_str())
                && let Some(pattern) = words.next()
            {
                patterns.push(pattern.clone());
            }
        } else if !in_expression {
            paths.push(word.clone());
        }
    }
    Candidate::Files { patterns, paths }
}

fn fd_search(args: &[String]) -> Candidate {
    let mut positional = Vec::new();
    let mut extensions = Vec::new();
    let mut words = args.iter();
    while let Some(word) = words.next() {
        if !word.starts_with('-') {
            positional.push(word.clone());
        } else if FD_VALUE_FLAGS.contains(&word.as_str())
            && let Some(value) = words.next()
            && matches!(word.as_str(), "-e" | "--extension")
        {
            extensions.push(format!("*.{value}"));
        }
    }
    let paths = positional.split_off(positional.len().min(1));
    let patterns = match extensions.is_empty() {
        true => positional,
        false => extensions,
    };
    Candidate::Files { patterns, paths }
}

struct Scope<'a> {
    cwd: &'a Path,
    root: &'a Path,
}

impl Scope<'_> {
    fn admit(&self, candidate: Candidate) -> Option<Event> {
        match candidate {
            Candidate::Content {
                pattern,
                paths,
                globs,
            } => {
                let known_files = !paths.is_empty() && paths.iter().all(|p| self.is_file(p));
                let admitted = graph_searchable(&pattern)
                    && (globs.is_empty() || globs.iter().any(|glob| glob_is_code(glob)))
                    && paths.iter().all(|p| self.contains(p) && code_or_dir(p))
                    && !known_files;
                admitted.then(|| Event::Search(format!("s:{}", pattern.to_lowercase())))
            }
            Candidate::Files { patterns, paths } => {
                let admitted = paths.iter().all(|p| self.contains(p))
                    && (patterns.is_empty() || patterns.iter().any(|p| glob_is_code(p)));
                admitted.then(|| Event::Search(format!("f:{}", patterns.join(" ").to_lowercase())))
            }
            Candidate::Read { key, paths } => {
                let sources: Vec<&String> = paths
                    .iter()
                    .filter(|p| is_source_path(p) && self.contains(p))
                    .collect();
                if sources.is_empty() {
                    return None;
                }
                let stamps: Option<Vec<String>> =
                    sources.iter().map(|p| self.modified(p)).collect();
                Some(Event::Read {
                    key: format!("r:{key}"),
                    stamp: stamps?.join(","),
                })
            }
        }
    }

    fn resolve(&self, path: &str) -> PathBuf {
        let path = match path.strip_prefix("~/").zip(dirs::home_dir()) {
            Some((rest, home)) => home.join(rest),
            None => PathBuf::from(path),
        };
        let path = match path.is_absolute() {
            true => path,
            false => self.cwd.join(path),
        };
        dunce::canonicalize(&path).unwrap_or_else(|_| normalize(&path))
    }

    fn contains(&self, path: &str) -> bool {
        let resolved = self.resolve(path);
        let Ok(relative) = resolved.strip_prefix(self.root) else {
            return false;
        };
        let mut components = relative.components().map(Component::as_os_str);
        let top_level_docs = relative
            .components()
            .next()
            .is_some_and(|first| DOC_DIRS.iter().any(|dir| first.as_os_str() == *dir));
        !top_level_docs && !components.any(|part| VENDORED_DIRS.iter().any(|dir| part == *dir))
    }

    fn is_file(&self, path: &str) -> bool {
        self.resolve(path).is_file()
    }

    fn modified(&self, path: &str) -> Option<String> {
        let modified = std::fs::metadata(self.resolve(path))
            .ok()?
            .modified()
            .ok()?;
        Some(
            modified
                .duration_since(UNIX_EPOCH)
                .ok()?
                .as_nanos()
                .to_string(),
        )
    }
}

fn normalize(path: &Path) -> PathBuf {
    path.components().fold(PathBuf::new(), |mut normal, part| {
        match part {
            Component::ParentDir => {
                normal.pop();
            }
            Component::CurDir => {}
            part => normal.push(part),
        }
        normal
    })
}

fn graph_searchable(pattern: &str) -> bool {
    let pattern = pattern.replace("\\|", "|").replace("\\b", "");
    pattern.chars().filter(|c| c.is_alphanumeric()).count() >= 3
        && pattern
            .chars()
            .all(|c| c.is_alphanumeric() || " _-.:|".contains(c))
}

fn glob_is_code(glob: &str) -> bool {
    if glob.starts_with('!') {
        return true;
    }
    let name = glob.rsplit('/').next().unwrap_or(glob);
    let exts: Vec<&str> = match name.split_once('{') {
        Some((_, braced)) => braced.trim_end_matches('}').split(',').collect(),
        None => name
            .rsplit_once('.')
            .map(|(_, ext)| ext)
            .into_iter()
            .collect(),
    };
    exts.is_empty()
        || exts.iter().any(|ext| {
            let ext = ext.trim_start_matches('.').to_ascii_lowercase();
            ext.contains('*') || SOURCE_EXTS.contains(&ext.as_str())
        })
}

fn code_or_dir(path: &str) -> bool {
    let name = path.rsplit('/').next().unwrap_or(path);
    match name.rsplit_once('.') {
        Some((stem, ext)) if !stem.is_empty() => glob_is_code(&format!("*.{ext}")),
        _ => true,
    }
}

fn split_shell(command: &str) -> Option<Vec<Vec<Vec<String>>>> {
    let mut lexer = Lexer::default();
    let mut chars = command.chars().peekable();
    while let Some(c) = chars.next() {
        match c {
            '\\' => match chars.next() {
                Some('\n') | None => {}
                Some(escaped) => lexer.push(escaped),
            },
            '\'' => {
                lexer.quoted();
                for quoted in chars.by_ref() {
                    if quoted == '\'' {
                        break;
                    }
                    lexer.push(quoted);
                }
            }
            '"' => {
                lexer.quoted();
                while let Some(quoted) = chars.next() {
                    match quoted {
                        '"' => break,
                        '\\' if chars.peek().is_some_and(|n| "\"\\$`".contains(*n)) => {
                            lexer.push(chars.next().unwrap_or('\\'));
                        }
                        quoted => lexer.push(quoted),
                    }
                }
            }
            ' ' | '\t' | '\r' => lexer.end_word(),
            '|' if chars.peek() != Some(&'|') => lexer.end_stage(),
            '|' | '&' | ';' | '\n' | '(' | ')' | '`' => {
                if (c == '|' || c == '&') && chars.peek() == Some(&c) {
                    chars.next();
                }
                lexer.end_statement();
            }
            '$' if chars.peek() == Some(&'(') => lexer.end_word(),
            '<' | '>' => {
                if c == '<' && chars.peek() == Some(&'<') {
                    return None;
                }
                lexer.redirect();
                while chars.peek().is_some_and(|n| matches!(n, '>' | '&' | '|')) {
                    chars.next();
                }
                let mut attached = false;
                while chars
                    .peek()
                    .is_some_and(|n| !n.is_whitespace() && !"|;&()`<>".contains(*n))
                {
                    attached = true;
                    chars.next();
                }
                lexer.skip_next = !attached;
            }
            c => lexer.push(c),
        }
    }
    lexer.end_statement();
    Some(lexer.statements)
}

#[derive(Default)]
struct Lexer {
    statements: Vec<Vec<Vec<String>>>,
    stages: Vec<Vec<String>>,
    words: Vec<String>,
    word: String,
    in_word: bool,
    skip_next: bool,
}

impl Lexer {
    fn push(&mut self, c: char) {
        self.word.push(c);
        self.in_word = true;
    }

    fn quoted(&mut self) {
        self.in_word = true;
    }

    fn redirect(&mut self) {
        if self.in_word && self.word.chars().all(|c| c.is_ascii_digit()) {
            self.word.clear();
            self.in_word = false;
        }
        self.end_word();
    }

    fn end_word(&mut self) {
        if !self.in_word {
            return;
        }
        let word = std::mem::take(&mut self.word);
        self.in_word = false;
        match std::mem::take(&mut self.skip_next) {
            true => {}
            false => self.words.push(word),
        }
    }

    fn end_stage(&mut self) {
        self.end_word();
        self.skip_next = false;
        if !self.words.is_empty() {
            self.stages.push(std::mem::take(&mut self.words));
        }
    }

    fn end_statement(&mut self) {
        self.end_stage();
        if !self.stages.is_empty() {
            self.statements.push(std::mem::take(&mut self.stages));
        }
    }
}

fn local_graph_exists() -> bool {
    workspace::resolve_db_path(None)
        .map(|path| path.is_file())
        .unwrap_or(false)
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

    struct Fixed {
        root: PathBuf,
        index: Index,
    }

    impl Probe for Fixed {
        fn root(&self, cwd: &Path) -> Option<PathBuf> {
            cwd.starts_with(&self.root).then(|| self.root.clone())
        }

        fn index(&self, _root: &Path) -> Index {
            self.index
        }
    }

    struct Repo {
        _dir: tempfile::TempDir,
        root: PathBuf,
        sessions: tempfile::TempDir,
    }

    impl Repo {
        fn new() -> Self {
            let dir = tempfile::tempdir().unwrap();
            let root = dunce::canonicalize(dir.path()).unwrap();
            for file in [
                "src/main.rs",
                "src/lib.rs",
                "README.md",
                "docs/guide.md",
                "node_modules/pkg/index.js",
            ] {
                let path = root.join(file);
                std::fs::create_dir_all(path.parent().unwrap()).unwrap();
                std::fs::write(path, "fn main() {}\n").unwrap();
            }
            Self {
                _dir: dir,
                root,
                sessions: tempfile::tempdir().unwrap(),
            }
        }

        fn context(&self, session: Option<&str>, graph_first: bool) -> Context {
            Context {
                cwd: self.root.clone(),
                session: session.and_then(|id| Session::open(self.sessions.path(), id)),
                graph_first,
            }
        }

        fn decide(
            &self,
            kind: Kind,
            call: &Value,
            context: &Context,
            index: Index,
        ) -> &'static str {
            let probe = Fixed {
                root: self.root.clone(),
                index,
            };
            match respond(kind, call, context, &probe) {
                None => "none",
                Some(out) if out["hookSpecificOutput"]["permissionDecision"] == "deny" => "deny",
                Some(out)
                    if out["hookSpecificOutput"]["additionalContext"]
                        == spec::read_nudge_text() =>
                {
                    "read"
                }
                Some(_) => "search",
            }
        }
    }

    fn bash(command: &str) -> Value {
        json!({"tool_name": "Bash", "tool_input": {"command": command}})
    }

    fn read(path: &Path) -> Value {
        json!({"tool_name": "Read", "tool_input": {"file_path": path}})
    }

    #[test]
    fn bash_code_searches_nudge() {
        let repo = Repo::new();
        let context = repo.context(None, false);
        for command in [
            "rg -n foo src/",
            "grep -r foo .",
            "find . -name '*.rs'",
            "sudo rg foo",
            "xargs -n1 grep foo",
            "/usr/bin/rg foo",
            "git grep foo",
            "RUST_LOG=debug rg foo",
            "timeout 5 rg foo",
            "cd src && rg -n 'rate limit'",
            "rg -g '*.rs' foo_bar",
            "grep -rn 'foo\\|bar' src",
            "rg foo 2>&1 | head -20",
            "find src -name '*.rs' | xargs grep handle_call",
            "rg -e foo -- src",
            "fd hook_guard",
            "rg --files | rg hook",
            "rg -g '!*.md' foo",
        ] {
            assert_eq!(
                repo.decide(Kind::Search, &bash(command), &context, Index::Indexed),
                "search",
                "{command}"
            );
        }
    }

    #[test]
    fn filters_and_text_searches_pass_untouched() {
        let repo = Repo::new();
        let context = repo.context(None, false);
        for command in [
            "cargo build",
            "ls -la",
            "git status",
            "git log --grep=foo",
            "git commit -m 'grep foo'",
            "echo 'x | rg foo'",
            "cat x.txt | grep foo",
            "cargo test 2>&1 | rg FAILED",
            "rg -v foo src",
            "grep -rv foo src",
            "rg 'fn\\s+\\w+' src",
            "rg ab",
            "rg foo src/main.rs",
            "rg foo README.md",
            "rg foo docs",
            "rg foo node_modules",
            "rg foo /etc",
            "rg -t md foo",
            "rg --type=yaml foo",
            "rg -g '*.md' foo",
            "rg -g '*.md' -g '*.yml' foo",
            "find . -name '*.md'",
            "fd -e yaml config",
            "python3 - <<'EOF'\nimport re  # rg foo src\nEOF",
            "sed -i 's/a/b/' src/main.rs",
            "cat README.md",
            "cat src/main.rs",
        ] {
            assert_eq!(
                repo.decide(Kind::Search, &bash(command), &context, Index::Indexed),
                "none",
                "{command}"
            );
        }
    }

    #[test]
    fn claude_tools_follow_the_same_filters() {
        let repo = Repo::new();
        let context = repo.context(None, false);
        let grep = |input: Value| json!({"tool_name": "Grep", "tool_input": input});
        let glob = |pattern: &str| json!({"tool_name": "Glob", "tool_input": {"pattern": pattern}});
        for (call, expected) in [
            (grep(json!({"pattern": "fn main"})), "search"),
            (grep(json!({"pattern": "Foo", "path": "src"})), "search"),
            (
                grep(json!({"pattern": "Foo", "path": "src/main.rs"})),
                "none",
            ),
            (grep(json!({"pattern": "Foo", "glob": "*.md"})), "none"),
            (grep(json!({"pattern": "Foo", "type": "yaml"})), "none"),
            (
                grep(json!({"pattern": "Foo", "path": "/elsewhere"})),
                "none",
            ),
            (grep(json!({"pattern": "impl\\s+Foo"})), "none"),
            (glob("**/*.rs"), "search"),
            (glob("**/*.md"), "none"),
            (json!({"pattern": "foo"}), "search"),
        ] {
            assert_eq!(
                repo.decide(Kind::Search, &call, &context, Index::Indexed),
                expected,
                "{call}"
            );
        }
    }

    #[test]
    fn nudges_only_in_indexed_repositories() {
        let repo = Repo::new();
        let context = repo.context(None, false);
        let call = bash("rg foo");
        assert_eq!(
            repo.decide(Kind::Search, &call, &context, Index::Missing),
            "none"
        );
        assert_eq!(
            repo.decide(Kind::Search, &call, &context, Index::Unknown),
            "search"
        );
        let outside = Context {
            cwd: std::env::temp_dir(),
            ..repo.context(None, false)
        };
        assert_eq!(
            repo.decide(Kind::Search, &call, &outside, Index::Indexed),
            "none"
        );
    }

    #[test]
    fn search_nudges_once_per_pattern_and_skip_patterns_orbit_answered() {
        let repo = Repo::new();
        let context = repo.context(Some("s"), false);
        for (command, expected) in [
            ("rg Foo src", "search"),
            ("rg -n foo .", "none"),
            ("rg Bar", "search"),
            ("orbit grep 'Baz|rate limit' --path src", "none"),
            ("rg baz", "none"),
            ("rg 'rate limit'", "none"),
            ("rg rate", "none"),
            ("rg qux", "search"),
        ] {
            assert_eq!(
                repo.decide(Kind::Search, &bash(command), &context, Index::Indexed),
                expected,
                "{command}"
            );
        }
    }

    #[test]
    fn read_nudges_only_on_unchanged_rereads() {
        let repo = Repo::new();
        let context = repo.context(Some("r"), false);
        let main = repo.root.join("src/main.rs");
        let decide = |call: &Value| repo.decide(Kind::Read, call, &context, Index::Indexed);
        assert_eq!(decide(&read(&main)), "none");
        assert_eq!(decide(&read(&main)), "read");
        let ranged = json!({"tool_name": "Read", "tool_input": {"file_path": main, "offset": 10}});
        assert_eq!(decide(&ranged), "none");
        let later = std::time::SystemTime::now() + std::time::Duration::from_secs(60);
        std::fs::File::options()
            .write(true)
            .open(&main)
            .unwrap()
            .set_modified(later)
            .unwrap();
        assert_eq!(decide(&read(&main)), "none");
        assert_eq!(decide(&read(&main)), "read");
        assert_eq!(decide(&read(&repo.root.join("README.md"))), "none");
        assert_eq!(decide(&read(&repo.root.join("README.md"))), "none");
        assert_eq!(decide(&read(Path::new("/elsewhere/lib.rs"))), "none");
        let search =
            |command: &str| repo.decide(Kind::Search, &bash(command), &context, Index::Indexed);
        assert_eq!(search("head -50 src/lib.rs"), "none");
        assert_eq!(search("head -50 src/lib.rs"), "read");
        let sessionless = repo.context(None, false);
        assert_eq!(
            repo.decide(Kind::Read, &read(&main), &sessionless, Index::Indexed),
            "none"
        );
    }

    #[test]
    fn graph_first_blocks_once_per_session_unless_orbit_ran_first() {
        let repo = Repo::new();
        let main = repo.root.join("src/main.rs");
        let mcp = json!({"tool_name": "mcp__orbit__run_sql", "tool_input": {}});
        let glob = json!({"tool_name": "Glob", "tool_input": {"pattern": "*.rs"}});
        let cases: &[(&str, Kind, Value, Index, &str)] = &[
            ("a", Kind::Read, read(&main), Index::Indexed, "deny"),
            ("a", Kind::Read, read(&main), Index::Indexed, "none"),
            ("a", Kind::Read, read(&main), Index::Indexed, "read"),
            (
                "b",
                Kind::Search,
                bash("cd src && rg foo"),
                Index::Indexed,
                "deny",
            ),
            ("b", Kind::Search, bash("rg foo"), Index::Indexed, "search"),
            ("c", Kind::Search, glob, Index::Indexed, "deny"),
            (
                "c2",
                Kind::Search,
                bash("find . -name '*.rs'"),
                Index::Indexed,
                "deny",
            ),
            (
                "d",
                Kind::Search,
                bash("glab orbit grep foo"),
                Index::Indexed,
                "none",
            ),
            ("d", Kind::Read, read(&main), Index::Indexed, "none"),
            (
                "g",
                Kind::Search,
                bash("time orbit grep foo"),
                Index::Indexed,
                "none",
            ),
            ("g", Kind::Search, bash("rg bar"), Index::Indexed, "search"),
            ("h", Kind::Search, mcp, Index::Indexed, "none"),
            ("h", Kind::Search, bash("rg bar"), Index::Indexed, "search"),
            (
                "e",
                Kind::Read,
                read(Path::new("/elsewhere/lib.rs")),
                Index::Indexed,
                "none",
            ),
            ("e", Kind::Search, bash("rg foo"), Index::Unknown, "search"),
            ("e", Kind::Search, bash("rg bar"), Index::Missing, "none"),
            ("e", Kind::Search, bash("rg baz"), Index::Indexed, "deny"),
        ];
        for (id, kind, call, index, expected) in cases {
            let context = repo.context(Some(*id), true);
            assert_eq!(
                repo.decide(*kind, call, &context, *index),
                *expected,
                "{id} {call}"
            );
        }
        let sessionless = repo.context(None, true);
        assert_eq!(
            repo.decide(Kind::Search, &bash("rg foo"), &sessionless, Index::Indexed),
            "search"
        );
        assert_eq!(
            session_id(&json!({"session_id": "../x"})).as_deref(),
            Some("___x")
        );
    }

    #[test]
    fn split_shell_respects_quotes_redirects_and_operators() {
        let words = |command: &str| split_shell(command).unwrap();
        assert_eq!(
            words("rg 'a | b' src 2>&1 | head -5 && echo \"x; y\" > out.txt"),
            vec![
                vec![vec!["rg", "a | b", "src"], vec!["head", "-5"]],
                vec![vec!["echo", "x; y"]],
            ]
        );
        assert_eq!(
            words("echo $(rg foo) `grep bar`"),
            vec![
                vec![vec!["echo"]],
                vec![vec!["rg", "foo"]],
                vec![vec!["grep", "bar"]]
            ]
        );
        assert!(split_shell("cat <<EOF\nrg foo\nEOF").is_none());
    }

    #[test]
    fn session_start_context_only_in_indexed_repositories() {
        let repo = Repo::new();
        let call = json!({"session_id": "s", "source": "startup"});
        let text = |graph_first: bool, index: Index| {
            let probe = Fixed {
                root: repo.root.clone(),
                index,
            };
            respond(
                Kind::Session,
                &call,
                &repo.context(Some("s"), graph_first),
                &probe,
            )
            .map(|out| out["hookSpecificOutput"]["additionalContext"].clone())
        };
        assert_eq!(
            text(false, Index::Indexed),
            Some(json!(spec::session_start_text(false)))
        );
        assert_eq!(
            text(true, Index::Indexed),
            Some(json!(spec::session_start_text(true)))
        );
        assert_eq!(text(true, Index::Missing), None);
        assert_eq!(text(true, Index::Unknown), None);
    }

    #[test]
    fn graph_first_env_overrides_the_installed_flag() {
        assert!(graph_first_enabled(false, Some("1")) && !graph_first_enabled(true, Some("off")));
        assert!(graph_first_enabled(true, None) && graph_first_enabled(true, Some("junk")));
    }

    #[test]
    fn dotfiles_are_not_source() {
        assert!(!is_source_path("/repo/.rs"));
        assert!(!is_source_path(""));
        assert!(is_source_path("C:\\repo\\src\\main.RS"));
    }

    #[test]
    fn nudge_text_names_the_launcher_verbs() {
        assert!(spec::search_nudge_text().contains("`orbit grep"));
        assert!(spec::read_nudge_text().contains("`orbit context"));
        assert!(spec::graph_first_deny_text().contains("`orbit grep"));
    }
}
