use std::path::{Path, PathBuf};
use std::time::UNIX_EPOCH;

use serde_json::Value;

use super::Kind;
use super::shell::{self, basename, strip_wrappers};

const CONTENT_SEARCH_COMMANDS: &[&str] = &[
    "ack", "ag", "egrep", "fgrep", "grep", "rg", "ripgrep", "ugrep",
];

const READ_COMMANDS: &[&str] = &["bat", "cat", "head", "less", "more", "sed", "tail"];

const SOURCE_EXTS: &[&str] = &[
    "py", "js", "cjs", "mjs", "ts", "tsx", "jsx", "vue", "svelte", "go", "rs", "java", "rb", "c",
    "h", "cpp", "hpp", "cc", "cs", "kt", "kts", "swift", "php", "scala", "lua", "sh", "pl",
];

const VENDORED_DIRS: &[&str] = &["node_modules", "target", "vendor", "dist", "build", ".git"];

const DOC_DIRS: &[&str] = &["doc", "docs", "documentation", ".github", ".gitlab"];

const NON_CODE_TYPES: &[&str] = &[
    "css", "html", "json", "markdown", "md", "toml", "txt", "xml", "yaml", "yml",
];

const SEARCH_VALUE_FLAGS: &[&str] = &[
    "-A", "-B", "-C", "-e", "-f", "-g", "-m", "-t", "--glob", "--regexp", "--type",
];

const FIND_NAME_TESTS: &[&str] = &["-iname", "-ipath", "-name", "-path", "-wholename"];

const ORBIT_GREP_VALUE_FLAGS: &[&str] = &[
    "-F", "--db", "--format", "--kind", "--limit", "--path", "--repo",
];

pub(super) enum Candidate {
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

pub(super) enum Target {
    Search(String),
    Read { key: String, stamp: String },
}

pub(super) fn candidates(kind: Kind, call: &Value) -> Vec<Candidate> {
    let optional = |key: &str| -> Vec<String> {
        Some(input_str(call, key))
            .filter(|value| !value.is_empty())
            .map(str::to_string)
            .into_iter()
            .collect()
    };
    let command = input_str(call, "command");
    let pattern = input_str(call, "pattern");
    match kind {
        Kind::Session => Vec::new(),
        Kind::Read => {
            let input = tool_input(call);
            let path = input_str(call, "file_path");
            let range = |key| input.get(key).unwrap_or(&Value::Null);
            vec![Candidate::Read {
                key: format!("{path}|{}|{}", range("offset"), range("limit")),
                paths: vec![path.to_string()],
            }]
        }
        Kind::Search if !command.is_empty() => bash_candidates(command),
        Kind::Search if pattern.is_empty() || NON_CODE_TYPES.contains(&input_str(call, "type")) => {
            Vec::new()
        }
        Kind::Search if call.get("tool_name").and_then(Value::as_str) == Some("Glob") => {
            vec![Candidate::Files {
                patterns: vec![pattern.to_string()],
                paths: optional("path"),
            }]
        }
        Kind::Search => vec![Candidate::Content {
            pattern: pattern.to_string(),
            paths: optional("path"),
            globs: optional("glob"),
        }],
    }
}

pub(super) fn runs_orbit(call: &Value) -> bool {
    let tool = call.get("tool_name").and_then(Value::as_str).unwrap_or("");
    tool.starts_with("mcp__orbit__") || !orbit_invocations(input_str(call, "command")).is_empty()
}

pub(super) fn orbit_grep_terms(call: &Value) -> Vec<String> {
    let mut terms = Vec::new();
    for args in orbit_invocations(input_str(call, "command")) {
        let mut words = args
            .iter()
            .skip_while(|word| matches!(word.as_str(), "local" | "remote"));
        if words.next().is_none_or(|verb| verb != "grep") {
            continue;
        }
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

fn orbit_invocations(command: &str) -> Vec<Vec<String>> {
    shell::split(command)
        .unwrap_or_default()
        .into_iter()
        .flatten()
        .filter_map(|stage| match strip_wrappers(&stage).1 {
            [first, rest @ ..] if basename(first) == "orbit" => Some(rest.to_vec()),
            [first, second, rest @ ..] if basename(first) == "glab" && second == "orbit" => {
                Some(rest.to_vec())
            }
            _ => None,
        })
        .collect()
}

fn bash_candidates(command: &str) -> Vec<Candidate> {
    shell::split(command)
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
        "fd" | "fdfind" => {
            let mut positional = args.iter().filter(|arg| !arg.starts_with('-')).cloned();
            Some(Candidate::Files {
                patterns: positional.next().into_iter().collect(),
                paths: positional.collect(),
            })
        }
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

fn content_search(args: &[String]) -> Option<Candidate> {
    let (mut pattern, mut positional, mut globs) = (None, Vec::new(), Vec::new());
    let mut words = args.iter();
    while let Some(word) = words.next() {
        if word == "--" {
            positional.extend(words.by_ref().cloned());
            break;
        }
        if !word.starts_with('-') || word == "-" {
            positional.push(word.clone());
            continue;
        }
        let short = !word.starts_with("--") && word[1..].chars().all(|c| c.is_ascii_alphabetic());
        if word == "--invert-match" || (short && word.contains('v')) {
            return None;
        }
        let (flag, value) = match word.split_once('=') {
            Some((flag, value)) => (flag, Some(value.to_string())),
            None if SEARCH_VALUE_FLAGS.contains(&word.as_str()) => {
                (word.as_str(), words.next().cloned())
            }
            None => continue,
        };
        match (flag, value) {
            ("-e" | "--regexp", Some(value)) => pattern = pattern.or(Some(value)),
            ("-g" | "--glob" | "--iglob" | "--include", Some(value)) => globs.push(value),
            ("-t" | "--type", Some(value)) if NON_CODE_TYPES.contains(&value.as_str()) => {
                return None;
            }
            _ => {}
        }
    }
    let pattern = match pattern {
        Some(pattern) => pattern,
        None if !positional.is_empty() => positional.remove(0),
        None => return None,
    };
    Some(Candidate::Content {
        pattern,
        paths: positional,
        globs,
    })
}

fn find_search(args: &[String]) -> Candidate {
    let (mut paths, mut patterns) = (Vec::new(), Vec::new());
    let mut in_expression = false;
    let mut words = args.iter();
    while let Some(word) = words.next() {
        if word.starts_with('-') || word == "(" || word == "!" {
            in_expression = true;
            if FIND_NAME_TESTS.contains(&word.as_str()) {
                patterns.extend(words.next().cloned());
            }
        } else if !in_expression {
            paths.push(word.clone());
        }
    }
    Candidate::Files { patterns, paths }
}

pub(super) struct Scope<'a> {
    pub(super) cwd: &'a Path,
    pub(super) root: &'a Path,
}

impl Scope<'_> {
    pub(super) fn admit(&self, candidate: Candidate) -> Option<Target> {
        match candidate {
            Candidate::Content {
                pattern,
                paths,
                globs,
            } => {
                let known_files =
                    !paths.is_empty() && paths.iter().all(|p| self.resolve(p).is_file());
                let admitted = searchable(&pattern)
                    && (globs.is_empty() || globs.iter().any(|glob| glob_is_code(glob)))
                    && paths.iter().all(|p| self.contains(p) && path_is_code(p))
                    && !known_files;
                admitted.then(|| Target::Search(format!("s:{}", pattern.to_lowercase())))
            }
            Candidate::Files { patterns, paths } => {
                let admitted = paths.iter().all(|p| self.contains(p))
                    && (patterns.is_empty() || patterns.iter().any(|p| glob_is_code(p)));
                admitted.then(|| Target::Search(format!("f:{}", patterns.join(" ").to_lowercase())))
            }
            Candidate::Read { key, paths } => {
                let stamps: Vec<String> = paths
                    .iter()
                    .filter(|p| is_source_path(p) && self.contains(p))
                    .map(|p| self.modified(p))
                    .collect::<Option<_>>()?;
                (!stamps.is_empty()).then(|| Target::Read {
                    key: format!("r:{key}"),
                    stamp: stamps.join(","),
                })
            }
        }
    }

    fn resolve(&self, path: &str) -> PathBuf {
        let path = match path.strip_prefix("~/").zip(dirs::home_dir()) {
            Some((rest, home)) => home.join(rest),
            None => self.cwd.join(path),
        };
        dunce::canonicalize(&path).unwrap_or(path)
    }

    fn contains(&self, path: &str) -> bool {
        let resolved = self.resolve(path);
        let Ok(relative) = resolved.strip_prefix(self.root) else {
            return false;
        };
        let mut parts = relative.iter();
        let docs = relative
            .iter()
            .next()
            .is_some_and(|first| DOC_DIRS.iter().any(|dir| first == *dir));
        !docs && !parts.any(|part| VENDORED_DIRS.iter().any(|dir| part == *dir))
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

fn tool_input(call: &Value) -> &Value {
    call.get("tool_input").unwrap_or(call)
}

fn input_str<'a>(call: &'a Value, key: &str) -> &'a str {
    tool_input(call)
        .get(key)
        .and_then(Value::as_str)
        .unwrap_or("")
}

fn searchable(pattern: &str) -> bool {
    let pattern = pattern.replace("\\|", "|").replace("\\b", "");
    pattern.chars().filter(|c| c.is_alphanumeric()).count() >= 3
        && pattern
            .chars()
            .all(|c| c.is_alphanumeric() || " _-.:|".contains(c))
}

fn extension(path: &str) -> Option<&str> {
    let name = path.rsplit(['/', '\\']).next().unwrap_or(path);
    name.rsplit_once('.')
        .filter(|(stem, _)| !stem.is_empty())
        .map(|(_, ext)| ext)
}

fn is_source_ext(ext: &str) -> bool {
    SOURCE_EXTS.contains(&ext.to_ascii_lowercase().as_str())
}

fn is_code_ext(ext: &str) -> bool {
    ext.contains('*') || is_source_ext(ext)
}

fn is_source_path(path: &str) -> bool {
    extension(path).is_some_and(is_source_ext)
}

fn path_is_code(path: &str) -> bool {
    extension(path).is_none_or(is_code_ext)
}

fn glob_is_code(glob: &str) -> bool {
    if glob.starts_with('!') {
        return true;
    }
    match glob.rsplit('/').next().unwrap_or(glob).split_once('{') {
        Some((_, braced)) => braced
            .trim_end_matches('}')
            .split(',')
            .any(|ext| is_code_ext(ext.trim_start_matches('.'))),
        None => path_is_code(glob),
    }
}
