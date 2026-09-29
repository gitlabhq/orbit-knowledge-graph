use std::path::Path;
use std::time::UNIX_EPOCH;

use serde_json::Value;

use super::Kind;
use super::shell::{self, basename, strip_wrappers};

const SOURCE_EXTS: &[&str] = &[
    "py", "js", "cjs", "mjs", "ts", "tsx", "jsx", "vue", "svelte", "go", "rs", "java", "rb", "c",
    "h", "cpp", "hpp", "cc", "cs", "kt", "kts", "swift", "php", "scala", "lua", "sh", "pl",
];
const NON_CODE_TYPES: &[&str] = &[
    "css", "html", "json", "markdown", "md", "toml", "txt", "xml", "yaml", "yml",
];
const SEARCH_VALUE_FLAGS: &[&str] = &[
    "-A", "-B", "-C", "-e", "-f", "-g", "-m", "-t", "--glob", "--regexp", "--type",
];
const ORBIT_VALUE_FLAGS: &[&str] = &[
    "-F", "--db", "--format", "--kind", "--limit", "--path", "--repo",
];

#[derive(Default)]
pub(super) struct Inspection {
    pub(super) orbit: bool,
    pub(super) terms: Vec<String>,
    lookups: Vec<Lookup>,
}

#[derive(Default, PartialEq, Eq)]
enum Mode {
    #[default]
    Content,
    Files,
    Read,
    Directory,
}

#[derive(Default)]
struct Lookup {
    mode: Mode,
    pattern: String,
    paths: Vec<String>,
    globs: Vec<String>,
}

pub(super) struct Target {
    pub(super) key: String,
    pub(super) stamp: Option<String>,
}

impl Inspection {
    pub(super) fn new(kind: Kind, call: &Value) -> Self {
        let input = call.get("tool_input").unwrap_or(call);
        let text = |key| input.get(key).and_then(Value::as_str).unwrap_or("");
        let tool = call.get("tool_name").and_then(Value::as_str).unwrap_or("");
        let parsed = shell::split(text("command"));
        let mut result = Self {
            orbit: tool.starts_with("mcp__orbit__"),
            ..Self::default()
        };
        let fallback = parsed
            .is_none()
            .then(|| shell::split(text("command").split("<<").next().unwrap_or("")))
            .flatten();
        for stage in parsed
            .as_ref()
            .or(fallback.as_ref())
            .into_iter()
            .flatten()
            .flatten()
        {
            let args = match strip_wrappers(stage).1 {
                [name, args @ ..] if basename(name) == "orbit" => args,
                [name, verb, args @ ..] if basename(name) == "glab" && verb == "orbit" => args,
                _ => continue,
            };
            result.orbit = true;
            let mut words = args
                .iter()
                .skip_while(|w| matches!(w.as_str(), "local" | "remote"));
            if words.next().is_none_or(|verb| verb != "grep") {
                continue;
            }
            while let Some(word) = words.next() {
                if word.starts_with('-') {
                    if ORBIT_VALUE_FLAGS.contains(&word.as_str()) {
                        words.next();
                    }
                    continue;
                }
                let query = word.to_lowercase();
                for term in query.split('|').map(str::trim).filter(|t| !t.is_empty()) {
                    result
                        .terms
                        .extend(term.split_whitespace().map(str::to_string));
                    result.terms.push(term.to_string());
                }
                result.terms.push(query);
                break;
            }
        }
        result.lookups = match kind {
            Kind::Session => Vec::new(),
            Kind::Read => vec![Lookup {
                mode: Mode::Read,
                pattern: format!(
                    "{}|{}|{}",
                    text("file_path"),
                    input["offset"],
                    input["limit"]
                ),
                paths: vec![text("file_path").to_string()],
                ..Lookup::default()
            }],
            Kind::Search if !text("command").is_empty() => parsed
                .unwrap_or_default()
                .iter()
                .filter_map(|stages| {
                    stages
                        .iter()
                        .enumerate()
                        .find_map(|(i, s)| from_stage(i, s))
                })
                .collect(),
            Kind::Search
                if text("pattern").is_empty() || NON_CODE_TYPES.contains(&text("type")) =>
            {
                Vec::new()
            }
            Kind::Search => vec![Lookup {
                mode: if tool == "Glob" {
                    Mode::Files
                } else {
                    Mode::Content
                },
                pattern: text("pattern").to_string(),
                paths: nonempty(text("path")),
                globs: nonempty(if tool == "Glob" {
                    text("pattern")
                } else {
                    text("glob")
                }),
            }],
        };
        result
    }

    pub(super) fn is_empty(&self) -> bool {
        self.lookups.is_empty()
    }

    pub(super) fn target(&self, cwd: &Path, root: &Path) -> Option<Target> {
        let mut cwd = cwd.to_path_buf();
        for lookup in &self.lookups {
            let resolve = |path: &str| {
                if path == "-"
                    || path.contains('$')
                    || (path.starts_with('~') && !path.starts_with("~/"))
                {
                    return None;
                }
                let path = match path.strip_prefix("~/").zip(dirs::home_dir()) {
                    Some((rest, home)) => home.join(rest),
                    None => cwd.join(path),
                };
                let path = dunce::canonicalize(&path).unwrap_or(path);
                let relative = path.strip_prefix(root).ok()?;
                let docs = ["doc", "docs", "documentation", ".github", ".gitlab"];
                let vendor = ["node_modules", "target", "vendor", "dist", "build", ".git"];
                let excluded = relative
                    .iter()
                    .next()
                    .is_some_and(|p| docs.iter().any(|d| p == *d))
                    || relative.iter().any(|p| vendor.iter().any(|d| p == *d));
                (!excluded).then_some(path)
            };
            if lookup.mode == Mode::Directory {
                cwd = resolve(&lookup.pattern)?;
                continue;
            }
            if lookup.mode == Mode::Read {
                let stamps = lookup
                    .paths
                    .iter()
                    .filter(|p| source_path(p))
                    .filter_map(|p| resolve(p))
                    .map(|p| modified(&p))
                    .collect::<Option<Vec<_>>>();
                if let Some(stamps) = stamps.filter(|s| !s.is_empty()) {
                    return Some(Target {
                        key: format!("r:{}", lookup.pattern),
                        stamp: Some(stamps.join(",")),
                    });
                }
                continue;
            }
            let Some(paths) = lookup
                .paths
                .iter()
                .map(|p| resolve(p))
                .collect::<Option<Vec<_>>>()
            else {
                continue;
            };
            let files = lookup.mode == Mode::Files;
            let known_files = !paths.is_empty() && paths.iter().all(|p| p.is_file());
            if (files
                || (searchable(&lookup.pattern)
                    && !known_files
                    && lookup.paths.iter().all(|p| code_path(p))))
                && (lookup.globs.is_empty() || lookup.globs.iter().any(|g| code_glob(g)))
            {
                let prefix = if files { "f" } else { "s" };
                return Some(Target {
                    key: format!("{prefix}:{}", lookup.pattern.to_lowercase()),
                    stamp: None,
                });
            }
        }
        None
    }
}

fn from_stage(index: usize, stage: &[String]) -> Option<Lookup> {
    let (xargs, words) = strip_wrappers(stage);
    if index > 0 && !xargs {
        return None;
    }
    let (name, args) = words.split_first()?;
    match basename(name) {
        "cd" | "pushd" if index == 0 => Some(Lookup {
            mode: Mode::Directory,
            pattern: args.first().cloned().unwrap_or_else(|| "~".into()),
            ..Lookup::default()
        }),
        "git" if args.first().is_some_and(|a| a == "grep") => search(&args[1..]),
        "ack" | "ag" | "egrep" | "fgrep" | "grep" | "rg" | "ripgrep" | "ugrep" => search(args),
        "find" | "fd" | "fdfind" => Some(files(args, name.ends_with("/find") || name == "find")),
        "sed"
            if args
                .iter()
                .any(|a| a.starts_with("-i") || a.starts_with("--in-place")) =>
        {
            None
        }
        "bat" | "cat" | "head" | "less" | "more" | "sed" | "tail" => Some(Lookup {
            mode: Mode::Read,
            pattern: words.join(" "),
            paths: args
                .iter()
                .filter(|a| !a.starts_with('-'))
                .cloned()
                .collect(),
            ..Lookup::default()
        }),
        _ => None,
    }
}

fn search(args: &[String]) -> Option<Lookup> {
    let mut lookup = Lookup::default();
    let mut pattern = None;
    let mut words = args.iter();
    while let Some(word) = words.next() {
        if word == "--" {
            lookup.paths.extend(words.cloned());
            break;
        }
        if !word.starts_with('-') || word == "-" {
            lookup.paths.push(word.clone());
            continue;
        }
        let short = !word.starts_with("--") && word[1..].chars().all(|c| c.is_ascii_alphabetic());
        if word == "--invert-match" || (short && word.contains('v')) {
            return None;
        }
        let (flag, value) = match word.split_once('=') {
            Some((flag, value)) => (flag, Some(value)),
            None if SEARCH_VALUE_FLAGS.contains(&word.as_str()) => {
                (word.as_str(), words.next().map(String::as_str))
            }
            _ => continue,
        };
        match (flag, value) {
            ("-e" | "--regexp", Some(value)) => {
                pattern.get_or_insert_with(|| value.to_string());
            }
            ("-g" | "--glob" | "--iglob" | "--include", Some(value)) => {
                lookup.globs.push(value.to_string())
            }
            ("-t" | "--type", Some(value)) if NON_CODE_TYPES.contains(&value) => return None,
            _ => {}
        }
    }
    lookup.pattern =
        pattern.or_else(|| (!lookup.paths.is_empty()).then(|| lookup.paths.remove(0)))?;
    Some(lookup)
}

fn files(args: &[String], find: bool) -> Lookup {
    let mut lookup = Lookup {
        mode: Mode::Files,
        ..Lookup::default()
    };
    let mut expression = false;
    let mut words = args
        .iter()
        .skip_while(|w| find && matches!(w.as_str(), "-H" | "-L" | "-P"));
    while let Some(word) = words.next() {
        if find {
            expression |= word.starts_with('-') || word == "(" || word == "!";
            if matches!(
                word.as_str(),
                "-iname" | "-ipath" | "-name" | "-path" | "-wholename"
            ) {
                lookup.globs.extend(words.next().cloned());
            } else if !expression {
                lookup.paths.push(word.clone());
            }
        } else {
            match word.as_str() {
                "-e" | "--extension" => lookup.globs.extend(words.next().map(|e| format!("*.{e}"))),
                "-t" | "--type" | "-E" | "--exclude" | "-d" | "--max-depth" => {
                    words.next();
                }
                flag if flag.starts_with('-') => {}
                _ => lookup.paths.push(word.clone()),
            }
        }
    }
    if !find && !lookup.paths.is_empty() {
        let pattern = lookup.paths.remove(0);
        if lookup.globs.is_empty() {
            lookup.globs.push(pattern);
        }
    }
    lookup.pattern = lookup.globs.join(" ");
    lookup
}

fn modified(path: &Path) -> Option<String> {
    let time = std::fs::metadata(path).ok()?.modified().ok()?;
    Some(time.duration_since(UNIX_EPOCH).ok()?.as_nanos().to_string())
}

fn nonempty(text: &str) -> Vec<String> {
    (!text.is_empty())
        .then(|| text.to_string())
        .into_iter()
        .collect()
}

fn searchable(pattern: &str) -> bool {
    let pattern = pattern.replace("\\|", "|").replace("\\b", "");
    pattern.chars().filter(|c| c.is_alphanumeric()).count() >= 3
        && pattern
            .chars()
            .all(|c| c.is_alphanumeric() || " _-.:|".contains(c))
}

fn extension(path: &str) -> Option<&str> {
    path.rsplit(['/', '\\'])
        .next()?
        .rsplit_once('.')
        .filter(|(stem, _)| !stem.is_empty())
        .map(|(_, ext)| ext)
}

fn source_path(path: &str) -> bool {
    extension(path).is_some_and(|e| SOURCE_EXTS.contains(&e.to_ascii_lowercase().as_str()))
}
fn code_extension(ext: &str) -> bool {
    ext.contains('*') || SOURCE_EXTS.contains(&ext.to_ascii_lowercase().as_str())
}
fn code_path(path: &str) -> bool {
    extension(path).is_none_or(code_extension)
}

fn code_glob(glob: &str) -> bool {
    if glob.starts_with('!') {
        return true;
    }
    match glob.rsplit('/').next().unwrap_or(glob).split_once('{') {
        Some((_, ext)) => ext
            .trim_end_matches('}')
            .split(',')
            .any(|e| code_extension(e.trim_start_matches('.'))),
        None => code_path(glob),
    }
}
