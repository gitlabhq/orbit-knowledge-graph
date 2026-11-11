use std::path::{Path, PathBuf};

use crate::lexer::{Token, lex};

/// A string literal in non-test code: its file, the line it opens on, and its source text.
pub struct Literal {
    pub file: PathBuf,
    pub line: usize,
    pub text: String,
}

/// The string literals of every non-test module reachable from the crate roots under `dir`.
pub fn literals(dir: &Path) -> Vec<Literal> {
    let mut out = Vec::new();
    for manifest in manifests(dir) {
        let root = manifest.parent().unwrap();
        for file in ["src/lib.rs", "src/main.rs", "build.rs"] {
            let file = root.join(file);
            if file.exists() {
                let modules = if file.ends_with("build.rs") {
                    root
                } else {
                    &root.join("src")
                };
                walk(&file, modules, &mut out);
            }
        }
    }
    out
}

fn manifests(dir: &Path) -> Vec<PathBuf> {
    let mut found = Vec::new();
    let mut entries: Vec<PathBuf> = std::fs::read_dir(dir)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .collect();
    entries.sort();
    for path in entries {
        let name = path.file_name().unwrap().to_string_lossy();
        if path.is_dir() && !matches!(&*name, "target" | "tests" | "benches" | "examples") {
            found.extend(manifests(&path));
        } else if name == "Cargo.toml" {
            found.push(path);
        }
    }
    found
}

/// Records `file`'s literals and walks the module files it declares, `modules` being their directory.
fn walk(file: &Path, modules: &Path, out: &mut Vec<Literal>) {
    let source = std::fs::read_to_string(file).unwrap();
    if source
        .lines()
        .next()
        .is_some_and(|line| line.contains("@generated"))
    {
        return;
    }
    let tokens = lex(&source);
    let (mut at, mut depth, mut inline) = (0, 0usize, vec![(0, modules.to_path_buf())]);
    while at < tokens.len() {
        match &tokens[at] {
            Token::Punct('#') => {
                let (gate, end) = attributes(&tokens, at);
                if gate.file {
                    return;
                }
                if gate.item {
                    at = skip_item(&tokens, end);
                    continue;
                }
                for token in &tokens[at..end] {
                    if let Token::Str(text, line) = token {
                        out.push(Literal {
                            file: file.to_path_buf(),
                            line: *line,
                            text: text.clone(),
                        });
                    }
                }
                at = end.max(at + 1);
                continue;
            }
            Token::Str(text, line) => out.push(Literal {
                file: file.to_path_buf(),
                line: *line,
                text: text.clone(),
            }),
            Token::Punct('{') => depth += 1,
            Token::Punct('}') => {
                if inline.last().is_some_and(|(open, _)| *open == depth) {
                    inline.pop();
                }
                depth -= 1;
            }
            Token::Ident(word) if word == "mod" => {
                let dir = inline.last().unwrap().1.clone();
                if let Some(Token::Ident(name)) = tokens.get(at + 1) {
                    match tokens.get(at + 2) {
                        Some(Token::Punct(';')) => walk_child(&dir, name, out),
                        Some(Token::Punct('{')) => inline.push((depth + 1, dir.join(name))),
                        _ => {}
                    }
                }
            }
            _ => {}
        }
        at += 1;
    }
}

fn walk_child(dir: &Path, name: &str, out: &mut Vec<Literal>) {
    let flat = dir.join(format!("{name}.rs"));
    let file = if flat.exists() {
        flat
    } else {
        dir.join(name).join("mod.rs")
    };
    assert!(
        file.exists(),
        "module {name} under {} has no file",
        dir.display()
    );
    walk(&file, &dir.join(name), out);
}

/// Whether a run of attributes gates its file (inner) or its next item (outer) to tests.
#[derive(Default)]
struct Gate {
    file: bool,
    item: bool,
}

/// The gate of the attributes starting at `at`, and the index past them.
fn attributes(tokens: &[Token], mut at: usize) -> (Gate, usize) {
    let mut gate = Gate::default();
    while tokens.get(at) == Some(&Token::Punct('#')) {
        let inner = tokens.get(at + 1) == Some(&Token::Punct('!'));
        let open = at + 1 + usize::from(inner);
        if tokens.get(open) != Some(&Token::Punct('[')) {
            break;
        }
        let end = close(tokens, open);
        let gated = test_only(&tokens[open + 1..end]);
        if inner {
            gate.file |= gated;
        } else {
            gate.item |= gated;
        }
        at = end + 1;
    }
    (gate, at)
}

/// The index of the bracket closing the group opened at `at`.
fn close(tokens: &[Token], mut at: usize) -> usize {
    let mut depth = 0;
    loop {
        match tokens[at] {
            Token::Punct('(' | '[' | '{') => depth += 1,
            Token::Punct(')' | ']' | '}') => depth -= 1,
            _ => {}
        }
        if depth == 0 {
            return at;
        }
        at += 1;
    }
}

/// The index past the item starting at `at`: its `;`, its closing brace, or its enclosing group's end.
fn skip_item(tokens: &[Token], mut at: usize) -> usize {
    let mut depth = 0i32;
    while at < tokens.len() {
        match tokens[at] {
            Token::Punct('(' | '[' | '{') => depth += 1,
            Token::Punct(')' | ']') => depth -= 1,
            Token::Punct('}') if depth == 1 => return at + 1,
            Token::Punct('}') => depth -= 1,
            Token::Punct(';') if depth == 0 => return at + 1,
            _ => {}
        }
        if depth < 0 {
            return at;
        }
        at += 1;
    }
    at
}

/// Whether an attribute enables its item only under test: a test attribute or a test `cfg`.
fn test_only(attribute: &[Token]) -> bool {
    match attribute {
        [
            Token::Ident(cfg),
            Token::Punct('('),
            predicate @ ..,
            Token::Punct(')'),
        ] if cfg == "cfg" => test_predicate(predicate),
        [Token::Ident(first), ..] if first == "rstest" || first == "test_case" => true,
        path => {
            let end = path.iter().position(|token| *token == Token::Punct('('));
            path[..end.unwrap_or(path.len())].last() == Some(&Token::Ident("test".into()))
        }
    }
}

fn test_predicate(predicate: &[Token]) -> bool {
    match predicate {
        [Token::Ident(word)] => word == "test",
        [Token::Ident(word), Token::Punct('='), Token::Str(value, _)] => {
            word == "feature" && value == "testkit"
        }
        [
            Token::Ident(op),
            Token::Punct('('),
            list @ ..,
            Token::Punct(')'),
        ] => {
            let mut parts = split(list).into_iter().map(test_predicate);
            match op.as_str() {
                "all" => parts.any(|gated| gated),
                "any" => parts.all(|gated| gated),
                _ => false,
            }
        }
        _ => false,
    }
}

/// The comma-separated predicates of a `cfg` list, split at its top level.
fn split(list: &[Token]) -> Vec<&[Token]> {
    let (mut parts, mut depth, mut start) = (Vec::new(), 0, 0);
    for (index, token) in list.iter().enumerate() {
        match token {
            Token::Punct('(') => depth += 1,
            Token::Punct(')') => depth -= 1,
            Token::Punct(',') if depth == 0 => {
                parts.push(&list[start..index]);
                start = index + 1;
            }
            _ => {}
        }
    }
    parts.push(&list[start..]);
    parts.retain(|part| !part.is_empty());
    parts
}
