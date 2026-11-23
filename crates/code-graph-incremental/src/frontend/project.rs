//! Project frontends turn a whole project into one tree: its paths, and the
//! files a frontend reads by name. The project tree is the first: a tree of
//! directories and files, with each named manifest's contents inlined.

use itertools::Itertools;
use rustc_hash::{FxHashMap, FxHashSet};

use crate::constants::PATH_SEP;
use crate::driver::SourceFile;
use crate::intern::Lang;
use crate::tree::{Node, Tree};

/// Turns a project's paths, and the files it reads, into one tree whose
/// kinds are the pipeline's.
pub trait ProjectFrontend: Send + Sync {
    fn name(&self) -> &str;

    /// Whether the frontend reads the contents of the file at `path`.
    fn reads(&self, path: &str) -> bool;

    /// The project's tree, from every path and the contents of the files it
    /// reads.
    fn build(&self, paths: &[&str], files: &[SourceFile], lang: &Lang) -> Tree;
}

#[derive(Clone)]
pub enum ParseFormat {
    Json,
    Toml,
    Raw(regex::Regex),
}

/// A manifest the project tree inlines, by file name, and how to read it.
#[derive(Clone)]
pub struct ParseFile {
    pub name: String,
    pub format: ParseFormat,
}

/// A tree of the project's directories and files; a file named in
/// `parse_files` gets its contents inlined under it.
#[derive(Default)]
pub struct ProjectTree {
    pub parse_files: Vec<ParseFile>,
}

impl ProjectFrontend for ProjectTree {
    fn name(&self) -> &str {
        "project"
    }

    fn reads(&self, path: &str) -> bool {
        let name = path.rsplit(PATH_SEP).next();
        self.parse_files
            .iter()
            .any(|p| Some(p.name.as_str()) == name)
    }

    fn build(&self, paths: &[&str], files: &[SourceFile], lang: &Lang) -> Tree {
        let contents: FxHashMap<&str, &str> = files
            .iter()
            .map(|f| (f.path.as_str(), f.content.as_str()))
            .collect();
        let mut kids: Vec<FxHashSet<(&str, bool)>> = vec![FxHashSet::default()];
        let mut dirs: FxHashMap<(usize, &str), usize> = FxHashMap::default();
        for &path in paths {
            let parts: Vec<&str> = path.split(PATH_SEP).filter(|p| !p.is_empty()).collect();
            let mut dir = 0;
            for (i, &part) in parts.iter().enumerate() {
                let is_file = i == parts.len() - 1;
                kids[dir].insert((part, is_file));
                if !is_file {
                    let next = kids.len();
                    dir = *dirs.entry((dir, part)).or_insert(next);
                    if dir == next {
                        kids.push(FxHashSet::default());
                    }
                }
            }
        }
        let kids: Vec<Vec<(&str, bool)>> = kids
            .into_iter()
            .map(|kids| {
                let mut kids: Vec<_> = kids.into_iter().collect();
                kids.sort();
                kids
            })
            .collect();
        let mut tree = Tree::new(Node {
            kind: lang.tree_kinds().root,
            named: true,
            ..Default::default()
        });
        let mut path: Vec<&str> = Vec::new();
        let mut stack = vec![(0, tree.root, 0)];
        while let Some((dir, parent, next)) = stack.pop() {
            let Some(&(segment, is_file)) = kids[dir].get(next) else {
                path.pop();
                continue;
            };
            stack.push((dir, parent, next + 1));
            let tk = lang.tree_kinds();
            let node = tree.append(
                parent,
                Node {
                    kind: if is_file { tk.file } else { tk.dir },
                    named: true,
                    sym: lang.syms.intern(segment),
                    ..Default::default()
                },
            );
            if !is_file {
                path.push(segment);
                stack.push((dirs[&(dir, segment)], node, 0));
                continue;
            }
            if let Some(spec) = self.parse_files.iter().find(|pf| pf.name == segment) {
                let full = path.iter().chain([&segment]).join(PATH_SEP);
                if let Some(content) = contents.get(full.as_str()) {
                    inline_config(content, &spec.format, node, &mut tree, lang);
                }
            }
        }
        tree
    }
}

fn inline_config(
    content: &str,
    format: &ParseFormat,
    parent: indextree::NodeId,
    tree: &mut Tree,
    lang: &Lang,
) {
    match format {
        ParseFormat::Raw(re) => {
            for cap in re.captures_iter(content) {
                let Some(key) = cap.get(1) else { continue };
                let Some(val) = cap.get(2) else { continue };
                let val_str = val.as_str().strip_prefix("./").unwrap_or(val.as_str());
                let field = tree.append(
                    parent,
                    Node {
                        kind: lang.tree_kinds().field,
                        named: true,
                        sym: lang.syms.intern(key.as_str()),
                        ..Default::default()
                    },
                );
                tree.append(
                    field,
                    Node {
                        kind: lang.tree_kinds().str,
                        named: true,
                        sym: lang.syms.intern(val_str),
                        ..Default::default()
                    },
                );
            }
        }
        _ => {
            let value: serde_json::Value = match format {
                ParseFormat::Json => match serde_json::from_str(content) {
                    Ok(v) => v,
                    Err(_) => return,
                },
                ParseFormat::Toml => match toml::from_str::<toml::Value>(content) {
                    Ok(tv) => toml_to_json(tv),
                    Err(_) => return,
                },
                ParseFormat::Raw(_) => unreachable!(),
            };
            emit_json_value(&value, parent, tree, lang);
        }
    }
}

fn toml_to_json(v: toml::Value) -> serde_json::Value {
    match v {
        toml::Value::String(s) => serde_json::Value::String(s),
        toml::Value::Integer(i) => serde_json::json!(i),
        toml::Value::Float(f) => serde_json::json!(f),
        toml::Value::Boolean(b) => serde_json::Value::Bool(b),
        toml::Value::Datetime(d) => serde_json::Value::String(d.to_string()),
        toml::Value::Array(a) => {
            serde_json::Value::Array(a.into_iter().map(toml_to_json).collect())
        }
        toml::Value::Table(t) => {
            serde_json::Value::Object(t.into_iter().map(|(k, v)| (k, toml_to_json(v))).collect())
        }
    }
}

fn emit_json_value(
    val: &serde_json::Value,
    parent: indextree::NodeId,
    tree: &mut Tree,
    lang: &Lang,
) {
    match val {
        serde_json::Value::Object(map) => {
            let obj = tree.append(
                parent,
                Node {
                    kind: lang.tree_kinds().obj,
                    named: true,
                    ..Default::default()
                },
            );
            for (key, child) in map {
                let field = tree.append(
                    obj,
                    Node {
                        kind: lang.tree_kinds().field,
                        named: true,
                        sym: lang.syms.intern(key),
                        ..Default::default()
                    },
                );
                emit_json_value(child, field, tree, lang);
            }
        }
        serde_json::Value::Array(arr) => {
            let arr_node = tree.append(
                parent,
                Node {
                    kind: lang.tree_kinds().arr,
                    named: true,
                    ..Default::default()
                },
            );
            for child in arr {
                emit_json_value(child, arr_node, tree, lang);
            }
        }
        serde_json::Value::String(s) => {
            tree.append(
                parent,
                Node {
                    kind: lang.tree_kinds().str,
                    named: true,
                    sym: lang.syms.intern(s),
                    ..Default::default()
                },
            );
        }
        serde_json::Value::Number(n) => {
            tree.append(
                parent,
                Node {
                    kind: lang.tree_kinds().num,
                    named: true,
                    sym: lang.syms.intern(&n.to_string()),
                    ..Default::default()
                },
            );
        }
        serde_json::Value::Bool(b) => {
            tree.append(
                parent,
                Node {
                    kind: lang.tree_kinds().bool,
                    named: true,
                    sym: lang.syms.intern(if *b { "true" } else { "false" }),
                    ..Default::default()
                },
            );
        }
        serde_json::Value::Null => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_path_under_many_directories_builds_on_a_small_stack() {
        let path = format!("{}notes.txt", "d/".repeat(5_000));
        let nodes = std::thread::Builder::new()
            .stack_size(256 * 1024)
            .spawn(move || {
                let lang = Lang::new();
                ProjectTree::default()
                    .build(&[path.as_str()], &[], &lang)
                    .len()
            })
            .unwrap()
            .join()
            .unwrap();
        assert_eq!(nodes, 5_002);
    }
}
