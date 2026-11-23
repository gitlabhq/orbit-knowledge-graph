use itertools::Itertools;
use rustc_hash::{FxHashMap, FxHashSet};

use crate::constants::PATH_SEP;
use crate::driver::SourceFile;
use crate::engine::{Engine, ParseFormat, Project};
use crate::intern::Lang;
use crate::tree::{Cursor, Node, Tag, Tree};

pub struct WalkResult {
    pub tree: Tree,
    pub file_tags: Vec<(String, Vec<Tag>)>,
    /// Every key a file tag can carry, so a reused tree can drop the ones
    /// that no longer apply before this run's are set.
    pub tag_keys: Vec<u32>,
}

pub struct ProjectTree<'a> {
    lang: &'a Lang,
    env: Option<&'a Engine>,
    project: &'a Project,
    paths: &'a [&'a str],
    files: Option<&'a [SourceFile]>,
    tree: Tree,
}

impl<'a> ProjectTree<'a> {
    pub fn directory_tree(lang: &'a Lang, paths: &'a [&'a str]) -> Tree {
        static NO_PROJECT: std::sync::LazyLock<Project> =
            std::sync::LazyLock::new(Project::default);
        let mut pt = Self::new(lang, None, &NO_PROJECT, paths, None);
        pt.build_dir_tree();
        pt.tree
    }

    pub fn tree(env: &'a Engine, paths: &'a [&'a str], files: Option<&'a [SourceFile]>) -> Tree {
        Self::build(env, paths, files).tree
    }

    fn new(
        lang: &'a Lang,
        env: Option<&'a Engine>,
        project: &'a Project,
        paths: &'a [&'a str],
        files: Option<&'a [SourceFile]>,
    ) -> Self {
        Self {
            lang,
            env,
            project,
            paths,
            files,
            tree: Tree::new(Node {
                kind: lang.tree_kinds().root,
                named: true,
                ..Default::default()
            }),
        }
    }

    pub fn build(
        env: &'a Engine,
        paths: &'a [&'a str],
        files: Option<&'a [SourceFile]>,
    ) -> WalkResult {
        let mut pt = Self::new(&env.lang, Some(env), &env.project, paths, files);
        let phase = env.pipeline.phase_over("project");
        let tag_keys = env
            .project
            .rules
            .iter()
            .flat_map(|&r| phase.tag_keys(r))
            .collect();
        pt.build_dir_tree();
        pt.run_stages();
        let file_tags = pt.collect_file_tags();
        WalkResult {
            tree: pt.tree,
            file_tags,
            tag_keys,
        }
    }

    fn build_dir_tree(&mut self) {
        let file_contents: FxHashMap<&str, &str> = self
            .files
            .unwrap_or(&[])
            .iter()
            .map(|f| (f.path.as_str(), f.content.as_str()))
            .collect();

        let mut kids: Vec<FxHashSet<(&str, bool)>> = vec![FxHashSet::default()];
        let mut dirs: FxHashMap<(usize, &str), usize> = FxHashMap::default();
        for &path in self.paths {
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

        self.add_children(&dirs, &kids, &file_contents);
    }

    fn add_children(
        &mut self,
        dirs: &FxHashMap<(usize, &str), usize>,
        kids: &[Vec<(&str, bool)>],
        file_contents: &FxHashMap<&str, &str>,
    ) {
        let mut path: Vec<&str> = Vec::new();
        let mut stack = vec![(0, self.tree.root, 0)];
        while let Some((dir, parent_nid, next)) = stack.pop() {
            let Some(&(segment, is_file)) = kids[dir].get(next) else {
                path.pop();
                continue;
            };
            stack.push((dir, parent_nid, next + 1));
            let tk = self.lang.tree_kinds();
            let kind = if is_file { tk.file } else { tk.dir };
            let sym = self.lang.syms.intern(segment);
            let nid = self.tree.append(
                parent_nid,
                Node {
                    kind,
                    named: true,
                    sym,
                    ..Default::default()
                },
            );
            if is_file {
                if let Some(spec) = self
                    .project
                    .parse_files
                    .iter()
                    .find(|pf| pf.name == segment)
                {
                    let full_path = path.iter().chain([&segment]).join(PATH_SEP);
                    if let Some(content) = file_contents.get(full_path.as_str()) {
                        inline_config(content, &spec.format, nid, &mut self.tree, self.lang);
                    }
                }
            } else {
                path.push(segment);
                stack.push((dirs[&(dir, segment)], nid, 0));
            }
        }
    }

    fn run_stages(&mut self) {
        let Some(env) = self.env else {
            return;
        };
        let phase = env.pipeline.phase_over("project");
        let run = crate::sentinel::Sentinel::disabled();
        for &rules in &env.project.rules {
            let _ = crate::engine::run_tree(phase, &mut self.tree, 0, Some(rules), env, None, &run);
        }
    }

    fn collect_file_tags(&self) -> Vec<(String, Vec<Tag>)> {
        self.tree.root().fold_tree(Vec::new(), |out, file, _w| {
            if !file.is(self.lang.tree_kinds().file) {
                return;
            }
            let mut tags: Vec<Tag> = Vec::new();
            for node in std::iter::once(file).chain(file.ancestors()) {
                for tag in self.tree.tags.get(&node.index()).into_iter().flatten() {
                    if !tags.iter().any(|t| t.key == tag.key) {
                        tags.push(*tag);
                    }
                }
            }
            out.push((self.node_path(file), tags));
        })
    }

    fn node_path(&self, cursor: Cursor) -> String {
        let mut parts: Vec<&str> = std::iter::once(cursor)
            .chain(cursor.ancestors())
            .take_while(|n| !n.is(self.lang.tree_kinds().root))
            .map(|n| self.lang.syms.resolve(n.sym()))
            .collect();
        parts.reverse();
        parts.join(PATH_SEP)
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
                ProjectTree::directory_tree(&lang, &[path.as_str()]).len()
            })
            .unwrap()
            .join()
            .unwrap();
        assert_eq!(nodes, 5_002);
    }
}
