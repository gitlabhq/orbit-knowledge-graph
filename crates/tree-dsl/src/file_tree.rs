//! File-tree walker. Builds a Tree from file paths, runs S-expression
//! resolve rules on it, and extracts source roots via the `climb` operation.

use rustc_hash::FxHashMap;

use crate::canonical::Canonical as C;
use crate::constants::PATH_SEP;
use crate::intern::Lang;
use crate::pattern;
use crate::rules::{ParseFileSpec, ParseFormat, ResolveConfig, ResolveStage};
use crate::tree::{Cursor, Node, Step, Tree};

pub struct WalkResult {
    /// Paths to try as prefixes when resolving absolute imports.
    pub lookup_prefixes: Vec<String>,
}

/// Build a file tree from paths, run resolve stages, return results.
/// `files` provides content for config files listed in `parse_files`.
pub fn walk(
    paths: &[String],
    files: &[(String, String)],
    lang: &Lang,
    config: &ResolveConfig,
) -> WalkResult {
    if config.stages.is_empty() && config.lookup_from.is_empty() {
        return WalkResult {
            lookup_prefixes: vec![],
        };
    }

    let mut tree = build_file_tree(paths, files, lang, &config.parse_files);

    for stage in &config.stages {
        match stage {
            ResolveStage::Rules(rules) => {
                pattern::apply_rewrites(&mut tree, lang, rules);
            }
            ResolveStage::Climb {
                while_kind,
                mark_kind,
            } => {
                climb(&mut tree, *while_kind, *mark_kind);
            }
        }
    }

    let mut prefixes = collect_marked_paths(&tree, lang, &config.lookup_from);
    let packages = collect_packages(&tree, lang);
    let detected = prefixes.clone();
    add_fallback_roots(paths, &detected, &packages, &mut prefixes);
    WalkResult {
        lookup_prefixes: prefixes,
    }
}

fn build_file_tree(
    paths: &[String],
    files: &[(String, String)],
    lang: &Lang,
    parse_files: &[ParseFileSpec],
) -> Tree {
    let file_contents: FxHashMap<&str, &str> = files
        .iter()
        .map(|(p, c)| (p.as_str(), c.as_str()))
        .collect();

    let mut children_map: FxHashMap<String, Vec<(String, bool)>> = FxHashMap::default();

    for path in paths {
        let parts: Vec<&str> = path.split(PATH_SEP).collect();
        for i in 0..parts.len() {
            let parent = if i == 0 {
                String::new()
            } else {
                parts[..i].join(PATH_SEP)
            };
            let segment = parts[i].to_string();
            let is_file = i == parts.len() - 1;
            let entry = children_map.entry(parent).or_default();
            if !entry.iter().any(|(s, f)| s == &segment && *f == is_file) {
                entry.push((segment, is_file));
            }
        }
    }

    for v in children_map.values_mut() {
        v.sort();
    }

    let mut tree = Tree::new(Node {
        kind: C::Root.into(),
        named: true,
        ..Default::default()
    });

    fn add_children(
        parent_path: &str,
        parent_nid: indextree::NodeId,
        children_map: &FxHashMap<String, Vec<(String, bool)>>,
        file_contents: &FxHashMap<&str, &str>,
        parse_files: &[ParseFileSpec],
        tree: &mut Tree,
        lang: &Lang,
    ) {
        use crate::canonical::Canonical as C;
        let Some(kids) = children_map.get(parent_path) else {
            return;
        };
        for (segment, is_file) in kids {
            let kind: u16 = if *is_file { C::File } else { C::Dir }.into();
            let sym = lang.syms.intern(segment);
            let nid = tree.append(
                parent_nid,
                Node {
                    kind,
                    named: true,
                    sym,
                    ..Default::default()
                },
            );
            if *is_file {
                if let Some(spec) = parse_files.iter().find(|pf| pf.name == *segment) {
                    let full_path = if parent_path.is_empty() {
                        segment.clone()
                    } else {
                        format!("{parent_path}{PATH_SEP}{segment}")
                    };
                    if let Some(content) = file_contents.get(full_path.as_str()) {
                        inline_config(content, spec.format, nid, tree, lang);
                    }
                }
            } else {
                let child_path = if parent_path.is_empty() {
                    segment.clone()
                } else {
                    format!("{parent_path}{PATH_SEP}{segment}")
                };
                add_children(
                    &child_path,
                    nid,
                    children_map,
                    file_contents,
                    parse_files,
                    tree,
                    lang,
                );
            }
        }
    }

    add_children(
        "",
        tree.root,
        &children_map,
        &file_contents,
        parse_files,
        &mut tree,
        lang,
    );

    tree
}

fn inline_config(
    content: &str,
    format: ParseFormat,
    parent: indextree::NodeId,
    tree: &mut Tree,
    lang: &Lang,
) {
    let value: serde_json::Value = match format {
        ParseFormat::Json => match serde_json::from_str(content) {
            Ok(v) => v,
            Err(_) => return,
        },
        ParseFormat::Toml => match toml::from_str::<toml::Value>(content) {
            Ok(tv) => toml_to_json(tv),
            Err(_) => return,
        },
    };
    emit_json_value(&value, parent, tree, lang);
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
                    kind: C::Obj.into(),
                    named: true,
                    ..Default::default()
                },
            );
            for (key, child) in map {
                let field = tree.append(
                    obj,
                    Node {
                        kind: C::ConfigField.into(),
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
                    kind: C::Arr.into(),
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
                    kind: C::Str.into(),
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
                    kind: C::ConfigNum.into(),
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
                    kind: C::ConfigBool.into(),
                    named: true,
                    sym: lang.syms.intern(if *b { "true" } else { "false" }),
                    ..Default::default()
                },
            );
        }
        serde_json::Value::Null => {}
    }
}

/// Walk up from each node with `while_kind`, mark the first ancestor without it.
fn climb(tree: &mut Tree, while_kind: u16, mark_kind: u16) {
    let marked: Vec<u32> = tree.root().fold_tree(Vec::new(), |marked, cursor, _w| {
        if !cursor.children().any(|c| c.kind() == while_kind) {
            return;
        }
        if let Some(target) = cursor.ascend(|anc| {
            if anc.children().any(|c| c.kind() == while_kind) {
                Step::Into
            } else {
                Step::Out(anc.index())
            }
        }) && !marked.contains(&target)
        {
            marked.push(target);
        }
    });

    for node in marked {
        let nid = tree.to_id(node);
        tree.append(
            nid,
            Node {
                kind: mark_kind,
                named: true,
                ..Default::default()
            },
        );
    }
}

/// Collect paths of nodes carrying any of the given synthetic markers.
fn collect_marked_paths(tree: &Tree, lang: &Lang, markers: &[u16]) -> Vec<String> {
    if markers.is_empty() {
        return vec![];
    }
    tree.root().fold_tree(Vec::new(), |paths, cursor, _w| {
        if cursor.is(C::Root) {
            return;
        }
        if cursor.children().any(|c| markers.contains(&c.kind())) {
            let path = node_path(cursor, lang);
            if !paths.contains(&path) {
                paths.push(path);
            }
        }
    })
}

/// Reconstruct the full path of a directory node by walking up parent pointers.
fn node_path(cursor: Cursor, lang: &Lang) -> String {
    let mut parts: Vec<String> = std::iter::once(cursor)
        .chain(cursor.ancestors())
        .take_while(|n| !n.is(C::Root))
        .filter(|n| n.is(C::Dir) && n.sym() != 0)
        .map(|n| lang.syms.resolve(n.sym()).to_string())
        .collect();
    parts.reverse();
    parts.join(PATH_SEP)
}

/// Collect paths of directories marked `__package` by the resolve rules.
fn collect_packages(tree: &Tree, lang: &Lang) -> Vec<String> {
    tree.root().fold_tree(Vec::new(), |pkgs, cursor, _w| {
        if cursor.children().any(|c| c.is(C::Package)) {
            pkgs.push(node_path(cursor, lang));
        }
    })
}

/// Add top-level directories that aren't descendants of any detected root
/// and weren't marked as packages by the resolve rules.
fn add_fallback_roots(
    paths: &[String],
    existing: &[String],
    packages: &[String],
    out: &mut Vec<String>,
) {
    let mut candidates: Vec<String> = Vec::new();
    for path in paths {
        let top = match path.split_once(PATH_SEP) {
            Some((dir, _)) => dir.to_string(),
            None => continue,
        };
        if !candidates.contains(&top) && !existing.contains(&top) {
            candidates.push(top);
        }
    }
    for c in candidates {
        if !packages.contains(&c) && !out.contains(&c) {
            out.push(c);
        }
    }
}
