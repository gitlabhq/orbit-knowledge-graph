use rustc_hash::{FxHashMap, FxHashSet};

use crate::canonical::Canonical as C;
use crate::constants::PATH_SEP;
use crate::intern::Lang;
use crate::pattern;
use crate::pipeline::SourceFile;
use crate::rules::{ParseFormat, ResolveConfig, ResolveStage};
use crate::tree::{Cursor, Node, Step, Tag, Tree};

pub struct WalkResult {
    pub prefixes: Vec<String>,
    pub aliases: Vec<(String, String)>,
    /// Per file: the tags rules put on it and on its ancestor directories,
    /// nearest first, plus `source_root_rel`, its path below the nearest source root.
    pub file_tags: Vec<(String, Vec<Tag>)>,
}

pub struct ProjectTree<'a> {
    lang: &'a Lang,
    config: &'a ResolveConfig,
    stages: &'a [ResolveStage],
    paths: &'a [&'a str],
    files: Option<&'a [SourceFile]>,
    tree: Tree,
    prefixes: Vec<String>,
    aliases: Vec<(String, String)>,
}

impl<'a> ProjectTree<'a> {
    /// Just the directory tree: `__root` over `__dir` and `__file` nodes whose
    /// `sym` is the path segment. The resolver and the exporter both walk it.
    pub fn directory_tree(lang: &'a Lang, paths: &'a [&'a str]) -> Tree {
        static NO_CONFIG: std::sync::LazyLock<ResolveConfig> =
            std::sync::LazyLock::new(ResolveConfig::default);
        let mut pt = Self {
            lang,
            config: &NO_CONFIG,
            stages: &[],
            paths,
            files: None,
            tree: Tree::new(Node {
                kind: C::Root.into(),
                named: true,
                ..Default::default()
            }),
            prefixes: vec![],
            aliases: vec![],
        };
        pt.build_dir_tree();
        pt.tree
    }

    pub fn build(
        lang: &'a Lang,
        config: &'a ResolveConfig,
        stages: &'a [ResolveStage],
        paths: &'a [&'a str],
        files: Option<&'a [SourceFile]>,
    ) -> WalkResult {
        let mut pt = Self {
            lang,
            config,
            stages,
            paths,
            files,
            tree: Tree::new(Node {
                kind: C::Root.into(),
                named: true,
                ..Default::default()
            }),
            prefixes: vec![],
            aliases: vec![],
        };
        if stages.is_empty() && config.lookup_from.is_empty() && config.parse_files.is_empty() {
            return WalkResult {
                prefixes: vec![],
                aliases: vec![],
                file_tags: vec![],
            };
        }
        pt.build_dir_tree();
        pt.run_stages();
        pt.collect_aliases();
        pt.collect_prefixes();
        let file_tags = pt.collect_file_tags();
        WalkResult {
            prefixes: pt.prefixes,
            aliases: pt.aliases,
            file_tags,
        }
    }

    fn build_dir_tree(&mut self) {
        let file_contents: FxHashMap<&str, &str> = self
            .files
            .unwrap_or(&[])
            .iter()
            .map(|f| (f.path.as_str(), f.content.as_str()))
            .collect();

        let mut children: FxHashMap<String, FxHashSet<(String, bool)>> = FxHashMap::default();
        for &path in self.paths {
            let parts: Vec<&str> = path.split(PATH_SEP).filter(|p| !p.is_empty()).collect();
            for i in 0..parts.len() {
                let parent = parts[..i].join(PATH_SEP);
                let is_file = i == parts.len() - 1;
                children
                    .entry(parent)
                    .or_default()
                    .insert((parts[i].to_string(), is_file));
            }
        }
        let children_map: FxHashMap<String, Vec<(String, bool)>> = children
            .into_iter()
            .map(|(parent, kids)| {
                let mut kids: Vec<_> = kids.into_iter().collect();
                kids.sort();
                (parent, kids)
            })
            .collect();

        self.add_children("", self.tree.root, &children_map, &file_contents);
    }

    fn add_children(
        &mut self,
        parent_path: &str,
        parent_nid: indextree::NodeId,
        children_map: &FxHashMap<String, Vec<(String, bool)>>,
        file_contents: &FxHashMap<&str, &str>,
    ) {
        let Some(kids) = children_map.get(parent_path) else {
            return;
        };
        let kids = kids.clone();
        for (segment, is_file) in &kids {
            let kind: u16 = if *is_file { C::File } else { C::Dir }.into();
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
            if *is_file {
                if let Some(spec) = self
                    .config
                    .parse_files
                    .iter()
                    .find(|pf| pf.name == *segment)
                {
                    let full_path = if parent_path.is_empty() {
                        segment.clone()
                    } else {
                        format!("{parent_path}{PATH_SEP}{segment}")
                    };
                    if let Some(content) = file_contents.get(full_path.as_str()) {
                        inline_config(content, &spec.format, nid, &mut self.tree, self.lang);
                    }
                }
            } else {
                let child_path = if parent_path.is_empty() {
                    segment.clone()
                } else {
                    format!("{parent_path}{PATH_SEP}{segment}")
                };
                self.add_children(&child_path, nid, children_map, file_contents);
            }
        }
    }

    fn run_stages(&mut self) {
        for stage in self.stages {
            match stage {
                ResolveStage::Rules(rules) => {
                    let _ = pattern::apply_rewrites(&mut self.tree, self.lang, rules, &[]);
                }
                ResolveStage::Climb {
                    while_kind,
                    mark_kind,
                } => {
                    self.climb(*while_kind, *mark_kind);
                }
            }
        }
    }

    fn climb(&mut self, while_kind: u16, mark_kind: u16) {
        let marked: Vec<u32> = self
            .tree
            .root()
            .fold_tree(Vec::new(), |marked, cursor, _w| {
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
            let nid = self.tree.to_id(node);
            self.tree.append(
                nid,
                Node {
                    kind: mark_kind,
                    named: true,
                    ..Default::default()
                },
            );
        }
    }

    /// Import-path aliases a rule file declared as `(__alias key (__str value))`
    /// on the project tree, longest key first so `@/lib/*` wins over `@/*`.
    fn collect_aliases(&mut self) {
        let mut aliases = self
            .tree
            .root()
            .fold_tree(Vec::new(), |aliases, cursor, _w| {
                if !cursor.is(C::Alias) || cursor.sym() == 0 {
                    return;
                }
                if let Some(val) = cursor.children().find(|c| c.is(C::Str) && c.sym() != 0) {
                    aliases.push((
                        self.lang.syms.resolve(cursor.sym()).to_string(),
                        self.lang.syms.resolve(val.sym()).to_string(),
                    ));
                }
            });
        aliases.sort_by_key(|(key, _)| std::cmp::Reverse(key.len()));
        self.aliases = aliases;
    }

    fn collect_file_tags(&self) -> Vec<(String, Vec<Tag>)> {
        let markers = &self.config.lookup_from;
        let source_root_rel = self.lang.syms.intern("source_root_rel");
        let is_source_root =
            |n: Cursor| n.is(C::Root) || n.children().any(|c| markers.contains(&c.kind()));
        self.tree.root().fold_tree(Vec::new(), |out, file, _w| {
            if !file.is(C::File) {
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
            let rel = self.path_below(file, is_source_root);
            tags.push(Tag {
                key: source_root_rel,
                val: self.lang.syms.intern(&rel),
            });
            out.push((self.node_path(file), tags));
        })
    }

    fn collect_prefixes(&mut self) {
        self.collect_marked_paths();
        self.add_fallback_roots();
    }

    fn collect_marked_paths(&mut self) {
        if self.config.lookup_from.is_empty() {
            return;
        }
        let markers = &self.config.lookup_from;
        self.prefixes = self.tree.root().fold_tree(Vec::new(), |paths, cursor, _w| {
            if cursor.is(C::Root) {
                return;
            }
            if cursor.children().any(|c| markers.contains(&c.kind())) {
                let path = self.node_path(cursor);
                if !paths.contains(&path) {
                    paths.push(path);
                }
            }
        });
    }

    fn node_path(&self, cursor: Cursor) -> String {
        self.path_below(cursor, |n| n.is(C::Root))
    }

    fn path_below(&self, cursor: Cursor, is_root: impl Fn(Cursor) -> bool) -> String {
        let mut parts: Vec<&str> = std::iter::once(cursor)
            .chain(cursor.ancestors())
            .take_while(|n| !is_root(*n))
            .map(|n| self.lang.syms.resolve(n.sym()))
            .collect();
        parts.reverse();
        parts.join(PATH_SEP)
    }

    fn add_fallback_roots(&mut self) {
        let packages = self.tree.root().fold_tree(Vec::new(), |pkgs, cursor, _w| {
            if cursor.children().any(|c| c.is(C::Package)) {
                pkgs.push(self.node_path(cursor));
            }
        });
        for &path in self.paths {
            let Some((top, _)) = path.split_once(PATH_SEP) else {
                continue;
            };
            if !self.prefixes.iter().any(|p| p == top) && !packages.iter().any(|p| p == top) {
                self.prefixes.push(top.to_string());
            }
        }
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
                        kind: C::ConfigField.into(),
                        named: true,
                        sym: lang.syms.intern(key.as_str()),
                        ..Default::default()
                    },
                );
                tree.append(
                    field,
                    Node {
                        kind: C::Str.into(),
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
