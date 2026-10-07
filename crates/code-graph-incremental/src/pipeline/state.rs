//! The graph a run builds: one tree per file, the edges between them, and
//! the resolver's memory of how it linked them; and its snapshot on disk.

use std::io::{self, Read, Write};
use std::path::Path;

use lasso::Key;
use smallvec::SmallVec;

use crate::env::Env;
use crate::intern::{Interner, Lang};
use crate::resolver::{ImportReq, Loc, Resolver};
use crate::sentinel::Limits;
use crate::tree::{Edge, Node, Tag, Tree};
use crate::treesitter::SupportLang;

pub struct SourceFile {
    pub path: String,
    pub content: String,
}

impl From<(String, String)> for SourceFile {
    fn from((path, content): (String, String)) -> Self {
        Self { path, content }
    }
}

pub struct State {
    pub trees: Vec<Tree>,
    pub edges: Vec<Edge>,
    pub resolver: Resolver,
    /// Manifest files (`parse_files`) the resolver reads for module roots.
    pub configs: Vec<SourceFile>,
}

impl State {
    pub fn new(env: &Env) -> Self {
        Self {
            trees: Vec::new(),
            edges: Vec::new(),
            resolver: Resolver::new(&env.lang),
            configs: Vec::new(),
        }
    }

    pub fn compact_symbols(
        &mut self,
        env: &mut Env,
        eligible: impl Fn(&str) -> bool,
    ) -> Result<SymbolCompaction, crate::LoadError> {
        let before = (env.lang.syms.len(), env.lang.syms.text_bytes());
        let fresh = Env::with_lang(
            env.lang_id,
            Lang {
                kinds: env.lang.kinds.clone(),
                fields: env.lang.fields.clone(),
                syms: Interner::default(),
            },
            env.limits,
        )?;
        let mut mapping = vec![0; before.0 as usize + 1];
        for (symbol, text) in env.lang.syms.rodeo.iter() {
            if !eligible(text) {
                mapping[symbol.into_usize() + 1] = fresh.lang.syms.intern(text);
            }
        }
        let mut remap = |symbol: &mut u32| {
            if *symbol == 0 {
                return;
            }
            let mapped = &mut mapping[*symbol as usize];
            if *mapped == 0 {
                *mapped = fresh.lang.syms.intern(env.lang.syms.resolve(*symbol));
            }
            *symbol = *mapped;
        };
        for tree in &mut self.trees {
            for node in tree.arena.iter_mut().filter(|node| !node.is_removed()) {
                remap(&mut node.get_mut().sym);
            }
            for tag in tree.tags.values_mut().flatten() {
                remap(&mut tag.key);
                remap(&mut tag.val);
            }
        }
        let mut resolver = self.resolver.to_snapshot();
        for (symbol, _) in resolver.visible.iter_mut().flatten() {
            remap(symbol);
        }
        self.resolver = Resolver::from_snapshot(resolver, &fresh.lang);
        let report = SymbolCompaction {
            symbols_before: before.0,
            symbols_after: fresh.lang.syms.len(),
            text_bytes_before: before.1,
            text_bytes_after: fresh.lang.syms.text_bytes(),
        };
        *env = fresh;
        Ok(report)
    }
}

pub struct SymbolCompaction {
    pub symbols_before: u32,
    pub symbols_after: u32,
    pub text_bytes_before: usize,
    pub text_bytes_after: usize,
}

#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
pub struct InternerSnapshot {
    names: Vec<String>,
}

impl From<&Interner> for InternerSnapshot {
    fn from(i: &Interner) -> Self {
        let mut pairs: Vec<(usize, String)> = i
            .rodeo
            .iter()
            .map(|(k, v)| (k.into_usize(), v.to_string()))
            .collect();
        pairs.sort_by_key(|(k, _)| *k);
        InternerSnapshot {
            names: pairs.into_iter().map(|(_, v)| v).collect(),
        }
    }
}

impl From<InternerSnapshot> for Interner {
    fn from(s: InternerSnapshot) -> Self {
        let i = Interner::default();
        for name in &s.names {
            i.intern(name);
        }
        i
    }
}

#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
pub struct LangSnapshot {
    pub kinds: InternerSnapshot,
    pub fields: InternerSnapshot,
    pub syms: InternerSnapshot,
}

impl From<&Lang> for LangSnapshot {
    fn from(l: &Lang) -> Self {
        LangSnapshot {
            kinds: InternerSnapshot::from(&l.kinds),
            fields: InternerSnapshot::from(&l.fields),
            syms: InternerSnapshot::from(&l.syms),
        }
    }
}

impl From<LangSnapshot> for Lang {
    fn from(s: LangSnapshot) -> Self {
        Lang {
            kinds: Interner::from(s.kinds),
            fields: Interner::from(s.fields),
            syms: Interner::from(s.syms),
        }
    }
}

const NONE: u32 = u32::MAX;

#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
pub struct SnapshotNode {
    pub kind: u16,
    pub field: u16,
    pub sym: u32,
    pub start: u32,
    pub end: u32,
    pub start_row: u32,
    pub start_col: u32,
    pub end_row: u32,
    pub end_col: u32,
    pub synth: bool,
    pub named: bool,
    pub parent: u32,
}

#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
pub struct TreeSnapshot {
    pub nodes: Vec<SnapshotNode>,
    pub label: String,
    pub tags: Vec<(u32, Vec<Tag>)>,
}

impl From<&Tree> for TreeSnapshot {
    fn from(tree: &Tree) -> Self {
        let ids: Vec<indextree::NodeId> = tree.root.descendants(&tree.arena).collect();
        let id_to_pos: rustc_hash::FxHashMap<indextree::NodeId, u32> = ids
            .iter()
            .enumerate()
            .map(|(i, &id)| (id, i as u32))
            .collect();
        let mut nodes = Vec::with_capacity(ids.len());
        for &id in &ids {
            let n = tree.arena[id].get();
            let parent = id.parent(&tree.arena).map_or(NONE, |p| id_to_pos[&p]);
            nodes.push(SnapshotNode {
                kind: n.kind,
                field: n.field,
                sym: n.sym,
                start: n.start,
                end: n.end,
                start_row: n.start_row,
                start_col: n.start_col,
                end_row: n.end_row,
                end_col: n.end_col,
                synth: n.synth,
                named: n.named,
                parent,
            });
        }
        let tags: Vec<(u32, Vec<Tag>)> = tree
            .tags
            .iter()
            .map(|(&node, tags)| (node, tags.to_vec()))
            .collect();
        Self {
            nodes,
            label: tree.label.clone(),
            tags,
        }
    }
}

impl From<TreeSnapshot> for Tree {
    fn from(snap: TreeSnapshot) -> Self {
        if snap.nodes.is_empty() {
            return Tree::new(Node::default());
        }
        let first = &snap.nodes[0];
        let mut tree = Tree::with_capacity(
            snap.nodes.len(),
            Node {
                kind: first.kind,
                field: first.field,
                sym: first.sym,
                start: first.start,
                end: first.end,
                start_row: first.start_row,
                start_col: first.start_col,
                end_row: first.end_row,
                end_col: first.end_col,
                synth: first.synth,
                named: first.named,
            },
        );
        let mut id_map = vec![tree.root];
        for sn in &snap.nodes[1..] {
            let parent = id_map[sn.parent as usize];
            let id = tree.append(
                parent,
                Node {
                    kind: sn.kind,
                    field: sn.field,
                    sym: sn.sym,
                    start: sn.start,
                    end: sn.end,
                    start_row: sn.start_row,
                    start_col: sn.start_col,
                    end_row: sn.end_row,
                    end_col: sn.end_col,
                    synth: sn.synth,
                    named: sn.named,
                },
            );
            id_map.push(id);
        }
        tree.label = snap.label;
        for (node, tags) in snap.tags {
            tree.tags.insert(node, SmallVec::from_vec(tags));
        }
        tree
    }
}

#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
pub struct ResolverSnapshot {
    pub visible: Vec<Vec<(u32, Loc)>>,
    pub reqs: Vec<ImportReq>,
}

impl Resolver {
    pub fn to_snapshot(&self) -> ResolverSnapshot {
        let visible = self
            .visible()
            .iter()
            .map(|map| map.iter().map(|(&sym, &loc)| (sym, loc)).collect())
            .collect();
        ResolverSnapshot {
            visible,
            reqs: self.reqs().to_vec(),
        }
    }

    pub fn from_snapshot(snap: ResolverSnapshot, lang: &Lang) -> Self {
        let visible = snap
            .visible
            .into_iter()
            .map(|entries| entries.into_iter().collect())
            .collect();
        Self::from_parts(visible, snap.reqs, lang)
    }
}

#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
struct FullSnapshot {
    trees: Vec<TreeSnapshot>,
    edges: Vec<Edge>,
    lang: LangSnapshot,
    resolver: ResolverSnapshot,
    configs: Vec<(String, String)>,
}

/// Bump when any snapshot struct changes shape; an older file then fails
/// with a clear message instead of a decode error.
pub const SNAPSHOT_VERSION: u32 = 2;

impl State {
    pub fn save(&self, env: &Env, path: &Path) -> io::Result<()> {
        let snap = FullSnapshot {
            trees: self.trees.iter().map(TreeSnapshot::from).collect(),
            edges: self.edges.clone(),
            lang: LangSnapshot::from(&env.lang),
            resolver: self.resolver.to_snapshot(),
            configs: self
                .configs
                .iter()
                .map(|c| (c.path.clone(), c.content.clone()))
                .collect(),
        };
        let bytes = rkyv::to_bytes::<rkyv::rancor::BoxedError>(&snap).map_err(io::Error::other)?;
        // zstd level 3: about 4x smaller for less time than serialising.
        let mut enc = zstd::Encoder::new(std::fs::File::create(path)?, 3)?;
        enc.write_all(&SNAPSHOT_VERSION.to_le_bytes())?;
        enc.write_all(&bytes)?;
        enc.finish()?;
        Ok(())
    }

    pub fn load(path: &Path, lang_id: SupportLang) -> io::Result<(Env, Self)> {
        let mut bytes = Vec::new();
        zstd::Decoder::new(std::fs::File::open(path)?)?.read_to_end(&mut bytes)?;
        let (header, payload) = bytes.split_at_checked(4).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "snapshot is too short to carry a version",
            )
        })?;
        let version = u32::from_le_bytes(header.try_into().expect("four bytes"));
        if version != SNAPSHOT_VERSION {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("snapshot format v{version}; this build reads v{SNAPSHOT_VERSION}"),
            ));
        }
        let snap: FullSnapshot =
            rkyv::from_bytes::<FullSnapshot, rkyv::rancor::BoxedError>(payload)
                .map_err(io::Error::other)?;
        let limits = Limits::load().map_err(io::Error::other)?;
        let env =
            Env::with_lang(lang_id, Lang::from(snap.lang), limits).map_err(io::Error::other)?;
        let state = State {
            trees: snap.trees.into_iter().map(|t| t.into()).collect(),
            edges: snap.edges,
            resolver: Resolver::from_snapshot(snap.resolver, &env.lang),
            configs: snap.configs.into_iter().map(Into::into).collect(),
        };
        Ok((env, state))
    }
}
