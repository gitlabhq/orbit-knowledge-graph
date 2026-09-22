use std::io::{self, Read, Write};
use std::path::Path;

use smallvec::SmallVec;

use lasso::Key;

use crate::intern::{Interner, Lang};
use crate::resolver::{ImportReq, Loc, Resolver};
use crate::tree::{Edge, Node, Tag, Tree};
use crate::treesitter::SupportLang;

use super::types::{Env, State};

// ── Interner ──

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

// ── Lang ──

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

// ── Tree ──

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
    pub size: u32,
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
            let size = id.descendants(&tree.arena).count() as u32;
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
                size,
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

// ── Resolver ──

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

// ── State save/load ──

#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
struct FullSnapshot {
    trees: Vec<TreeSnapshot>,
    edges: Vec<Edge>,
    lang: LangSnapshot,
    resolver: ResolverSnapshot,
}

impl State {
    pub fn save(&self, env: &Env, path: &Path) -> io::Result<()> {
        let snap = FullSnapshot {
            trees: self.trees.iter().map(TreeSnapshot::from).collect(),
            edges: self.edges.clone(),
            lang: LangSnapshot::from(&env.lang),
            resolver: self.resolver.to_snapshot(),
        };
        let bytes = rkyv::to_bytes::<rkyv::rancor::BoxedError>(&snap).map_err(io::Error::other)?;
        let mut f = std::fs::File::create(path)?;
        f.write_all(&bytes)
    }

    pub fn load(path: &Path, lang_id: SupportLang) -> io::Result<(Env, Self)> {
        let mut f = std::fs::File::open(path)?;
        let mut bytes = Vec::new();
        f.read_to_end(&mut bytes)?;
        let snap: FullSnapshot = rkyv::from_bytes::<FullSnapshot, rkyv::rancor::BoxedError>(&bytes)
            .map_err(io::Error::other)?;
        let mut env = Env::for_lang(lang_id);
        env.lang = Lang::from(snap.lang);
        let state = State {
            trees: snap.trees.into_iter().map(|t| t.into()).collect(),
            edges: snap.edges,
            resolver: Resolver::from_snapshot(snap.resolver, &env.lang),
        };
        Ok((env, state))
    }
}
