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

/// Everything but the trees; those follow one frame each, so neither
/// saving nor loading holds more than one tree's snapshot at a time.
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
struct Header {
    edges: Vec<Edge>,
    lang: LangSnapshot,
    resolver: ResolverSnapshot,
    configs: Vec<(String, String)>,
    trees: u64,
}

/// Bump when any snapshot struct changes shape; an older file then fails
/// with a clear message instead of a decode error.
pub const SNAPSHOT_VERSION: u32 = 7;

type Error = rkyv::rancor::BoxedError;

fn write_frame<T>(out: &mut impl Write, value: &T) -> io::Result<()>
where
    T: for<'a> rkyv::Serialize<
            rkyv::api::high::HighSerializer<
                rkyv::util::AlignedVec,
                rkyv::ser::allocator::ArenaHandle<'a>,
                Error,
            >,
        >,
{
    let bytes = rkyv::to_bytes::<Error>(value).map_err(io::Error::other)?;
    out.write_all(&(bytes.len() as u64).to_le_bytes())?;
    out.write_all(&bytes)
}

fn read_frame<T>(input: &mut impl Read, buf: &mut rkyv::util::AlignedVec) -> io::Result<T>
where
    T: rkyv::Archive,
    T::Archived: for<'a> rkyv::bytecheck::CheckBytes<rkyv::api::high::HighValidator<'a, Error>>
        + rkyv::Deserialize<T, rkyv::api::high::HighDeserializer<Error>>,
{
    let mut len = [0u8; 8];
    input.read_exact(&mut len)?;
    buf.clear();
    buf.resize(u64::from_le_bytes(len) as usize, 0);
    input.read_exact(buf)?;
    rkyv::from_bytes::<T, Error>(buf).map_err(io::Error::other)
}

impl State {
    pub fn save(&self, env: &Env, path: &Path) -> io::Result<()> {
        let header = Header {
            edges: self.edges.clone(),
            lang: LangSnapshot::from(&env.lang),
            resolver: self.resolver.to_snapshot(),
            configs: self
                .configs
                .iter()
                .map(|c| (c.path.clone(), c.content.clone()))
                .collect(),
            trees: self.trees.len() as u64,
        };
        // zstd level 3: about 4x smaller for less time than serialising.
        let mut enc = zstd::Encoder::new(std::fs::File::create(path)?, 3)?;
        enc.write_all(&SNAPSHOT_VERSION.to_le_bytes())?;
        write_frame(&mut enc, &header)?;
        for tree in &self.trees {
            write_frame(&mut enc, &TreeSnapshot::from(tree))?;
        }
        enc.finish()?;
        Ok(())
    }

    pub fn load(path: &Path, lang_id: SupportLang) -> io::Result<(Env, Self)> {
        let mut input = zstd::Decoder::new(std::fs::File::open(path)?)?;
        let mut version = [0u8; 4];
        input.read_exact(&mut version).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "snapshot is too short to carry a version",
            )
        })?;
        let version = u32::from_le_bytes(version);
        if version != SNAPSHOT_VERSION {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("snapshot format v{version}; this build reads v{SNAPSHOT_VERSION}"),
            ));
        }
        let mut buf = rkyv::util::AlignedVec::new();
        let header: Header = read_frame(&mut input, &mut buf)?;
        let limits = Limits::load().map_err(io::Error::other)?;
        let env =
            Env::with_lang(lang_id, Lang::from(header.lang), limits).map_err(io::Error::other)?;
        let mut trees = Vec::with_capacity(header.trees as usize);
        for _ in 0..header.trees {
            let tree: TreeSnapshot = read_frame(&mut input, &mut buf)?;
            trees.push(tree.into());
        }
        let state = State {
            trees,
            edges: header.edges,
            resolver: Resolver::from_snapshot(header.resolver, &env.lang),
            configs: header.configs.into_iter().map(Into::into).collect(),
        };
        Ok((env, state))
    }
}
