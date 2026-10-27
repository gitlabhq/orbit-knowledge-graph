//! The graph a run builds: one tree per file, the edges between them, and
//! the resolver's memory of how it linked them; and its snapshot on disk.

use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};

use lasso::Key;
use rayon::prelude::*;
use rustc_hash::{FxHashMap, FxHashSet};
use smallvec::SmallVec;

use crate::env::Env;
use crate::file_tree::{ProjectTree, WalkResult};
use crate::intern::{Interner, Lang};
use crate::resolver::{ImportReq, Loc, Resolver};
use crate::sentinel::Limits;
use crate::tree::{Compact, CompactNode, Edge, Node, Tag, Tree};
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
    pub trees: Vec<Tree<Compact>>,
    pub edges: Vec<Edge>,
    pub resolver: Resolver,
    /// Manifest files (`parse_files`) the resolver reads for module roots.
    pub configs: Vec<SourceFile>,
}

impl State {
    pub(crate) fn project_tree(&self, env: &Env) -> WalkResult {
        let paths: Vec<_> = self
            .trees
            .iter()
            .map(|tree| tree.label.as_str())
            .chain(self.configs.iter().map(|file| file.path.as_str()))
            .collect();
        ProjectTree::build(
            &env.lang,
            &env.resolve.config,
            &env.resolve.stages,
            &paths,
            Some(&self.configs),
        )
    }

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
    pub id: u32,
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
    pub lifted: bool,
    pub labelled: bool,
    pub parent: u32,
}

#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
pub struct TreeSnapshot {
    pub nodes: Vec<SnapshotNode>,
    pub label: String,
    pub tags: Vec<(u32, Vec<Tag>)>,
}

impl From<&Tree<Compact>> for TreeSnapshot {
    fn from(tree: &Tree<Compact>) -> Self {
        let mut nodes = Vec::with_capacity(tree.len() as usize);
        for id in std::iter::once(tree.root()).chain(tree.root().descendants()) {
            let n = id.node();
            let parent = id.parent().map_or(NONE, |p| p.index());
            nodes.push(SnapshotNode {
                id: id.index(),
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
                lifted: n.lifted,
                labelled: n.labelled,
                parent,
            });
        }
        let mut tags: Vec<(u32, Vec<Tag>)> = tree
            .tags
            .iter()
            .map(|(&node, tags)| (node, tags.to_vec()))
            .collect();
        tags.sort_unstable_by_key(|(node, _)| *node);
        Self {
            nodes,
            label: tree.label.clone(),
            tags,
        }
    }
}

impl From<TreeSnapshot> for Tree<Compact> {
    fn from(snap: TreeSnapshot) -> Self {
        if snap.nodes.is_empty() {
            return Tree::new(Node::default()).into();
        }
        let slots = snap
            .nodes
            .iter()
            .map(|node| node.id as usize)
            .max()
            .unwrap_or(0)
            + 1;
        let root = snap.nodes[0].id;
        let mut arena: Vec<_> = (0..slots)
            .map(|id| CompactNode::new(Node::default(), id as u32))
            .collect();
        for sn in &snap.nodes {
            arena[sn.id as usize] = CompactNode::new(
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
                    lifted: sn.lifted,
                    labelled: sn.labelled,
                },
                sn.parent,
            );
        }
        for sn in snap.nodes {
            if sn.parent != NONE {
                CompactNode::link(&mut arena, sn.parent, sn.id);
            }
        }
        Self {
            storage: Compact(arena),
            root,
            label: snap.label,
            tags: snap
                .tags
                .into_iter()
                .map(|(node, tags)| (node, SmallVec::from_vec(tags)))
                .collect(),
            source: std::sync::Arc::from(""),
        }
    }
}

/// One file as a block stores it. Every file it refers to, itself
/// included, is an index into the block's `paths`, so the block does not
/// change when other files move in the tree list.
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
struct StoredFile {
    tree: TreeSnapshot,
    edges: Vec<Edge>,
    visible: Vec<(u32, Loc)>,
    reqs: Vec<ImportReq>,
}

#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
struct Block {
    paths: Vec<String>,
    files: Vec<StoredFile>,
}

/// The tree order and the blocks that hold the trees; read on its own by
/// [`State::labels`].
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
struct Index {
    labels: Vec<String>,
    buckets: u32,
    blocks: Vec<(u32, String)>,
    resolved: bool,
}

#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
struct Shared {
    lang: LangSnapshot,
    configs: Vec<(String, String)>,
}

/// Bump when any snapshot struct changes shape; an older file then fails
/// with a clear message instead of a decode error.
pub const SNAPSHOT_VERSION: u32 = 17;

const FILES_PER_BLOCK: usize = 64;

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

fn invalid(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message.into())
}

/// The block files of the snapshot at `path`, in a directory beside it.
fn blocks_dir(path: &Path) -> PathBuf {
    path.with_extension("blocks")
}

/// Writes through a temporary file, so a reader never sees half a file.
fn write_atomically(
    path: &Path,
    write: impl FnOnce(&mut std::fs::File) -> io::Result<()>,
) -> io::Result<()> {
    let temporary = path.with_extension("tmp");
    let mut file = std::fs::File::create(&temporary)?;
    write(&mut file)?;
    std::fs::rename(&temporary, path)
}

fn open_index(
    path: &Path,
) -> io::Result<(zstd::Decoder<'static, io::BufReader<std::fs::File>>, Index)> {
    let mut input = zstd::Decoder::new(std::fs::File::open(path)?)?;
    let mut version = [0u8; 4];
    input
        .read_exact(&mut version)
        .map_err(|_| invalid("snapshot is too short to carry a version"))?;
    let version = u32::from_le_bytes(version);
    if version != SNAPSHOT_VERSION {
        return Err(invalid(format!(
            "snapshot format v{version}; this build reads v{SNAPSHOT_VERSION}"
        )));
    }
    let index = read_frame(&mut input, &mut rkyv::util::AlignedVec::new())?;
    Ok((input, index))
}

impl State {
    /// Saves the graph as a small index file at `path` plus blocks of about
    /// [`FILES_PER_BLOCK`] files, grouped by a hash of their path. A block is
    /// named by the hash of its content, so a save writes only the blocks
    /// whose files changed and removes the ones nothing refers to any more.
    pub fn save(&self, env: &Env, path: &Path) -> io::Result<()> {
        self.write(env, path, path, None)?;
        remove_unreferenced_blocks(path)
    }

    /// Writes the blocks beside the snapshot at `path` and the index to
    /// `staged`; the snapshot at `path` stays as it was until
    /// [`State::commit_staged`] puts the staged one in its place. With
    /// `touched`, the files that changed since the graph was loaded from
    /// `path`, a block with none of them, and the same files as before, keeps
    /// its file without being serialised.
    pub fn stage(
        &self,
        env: &Env,
        path: &Path,
        staged: &Path,
        touched: Option<&FxHashSet<u32>>,
    ) -> io::Result<()> {
        self.write(env, path, staged, touched)
    }

    /// Puts the index staged for `path` in place and removes the blocks the
    /// old one alone referred to.
    pub fn commit_staged(staged: &Path, path: &Path) -> io::Result<()> {
        std::fs::rename(staged, path)?;
        remove_unreferenced_blocks(path)
    }

    fn write(
        &self,
        env: &Env,
        path: &Path,
        index_path: &Path,
        touched: Option<&FxHashSet<u32>>,
    ) -> io::Result<()> {
        let dir = blocks_dir(path);
        std::fs::create_dir_all(&dir)?;
        let buckets = self
            .trees
            .len()
            .div_ceil(FILES_PER_BLOCK)
            .max(1)
            .next_power_of_two() as u32;
        let resolved = !self.resolver.visible().is_empty();
        let kept = match touched {
            Some(touched) => self.unchanged_blocks(path, buckets, resolved, touched),
            None => FxHashMap::default(),
        };
        let blocks: Vec<(u32, String)> = self
            .blocks(buckets)
            .into_par_iter()
            .map(|(bucket, files)| match kept.get(&bucket) {
                Some(name) => Ok((bucket, name.clone())),
                None => Ok((bucket, self.save_block(&files, &dir)?)),
            })
            .collect::<io::Result<_>>()?;
        let index = Index {
            labels: self.trees.iter().map(|tree| tree.label.clone()).collect(),
            buckets,
            blocks,
            resolved,
        };
        let shared = Shared {
            lang: LangSnapshot::from(&env.lang),
            configs: self
                .configs
                .iter()
                .map(|c| (c.path.clone(), c.content.clone()))
                .collect(),
        };
        write_atomically(index_path, |file| {
            let mut out = zstd::Encoder::new(file, 3)?;
            out.write_all(&SNAPSHOT_VERSION.to_le_bytes())?;
            write_frame(&mut out, &index)?;
            write_frame(&mut out, &shared)?;
            out.finish().map(drop)
        })
    }

    /// The blocks of the saved snapshot at `path` that still hold exactly
    /// the same files, none of them touched, by bucket.
    fn unchanged_blocks(
        &self,
        path: &Path,
        buckets: u32,
        resolved: bool,
        touched: &FxHashSet<u32>,
    ) -> FxHashMap<u32, String> {
        let Ok((_, previous)) = open_index(path) else {
            return FxHashMap::default();
        };
        if previous.buckets != buckets || previous.resolved != resolved {
            return FxHashMap::default();
        }
        let before: FxHashSet<&str> = previous.labels.iter().map(String::as_str).collect();
        let now: FxHashSet<&str> = self.trees.iter().map(|tree| tree.label.as_str()).collect();
        let changed: FxHashSet<u32> = touched
            .iter()
            .map(|&fi| self.trees[fi as usize].label.as_str())
            .chain(before.symmetric_difference(&now).copied())
            .map(|label| bucket(label, buckets))
            .collect();
        previous
            .blocks
            .into_iter()
            .filter(|(b, _)| !changed.contains(b))
            .collect()
    }

    /// The file indices of each non-empty bucket.
    fn blocks(&self, buckets: u32) -> Vec<(u32, Vec<u32>)> {
        let mut blocks: FxHashMap<u32, Vec<u32>> = FxHashMap::default();
        for (fi, tree) in self.trees.iter().enumerate() {
            let fi = u32::try_from(fi).expect("file index exceeds u32");
            blocks
                .entry(bucket(&tree.label, buckets))
                .or_default()
                .push(fi);
        }
        blocks.into_iter().collect()
    }

    fn save_block(&self, files: &[u32], dir: &Path) -> io::Result<String> {
        let block = self.block(files);
        let bytes = rkyv::to_bytes::<Error>(&block).map_err(io::Error::other)?;
        let name = format!("{:032x}.bin", twox_hash::XxHash3_128::oneshot(&bytes));
        let path = dir.join(&name);
        if !path.exists() {
            write_atomically(&path, |file| {
                let mut out = zstd::Encoder::new(file, 3)?;
                out.write_all(&bytes)?;
                out.finish().map(drop)
            })?;
        }
        Ok(name)
    }

    fn block(&self, files: &[u32]) -> Block {
        let label = |fi: &u32| self.trees[*fi as usize].label.as_str();
        let mut files = files.to_vec();
        files.sort_unstable_by(|a, b| label(a).cmp(label(b)));
        let members: FxHashSet<u32> = files.iter().copied().collect();
        let mut edges: FxHashMap<u32, Vec<Edge>> = FxHashMap::default();
        for edge in self.edges.iter().filter(|e| members.contains(&e.from_tree)) {
            edges.entry(edge.from_tree).or_default().push(*edge);
        }
        let mut reqs: FxHashMap<u32, Vec<ImportReq>> = FxHashMap::default();
        for req in self
            .resolver
            .reqs()
            .iter()
            .filter(|r| members.contains(&r.fi))
        {
            reqs.entry(req.fi).or_default().push(req.clone());
        }
        let mut stored: Vec<StoredFile> = files
            .iter()
            .map(|&fi| StoredFile {
                tree: TreeSnapshot::from(&self.trees[fi as usize]),
                edges: edges.remove(&fi).unwrap_or_default(),
                visible: self.visible_names(fi),
                reqs: reqs.remove(&fi).unwrap_or_default(),
            })
            .collect();
        let mut paths: Vec<u32> = files
            .iter()
            .copied()
            .chain(stored.iter().flat_map(references))
            .collect();
        paths.sort_unstable_by(|a, b| label(a).cmp(label(b)));
        paths.dedup();
        let local: FxHashMap<u32, u32> = paths
            .iter()
            .enumerate()
            .map(|(i, &fi)| (fi, u32::try_from(i).expect("block index exceeds u32")))
            .collect();
        for file in &mut stored {
            renumber(file, |fi| local[&fi]);
        }
        Block {
            paths: paths.iter().map(|fi| label(fi).to_string()).collect(),
            files: stored,
        }
    }

    fn visible_names(&self, fi: u32) -> Vec<(u32, Loc)> {
        let mut names: Vec<(u32, Loc)> = self
            .resolver
            .visible()
            .get(fi as usize)
            .into_iter()
            .flatten()
            .map(|(&sym, &loc)| (sym, loc))
            .collect();
        names.sort_unstable_by_key(|(sym, _)| *sym);
        names
    }

    /// The labels of the snapshot at `path`, in tree order, without reading
    /// its blocks.
    pub fn labels(path: &Path) -> io::Result<Vec<String>> {
        Ok(open_index(path)?.1.labels)
    }

    pub fn load(path: &Path, lang_id: SupportLang) -> io::Result<(Env, Self)> {
        let (mut input, index) = open_index(path)?;
        let shared: Shared = read_frame(&mut input, &mut rkyv::util::AlignedVec::new())?;
        let env = Env::with_lang(lang_id, Lang::from(shared.lang), Limits::UNLIMITED)
            .map_err(io::Error::other)?;
        let position: FxHashMap<&str, u32> = index
            .labels
            .iter()
            .enumerate()
            .map(|(fi, label)| {
                (
                    label.as_str(),
                    u32::try_from(fi).expect("file index exceeds u32"),
                )
            })
            .collect();
        let dir = blocks_dir(path);
        let blocks: Vec<Block> = index
            .blocks
            .par_iter()
            .map(|(_, name)| read_block(&dir.join(name)))
            .collect::<io::Result<_>>()?;
        let mut files: Vec<Option<StoredFile>> = (0..index.labels.len()).map(|_| None).collect();
        for block in blocks {
            let global: Vec<u32> = block
                .paths
                .iter()
                .map(|label| {
                    position.get(label.as_str()).copied().ok_or_else(|| {
                        invalid(format!(
                            "snapshot block names {label}, which the index does not list"
                        ))
                    })
                })
                .collect::<io::Result<_>>()?;
            for mut file in block.files {
                renumber(&mut file, |local| global[local as usize]);
                let fi = position[file.tree.label.as_str()] as usize;
                files[fi] = Some(file);
            }
        }
        let files: Vec<StoredFile> = files
            .into_iter()
            .zip(&index.labels)
            .map(|(file, label)| {
                file.ok_or_else(|| invalid(format!("snapshot has no block for {label}")))
            })
            .collect::<io::Result<_>>()?;
        let mut trees = Vec::with_capacity(files.len());
        let mut edges = Vec::new();
        let mut visible = Vec::with_capacity(files.len());
        let mut reqs = Vec::new();
        for file in files {
            trees.push(file.tree.into());
            edges.extend(file.edges);
            visible.push(file.visible.into_iter().collect());
            reqs.extend(file.reqs);
        }
        if !index.resolved {
            visible = Vec::new();
        }
        let mut state = State {
            trees,
            edges,
            resolver: Resolver::from_parts(visible, reqs, &env.lang),
            configs: shared.configs.into_iter().map(Into::into).collect(),
        };
        let walk = state.project_tree(&env);
        state
            .resolver
            .rebuild_file_index(&state.trees, &env, &walk.entrypoints);
        state.resolver.lookup = walk.lookup;
        Ok((env, state))
    }
}

fn bucket(label: &str, buckets: u32) -> u32 {
    (twox_hash::XxHash3_64::oneshot(label.as_bytes()) % u64::from(buckets)) as u32
}

/// Removes the blocks beside `path` that its index does not refer to.
fn remove_unreferenced_blocks(path: &Path) -> io::Result<()> {
    let (_, index) = open_index(path)?;
    let referenced: FxHashSet<&str> = index.blocks.iter().map(|(_, name)| name.as_str()).collect();
    for entry in std::fs::read_dir(blocks_dir(path))? {
        let entry = entry?;
        if !referenced.contains(entry.file_name().to_string_lossy().as_ref()) {
            std::fs::remove_file(entry.path())?;
        }
    }
    Ok(())
}

fn read_block(path: &Path) -> io::Result<Block> {
    let mut bytes = rkyv::util::AlignedVec::<16>::new();
    let mut input = zstd::Decoder::new(std::fs::File::open(path)?)?;
    let mut chunk = [0u8; 1 << 16];
    loop {
        let read = input.read(&mut chunk)?;
        if read == 0 {
            break;
        }
        bytes.extend_from_slice(&chunk[..read]);
    }
    rkyv::from_bytes::<Block, Error>(&bytes).map_err(io::Error::other)
}

/// Every file `file` refers to.
fn references(file: &StoredFile) -> impl Iterator<Item = u32> + '_ {
    let edges = file.edges.iter().flat_map(|e| [e.from_tree, e.to_tree]);
    let names = file.visible.iter().map(|(_, loc)| loc.fi);
    let reqs = file.reqs.iter().flat_map(|r| [r.fi, r.target_fi]);
    edges.chain(names).chain(reqs)
}

/// Rewrites every file reference in `file` through `to`.
fn renumber(file: &mut StoredFile, to: impl Fn(u32) -> u32) {
    for edge in &mut file.edges {
        edge.from_tree = to(edge.from_tree);
        edge.to_tree = to(edge.to_tree);
    }
    for (_, loc) in &mut file.visible {
        loc.fi = to(loc.fi);
    }
    for req in &mut file.reqs {
        req.fi = to(req.fi);
        req.target_fi = to(req.target_fi);
    }
}
