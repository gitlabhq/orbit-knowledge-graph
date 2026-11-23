//! Runs a phase over trees. A pass fires its cases at each node in order,
//! and each write lands where the walk can see it: edges and rows when the
//! phase ends, tree writes before the next node, since the walk reads the
//! tree it writes.

use indextree::NodeId;
use rayon::prelude::*;
use rustc_hash::{FxHashMap, FxHashSet};
use smallvec::SmallVec;

use super::graph::End;
use super::pattern::{self, Cap, Pat};
use super::program::{Pass, Phase, Scratch, Template, order};
use super::{
    Compact, Cursor, Graph, Indexed, Input, Loc, Outputs, Run, Store, Tree, WHOLE, Write, vm,
};
use crate::engine::Engine;
use crate::intern::Lang;
use crate::sentinel::{Killed, Sentinel};
use crate::tree::{Edge, Mutable, Storage};

/// Runs a phase on one tree with the rules at `rules`, or the tree's own;
/// returns the edges it wrote and its backends' outputs. `fi` is the tree's
/// file in `graph`.
pub fn run_tree<S: Store>(
    phase: &Phase,
    tree: &mut Tree<S>,
    fi: u32,
    rules: Option<usize>,
    engine: &Engine,
    graph: Option<&Graph>,
    budget: &Sentinel,
) -> Result<(Vec<Edge>, Outputs), Killed> {
    let file = tree_budget(phase, engine, &tree.label);
    let own = rules.unwrap_or_else(|| engine.rules_index(&tree.label));
    let (mut edges, mut outputs) = (Vec::new(), Outputs::default());
    let mut done = FxHashSet::default();
    let mut host = true;
    loop {
        let pending: Vec<_> = regions(tree, engine, own)
            .into_iter()
            .enumerate()
            .filter(|(at, (root, _))| match at {
                0 => std::mem::take(&mut host),
                _ => done.insert(*root),
            })
            .map(|(_, region)| region)
            .collect();
        if pending.is_empty() {
            break;
        }
        for (region, rules) in pending {
            let host = region == HOST;
            let walks = [
                (
                    own,
                    phase
                        .region_passes(own)
                        .filter(|_| !host)
                        .collect::<Vec<_>>(),
                ),
                (rules, phase.passes(rules).collect()),
            ];
            for (rules, passes) in walks {
                let on = On {
                    phase,
                    rules,
                    region,
                    engine,
                    graph,
                    fi,
                    budget,
                    file: &file,
                };
                for pass in passes {
                    settle(|| {
                        if !(on.walk(pass, tree, &mut edges, &mut outputs)? && pass.repeat) {
                            return Ok(false);
                        }
                        budget.check()?;
                        file.check()?;
                        Ok(true)
                    })?;
                }
            }
        }
    }
    Ok((edges, outputs))
}

/// A tree's regions: the tree itself under its own rules, then each region
/// node under the rules of the frontend that built it, in source order.
fn regions<S: Store>(tree: &Tree<S>, engine: &Engine, own: usize) -> Vec<(u32, usize)> {
    let lang = &engine.lang;
    let (kind, key) = (lang.tree_kinds().region, lang.syms.intern(REGION));
    let mut out = vec![(HOST, own)];
    for c in tree.root().descendants().filter(|c| c.kind() == kind) {
        let found = c
            .tag(key)
            .and_then(|name| engine.frontend_named(lang.syms.resolve(name)));
        if let Some(mounted) = found {
            out.push((c.index(), mounted.rules));
        }
    }
    out
}

/// The region of a tree's own nodes, whatever its root is.
const HOST: u32 = u32::MAX;

/// The nodes of `region` in walk order: those whose nearest region at or
/// above them is it.
fn region_nodes<S: Store>(
    tree: &Tree<S>,
    regions: &[(u32, usize)],
    region: u32,
    post: bool,
) -> Vec<u32> {
    let nodes = order(tree, post);
    if regions.len() == 1 {
        return nodes;
    }
    let roots: FxHashSet<u32> = regions.iter().map(|(r, _)| *r).collect();
    let root = tree.root();
    let mut of: FxHashMap<u32, u32> = FxHashMap::default();
    of.insert(root.index(), HOST);
    for c in root.descendants() {
        let parent = c.parent().map_or(HOST, |p| of[&p.index()]);
        of.insert(
            c.index(),
            if roots.contains(&c.index()) {
                c.index()
            } else {
                parent
            },
        );
    }
    nodes
        .into_iter()
        .filter(|n| of.get(n) == Some(&region))
        .collect()
}

/// Runs a phase on a family's files, the project tree last in `trees`, with
/// the graph so far; an `until: stable` phase reruns with its own edges in
/// the graph until they stop changing. A rerun walks only the files that
/// read an end of an edge the last round added or dropped; the others would
/// write what they wrote. The edges it adds, and the reason it stopped early,
/// if it did.
pub fn run_family(
    phase: &Phase,
    trees: &[Tree<Compact>],
    base: &[Edge],
    engine: &Engine,
    budget: &Sentinel,
) -> Result<(Vec<Edge>, Option<Killed>), Killed> {
    let project = trees.len() as u32 - 1;
    let known: FxHashSet<Edge> = base.iter().copied().collect();
    let mut runs: Vec<Option<FileRun>> = (0..project).map(|_| None).collect();
    let mut walk: Vec<usize> = (0..project as usize).collect();
    let (mut last, mut stopped): (Vec<Edge>, _) = (Vec::new(), None);
    let found_in = |runs: &[Option<FileRun>]| {
        let mut seen = FxHashSet::default();
        runs.iter()
            .flatten()
            .flat_map(|run| run.edges.iter().copied())
            .filter(|e| !known.contains(e) && seen.insert(*e))
            .collect::<Vec<Edge>>()
    };
    settle(|| {
        let graph = Graph::new(base.iter().chain(&last));
        let done = match run_files(phase, trees, &walk, Some(project), &graph, engine, budget) {
            Ok(done) => done,
            Err(killed) if !killed.cancelled => {
                stopped = Some(killed);
                return Ok(false);
            }
            Err(killed) => return Err(killed),
        };
        for (&fi, run) in walk.iter().zip(done) {
            runs[fi] = Some(run);
        }
        let found = found_in(&runs);
        let (now, before): (FxHashSet<Edge>, FxHashSet<Edge>) = (
            found.iter().copied().collect(),
            last.iter().copied().collect(),
        );
        let changed: FxHashSet<End> = now
            .symmetric_difference(&before)
            .flat_map(super::graph::ends)
            .collect();
        last = found;
        walk = (0..project as usize)
            .filter(|&fi| {
                runs[fi]
                    .as_ref()
                    .is_some_and(|run| !run.reads.is_disjoint(&changed))
            })
            .collect();
        Ok(phase.until_stable && !walk.is_empty())
    })?;
    Ok((found_in(&runs), stopped))
}

/// Runs `round` until it reports no change, or the fixpoint bound.
fn settle<E>(mut round: impl FnMut() -> Result<bool, E>) -> Result<(), E> {
    for _ in 0..super::MAX_FIXPOINT_ROUNDS {
        if !round()? {
            break;
        }
    }
    Ok(())
}

/// Runs a phase on the first `walk` trees of a family in parallel, over all
/// its trees and the graph so far; returns the edges it wrote and its
/// backends' outputs. `project` is the project tree's file, if the family has
/// one.
pub fn run_trees(
    phase: &Phase,
    trees: &[Tree<Compact>],
    walk: usize,
    project: Option<u32>,
    graph: &Graph,
    engine: &Engine,
    budget: &Sentinel,
) -> Result<(Vec<Edge>, Outputs), Killed> {
    let files: Vec<usize> = (0..walk).collect();
    let (mut edges, mut outputs) = (Vec::new(), Outputs::default());
    for run in run_files(phase, trees, &files, project, graph, engine, budget)? {
        edges.extend(run.edges);
        outputs.merge(run.outputs);
    }
    Ok((edges, outputs))
}

/// What one file's run of a phase wrote, and the graph ends it read.
struct FileRun {
    edges: Vec<Edge>,
    outputs: Outputs,
    reads: FxHashSet<End>,
}

/// Runs a phase on the given files of a family in parallel, each region of a
/// file under its own rules.
fn run_files(
    phase: &Phase,
    trees: &[Tree<Compact>],
    walk: &[usize],
    project: Option<u32>,
    graph: &Graph,
    engine: &Engine,
    budget: &Sentinel,
) -> Result<Vec<FileRun>, Killed> {
    let scope = scope_budget(phase, engine, budget);
    let files = project.unwrap_or(trees.len() as u32);
    let indexed = Indexed::new(phase, trees, files as usize, engine, &scope);
    walk.par_iter()
        .map(|&fi| {
            let tree = &trees[fi];
            let file = tree_budget(phase, engine, &tree.label);
            let own = engine.rules_index(&tree.label);
            let regions = regions(tree, engine, own);
            let mut done = FileRun {
                edges: Vec::new(),
                outputs: Outputs::default(),
                reads: FxHashSet::default(),
            };
            for &(region, rules) in &regions {
                let inside: Vec<_> = match region == HOST {
                    true => Vec::new(),
                    false => phase.region_passes(own).collect(),
                };
                for (rules, passes) in [(own, inside), (rules, phase.passes(rules).collect())] {
                    if passes.is_empty() {
                        continue;
                    }
                    let input = Input {
                        trees,
                        fi: fi as u32,
                        rules,
                        graph: Some(graph),
                        graph_fi: fi as u32,
                        indexed: Some(&indexed),
                        files,
                        project,
                        budget: &scope,
                        file: &file,
                    };
                    let mut run = Run::new(input, phase, engine);
                    run.start_modules();
                    for pass in passes {
                        let nodes = region_nodes(tree, &regions, region, pass.post);
                        fire_from(&mut run, pass, &nodes, 0);
                    }
                    let (edges, outputs) = run.end()?;
                    done.edges.extend(edges);
                    done.outputs.merge(outputs);
                    done.reads.extend(run.graph_reads.take());
                }
            }
            Ok(done)
        })
        .collect()
}

/// What a walk over one tree reads besides the tree.
struct On<'a> {
    phase: &'a Phase,
    rules: usize,
    /// The region the walk covers: the tree's root or a region node.
    region: u32,
    engine: &'a Engine,
    graph: Option<&'a Graph>,
    fi: u32,
    budget: &'a Sentinel,
    file: &'a Sentinel,
}

impl On<'_> {
    /// One walk of a pass over the tree; whether it replaced a node. A run
    /// reads the tree until a node writes it; the writes commit, and a new
    /// run goes on from the next node.
    fn walk<S: Store>(
        &self,
        pass: &Pass,
        tree: &mut Tree<S>,
        edges: &mut Vec<Edge>,
        outputs: &mut Outputs,
    ) -> Result<bool, Killed> {
        let regions = regions(tree, self.engine, self.rules);
        let nodes = region_nodes(tree, &regions, self.region, pass.post);
        let (mut at, mut replaced) = (0, false);
        while at < nodes.len() {
            let writes = {
                let trees = std::slice::from_ref(&*tree);
                let indexed = Indexed::new(self.phase, trees, 1, self.engine, self.budget);
                let input = Input {
                    trees,
                    fi: 0,
                    rules: self.rules,
                    graph: self.graph,
                    graph_fi: self.fi,
                    indexed: Some(&indexed),
                    files: 1,
                    project: None,
                    budget: self.budget,
                    file: self.file,
                };
                let mut run = Run::new(input, self.phase, self.engine);
                run.start_modules();
                at = fire_from(&mut run, pass, &nodes, at);
                let writes = std::mem::take(&mut run.writes);
                let (written, out) = run.end()?;
                edges.extend(written);
                outputs.merge(out);
                writes
            };
            replaced |= commit(tree, pass, self.engine, self.file, writes);
        }
        Ok(replaced)
    }
}

/// Fires the pass at `nodes` from `at` until a node writes the tree or the
/// run stops; returns where to go on from.
fn fire_from<'t, S: Store>(
    run: &mut Run<'t, S>,
    pass: &'t Pass,
    nodes: &[u32],
    mut at: usize,
) -> usize {
    while let Some(&node) = nodes.get(at) {
        at += 1;
        if !run.step() {
            return nodes.len();
        }
        let tree = &run.input.trees[run.input.fi as usize];
        if tree.storage.is_removed(tree.storage.id(node))
            || !pass.cases.tries(tree.storage.node(node).kind)
        {
            continue;
        }
        let fi = run.input.fi;
        vm::fire(run, &pass.cases, pass.slots, Loc { fi, node });
        if !run.writes.is_empty() {
            break;
        }
    }
    at
}

/// Applies one node's tree writes in order; whether a build replaced it.
/// Builds read their case's captures before any write lands.
fn commit<S: Store>(
    tree: &mut Tree<S>,
    pass: &Pass,
    engine: &Engine,
    budget: &Sentinel,
    writes: Vec<Write>,
) -> bool {
    let lang = &engine.lang;
    let mut caps: Vec<Vec<Cap<NodeId>>> = writes
        .iter()
        .filter_map(|w| match w {
            Write::Build { template, at, .. } => {
                let tree = S::writable(tree).expect("a phase that builds runs on mutable trees");
                Some(captures(
                    tree,
                    lang,
                    pass,
                    &pass.templates[usize::from(*template)],
                    *at,
                ))
            }
            Write::Tag { .. } | Write::Parse { .. } => None,
        })
        .collect::<Vec<_>>();
    caps.reverse();
    for write in writes {
        match write {
            Write::Tag { at, key, on, val } => {
                let at = first_of_kind(tree, at, on);
                tree.set_tag(at, key, val);
            }
            Write::Parse { at, frontend } => {
                let tree = S::writable(tree).expect("a phase that parses runs on mutable trees");
                tree.indexes.clear();
                embed(tree, engine, at, frontend, budget);
            }
            Write::Build { template, at, syms } => {
                let t = &pass.templates[usize::from(template)];
                let caps = caps.pop().unwrap_or_default();
                let tree = S::writable(tree).expect("a phase that builds runs on mutable trees");
                tree.indexes.clear();
                if build(tree, lang, t, at, &caps, &syms) {
                    return true;
                }
            }
        }
    }
    false
}

/// How deep regions nest: a parse under this many is skipped.
const MAX_REGIONS: usize = 8;

/// Parses node `at`'s text with the frontend `name` names and grafts its tree
/// under the node, inside a region node tagged with the name.
fn embed(tree: &mut Tree<Mutable>, engine: &Engine, at: u32, name: u32, budget: &Sentinel) {
    let lang = &engine.lang;
    let region = lang.tree_kinds().region;
    let cursor = tree.cursor(at);
    let depth = std::iter::once(cursor)
        .chain(cursor.ancestors())
        .filter(|c| c.kind() == region)
        .count();
    let Some(mounted) = engine.frontend_named(lang.syms.resolve(name)) else {
        return;
    };
    if depth >= MAX_REGIONS {
        return;
    }
    let target = tree.storage.id(at);
    let base = *tree.node(target);
    let text = tree.source[base.start as usize..base.end as usize].to_owned();
    let label = tree.label.clone();
    let Ok((sub, _)) = mounted
        .frontend
        .parse(&text, &label, lang, &engine.limits, budget)
    else {
        return;
    };
    let wrapper = tree.append(
        target,
        crate::tree::Node {
            kind: region,
            named: true,
            sym: 0,
            ..base
        },
    );
    tree.set_tag(Tree::to_raw(wrapper), lang.syms.intern(REGION), name);
    let root = tree.append(wrapper, shifted(*sub.node(sub.root), &base));
    let mut stack = vec![(sub.root, root)];
    while let Some((from, into)) = stack.pop() {
        for child in from.children(&sub.storage.0) {
            let id = tree.append(into, shifted(*sub.node(child), &base));
            stack.push((child, id));
        }
    }
}

/// The tag a region node carries: the name of the frontend that built it.
pub(super) const REGION: &str = "region";

/// A node of a tree parsed from `base`'s text, placed in `base`'s file.
fn shifted(mut node: crate::tree::Node, base: &crate::tree::Node) -> crate::tree::Node {
    if node.start_row == 0 {
        node.start_col += base.start_col;
    }
    if node.end_row == 0 {
        node.end_col += base.start_col;
    }
    node.start += base.start;
    node.end += base.start;
    node.start_row += base.start_row;
    node.end_row += base.start_row;
    node
}

/// The captures of a template's case at `at`, slot 0 holding the node.
fn captures(
    tree: &Tree<Mutable>,
    lang: &Lang,
    pass: &Pass,
    t: &Template,
    at: u32,
) -> Vec<Cap<NodeId>> {
    let mut caps = Vec::new();
    if let Some(m) = &pass.cases.items[usize::from(t.case)].0 {
        m.capture(lang, tree.cursor(at), &mut caps);
    }
    if caps.is_empty() {
        caps.push(SmallVec::new());
    }
    caps[0] = SmallVec::from_elem(tree.storage.id(at), 1);
    caps
}

/// Builds a template at `at`, its tag values then its texts in `syms`:
/// appended under the node, or in its place unless `unique` finds a sibling
/// with the same key. Whether it replaced the node.
fn build(
    tree: &mut Tree<Mutable>,
    lang: &Lang,
    t: &Template,
    at: u32,
    caps: &[Cap<NodeId>],
    syms: &[Option<u32>],
) -> bool {
    let texts: Vec<u32> = syms[t.tags.len()..]
        .iter()
        .map(|s| s.unwrap_or(0))
        .collect();
    let target = tree.storage.id(at);
    if !t.replace {
        pattern::build(tree, lang, target, &t.tpl, caps, None, &mut |i, _| texts[i]);
        return false;
    }
    let mv = t.unique.is_none().then_some(target);
    let holder = tree.storage.0.new_node(crate::tree::Node::default());
    let built = pattern::build(tree, lang, holder, &t.tpl, caps, mv, &mut |i, _| texts[i]);
    for &n in &built {
        n.detach(&mut tree.storage.0);
    }
    holder.remove(&mut tree.storage.0);
    if let Some(unique) = &t.unique
        && taken(tree, lang, target, built.first().copied(), unique)
    {
        built
            .into_iter()
            .for_each(|n| n.remove_subtree(&mut tree.storage.0));
        return false;
    }
    let first = built.first().copied();
    tree.replace(target, built);
    if let Some(root) = first {
        let root = first_of_kind(tree, Tree::to_raw(root), t.tag_on.unwrap_or(0));
        for (&key, val) in t.tags.iter().zip(syms) {
            if let Some(val) = val {
                tree.set_tag(root, key, *val);
            }
        }
    }
    true
}

/// The first node of `kind` at or under `at` in source order; `at` itself
/// for kind 0 or when there is none.
fn first_of_kind<S: Store>(tree: &Tree<S>, at: u32, kind: u16) -> u32 {
    if kind == 0 {
        return at;
    }
    std::iter::once(tree.cursor(at))
        .chain(tree.cursor(at).descendants())
        .find(|n| n.kind() == kind)
        .map_or(at, |n| n.index())
}

/// Whether a sibling of `target` matching `unique` has the built node's key.
fn taken(
    tree: &Tree<Mutable>,
    lang: &Lang,
    target: NodeId,
    built: Option<NodeId>,
    unique: &(Pat, u16, usize),
) -> bool {
    let (pat, kind, slots) = unique;
    let Some(built) = built else {
        return false;
    };
    let key = tree.cursor(Tree::to_raw(built)).child_sym_of_kind(*kind);
    let mut caps: Vec<Cap<NodeId>> = vec![SmallVec::new(); *slots];
    target.parent(&tree.storage.0).is_some_and(|parent| {
        parent
            .children(&tree.storage.0)
            .filter(|&c| c != target)
            .any(|c| {
                pattern::matches(tree, lang, c, None, pat, &mut caps)
                    && tree.cursor(Tree::to_raw(c)).child_sym_of_kind(*kind) == key
            })
    })
}

/// One tree's piece of the declared index `id`: each match filed under its
/// innermost enclosing scope and under the whole tree.
pub(super) fn piece<'t, S: Store>(
    phase: &'t Phase,
    run: &mut Run<'t, S>,
    root: Cursor<'_, S>,
    id: u32,
) -> crate::tree::Piece {
    let mut out = crate::tree::Piece::default();
    let mine: Vec<_> = phase.indexers.iter().filter(|i| i.id == id).collect();
    let slots = mine.iter().map(|i| i.at.slots.len()).max().unwrap_or(1);
    let mut w = Scratch::<S>::new(slots);
    for c in std::iter::once(root).chain(root.descendants()) {
        for indexer in mine
            .iter()
            .filter(|i| i.at.kinds.iter().any(|&k| k == 0 || k == c.kind()))
        {
            if !w.matches(run, &indexer.at, c) {
                continue;
            }
            let owner = c
                .ancestors()
                .find(|a| phase.enclosing.contains(&a.kind()) || a.parent().is_none())
                .map_or(c.index(), |a| a.index());
            for key in indexer
                .key
                .eval(run, &w.vals)
                .iter()
                .filter_map(|&k| run.text_of(k))
            {
                out.entry((owner, key)).or_default().push(c.index());
                out.entry((WHOLE, key)).or_default().push(c.index());
            }
        }
    }
    out
}

/// The phase's per-tree budget, on thread CPU time; unbounded when the phase
/// declares none.
fn tree_budget(phase: &Phase, engine: &Engine, label: &str) -> Sentinel {
    let ms = phase
        .budget
        .tree
        .as_deref()
        .and_then(|name| engine.limits.ms(name))
        .unwrap_or(u64::MAX);
    Sentinel::new(phase.label, label, ms).on_thread_cpu()
}

/// A phase's per-scope budget, a child of `run`.
pub fn scope_budget(phase: &Phase, engine: &Engine, run: &Sentinel) -> Sentinel {
    let ms = phase
        .budget
        .scope
        .as_deref()
        .and_then(|name| engine.limits.ms(name))
        .unwrap_or(u64::MAX);
    run.child(phase.label, "", ms)
}
