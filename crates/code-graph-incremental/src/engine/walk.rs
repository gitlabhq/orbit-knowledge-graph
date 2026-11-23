//! Runs a phase over trees. A pass fires its cases at each node in order,
//! and each write lands where the walk can see it: edges and rows when the
//! phase ends, tree writes before the next node, since the walk reads the
//! tree it writes.

use indextree::NodeId;
use rayon::prelude::*;
use rustc_hash::FxHashSet;
use smallvec::SmallVec;

use super::pattern::{self, Cap, Pat};
use super::program::{Pass, Phase, Scratch, Template, order};
use super::{
    Compact, Cursor, Graph, Indexed, Loc, Outputs, Run, Store, Tree, WHOLE, Write, machine,
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
    env: &Engine,
    graph: Option<&Graph>,
    budget: &Sentinel,
) -> Result<(Vec<Edge>, Outputs), Killed> {
    let file = tree_budget(phase, env, &tree.label);
    let rules = rules.unwrap_or_else(|| env.rules_index(&tree.label));
    let on = On {
        phase,
        rules,
        env,
        graph,
        fi,
        budget,
        file: &file,
    };
    let (mut edges, mut outputs) = (Vec::new(), Outputs::default());
    for pass in phase.passes(rules) {
        settle(|| {
            if !(on.walk(pass, tree, &mut edges, &mut outputs)? && pass.repeat) {
                return Ok(false);
            }
            budget.check()?;
            file.check()?;
            Ok(true)
        })?;
    }
    Ok((edges, outputs))
}

/// Runs a phase on a family's files, the project tree last in `trees`, with
/// the graph so far; an `until: stable` phase reruns with its own edges in
/// the graph until they stop changing. The edges it adds, and the reason it
/// stopped early, if it did.
pub fn run_family(
    phase: &Phase,
    trees: &[Tree<Compact>],
    base: &[Edge],
    env: &Engine,
    budget: &Sentinel,
) -> Result<(Vec<Edge>, Option<Killed>), Killed> {
    let project = trees.len() as u32 - 1;
    let known: FxHashSet<Edge> = base.iter().copied().collect();
    let (mut found, mut last, mut stopped) = (Vec::new(), FxHashSet::default(), None);
    settle(|| {
        let graph = Graph::new(base.iter().chain(&found));
        let edges = match run_trees(
            phase,
            trees,
            project as usize,
            Some(project),
            &graph,
            env,
            budget,
        ) {
            Ok((edges, _)) => edges,
            Err(killed) if !killed.cancelled => {
                stopped = Some(killed);
                return Ok(false);
            }
            Err(killed) => return Err(killed),
        };
        let mut seen = FxHashSet::default();
        found = edges
            .into_iter()
            .filter(|e| !known.contains(e) && seen.insert(*e))
            .collect();
        let changed = seen != last;
        last = seen;
        Ok(phase.until_stable && changed)
    })?;
    Ok((found, stopped))
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
    env: &Engine,
    budget: &Sentinel,
) -> Result<(Vec<Edge>, Outputs), Killed> {
    let scope = scope_budget(phase, env, budget);
    let files = project.unwrap_or(trees.len() as u32);
    let indexed = Indexed::new(phase, trees, files as usize, env, &scope);
    let found: Vec<Result<(Vec<Edge>, Outputs), Killed>> = (0..walk)
        .into_par_iter()
        .map(|fi| {
            let file = tree_budget(phase, env, &trees[fi].label);
            let rules = env.rules_index(&trees[fi].label);
            let mut run = Run::new(trees, fi as u32, rules, env, phase, &scope, &file);
            run.graph = Some(graph);
            run.graph_fi = fi as u32;
            run.indexed = Some(&indexed);
            run.files = files;
            run.project = project;
            run.start_modules();
            for pass in phase.passes(rules) {
                fire_from(&mut run, pass, &order(&trees[fi], pass.post), 0);
            }
            run.end()
        })
        .collect();
    let (mut edges, mut outputs) = (Vec::new(), Outputs::default());
    for result in found {
        let (e, o) = result?;
        edges.extend(e);
        outputs.merge(o);
    }
    Ok((edges, outputs))
}

/// What a walk over one tree reads besides the tree.
struct On<'a> {
    phase: &'a Phase,
    rules: usize,
    env: &'a Engine,
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
        let nodes = order(tree, pass.post);
        let (mut at, mut replaced) = (0, false);
        while at < nodes.len() {
            let writes = {
                let trees = std::slice::from_ref(&*tree);
                let indexed = Indexed::new(self.phase, trees, 1, self.env, self.budget);
                let mut run = Run::new(
                    trees,
                    0,
                    self.rules,
                    self.env,
                    self.phase,
                    self.budget,
                    self.file,
                );
                run.graph = self.graph;
                run.graph_fi = self.fi;
                run.indexed = Some(&indexed);
                run.start_modules();
                at = fire_from(&mut run, pass, &nodes, at);
                let writes = std::mem::take(&mut run.writes);
                let (written, out) = run.end()?;
                edges.extend(written);
                outputs.merge(out);
                writes
            };
            replaced |= commit(tree, pass, &self.env.lang, writes);
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
        let tree = &run.trees[run.fi as usize];
        if tree.storage.is_removed(tree.storage.id(node))
            || !pass.cases.tries(tree.storage.node(node).kind)
        {
            continue;
        }
        let fi = run.fi;
        machine::fire(run, &pass.cases, pass.slots, Loc { fi, node });
        if !run.writes.is_empty() {
            break;
        }
    }
    at
}

/// Applies one node's tree writes in order; whether a build replaced it.
/// Builds read their case's captures before any write lands.
fn commit<S: Store>(tree: &mut Tree<S>, pass: &Pass, lang: &Lang, writes: Vec<Write>) -> bool {
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
            Write::Tag { .. } => None,
        })
        .collect::<Vec<_>>();
    caps.reverse();
    for write in writes {
        match write {
            Write::Tag { at, key, on, val } => {
                let at = first_of_kind(tree, at, on);
                tree.set_tag(at, key, val);
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
fn tree_budget(phase: &Phase, env: &Engine, label: &str) -> Sentinel {
    let ms = phase
        .budget
        .tree
        .as_deref()
        .and_then(|name| env.limits.ms(name))
        .unwrap_or(u64::MAX);
    Sentinel::new(phase.label, label, ms).on_thread_cpu()
}

/// A phase's per-scope budget, a child of `run`.
pub fn scope_budget(phase: &Phase, env: &Engine, run: &Sentinel) -> Sentinel {
    let ms = phase
        .budget
        .scope
        .as_deref()
        .and_then(|name| env.limits.ms(name))
        .unwrap_or(u64::MAX);
    run.child(phase.label, "", ms)
}
