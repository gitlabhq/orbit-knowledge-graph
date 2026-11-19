mod names;
mod paths;

use crate::canonical::Canonical as C;
use crate::engine::{Cx, Function, IndexDecl, Module, Store, Val, Vals, of, one, yes};
use crate::modules::{arg, function, node};
use crate::tree::{Compact, EdgeKind, Tree};

pub struct Scope;

impl Module for Scope {
    const NAME: &'static str = "scope";
    type State = ();

    fn functions() -> &'static [Function<Self>] {
        FUNCTIONS
    }

    fn indexes() -> &'static [IndexDecl] {
        &[IndexDecl {
            name: "owners",
            build: owners,
        }]
    }
}

/// For each tree, every node's nearest enclosing def or the root.
type Owners = Vec<Vec<u32>>;

fn owners(
    _: &crate::intern::Lang,
    trees: &[Tree<Compact>],
    _: &[crate::tree::Edge],
) -> Box<dyn std::any::Any + Send + Sync> {
    let owners: Owners = trees
        .iter()
        .map(|tree| {
            let root = tree.root();
            let mut owner = vec![root.index(); tree.len() as usize];
            for n in root.descendants() {
                if let Some(p) = n.parent().filter(|p| p.index() != root.index()) {
                    owner[n.index() as usize] = match p.is(C::Def) {
                        true => p.index(),
                        false => owner[p.index() as usize],
                    };
                }
            }
            owner
        })
        .collect();
    Box::new(owners)
}

fn sited<S: Store>(cx: &Cx<'_, '_, Scope, S>, a: &[Vals], kind: EdgeKind) -> Vals {
    a[0].first()
        .and_then(|v| v.loc())
        .map_or_else(Vals::new, |site| {
            of(cx
                .at_site(kind, site.fi, site.node)
                .iter()
                .map(|&(_, to)| cx.jump(to)))
        })
}

fn top<S: Store>(file: crate::engine::Cursor<'_, S>) -> crate::engine::Cursor<'_, S> {
    file.child(crate::canonical::Canonical::SourceFile)
        .unwrap_or(file)
}

fn keep(vals: &Vals, pred: impl Fn(Val) -> bool) -> Vals {
    vals.iter().copied().filter(|&v| pred(v)).collect()
}

static FUNCTIONS: &[Function<Scope>] = &[
    function!(Pure, "read", [one], @uses ["scope"."lookup"], |cx, a| match arg(cx, a, 0) {
        Some(c) => cx.read(c),
        None => Vals::new(),
    }),
    function!(
        Pure, "lookup", [one, one],
        @uses ["scope"."lookup", "scope"."exported"],
        |cx, a| match (arg(cx, a, 0), a[1].first().and_then(|&v| cx.text(v))) {
            (Some(scope), Some(name)) => cx.lookup(scope, name),
            _ => Vals::new(),
        }
    ),
    function!(
        Pure, "exported", [one, one],
        @uses ["scope"."exported"],
        |cx, a| match (arg(cx, a, 0), a[1].first().and_then(|&v| cx.text(v))) {
            (Some(file), Some(name)) => cx.exported(file, name),
            _ => Vals::new(),
        }
    ),
    function!(
        Pure, "export", [one, one],
        @uses ["scope"."exported", "scope"."files", "scope"."keys", "scope"."roots"],
        |cx, a| match (arg(cx, a, 0), a[1].first()) {
            (Some(module), Some(&name)) => cx.export(module, name),
            _ => Vals::new(),
        }
    ),
    function!(
        Pure, "files", [one],
        @uses ["scope"."keys", "scope"."roots"],
        |cx, a| match a[0].first().and_then(|&v| cx.text(v)) {
            Some(path) => cx.files_at(cx.str(path)),
            None => Vals::new(),
        }
    ),
    function!(Pure, "keys", [one], |cx, a| cx.keys(node(cx, a, 0))),
    function!(Pure, "roots", [], |cx, _| cx.roots()),
    function!(Pure, "aliases", [], |cx, _| cx.aliases()),
    function!(
        Pure, "alias", [one, set],
        @uses ["scope"."aliases", "scope"."files", "scope"."keys", "scope"."roots"],
        |cx, a| match arg(cx, a, 0) {
            Some(import) => cx.alias(import, a[1].first().and_then(|&v| cx.text(v))),
            None => Vals::new(),
        }
    ),
    function!(Pure, "source", [one], |cx, a| match arg(cx, a, 0) {
        Some(import) => cx
            .source(import)
            .map_or_else(Vals::new, |s| one(Val::Sym(s))),
        None => Vals::new(),
    }),
    function!(Pure, "under", [one, set], |cx, a| {
        let Some(path) = a[0].first().and_then(|&v| cx.text(v)).map(|s| cx.str(s)) else {
            return Vals::new();
        };
        let within = a[1].iter().filter_map(|&r| cx.text(r)).any(|r| {
            let root = cx.str(r);
            path.strip_prefix(root)
                .is_some_and(|rest| rest.is_empty() || rest.starts_with('/'))
        });
        yes(within)
    }),
    function!(Pure, "producers", [one], |cx, a| sited(
        cx,
        a,
        EdgeKind::TypeFlow
    )),
    function!(Pure, "callees", [one], |cx, a| sited(
        cx,
        a,
        EdgeKind::Calls
    )),
    function!(Pure, "module", [one], |cx, a| keep(&a[0], |v| cx.module(v))),
    function!(Pure, "foreign", [one], |cx, a| keep(&a[0], |v| cx
        .foreign(v))),
];
