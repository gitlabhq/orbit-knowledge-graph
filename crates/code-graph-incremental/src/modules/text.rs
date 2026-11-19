use std::borrow::Cow;
use std::cell::RefCell;

use regex::Regex;
use rustc_hash::FxHashMap;
use smallvec::SmallVec;

use crate::engine::{Cursor, Cx, Function, Module, Store, Val, Vals, one, yes};
use crate::intern::Lang;
use crate::modules::function;
use crate::tree::{EdgeKind, Node, Storage};

pub struct Text;

impl Module for Text {
    const NAME: &'static str = "text";
    type State = ();

    fn functions() -> &'static [Function<Self>] {
        FUNCTIONS
    }
}

macro_rules! on_text {
    ($name:literal, [$($arg:ident),*], $f:ident) => {
        function!(Pure, $name, [$($arg),*], |c, a| edit(c, a, $f))
    };
}

macro_rules! on_node {
    ($name:literal, [$($arg:ident),*], $f:expr) => {
        function!(Pure, $name, [$($arg),*], |c, a| at(c, a, $f))
    };
}

static FUNCTIONS: &[Function<Text>] = &[
    on_text!("strip", [one, one], strip),
    on_text!("strip_leading", [one, one], strip_leading),
    on_text!("strip_suffix", [one, one], strip_suffix),
    on_text!("split_last", [one, one], split_last),
    on_text!("split_first", [one, one], split_first),
    on_text!("replace", [one, one, one], replace),
    on_text!("prepend", [one, one], prepend),
    on_text!("lowercase", [one], lowercase),
    on_text!("regex_replace", [one, one, one], regex_replace),
    on_text!("regex_first", [one, one, one], regex_first),
    on_text!("regex_loop", [one, one, one], regex_loop),
    function!(Pure, "regex_match", [one, one], |cx, a| {
        let o = opts(cx, a);
        match text(cx, a[0][0]) {
            Some(s) => yes(regex_match(s, &o)),
            None => Vals::new(),
        }
    }),
    on_text!("keep_last", [one, one, one, one], keep_last),
    on_text!("stem", [one], stem),
    on_text!("collapse_index", [one, set], collapse_index),
    on_text!("map", [one, set], map),
    function!(Flow, "foreign", [set], |cx, a| {
        let here = cx.fi();
        a[0].iter()
            .copied()
            .filter(|v| v.loc().is_some_and(|l| l.fi != here))
            .collect()
    }),
    function!(Pure, "is_file", [set], |_, a| {
        a[0].iter()
            .copied()
            .filter(|v| matches!(v, Val::Node(l) if l.node == 0))
            .collect()
    }),
    function!(Pure, "default", [set, set], |cx, a| {
        let present = |at: usize| -> Vals {
            a[at]
                .iter()
                .filter(|&&v| text(cx, v).is_some_and(|s| !s.is_empty()))
                .copied()
                .collect()
        };
        match present(0) {
            found if found.is_empty() => present(1),
            found => found,
        }
    }),
    function!(Pure, "concat", [set, one, set], |cx, a| {
        let o = opts(cx, a);
        let first = |at: usize| {
            a[at]
                .iter()
                .find_map(|&v| text(cx, v).filter(|s| !s.is_empty()))
        };
        sym(o.lang, &concat(first(0), o.lit(1), first(2)))
    }),
    function!(Pure, "eq", [set, set], |cx, a| {
        yes(a[0].first().and_then(|&v| text(cx, v)) == a[1].first().and_then(|&v| text(cx, v)))
    }),
    function!(Pure, "ne", [set, set], |cx, a| {
        yes(a[0].first().and_then(|&v| text(cx, v)) != a[1].first().and_then(|&v| text(cx, v)))
    }),
    function!(Pure, "has_incoming", [one, one], |cx, a| has_edge(
        cx, a, true
    )),
    function!(Pure, "has_outgoing", [one, one], |cx, a| has_edge(
        cx, a, false
    )),
    on_node!("field", [one, one], field),
    on_node!("child", [one, one], child),
    on_node!("field_child", [one, one, one], field_child),
    on_node!("child_sym", [one, one], child_sym),
    on_node!("parent_sym", [one, one], parent_sym),
    on_node!("ancestor_sym", [one, one], ancestor_sym),
    on_node!("ancestor_tag", [one, one], ancestor_tag),
    on_node!("tag", [one, one], tag),
    on_node!("tree_path", [one, one], tree_path),
    on_node!("sibling_index", [one], sibling_index),
    on_node!("tree_node_id", [one], tree_node_id),
    on_node!("kind_name", [one], kind_name),
    on_node!("const", [one, one], constant),
    on_node!("start_line", [one], |c, o| number(o, c.start_row() + 1)),
    on_node!("end_line", [one], |c, o| number(o, c.end_row() + 1)),
    on_node!("start_byte", [one], |c, o| number(o, c.start())),
    on_node!("end_byte", [one], |c, o| number(o, c.end())),
    on_node!("start_col", [one], |c, o| number(o, c.start_col() + 1)),
    on_node!("end_col", [one], |c, o| number(o, c.end_col() + 1)),
    on_node!("span", [one], |c, o| text_val(
        o,
        &format!("{}:{}", c.start(), c.end())
    )),
    on_node!("tree_root", [one], |c, _| Some(Val::Node(
        c.jump(c.fi(), 0).into()
    ))),
    function!(Pure, "out", [one, one], |cx, a| targets(cx, a, false)),
    function!(Pure, "out_unresolved", [one, one], |cx, a| targets(
        cx, a, true
    )),
    function!(Pure, "file_dir", [one], |cx, a| file_dir(cx, a)),
];

const MAX_REGEX_LOOP_PASSES: usize = 64;

struct Opts<'s> {
    lang: &'s Lang,
    args: &'s [Vals],
}

impl<'s> Opts<'s> {
    fn lit(&self, at: usize) -> &'s str {
        self.args[at].first().map_or("", |&v| lit(self.lang, v))
    }

    /// Unquoted numbers arrive as their own value in a `Sym`, not interned.
    fn num(&self, at: usize) -> usize {
        match self.args[at].first() {
            Some(&Val::Sym(n)) => n as usize,
            _ => 0,
        }
    }

    fn kind(&self, at: usize) -> u16 {
        match self.args[at].first() {
            Some(&Val::Kind(k)) => k,
            _ => self.lang.lookup_kind(self.lit(at)),
        }
    }

    fn field(&self, at: usize) -> u16 {
        self.lang.fields.lookup(self.lit(at)) as u16
    }

    fn key(&self, at: usize) -> u32 {
        self.lang.syms.lookup(self.lit(at))
    }

    fn set(&self, at: usize) -> impl Iterator<Item = &'s str> + use<'s> {
        let lang = self.lang;
        self.args[at].iter().map(move |&v| lit(lang, v))
    }
}

fn lit(lang: &Lang, v: Val) -> &str {
    match v {
        Val::Sym(s) => lang.syms.resolve(s),
        Val::Kind(k) => lang.kind_name(k),
        Val::Bool(true) => "true",
        Val::Bool(false) => "false",
        _ => "",
    }
}

fn opts<'t: 's, 's, S: Store>(cx: &Cx<'_, 't, Text, S>, args: &'s [Vals]) -> Opts<'s> {
    Opts {
        lang: cx.lang(),
        args,
    }
}

fn text<'t, S: Store>(cx: &Cx<'_, 't, Text, S>, v: Val) -> Option<&'t str> {
    let lang = cx.lang();
    match v {
        Val::Node(_) | Val::Type(_) => Some(cx.text(v).map_or("", |s| lang.syms.resolve(s))),
        Val::Opaque => None,
        v => Some(lit(lang, v)),
    }
}

fn targets<S: Store>(cx: &Cx<'_, '_, Text, S>, a: &[Vals], unresolved: bool) -> Vals {
    let (Some(loc), Ok(kind)) = (a[0][0].loc(), opts(cx, a).lit(1).parse::<EdgeKind>()) else {
        return Vals::new();
    };
    let found = match unresolved {
        true => cx.unresolved_targets(kind, loc),
        false => cx.targets(kind, loc),
    };
    found.iter().map(|&l| Val::Node(l)).collect()
}

/// The project tree's directory that holds a file tree, found by its path.
fn file_dir<S: Store>(cx: &Cx<'_, '_, Text, S>, a: &[Vals]) -> Vals {
    let (Some(loc), Some(mut at)) = (a[0][0].loc(), cx.project()) else {
        return Vals::new();
    };
    let syms = &cx.lang().syms;
    let label = &cx.jump(loc).tree().label;
    let dirs = label.rsplit_once('/').map_or("", |(dirs, _)| dirs);
    for segment in dirs.split('/').filter(|s| !s.is_empty()) {
        let Some(dir) = at
            .children()
            .find(|c| c.is(crate::canonical::Canonical::Dir) && syms.resolve(c.sym()) == segment)
        else {
            return Vals::new();
        };
        at = dir;
    }
    one(Val::Node(at.into()))
}

fn has_edge<S: Store>(cx: &Cx<'_, '_, Text, S>, a: &[Vals], incoming: bool) -> Vals {
    let (Some(loc), Ok(kind)) = (a[0][0].loc(), opts(cx, a).lit(1).parse::<EdgeKind>()) else {
        return Vals::new();
    };
    yes(cx.has_edge(kind, incoming, loc.node))
}

fn sym(lang: &Lang, s: &str) -> Vals {
    match s.is_empty() {
        true => Vals::new(),
        false => one(Val::Sym(lang.syms.intern(s))),
    }
}

type Edit = for<'s> fn(&'s str, &Opts<'s>) -> Cow<'s, str>;

fn edit<S: Store>(cx: &Cx<'_, '_, Text, S>, a: &[Vals], f: Edit) -> Vals {
    let lang = cx.lang();
    let o = Opts { lang, args: a };
    match text(cx, a[0][0]) {
        Some(s) if !s.is_empty() => sym(lang, &f(s, &o)),
        _ => Vals::new(),
    }
}

fn at<'t, S: Store>(
    cx: &Cx<'_, 't, Text, S>,
    a: &[Vals],
    f: impl FnOnce(Cursor<'t, S>, &Opts) -> Option<Val>,
) -> Vals {
    let Some(loc) = a[0][0].loc() else {
        return Vals::new();
    };
    let o = Opts {
        lang: cx.lang(),
        args: a,
    };
    match f(cx.jump(loc), &o) {
        Some(Val::Sym(0)) | None => Vals::new(),
        Some(v) => one(v),
    }
}

fn regex(src: &str) -> Option<Regex> {
    thread_local! {
        static CACHE: RefCell<FxHashMap<Box<str>, Option<Regex>>> = RefCell::default();
    }
    CACHE.with_borrow_mut(|cache| {
        if let Some(re) = cache.get(src) {
            return re.clone();
        }
        let re = Regex::new(src).ok();
        cache.insert(src.into(), re.clone());
        re
    })
}

fn strip<'s>(s: &'s str, o: &Opts<'s>) -> Cow<'s, str> {
    s.strip_prefix(o.lit(1)).unwrap_or(s).into()
}

fn strip_leading<'s>(s: &'s str, o: &Opts<'s>) -> Cow<'s, str> {
    let chars = o.lit(1);
    s.trim_start_matches(|c| chars.contains(c)).into()
}

fn strip_suffix<'s>(s: &'s str, o: &Opts<'s>) -> Cow<'s, str> {
    s.strip_suffix(o.lit(1)).unwrap_or(s).into()
}

fn split_last<'s>(s: &'s str, o: &Opts<'s>) -> Cow<'s, str> {
    s.rsplit_once(o.lit(1)).map_or(s, |(_, r)| r).into()
}

fn split_first<'s>(s: &'s str, o: &Opts<'s>) -> Cow<'s, str> {
    s.split_once(o.lit(1)).map_or(s, |(l, _)| l).into()
}

fn replace<'s>(s: &'s str, o: &Opts<'s>) -> Cow<'s, str> {
    match s.contains(o.lit(1)) {
        true => s.replace(o.lit(1), o.lit(2)).into(),
        false => s.into(),
    }
}

fn prepend<'s>(s: &'s str, o: &Opts<'s>) -> Cow<'s, str> {
    format!("{}{s}", o.lit(1)).into()
}

fn lowercase<'s>(s: &'s str, _: &Opts<'s>) -> Cow<'s, str> {
    s.to_lowercase().into()
}

fn regex_replace<'s>(s: &'s str, o: &Opts<'s>) -> Cow<'s, str> {
    regex(o.lit(1)).map_or(s.into(), |re| {
        re.replace_all(s, o.lit(2)).into_owned().into()
    })
}

fn regex_first<'s>(s: &'s str, o: &Opts<'s>) -> Cow<'s, str> {
    regex(o.lit(1)).map_or(s.into(), |re| re.replace(s, o.lit(2)).into_owned().into())
}

fn regex_loop<'s>(s: &'s str, o: &Opts<'s>) -> Cow<'s, str> {
    let Some(re) = regex(o.lit(1)) else {
        return s.into();
    };
    let mut cur = Cow::Borrowed(s);
    for _ in 0..MAX_REGEX_LOOP_PASSES {
        let next = match re.replace_all(&cur, o.lit(2)) {
            Cow::Borrowed(_) => break,
            Cow::Owned(next) => next,
        };
        cur = Cow::Owned(next);
    }
    cur
}

fn regex_match(s: &str, o: &Opts) -> bool {
    regex(o.lit(1)).is_some_and(|re| re.is_match(s))
}

fn keep_last<'s>(s: &'s str, o: &Opts<'s>) -> Cow<'s, str> {
    let (sep, segments, bytes) = (o.lit(1), o.num(2), o.num(3));
    if sep.is_empty() || segments == 0 {
        return s.into();
    }
    let mut start = s
        .rmatch_indices(sep)
        .nth(segments - 1)
        .map_or(0, |(i, _)| i + sep.len());
    if s.len() - start > bytes {
        let min = s.ceil_char_boundary(s.len() - bytes);
        start = s[min..]
            .find(sep)
            .map(|i| min + i + sep.len())
            .filter(|&i| i < s.len())
            .unwrap_or(min);
    }
    s[start..].into()
}

fn stem<'s>(s: &'s str, _: &Opts<'s>) -> Cow<'s, str> {
    let path = std::path::Path::new(s);
    path.with_extension("")
        .to_string_lossy()
        .into_owned()
        .into()
}

fn collapse_index<'s>(s: &'s str, o: &Opts<'s>) -> Cow<'s, str> {
    for name in o.set(1) {
        if let Some(dir) = s.strip_suffix(name).and_then(|p| p.strip_suffix('/')) {
            return dir.into();
        }
        if s == name {
            return "".into();
        }
    }
    s.into()
}

fn map<'s>(s: &'s str, o: &Opts<'s>) -> Cow<'s, str> {
    o.set(1)
        .filter_map(|pair| pair.split_once(':'))
        .find_map(|(from, to)| (from == s).then_some(to))
        .unwrap_or(s)
        .into()
}

fn concat(a: Option<&str>, sep: &str, b: Option<&str>) -> String {
    match (a, b) {
        (Some(a), Some(b)) => format!("{a}{sep}{b}"),
        (Some(x), None) | (None, Some(x)) => x.to_string(),
        (None, None) => String::new(),
    }
}

fn text_sym<S: Storage<Node = Node>>(c: Cursor<'_, S>, lang: &Lang) -> Option<Val> {
    match c.tree().sym_at(c.index(), lang) {
        0 => None,
        sym => Some(Val::Sym(sym)),
    }
}

fn child_of<'s, S: Storage<Node = Node>>(c: Cursor<'s, S>, kind: u16) -> Option<Cursor<'s, S>> {
    c.children().find(|k| k.node().kind == kind)
}

fn field<S: Storage<Node = Node>>(c: Cursor<'_, S>, o: &Opts) -> Option<Val> {
    let field = o.field(1);
    let sym = |c: Cursor<'_, S>| c.tree().sym_at(c.index(), o.lang);
    let found = c
        .children()
        .find(|k| k.node().field == field)
        .map_or(0, sym);
    Some(Val::Sym(if found != 0 { found } else { sym(c) }))
}

fn child<S: Storage<Node = Node>>(c: Cursor<'_, S>, o: &Opts) -> Option<Val> {
    child_of(c, o.kind(1)).map(|k| Val::Node(k.into()))
}

fn field_child<S: Storage<Node = Node>>(c: Cursor<'_, S>, o: &Opts) -> Option<Val> {
    let (field, kind) = (o.field(1), o.kind(2));
    let holder = c.children().find(|k| k.node().field == field)?;
    text_sym(child_of(holder, kind)?, o.lang)
}

fn child_sym<S: Storage<Node = Node>>(c: Cursor<'_, S>, o: &Opts) -> Option<Val> {
    text_sym(child_of(c, o.kind(1))?, o.lang)
}

fn parent_sym<S: Storage<Node = Node>>(c: Cursor<'_, S>, o: &Opts) -> Option<Val> {
    text_sym(child_of(c.parent()?, o.kind(1))?, o.lang)
}

fn ancestor_sym<S: Storage<Node = Node>>(c: Cursor<'_, S>, o: &Opts) -> Option<Val> {
    let kind = o.kind(1);
    let found = std::iter::once(c)
        .chain(c.ancestors())
        .find_map(|a| child_of(a, kind).filter(|k| k.tree().sym_at(k.index(), o.lang) != 0))?;
    text_sym(found, o.lang)
}

fn ancestor_tag<S: Storage<Node = Node>>(c: Cursor<'_, S>, o: &Opts) -> Option<Val> {
    let key = o.key(1);
    c.ancestors().find_map(|a| a.tag(key)).map(Val::Sym)
}

fn tag<S: Storage<Node = Node>>(c: Cursor<'_, S>, o: &Opts) -> Option<Val> {
    c.tag(o.key(1)).map(Val::Sym)
}

fn tree_path<S: Storage<Node = Node>>(c: Cursor<'_, S>, o: &Opts) -> Option<Val> {
    let mut segments: SmallVec<[&str; 8]> = std::iter::once(c)
        .chain(c.ancestors())
        .filter(|a| a.parent().is_some())
        .map(|a| a.tree().text_at(a.index(), o.lang))
        .collect();
    segments.reverse();
    Some(Val::Sym(o.lang.syms.intern(&segments.join(o.lit(1)))))
}

fn sibling_index<S: Storage<Node = Node>>(c: Cursor<'_, S>, o: &Opts) -> Option<Val> {
    let storage = &c.tree().storage;
    let index = std::iter::successors(storage.previous_sibling(c.index()), |&id| {
        storage.previous_sibling(id)
    })
    .filter(|&id| storage.node(id).named)
    .count();
    Some(Val::Sym(o.lang.syms.intern(&index.to_string())))
}

fn tree_node_id<S: Storage<Node = Node>>(c: Cursor<'_, S>, o: &Opts) -> Option<Val> {
    Some(Val::Sym(o.lang.syms.intern(&c.index().to_string())))
}

fn kind_name<S: Storage<Node = Node>>(c: Cursor<'_, S>, o: &Opts) -> Option<Val> {
    Some(Val::Sym(
        o.lang.syms.intern(o.lang.kind_name(c.node().kind)),
    ))
}

fn number(o: &Opts, n: u32) -> Option<Val> {
    text_val(o, &n.to_string())
}

fn text_val(o: &Opts, s: &str) -> Option<Val> {
    Some(Val::Sym(o.lang.syms.intern(s)))
}

fn constant<S: Storage<Node = Node>>(_: Cursor<'_, S>, o: &Opts) -> Option<Val> {
    Some(Val::Sym(o.lang.syms.intern(o.lit(1))))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tree::{Mutable, Tree};

    fn args(lang: &Lang, lits: &[&str]) -> Vec<Vals> {
        std::iter::once(Vals::new())
            .chain(lits.iter().map(|l| one(Val::Sym(lang.syms.intern(l)))))
            .collect()
    }

    fn run(f: Edit, s: &str, lits: &[&str]) -> String {
        let lang = Lang::new();
        let args = args(&lang, lits);
        f(
            s,
            &Opts {
                lang: &lang,
                args: &args,
            },
        )
        .into_owned()
    }

    #[test]
    fn affixes_and_splits() {
        assert_eq!(run(strip, "::A::B", &["::"]), "A::B");
        assert_eq!(run(strip, "A", &["::"]), "A");
        assert_eq!(run(strip_leading, "@@:a", &["@:"]), "a");
        assert_eq!(run(strip_suffix, "List<T>", &[">"]), "List<T");
        assert_eq!(run(split_last, "a.b.c", &["."]), "c");
        assert_eq!(run(split_first, "a.b.c", &["."]), "a");
        assert_eq!(run(split_last, "abc", &["."]), "abc");
        assert_eq!(run(replace, "a::b::c", &["::", "/"]), "a/b/c");
        assert_eq!(run(prepend, "x", &["./"]), "./x");
        assert_eq!(run(lowercase, "AbC", &[]), "abc");
    }

    #[test]
    fn regexes() {
        assert_eq!(
            run(regex_replace, "FooBar", &["([a-z0-9])([A-Z])", "${1}_${2}"]),
            "Foo_Bar"
        );
        assert_eq!(run(regex_first, "a/a/a", &["a", "b"]), "b/a/a");
        assert_eq!(run(regex_loop, "a/b/../../c", &["[^/.]+/\\.\\./", ""]), "c");
        assert_eq!(
            run(regex_replace, "abc", &["(", "x"]),
            "abc",
            "a bad regex edits nothing"
        );
        let lang = Lang::new();
        let a = args(&lang, &["^a.c$"]);
        let o = Opts {
            lang: &lang,
            args: &a,
        };
        assert!(regex_match("abc", &o));
        assert!(!regex_match("abd", &o));
    }

    #[test]
    fn paths_and_lookups() {
        let lang = Lang::new();
        let mut a = args(&lang, &["::"]);
        a.extend([one(Val::Sym(2)), one(Val::Sym(8))]);
        let o = Opts {
            lang: &lang,
            args: &a,
        };
        assert_eq!(keep_last("a::b::c::d", &o), "c::d");
        assert_eq!(keep_last("a::b::cccccccc", &o), "cccccccc");
        assert_eq!(run(stem, "pkg/mod.py", &[]), "pkg/mod");
        let names: Vals = ["__init__", "index"]
            .iter()
            .map(|n| Val::Sym(lang.syms.intern(n)))
            .collect();
        let a = vec![Vals::new(), names];
        let o = Opts {
            lang: &lang,
            args: &a,
        };
        assert_eq!(collapse_index("pkg/__init__", &o), "pkg");
        assert_eq!(collapse_index("index", &o), "");
        assert_eq!(collapse_index("pkg/mod", &o), "pkg/mod");
        let pairs: Vals = ["rb:ruby", "+=:add_assign"]
            .iter()
            .map(|n| Val::Sym(lang.syms.intern(n)))
            .collect();
        let a = vec![Vals::new(), pairs];
        let o = Opts {
            lang: &lang,
            args: &a,
        };
        assert_eq!(map("+=", &o), "add_assign");
        assert_eq!(map("py", &o), "py");
        assert_eq!(concat(Some("a"), "::", Some("b")), "a::b");
        assert_eq!(concat(None, "::", Some("b")), "b");
        assert_eq!(concat(None, "::", None), "");
    }

    fn node(
        tree: &mut Tree<Mutable>,
        lang: &Lang,
        parent: indextree::NodeId,
        kind: &str,
        field: &str,
        text: &str,
    ) -> indextree::NodeId {
        let n = Node {
            kind: lang.intern_kind(kind),
            field: if field.is_empty() {
                0
            } else {
                lang.intern_field(field)
            },
            sym: if text.is_empty() {
                0
            } else {
                lang.syms.intern(text)
            },
            named: true,
            ..Default::default()
        };
        tree.append(parent, n)
    }

    #[test]
    fn node_functions() {
        let lang = Lang::new();
        let mut tree = Tree::new(Node {
            kind: lang.intern_kind("file"),
            sym: lang.syms.intern("root"),
            named: true,
            ..Default::default()
        });
        let root = tree.root;
        let class = node(&mut tree, &lang, root, "class", "", "C");
        node(&mut tree, &lang, class, "name", "", "Klass");
        let body = node(&mut tree, &lang, class, "body", "body", "B");
        node(&mut tree, &lang, body, "punct", "", "");
        let target = node(&mut tree, &lang, body, "method", "", "m");
        let path = node(&mut tree, &lang, target, "path", "path", "p");
        node(&mut tree, &lang, path, "leaf", "", "L");
        let fqn = lang.syms.intern("fqn");
        tree.set_tag(Tree::to_raw(class), fqn, lang.syms.intern("pkg.C"));
        tree.set_tag(Tree::to_raw(target), fqn, lang.syms.intern("pkg.C.m"));
        let c = tree.cursor(Tree::to_raw(target));
        let text = |v: Option<Val>| match v {
            Some(Val::Sym(s)) => lang.syms.resolve(s).to_string(),
            other => format!("{other:?}"),
        };
        let call = |f: fn(Cursor<'_, Mutable>, &Opts) -> Option<Val>, lits: &[&str]| {
            let a = args(&lang, lits);
            text(f(
                c,
                &Opts {
                    lang: &lang,
                    args: &a,
                },
            ))
        };
        assert_eq!(call(field, &["path"]), "p");
        assert_eq!(call(field, &["other"]), "m");
        assert_eq!(call(field_child, &["path", "leaf"]), "L");
        assert_eq!(call(child_sym, &["path"]), "p");
        assert_eq!(call(child_sym, &["leaf"]), "None");
        assert_eq!(call(parent_sym, &["punct"]), "None");
        assert_eq!(call(ancestor_sym, &["name"]), "Klass");
        assert_eq!(call(ancestor_tag, &["fqn"]), "pkg.C");
        assert_eq!(call(tag, &["fqn"]), "pkg.C.m");
        assert_eq!(call(tree_path, &["/"]), "C/B/m");
        assert_eq!(call(sibling_index, &[]), "1");
        assert_eq!(call(tree_node_id, &[]), Tree::to_raw(target).to_string());
        assert_eq!(call(kind_name, &[]), "method");
        assert_eq!(call(constant, &["x"]), "x");
        let a = args(&lang, &["path"]);
        let found = child(
            c,
            &Opts {
                lang: &lang,
                args: &a,
            },
        );
        assert_eq!(
            found,
            Some(Val::Node(tree.cursor(Tree::to_raw(path)).into()))
        );
    }
}
