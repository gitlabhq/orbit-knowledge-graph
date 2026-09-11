use std::collections::HashMap;
use std::marker::PhantomData;

use crate::lang::Lang;
use crate::tree::{NONE, Node, Tree, copy_subtree, elems, live};

// ── Phase markers ──

pub struct Match;
pub struct Template;

pub trait Phase {
    fn resolve_slot(
        slots: &mut HashMap<Box<str>, u16>,
        filters: &mut Vec<Vec<u16>>,
        n: &str,
    ) -> u16;
    fn apply_filter(filters: &mut [Vec<u16>], slot: u16, kinds: Vec<u16>);
}

impl Phase for Match {
    fn resolve_slot(
        slots: &mut HashMap<Box<str>, u16>,
        filters: &mut Vec<Vec<u16>>,
        n: &str,
    ) -> u16 {
        let next = slots.len() as u16;
        let s = *slots.entry(n.into()).or_insert(next);
        if filters.len() <= s as usize {
            filters.resize(s as usize + 1, Vec::new());
        }
        s
    }
    fn apply_filter(filters: &mut [Vec<u16>], slot: u16, kinds: Vec<u16>) {
        filters[slot as usize] = kinds;
    }
}

impl Phase for Template {
    fn resolve_slot(
        slots: &mut HashMap<Box<str>, u16>,
        _filters: &mut Vec<Vec<u16>>,
        n: &str,
    ) -> u16 {
        *slots
            .get(n)
            .unwrap_or_else(|| panic!("template references unknown slot: {n}"))
    }
    fn apply_filter(_filters: &mut [Vec<u16>], _slot: u16, _kinds: Vec<u16>) {}
}

#[derive(Clone)]
pub enum Tf {
    Id,
    Strip(Box<str>),
    Field(u16),
    Const(&'static str),
    Child(u16),
    FieldChild(u16, u16),
    StripLeading(char),
    SplitLast(Box<str>),
    Replace(Box<str>, Box<str>),
    StripSuffix(Box<str>),
    Prepend(Box<str>),
    ToRel(char),
    Lowercase,
    Pipeline(Vec<Tf>),
}

impl Tf {
    fn apply_to_str(&self, s: &str) -> String {
        match self {
            Tf::Id => s.to_string(),
            Tf::Strip(p) => s.strip_prefix(&**p).unwrap_or(s).to_string(),
            Tf::StripSuffix(p) => s.strip_suffix(&**p).unwrap_or(s).to_string(),
            Tf::StripLeading(ch) => s.trim_start_matches(*ch).to_string(),
            Tf::SplitLast(sep) => s.rsplit_once(&**sep).map_or(s, |(_, r)| r).to_string(),
            Tf::Replace(from, to) => s.replace(&**from, to),
            Tf::Prepend(p) => format!("{p}{s}"),
            Tf::Lowercase => s.to_lowercase(),
            Tf::ToRel(ch) => {
                let count = s.chars().take_while(|c| c == ch).count();
                let rest = s[count..].replace(*ch, "/");
                match count {
                    0 => rest,
                    1 => format!("./{rest}"),
                    n => {
                        let prefix = "../".repeat(n - 1);
                        format!("{prefix}{rest}")
                    }
                }
            }
            Tf::Pipeline(steps) => {
                let mut result = s.to_string();
                for step in steps {
                    result = step.apply_to_str(&result);
                }
                result
            }
            Tf::Field(_) | Tf::Child(_) | Tf::FieldChild(_, _) | Tf::Const(_) => {
                unreachable!("tree-context transform used as string transform")
            }
        }
    }

    fn apply_sym(&self, t: &Tree, lang: &mut Lang, i: u32) -> u32 {
        match self {
            Tf::Id => t.sym(i),
            Tf::Field(f) => t.child_by_field(i, *f).map_or(t.sym(i), |c| t.sym(c)),
            Tf::Const(s) => lang.syms.intern(s),
            Tf::Child(k) => t
                .children(i)
                .find(|&c| t.kind(c) == *k)
                .map_or(0, |c| t.sym(c)),
            Tf::FieldChild(f, k) => t
                .child_by_field(i, *f)
                .and_then(|n| t.children(n).find(|&c| t.kind(c) == *k))
                .map_or(0, |c| t.sym(c)),
            _ => {
                let sym = t.sym(i);
                if sym == 0 {
                    return 0;
                }
                let s = lang.syms.resolve(sym).to_string();
                let result = self.apply_to_str(&s);
                lang.syms.intern(&result)
            }
        }
    }
}

pub enum Text {
    Any,
    Lit(u32),
    From(u16, Tf),
}

pub enum Pat {
    Node {
        kind: u16,
        field: u16,
        text: Text,
        kids: Vec<Pat>,
    },
    Cap {
        slot: u16,
        field: u16,
        kind: Option<u16>,
        rekind: Option<u16>,
    },
    Var {
        slot: u16,
        field: u16,
        rekind: Option<u16>,
        leaf_only: bool,
    },
}

pub enum Out {
    Remove,
    SetKind(u16),
    Retag {
        kind: u16,
        fields: Vec<(u16, u16)>,
    },
    SetText {
        target: u16,
        from: u16,
        tf: Tf,
    },
    Append {
        under: u16,
        each: u16,
        kind: u16,
        tf: Tf,
    },
    Replace(Pat),
}

pub struct Rewrite {
    pub pat: Pat,
    pub out: Out,
    pub nslots: usize,
    pub filters: Vec<Vec<u16>>,
}

pub struct Ctx<'l, P: Phase> {
    pub lang: &'l mut Lang,
    slots: HashMap<Box<str>, u16>,
    filters: Vec<Vec<u16>>,
    _phase: PhantomData<P>,
}

impl<'l, P: Phase> Ctx<'l, P> {
    pub fn slot(&mut self, n: &str) -> u16 {
        P::resolve_slot(&mut self.slots, &mut self.filters, n)
    }

    pub fn intern_kind(&mut self, k: &str) -> u16 {
        self.lang.intern_kind(k)
    }

    pub fn intern_field(&mut self, f: &str) -> u16 {
        self.lang.intern_field(f)
    }

    fn apply_filter(&mut self, slot: u16, kinds: Vec<u16>) {
        P::apply_filter(&mut self.filters, slot, kinds);
    }
}

impl<'l> Ctx<'l, Match> {
    fn new(lang: &'l mut Lang) -> Self {
        Ctx {
            lang,
            slots: HashMap::new(),
            filters: Vec::new(),
            _phase: PhantomData,
        }
    }

    fn freeze(self) -> Ctx<'l, Template> {
        Ctx {
            lang: self.lang,
            slots: self.slots,
            filters: self.filters,
            _phase: PhantomData,
        }
    }
}

impl Ctx<'_, Template> {
    pub fn template(&mut self, src: &str) -> Pat {
        parse(self, src)
    }
}

impl Rewrite {
    pub fn new(lang: &mut Lang, src: &str, out: impl FnOnce(&mut Ctx<Template>) -> Out) -> Rewrite {
        let mut mc = Ctx::<Match>::new(lang);
        mc.slot("ROOT");
        let pat = parse(&mut mc, src);
        let mut tc = mc.freeze();
        let out = out(&mut tc);
        let nslots = tc.slots.len();
        Rewrite {
            pat,
            out,
            nslots,
            filters: tc.filters,
        }
    }
}

// ── Pest-based parser ──
//
// Grammar lives in pattern.pest. Pest produces the parse tree,
// the visitor below walks it and calls Ctx to intern kinds/slots/filters.

use pest_derive::Parser;

#[derive(Parser)]
#[grammar = "pattern.pest"]
struct PatParser;

#[pest_consume::parser]
impl PatParser {}

fn parse<P: Phase>(c: &mut Ctx<'_, P>, src: &str) -> Pat {
    let root = <PatParser as pest_consume::Parser>::parse(Rule::Pattern, src)
        .unwrap_or_else(|e| panic!("pattern parse error: {e}"))
        .single()
        .expect("Pattern produces one pair");
    visit_element(c, root.into_children().next().unwrap(), 0)
}

type PNode<'i> = pest_consume::Node<'i, Rule, ()>;

fn visit_element<P: Phase>(c: &mut Ctx<'_, P>, node: PNode<'_>, field: u16) -> Pat {
    match node.as_rule() {
        Rule::Node => visit_node(c, node, field),
        Rule::Variadic => visit_variadic(c, node, field),
        Rule::CapRef => visit_cap_ref(c, node, field),
        Rule::Capture => visit_capture(c, node, field),
        Rule::TextField => visit_text_field_as_cap(c, node, field),
        r => panic!("unexpected rule in element: {r:?}"),
    }
}

fn visit_node<P: Phase>(c: &mut Ctx<'_, P>, node: PNode<'_>, field: u16) -> Pat {
    let mut children = node.into_children();
    let kind = c.intern_kind(children.next().expect("Node has Ident").as_str());

    let mut kids = Vec::new();
    let mut text = Text::Any;

    for child in children {
        match child.as_rule() {
            Rule::Quoted => {
                text = Text::Lit(c.lang.syms.intern(quoted_inner(&child)));
            }
            Rule::TextField => {
                let (slot, tf) = visit_text_field(c, child);
                text = Text::From(slot, tf);
            }
            Rule::FieldChild => {
                let mut fc = child.into_children();
                let f = c.intern_field(fc.next().unwrap().as_str());
                kids.push(visit_element(c, fc.next().unwrap(), f));
            }
            Rule::Node | Rule::Variadic | Rule::CapRef | Rule::Capture => {
                kids.push(visit_element(c, child, 0));
            }
            r => panic!("unexpected content in Node: {r:?}"),
        }
    }

    Pat::Node {
        kind,
        field,
        text,
        kids,
    }
}

fn visit_text_field<P: Phase>(c: &mut Ctx<'_, P>, node: PNode<'_>) -> (u16, Tf) {
    let mut children = node.into_children();
    let slot = c.slot(children.next().unwrap().as_str());
    let tf = match children.next() {
        Some(chain) if chain.as_rule() == Rule::TfChain => visit_tf_chain(c, chain),
        _ => Tf::Id,
    };
    (slot, tf)
}

fn visit_text_field_as_cap<P: Phase>(c: &mut Ctx<'_, P>, node: PNode<'_>, field: u16) -> Pat {
    let name = node.into_children().next().unwrap().as_str();
    Pat::Cap {
        slot: c.slot(name),
        field,
        kind: None,
        rekind: None,
    }
}

fn visit_variadic<P: Phase>(c: &mut Ctx<'_, P>, node: PNode<'_>, field: u16) -> Pat {
    let mut children = node.into_children();
    let slot = c.slot(children.next().unwrap().as_str());

    let mut leaf_only = false;
    let mut rekind = None;

    for child in children {
        match child.as_rule() {
            Rule::Filter => {
                let filter: Vec<u16> = child
                    .into_children()
                    .map(|k| c.intern_kind(k.as_str()))
                    .collect();
                c.apply_filter(slot, filter);
            }
            Rule::Arrow => leaf_only = child.as_str() == "=>",
            Rule::Ident => rekind = Some(c.intern_kind(child.as_str())),
            r => panic!("unexpected child in Variadic: {r:?}"),
        }
    }

    Pat::Var {
        slot,
        field,
        rekind,
        leaf_only,
    }
}

fn visit_cap_ref<P: Phase>(c: &mut Ctx<'_, P>, node: PNode<'_>, field: u16) -> Pat {
    let mut children = node.into_children();
    let name = children.next().unwrap().as_str();
    let _arrow = children.next();
    let rekind = c.intern_kind(children.next().unwrap().as_str());
    Pat::Cap {
        slot: c.slot(name),
        field,
        kind: None,
        rekind: Some(rekind),
    }
}

fn visit_capture<P: Phase>(c: &mut Ctx<'_, P>, node: PNode<'_>, field: u16) -> Pat {
    let mut children = node.into_children();
    let name = children.next().unwrap().as_str();
    let kind = children.next().map(|k| c.intern_kind(k.as_str()));
    Pat::Cap {
        slot: c.slot(name),
        field,
        kind,
        rekind: None,
    }
}

fn visit_tf_chain<P: Phase>(c: &mut Ctx<'_, P>, node: PNode<'_>) -> Tf {
    let tfs: Vec<Tf> = node.into_children().map(|e| visit_tf_expr(c, e)).collect();
    if tfs.len() == 1 {
        tfs.into_iter().next().unwrap()
    } else {
        Tf::Pipeline(tfs)
    }
}

fn visit_tf_expr<P: Phase>(c: &mut Ctx<'_, P>, node: PNode<'_>) -> Tf {
    let inner = node.into_children().next().unwrap();
    match inner.as_rule() {
        Rule::TfFunc => {
            let mut ch = inner.into_children();
            let func = ch.next().unwrap().as_str();
            let args: Vec<&str> = ch
                .next()
                .unwrap()
                .into_children()
                .map(|q| quoted_inner(&q))
                .collect();
            match func {
                "replace" => {
                    assert_eq!(args.len(), 2, "replace needs 2 args");
                    Tf::Replace(args[0].into(), args[1].into())
                }
                "strip_prefix" => Tf::Strip(args[0].into()),
                "strip_suffix" => Tf::StripSuffix(args[0].into()),
                "prepend" => Tf::Prepend(args[0].into()),
                "to_rel" => Tf::ToRel(args[0].chars().next().expect("to_rel arg")),
                "split_last" => Tf::SplitLast(args[0].into()),
                _ => panic!("unknown transform: {func}"),
            }
        }
        Rule::TfLegacy => {
            let mut ch = inner.into_children();
            let name = ch.next().unwrap().as_str();
            let val = ch.next().unwrap().as_str();
            match name {
                "strip" => Tf::Strip(val.into()),
                "field" => Tf::Field(c.intern_field(val)),
                _ => panic!("unknown legacy transform: {name}"),
            }
        }
        Rule::TfBare => {
            let name = inner.into_children().next().unwrap().as_str();
            match name {
                "lowercase" => Tf::Lowercase,
                _ => panic!("unknown bare transform: {name}"),
            }
        }
        r => panic!("unexpected tf rule: {r:?}"),
    }
}

fn quoted_inner<'i>(node: &PNode<'i>) -> &'i str {
    node.clone()
        .into_children()
        .find(|c| c.as_rule() == Rule::Inner)
        .map(|c| c.as_str())
        .unwrap_or("")
}

// ── Runtime: matching, materialization, rewriting ──

fn matches(t: &Tree, i: u32, p: &Pat, caps: &mut [(u32, u32)]) -> bool {
    let n = t.node(i);
    let field_ok = |f: u16| f == 0 || f == n.field;
    match p {
        Pat::Var { .. } => false,
        Pat::Cap {
            slot, field, kind, ..
        } => {
            if !field_ok(*field) || kind.is_some_and(|k| k != n.kind) {
                return false;
            }
            caps[*slot as usize] = (i, t.hop(i));
            true
        }
        Pat::Node {
            kind,
            field,
            text,
            kids,
        } => {
            if n.kind != *kind || !field_ok(*field) {
                return false;
            }
            if let Text::Lit(s) = text
                && n.sym != *s
            {
                return false;
            }
            let end = t.hop(i);
            let mut c = live(t, i + 1, end);
            for (k, kid) in kids.iter().enumerate() {
                if let Pat::Var { slot, .. } = kid {
                    let start = c;
                    while c < end && !kids.get(k + 1).is_some_and(|nx| matches(t, c, nx, caps)) {
                        c = live(t, t.hop(c), end);
                    }
                    caps[*slot as usize] = (start, c);
                } else {
                    while c < end && !matches(t, c, kid, caps) {
                        c = live(t, t.hop(c), end);
                    }
                    if c >= end {
                        return false;
                    }
                    c = live(t, t.hop(c), end);
                }
            }
            true
        }
    }
}

fn materialize(
    t: &Tree,
    lang: &mut Lang,
    p: &Pat,
    caps: &[(u32, u32)],
    filters: &[Vec<u16>],
    out: &mut Vec<Node>,
    parent: u32,
    span: (u32, u32),
) {
    match p {
        Pat::Cap {
            slot,
            field,
            rekind,
            ..
        } => {
            let at = out.len();
            copy_subtree(t, caps[*slot as usize].0, out, parent);
            if *field != 0 {
                out[at].field = *field;
            }
            if let Some(k) = rekind {
                out[at].kind = *k;
                out[at].field = 0;
            }
        }
        Pat::Var {
            slot,
            rekind,
            leaf_only,
            ..
        } => {
            for e in elems(t, caps[*slot as usize], &filters[*slot as usize]) {
                let at = out.len();
                if *leaf_only {
                    let n = t.node(e);
                    if n.sym == 0 {
                        continue;
                    }
                    out.push(Node {
                        kind: rekind.unwrap_or(n.kind),
                        field: 0,
                        named: true,
                        synth: true,
                        sym: n.sym,
                        size: 1,
                        parent,
                        start: n.start,
                        end: n.end,
                        ..Default::default()
                    });
                } else {
                    copy_subtree(t, e, out, parent);
                    if let Some(k) = rekind {
                        out[at].kind = *k;
                    }
                }
            }
        }
        Pat::Node {
            kind,
            field,
            text,
            kids,
        } => {
            let at = out.len();
            let sym = match text {
                Text::Any => 0,
                Text::Lit(s) => *s,
                Text::From(slot, tf) => tf.apply_sym(t, lang, caps[*slot as usize].0),
            };
            out.push(Node {
                kind: *kind,
                field: *field,
                named: true,
                synth: true,
                sym,
                parent,
                start: span.0,
                end: span.1,
                ..Default::default()
            });
            for k in kids {
                materialize(t, lang, k, caps, filters, out, at as u32, span);
            }
            out[at].size = (out.len() - at) as u32;
        }
    }
}

enum Edit {
    Remove(u32),
    SetKind(u32, u16),
    SetField(u32, u16),
    SetText(u32, u32),
    Append(u32, Node),
    Replace(u32, u32, u32),
}

pub fn apply_rewrites(t: &mut Tree, lang: &mut Lang, rules: &[Rewrite]) -> Vec<u32> {
    let mut caps = vec![(0u32, 0u32); rules.iter().map(|r| r.nslots).max().unwrap_or(1)];
    let mut buf: Vec<Node> = Vec::new();
    for i in (0..t.nodes.len() as u32).rev() {
        if t.nodes[i as usize].dead {
            continue;
        }
        for r in rules {
            if !matches(t, i, &r.pat, &mut caps) {
                continue;
            }
            caps[0] = (i, t.hop(i));
            let root = t.nodes[i as usize];
            match &r.out {
                Out::Remove => t.remove(i),
                Out::SetKind(k) => t.set_kind(i, *k),
                Out::Retag { kind, fields } => {
                    t.set_kind(i, *kind);
                    for (slot, f) in fields {
                        t.set_field(caps[*slot as usize].0, *f);
                    }
                }
                Out::SetText { target, from, tf } => {
                    let sym = tf.apply_sym(t, lang, caps[*from as usize].0);
                    t.set_text(caps[*target as usize].0, sym);
                }
                Out::Append {
                    under,
                    each,
                    kind,
                    tf,
                } => {
                    let items: Vec<(u32, u32, u32)> =
                        elems(t, caps[*each as usize], &r.filters[*each as usize])
                            .map(|e| {
                                (
                                    tf.apply_sym(t, lang, e),
                                    t.nodes[e as usize].start,
                                    t.nodes[e as usize].end,
                                )
                            })
                            .collect();
                    for (sym, start, end) in items {
                        t.append(
                            caps[*under as usize].0,
                            Node {
                                kind: *kind,
                                named: true,
                                synth: true,
                                sym,
                                start,
                                end,
                                size: 1,
                                ..Default::default()
                            },
                        );
                    }
                }
                Out::Replace(tpl) => {
                    let s = buf.len() as u32;
                    materialize(
                        t,
                        lang,
                        tpl,
                        &caps,
                        &r.filters,
                        &mut buf,
                        NONE,
                        (root.start, root.end),
                    );
                    let l = buf.len() as u32 - s;
                    t.replace(i, &buf[s as usize..(s + l) as usize]);
                    break;
                }
            }
        }
    }
    t.compact()
}
