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
        /// If set, override the root kind of the copied subtree.
        rekind: Option<u16>,
    },
    Var {
        slot: u16,
        field: u16,
        /// If set, override the kind of each copied element.
        rekind: Option<u16>,
        /// If true, copy only the root node (as a leaf), discard children.
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

// ── Winnow-based parser ──
//
// Parses S-expression patterns directly from &str, no tokenizer.
// Winnow handles leaf parsing (strings, identifiers, delimiters);
// explicit recursion handles structure (Ctx<P> can't go into Stateful).

use winnow::Parser;
use winnow::combinator::{alt, delimited, opt, preceded};
use winnow::token::{literal, take_while};

/// Consume a literal string, panicking if absent.
fn eat(i: &mut &str, s: &str) {
    literal::<_, _, winnow::error::ContextError>(s)
        .void()
        .parse_next(i)
        .unwrap_or_else(|_| panic!("expected {s:?}"));
}

fn ws(i: &mut &str) -> winnow::Result<()> {
    take_while(0.., |c: char| c.is_whitespace())
        .void()
        .parse_next(i)
}

fn quoted<'i>(i: &mut &'i str) -> winnow::Result<&'i str> {
    delimited('"', take_while(0.., |c: char| c != '"'), '"').parse_next(i)
}

fn ident<'i>(i: &mut &'i str) -> winnow::Result<&'i str> {
    take_while(1.., |c: char| c.is_alphanumeric() || c == '_' || c == '.').parse_next(i)
}

fn cap_name<'i>(i: &mut &'i str) -> winnow::Result<&'i str> {
    take_while(1.., |c: char| c.is_alphanumeric() || c == '_').parse_next(i)
}

/// Consume `->` or `=>`, returning true for `=>` (leaf).
fn arrow(i: &mut &str) -> winnow::Result<bool> {
    alt((literal("=>").value(true), literal("->").value(false))).parse_next(i)
}

/// Take a balanced transform segment. Stops at `|`, `)`, or whitespace
/// at depth 0 but tracks `(`/`)` and `"` for function args.
fn tf_segment<'i>(i: &mut &'i str) -> &'i str {
    let start = *i;
    let mut depth = 0u32;
    let mut in_str = false;
    for (pos, ch) in start.char_indices() {
        if in_str {
            if ch == '"' {
                in_str = false;
            }
            continue;
        }
        match ch {
            '"' => in_str = true,
            '(' => depth += 1,
            ')' if depth > 0 => depth -= 1,
            '|' | ')' if depth == 0 => {
                *i = &start[pos..];
                return &start[..pos];
            }
            c if c.is_whitespace() && depth == 0 => {
                *i = &start[pos..];
                return &start[..pos];
            }
            _ => {}
        }
    }
    *i = "";
    start
}

fn parse_single_tf<P: Phase>(c: &mut Ctx<'_, P>, tf: &str) -> Tf {
    if let Some(paren) = tf.find('(') {
        let func = &tf[..paren];
        let mut args_input = &tf[paren..];
        let args: Vec<&str> = delimited(
            '(',
            winnow::combinator::separated::<_, _, Vec<_>, _, _, _, _>(
                0..,
                preceded(ws, quoted),
                (ws, ',', ws),
            ),
            preceded(ws, ')'),
        )
        .parse_next(&mut args_input)
        .expect("bad transform args");

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
    } else {
        match tf {
            "lowercase" => Tf::Lowercase,
            _ => match tf.split_once('=') {
                Some(("strip", p)) => Tf::Strip(p.into()),
                Some(("field", f)) => Tf::Field(c.intern_field(f)),
                _ => panic!("unknown transform: {tf}"),
            },
        }
    }
}

fn parse_tf_chain<P: Phase>(c: &mut Ctx<'_, P>, i: &mut &str) -> Tf {
    let mut tfs = vec![parse_single_tf(c, tf_segment(i))];
    while i.starts_with('|') {
        eat(i, "|");
        tfs.push(parse_single_tf(c, tf_segment(i)));
    }
    if tfs.len() == 1 {
        tfs.into_iter().next().unwrap()
    } else {
        Tf::Pipeline(tfs)
    }
}

fn parse<P: Phase>(c: &mut Ctx<'_, P>, src: &str) -> Pat {
    let i = &mut src.as_ref();
    ws(i).unwrap();
    let pat = parse_pat(c, i, 0);
    ws(i).unwrap();
    assert!(i.is_empty(), "trailing input: {i:?}");
    pat
}

fn parse_pat<P: Phase>(c: &mut Ctx<'_, P>, i: &mut &str, field: u16) -> Pat {
    ws(i).unwrap();
    if i.starts_with('(') {
        parse_node(c, i, field)
    } else if i.starts_with("$$$") {
        eat(i, "$$$");
        parse_variadic(c, i, field)
    } else if i.starts_with("@$") {
        eat(i, "@$");
        parse_cap_ref(c, i, field)
    } else if i.starts_with('$') {
        eat(i, "$");
        parse_capture(c, i, field)
    } else {
        panic!("expected pattern at: {:?}", &i[..i.len().min(30)])
    }
}

fn parse_node<P: Phase>(c: &mut Ctx<'_, P>, i: &mut &str, field: u16) -> Pat {
    eat(i, "(");
    ws(i).unwrap();
    let kind = c.intern_kind(ident(i).expect("expected kind"));
    ws(i).unwrap();

    let mut kids = Vec::new();
    let mut text = Text::Any;

    while !i.starts_with(')') {
        ws(i).unwrap();
        if i.starts_with(')') {
            break;
        }
        if let Some(lit) = opt(quoted).parse_next(i).unwrap() {
            text = Text::Lit(c.lang.syms.intern(lit));
        } else if i.starts_with("@$") {
            eat(i, "@$");
            let n = cap_name(i).expect("expected capture name");
            if let Some(_leaf) = opt(arrow).parse_next(i).unwrap() {
                let rekind = c.intern_kind(ident(i).expect("expected kind"));
                kids.push(Pat::Cap {
                    slot: c.slot(n),
                    field: 0,
                    kind: None,
                    rekind: Some(rekind),
                });
            } else if opt(literal::<_, _, ()>('|'))
                .parse_next(i)
                .unwrap()
                .is_some()
            {
                text = Text::From(c.slot(n), parse_tf_chain(c, i));
            } else {
                text = Text::From(c.slot(n), Tf::Id);
            }
        } else if let Some(f) = try_field(i) {
            let f = c.intern_field(f);
            ws(i).unwrap();
            kids.push(parse_pat(c, i, f));
        } else {
            kids.push(parse_pat(c, i, 0));
        }
        ws(i).unwrap();
    }
    eat(i, ")");
    Pat::Node {
        kind,
        field,
        text,
        kids,
    }
}

fn parse_variadic<P: Phase>(c: &mut Ctx<'_, P>, i: &mut &str, field: u16) -> Pat {
    let full = take_while::<_, _, ()>(1.., |c: char| !c.is_whitespace() && c != ')')
        .parse_next(i)
        .expect("expected variadic body");
    let (rest, rekind_str, leaf_only) = if let Some((b, a)) = full.split_once("=>") {
        (b, Some(a), true)
    } else if let Some((b, a)) = full.split_once("->") {
        (b, Some(a), false)
    } else {
        (full, None, false)
    };
    let (n, explicit_filter) = match rest.split_once(':') {
        Some((n, k)) => (n, Some(k)),
        None => (rest, None),
    };
    let slot = c.slot(n);
    if let Some(kinds) = explicit_filter {
        let filter: Vec<u16> = kinds
            .split('|')
            .filter(|k| !k.is_empty())
            .map(|k| c.intern_kind(k))
            .collect();
        c.apply_filter(slot, filter);
    }
    Pat::Var {
        slot,
        field,
        rekind: rekind_str.map(|k| c.intern_kind(k)),
        leaf_only,
    }
}

fn parse_cap_ref<P: Phase>(c: &mut Ctx<'_, P>, i: &mut &str, field: u16) -> Pat {
    let n = cap_name(i).expect("expected capture name");
    let rekind = if i.starts_with("->") || i.starts_with("=>") {
        let _leaf = arrow(i).unwrap();
        Some(c.intern_kind(ident(i).expect("expected kind")))
    } else {
        None
    };
    Pat::Cap {
        slot: c.slot(n),
        field,
        kind: None,
        rekind,
    }
}

fn parse_capture<P: Phase>(c: &mut Ctx<'_, P>, i: &mut &str, field: u16) -> Pat {
    let n = cap_name(i).expect("expected capture name");
    let kind = opt(preceded(literal(':'), ident))
        .parse_next(i)
        .unwrap()
        .map(|k| c.intern_kind(k));
    Pat::Cap {
        slot: c.slot(n),
        field,
        kind,
        rekind: None,
    }
}

/// Try to parse `field_name:` prefix. Returns the field name if present.
fn try_field<'i>(i: &mut &'i str) -> Option<&'i str> {
    let saved = *i;
    if let Ok(id) = ident(i) {
        if i.starts_with(':') {
            eat(i, ":");
            return Some(id);
        }
    }
    *i = saved;
    None
}

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
                out[at].field = 0; // clear inherited field
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
    let (mut edits, mut buf) = (Vec::new(), Vec::new());
    for i in 0..t.nodes.len() as u32 {
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
                Out::Remove => edits.push(Edit::Remove(i)),
                Out::SetKind(k) => edits.push(Edit::SetKind(i, *k)),
                Out::Retag { kind, fields } => {
                    edits.push(Edit::SetKind(i, *kind));
                    for (slot, f) in fields {
                        edits.push(Edit::SetField(caps[*slot as usize].0, *f));
                    }
                }
                Out::SetText { target, from, tf } => {
                    let sym = tf.apply_sym(t, lang, caps[*from as usize].0);
                    edits.push(Edit::SetText(caps[*target as usize].0, sym));
                }
                Out::Append {
                    under,
                    each,
                    kind,
                    tf,
                } => {
                    for e in elems(t, caps[*each as usize], &r.filters[*each as usize]) {
                        let sym = tf.apply_sym(t, lang, e);
                        let src = t.nodes[e as usize];
                        edits.push(Edit::Append(
                            caps[*under as usize].0,
                            Node {
                                kind: *kind,
                                named: true,
                                synth: true,
                                sym,
                                start: src.start,
                                end: src.end,
                                size: 1,
                                ..Default::default()
                            },
                        ));
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
                    edits.push(Edit::Replace(i, s, buf.len() as u32 - s));
                }
            }
        }
    }
    for e in edits {
        match e {
            Edit::Remove(i) => t.remove(i),
            Edit::SetKind(i, k) => t.set_kind(i, k),
            Edit::SetField(i, f) => t.set_field(i, f),
            Edit::SetText(i, s) => t.set_text(i, s),
            Edit::Append(p, n) => t.append(p, n),
            Edit::Replace(i, s, l) => t.replace(i, &buf[s as usize..(s + l) as usize]),
        }
    }
    t.compact()
}
