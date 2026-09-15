use indextree::NodeId;
use smallvec::SmallVec;
use std::collections::HashMap;

use crate::lang::Lang;
use crate::tree::{MutableTree, Node};

// ── Phase markers ──

#[derive(Clone, Copy, PartialEq, Eq)]
enum ParseMode {
    Match,
    Template,
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
    SplitFirst(Box<str>),
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
            Tf::SplitFirst(sep) => s.split_once(&**sep).map_or(s, |(l, _)| l).to_string(),
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

    fn apply_sym(&self, t: &MutableTree, lang: &mut Lang, i: NodeId) -> u32 {
        match self {
            Tf::Id => t.node(i).sym,
            Tf::Field(f) => t
                .child_by_field(i, *f)
                .map_or(t.node(i).sym, |c| t.node(c).sym),
            Tf::Const(s) => lang.syms.intern(s),
            Tf::Child(k) => t
                .children(i)
                .find(|&c| t.node(c).kind == *k)
                .map_or(0, |c| t.node(c).sym),
            Tf::FieldChild(f, k) => t
                .child_by_field(i, *f)
                .and_then(|n| t.children(n).find(|&c| t.node(c).kind == *k))
                .map_or(0, |c| t.node(c).sym),
            _ => {
                let sym = t.node(i).sym;
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

enum Pat {
    Node {
        kind: u16,
        field: u16,
        text: Text,
        kids: Vec<Pat>,
        optional: bool,
    },
    Cap {
        capture: CaptureSpec,
        field: u16,
        optional: bool,
    },
    Var {
        capture: CaptureSpec,
        leaf_only: bool,
    },
    Not(Box<Pat>),
    Desc(Box<Pat>),
    Spread {
        slot: u16,
        inject: Vec<Pat>,
    },
}

struct CaptureSpec {
    slot: u16,
    kind: Option<u16>,
    rekind: Option<u16>,
    guard: Option<Box<Pat>>,
    named_only: bool,
}

type Capture = SmallVec<[NodeId; 4]>;

pub struct Rewrite {
    pat: Pat,
    replacement: Pat,
    nslots: usize,
    filters: Vec<Vec<u16>>,
    guards: Vec<(u16, u16, bool)>,
}

struct ParseCtx<'l> {
    lang: &'l mut Lang,
    slots: HashMap<Box<str>, u16>,
    filters: Vec<Vec<u16>>,
    mode: ParseMode,
}

impl<'l> ParseCtx<'l> {
    fn slot(&mut self, n: &str) -> u16 {
        if let Some(&slot) = self.slots.get(n) {
            return slot;
        }
        if self.mode == ParseMode::Template {
            panic!("template references unknown slot: {n}");
        }
        let slot = self.slots.len() as u16;
        self.slots.insert(n.into(), slot);
        self.filters.push(Vec::new());
        slot
    }

    fn intern_kind(&mut self, k: &str) -> u16 {
        self.lang.intern_kind(k)
    }

    fn intern_field(&mut self, f: &str) -> u16 {
        self.lang.intern_field(f)
    }

    fn apply_filter(&mut self, slot: u16, kinds: Vec<u16>) {
        if self.mode == ParseMode::Match {
            self.filters[slot as usize] = kinds;
        }
    }

    fn template(&mut self, src: &str) -> Pat {
        self.mode = ParseMode::Template;
        parse(self, src)
    }
}

impl Rewrite {
    pub fn new(lang: &mut Lang, src: &str, replacement: &str) -> Rewrite {
        Self::compile(lang, src, replacement, None)
    }

    pub(crate) fn compile(
        lang: &mut Lang,
        src: &str,
        replacement: &str,
        where_clause: Option<&str>,
    ) -> Rewrite {
        let mut ctx = ParseCtx {
            lang,
            slots: HashMap::new(),
            filters: Vec::new(),
            mode: ParseMode::Match,
        };
        ctx.slot("ROOT");
        let pat = parse(&mut ctx, src);
        let replacement = ctx.template(replacement);
        let nslots = ctx.slots.len();
        let guards = where_clause.map_or_else(Vec::new, |clause| guards(clause, &ctx.slots));
        Rewrite {
            pat,
            replacement,
            nslots,
            filters: ctx.filters,
            guards,
        }
    }
}

fn guards(clause: &str, slots: &HashMap<Box<str>, u16>) -> Vec<(u16, u16, bool)> {
    clause
        .split("&&")
        .map(|part| {
            let part = part.trim();
            let (a, b, eq) = if let Some((l, r)) = part.split_once("==") {
                (l.trim(), r.trim(), true)
            } else if let Some((l, r)) = part.split_once("!=") {
                (l.trim(), r.trim(), false)
            } else {
                panic!("invalid where clause: {part}");
            };
            let slot = |name: &str| {
                *slots
                    .get(name.trim_start_matches('$'))
                    .unwrap_or_else(|| panic!("unknown capture in where: {name}"))
            };
            (slot(a), slot(b), eq)
        })
        .collect()
}

// ── Pest-based parser ──
//
// Grammar lives in pattern.pest. Pest produces the parse tree,
// the visitor below walks it and calls Ctx to intern kinds/slots/filters.

use pest_derive::Parser;

#[derive(Parser)]
#[grammar = "src/dsl/pattern.pest"]
struct PatParser;

#[pest_consume::parser]
impl PatParser {}

fn parse(c: &mut ParseCtx<'_>, src: &str) -> Pat {
    let root = <PatParser as pest_consume::Parser>::parse(Rule::Pattern, src)
        .unwrap_or_else(|e| panic!("pattern parse error: {e}"))
        .single()
        .expect("Pattern produces one pair");
    visit_element(c, root.into_children().next().unwrap(), 0)
}

type PNode<'i> = pest_consume::Node<'i, Rule, ()>;

fn visit_element(c: &mut ParseCtx<'_>, node: PNode<'_>, field: u16) -> Pat {
    match node.as_rule() {
        Rule::Node => visit_node(c, node, field),
        Rule::Variadic => visit_variadic(c, node, field),
        Rule::CapRef => visit_cap_ref(c, node, field),
        Rule::Capture => visit_capture(c, node, field),
        Rule::TextField => visit_text_field_as_cap(c, node, field),
        Rule::Spread => visit_spread(c, node),
        Rule::Negation => {
            let inner = node.into_children().next().unwrap();
            Pat::Not(Box::new(visit_element(c, inner, 0)))
        }
        Rule::Descendant => {
            let inner = node.into_children().next().unwrap();
            Pat::Desc(Box::new(visit_element(c, inner, 0)))
        }
        r => panic!("unexpected rule in element: {r:?}"),
    }
}

fn visit_node(c: &mut ParseCtx<'_>, node: PNode<'_>, field: u16) -> Pat {
    let mut children = node.into_children();
    let kind = c.intern_kind(children.next().expect("Node has Ident").as_str());

    let mut kids = Vec::new();
    let mut text = Text::Any;
    let mut optional = false;

    for child in children {
        match child.as_rule() {
            Rule::Opt => optional = true,
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
                let next = fc.next().unwrap();
                let (opt, elem) = if next.as_rule() == Rule::Opt {
                    (true, fc.next().unwrap())
                } else {
                    (false, next)
                };
                let mut pat = visit_element(c, elem, f);
                if opt {
                    set_optional(&mut pat);
                }
                kids.push(pat);
            }
            Rule::Negation => {
                let inner = child.into_children().next().unwrap();
                kids.push(Pat::Not(Box::new(visit_element(c, inner, 0))));
            }
            Rule::Descendant => {
                let inner = child.into_children().next().unwrap();
                kids.push(Pat::Desc(Box::new(visit_element(c, inner, 0))));
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
        optional,
    }
}

fn set_optional(pat: &mut Pat) {
    match pat {
        Pat::Cap { optional, .. } | Pat::Node { optional, .. } => *optional = true,
        _ => panic!("optional (?) only valid on captures and nodes"),
    }
}

fn visit_text_field(c: &mut ParseCtx<'_>, node: PNode<'_>) -> (u16, Tf) {
    let mut children = node.into_children();
    let slot = c.slot(children.next().unwrap().as_str());
    let tf = match children.next() {
        Some(chain) if chain.as_rule() == Rule::TfChain => visit_tf_chain(c, chain),
        _ => Tf::Id,
    };
    (slot, tf)
}

fn visit_text_field_as_cap(c: &mut ParseCtx<'_>, node: PNode<'_>, field: u16) -> Pat {
    let name = node.into_children().next().unwrap().as_str();
    Pat::Cap {
        capture: CaptureSpec {
            slot: c.slot(name),
            kind: None,
            rekind: None,
            guard: None,
            named_only: false,
        },
        field,
        optional: false,
    }
}

fn visit_variadic(c: &mut ParseCtx<'_>, node: PNode<'_>, _field: u16) -> Pat {
    let mut children = node.into_children();
    let slot = c.slot(children.next().unwrap().as_str());

    let mut leaf_only = false;
    let mut rekind = None;
    let mut guard = None;
    let mut named_only = false;

    for child in children {
        match child.as_rule() {
            Rule::Filter => {
                let kinds: Vec<&str> = child.clone().into_children().map(|k| k.as_str()).collect();
                if kinds == ["_*_named"] {
                    named_only = true;
                } else {
                    let filter: Vec<u16> = kinds.iter().map(|k| c.intern_kind(k)).collect();
                    c.apply_filter(slot, filter);
                }
            }
            Rule::Node => guard = Some(Box::new(visit_node(c, child, 0))),
            Rule::Arrow => leaf_only = child.as_str() == "=>",
            Rule::Ident => rekind = Some(c.intern_kind(child.as_str())),
            r => panic!("unexpected child in Variadic: {r:?}"),
        }
    }

    Pat::Var {
        capture: CaptureSpec {
            slot,
            kind: None,
            rekind,
            guard,
            named_only,
        },
        leaf_only,
    }
}

fn visit_spread(c: &mut ParseCtx<'_>, node: PNode<'_>) -> Pat {
    let mut children = node.into_children();
    let name = children.next().unwrap().as_str();
    let slot = c.slot(name);
    let inject: Vec<Pat> = children
        .filter(|ch| {
            matches!(
                ch.as_rule(),
                Rule::Node | Rule::Variadic | Rule::CapRef | Rule::Capture | Rule::Spread
            )
        })
        .map(|ch| visit_element(c, ch, 0))
        .collect();
    Pat::Spread { slot, inject }
}

fn visit_cap_ref(c: &mut ParseCtx<'_>, node: PNode<'_>, field: u16) -> Pat {
    let mut children = node.into_children();
    let name = children.next().unwrap().as_str();
    let _arrow = children.next();
    let rekind = c.intern_kind(children.next().unwrap().as_str());
    Pat::Cap {
        capture: CaptureSpec {
            slot: c.slot(name),
            kind: None,
            rekind: Some(rekind),
            guard: None,
            named_only: false,
        },
        field,
        optional: false,
    }
}

fn visit_capture(c: &mut ParseCtx<'_>, node: PNode<'_>, field: u16) -> Pat {
    let mut children = node.into_children();
    let name = children.next().unwrap().as_str();
    let mut kind = None;
    let mut guard = None;
    let mut optional = false;
    let mut named_only = false;
    for child in children {
        match child.as_rule() {
            Rule::Opt => optional = true,
            Rule::Ident if child.as_str() == "_*_named" => named_only = true,
            Rule::Ident => kind = Some(c.intern_kind(child.as_str())),
            Rule::Node => guard = Some(Box::new(visit_node(c, child, 0))),
            r => panic!("unexpected capture filter: {r:?}"),
        }
    }
    Pat::Cap {
        capture: CaptureSpec {
            slot: c.slot(name),
            kind,
            rekind: None,
            guard,
            named_only,
        },
        field,
        optional,
    }
}

fn visit_tf_chain(c: &mut ParseCtx<'_>, node: PNode<'_>) -> Tf {
    let tfs: Vec<Tf> = node.into_children().map(|e| visit_tf_expr(c, e)).collect();
    if tfs.len() == 1 {
        tfs.into_iter().next().unwrap()
    } else {
        Tf::Pipeline(tfs)
    }
}

fn visit_tf_expr(c: &mut ParseCtx<'_>, node: PNode<'_>) -> Tf {
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
                "split_first" => Tf::SplitFirst(args[0].into()),
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

fn matches(t: &MutableTree, i: NodeId, p: &Pat, caps: &mut [Capture]) -> bool {
    let n = t.node(i);
    let field_ok = |f: u16| f == 0 || f == n.field;
    match p {
        Pat::Var { .. } | Pat::Spread { .. } => false,
        Pat::Not(_) | Pat::Desc(_) => false,
        Pat::Cap { capture, field, .. } => {
            if !field_ok(*field) || capture.kind.is_some_and(|k| k != n.kind) {
                return false;
            }
            if capture.named_only && !n.named {
                return false;
            }
            if let Some(g) = &capture.guard
                && !matches(t, i, g, caps)
            {
                return false;
            }
            caps[capture.slot as usize].clear();
            caps[capture.slot as usize].push(i);
            true
        }
        Pat::Node {
            kind,
            field,
            text,
            kids,
            ..
        } => {
            if n.kind != *kind || !field_ok(*field) {
                return false;
            }
            if let Text::Lit(s) = text
                && n.sym != *s
            {
                return false;
            }
            let children: SmallVec<[NodeId; 8]> = t.children(i).collect();
            let mut c = 0;
            for (k, kid) in kids.iter().enumerate() {
                match kid {
                    Pat::Var { capture, .. } => {
                        let start = c;
                        while c < children.len()
                            && !kids
                                .get(k + 1)
                                .is_some_and(|nx| matches(t, children[c], nx, caps))
                        {
                            c += 1;
                        }
                        if let Some(g) = &capture.guard {
                            let any_match =
                                children[start..c].iter().any(|&e| matches(t, e, g, caps));
                            caps[capture.slot as usize] = if any_match {
                                children[start..c].iter().copied().collect()
                            } else {
                                Capture::new()
                            };
                        } else {
                            caps[capture.slot as usize] =
                                children[start..c].iter().copied().collect();
                        }
                    }
                    Pat::Not(inner) => {
                        for &scan in &children[c..] {
                            if matches(t, scan, inner, caps) {
                                return false;
                            }
                        }
                    }
                    Pat::Desc(inner) => {
                        if !t.descendants(i).any(|d| matches(t, d, inner, caps)) {
                            return false;
                        }
                    }
                    _ => {
                        let saved = c;
                        while c < children.len() && !matches(t, children[c], kid, caps) {
                            c += 1;
                        }
                        if c >= children.len() {
                            if is_optional(kid) {
                                mark_empty(kid, caps);
                                c = saved;
                                continue;
                            }
                            return false;
                        }
                        c += 1;
                    }
                }
            }
            true
        }
    }
}

fn is_optional(p: &Pat) -> bool {
    matches!(
        p,
        Pat::Cap { optional: true, .. } | Pat::Node { optional: true, .. }
    )
}

fn mark_empty(p: &Pat, caps: &mut [Capture]) {
    if let Pat::Cap { capture, .. } = p {
        caps[capture.slot as usize].clear();
    }
}

fn materialize(
    t: &mut MutableTree,
    lang: &mut Lang,
    p: &Pat,
    caps: &[Capture],
    filters: &[Vec<u16>],
    parent: Option<NodeId>,
    span: (u32, u32),
) -> Vec<NodeId> {
    match p {
        Pat::Cap { capture, field, .. } => {
            if caps[capture.slot as usize].is_empty() {
                return Vec::new();
            }
            let root = t.clone_subtree(caps[capture.slot as usize][0], parent);
            if *field != 0 {
                t.node_mut(root).field = *field;
            }
            if let Some(k) = capture.rekind {
                let node = t.node_mut(root);
                node.kind = k;
                node.field = 0;
            }
            vec![root]
        }
        Pat::Var { capture, leaf_only } => {
            let cap = &caps[capture.slot as usize];
            if cap.is_empty() {
                return Vec::new();
            }
            let filter = &filters[capture.slot as usize];
            let mut scratch = vec![Capture::new(); caps.len()];
            let mut roots = Vec::new();
            for &e in cap {
                if !filter.is_empty() && !filter.contains(&t.node(e).kind) {
                    continue;
                }
                if capture.named_only && !t.node(e).named {
                    continue;
                }
                if let Some(g) = &capture.guard
                    && !matches(t, e, g, &mut scratch)
                {
                    continue;
                }
                let root = if *leaf_only {
                    let n = t.node(e);
                    if n.sym == 0 {
                        continue;
                    }
                    t.create(
                        Node {
                            kind: capture.rekind.unwrap_or(n.kind),
                            field: 0,
                            named: true,
                            synth: true,
                            sym: n.sym,
                            size: 1,
                            start: n.start,
                            end: n.end,
                            start_row: n.start_row,
                            start_col: n.start_col,
                            end_row: n.end_row,
                            end_col: n.end_col,
                            ..Default::default()
                        },
                        parent,
                    )
                } else {
                    let root = t.clone_subtree(e, parent);
                    if let Some(k) = capture.rekind {
                        t.node_mut(root).kind = k;
                    }
                    root
                };
                roots.push(root);
            }
            roots
        }
        Pat::Node {
            kind,
            field,
            text,
            kids,
            optional,
        } => {
            if *optional {
                let text_empty =
                    matches!(text, Text::From(slot, _) if caps[*slot as usize].is_empty());
                let kids_empty = kids.is_empty()
                    || kids.iter().all(|k| match k {
                        Pat::Cap { capture, .. } | Pat::Var { capture, .. } => {
                            caps[capture.slot as usize].is_empty()
                        }
                        _ => false,
                    });
                if text_empty || (kids_empty && matches!(text, Text::Any)) {
                    return Vec::new();
                }
            }
            let sym = match text {
                Text::Any => 0,
                Text::Lit(s) => *s,
                Text::From(slot, tf) => {
                    if caps[*slot as usize].is_empty() {
                        return Vec::new();
                    }
                    tf.apply_sym(t, lang, caps[*slot as usize][0])
                }
            };
            let pos_src = match text {
                Text::From(slot, _) if !caps[*slot as usize].is_empty() => caps[*slot as usize][0],
                _ => caps[0][0],
            };
            let src = t.node(pos_src);
            let root = t.create(
                Node {
                    kind: *kind,
                    field: *field,
                    named: true,
                    synth: true,
                    sym,
                    start: span.0,
                    end: span.1,
                    start_row: src.start_row,
                    start_col: src.start_col,
                    end_row: src.end_row,
                    end_col: src.end_col,
                    ..Default::default()
                },
                parent,
            );
            for k in kids {
                materialize(t, lang, k, caps, filters, Some(root), span);
            }
            vec![root]
        }
        Pat::Spread { slot, inject } => {
            if caps[*slot as usize].is_empty() {
                return Vec::new();
            }
            let root = t.clone_subtree(caps[*slot as usize][0], parent);
            for kid in inject {
                materialize(t, lang, kid, caps, filters, Some(root), span);
            }
            vec![root]
        }
        Pat::Not(_) | Pat::Desc(_) => Vec::new(),
    }
}

pub fn apply_rewrites(t: &mut MutableTree, lang: &mut Lang, rules: &[Rewrite]) {
    let mut caps = vec![Capture::new(); rules.iter().map(|r| r.nslots).max().unwrap_or(1)];
    for i in t.postorder() {
        for r in rules {
            caps.iter_mut().for_each(SmallVec::clear);
            if !matches(t, i, &r.pat, &mut caps) {
                continue;
            }
            caps[0].push(i);
            if !r.guards.is_empty() {
                let mut guard_ok = true;
                for &(a, b, eq) in &r.guards {
                    let sym_a = t.node(caps[a as usize][0]).sym;
                    let sym_b = t.node(caps[b as usize][0]).sym;
                    if (sym_a == sym_b) != eq {
                        guard_ok = false;
                        break;
                    }
                }
                if !guard_ok {
                    continue;
                }
            }
            let root = *t.node(i);
            let replacement = materialize(
                t,
                lang,
                &r.replacement,
                &caps,
                &r.filters,
                None,
                (root.start, root.end),
            );
            t.replace(i, replacement);
            break;
        }
    }
}
