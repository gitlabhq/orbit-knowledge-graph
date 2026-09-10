use std::collections::HashMap;

use crate::lang::{Lang, NAMED, NONE};
use crate::tree::{Node, Tree, copy_subtree, elems, live};

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
            _ => s.to_string(),
        }
    }

    fn apply_sym(&self, t: &Tree, lang: &mut Lang, i: u32) -> u32 {
        match self {
            Tf::Id => t.sym(i),
            Tf::Field(f) => t.child_by_field(i, *f).map_or(t.sym(i), |c| t.sym(c)),
            Tf::Const(s) => lang.syms.get(s),
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
                lang.syms.get(&result)
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

pub struct Ctx<'l> {
    pub lang: &'l mut Lang,
    pub slots: HashMap<Box<str>, u16>,
    pub filters: Vec<Vec<u16>>,
}

impl Ctx<'_> {
    pub fn slot(&mut self, n: &str) -> u16 {
        let next = self.slots.len() as u16;
        let s = *self.slots.entry(n.into()).or_insert(next);
        if self.filters.len() <= s as usize {
            self.filters.resize(s as usize + 1, Vec::new());
        }
        s
    }

    pub fn kind(&mut self, k: &str) -> u16 {
        self.lang.kind(k)
    }

    pub fn field(&mut self, f: &str) -> u16 {
        self.lang.field(f)
    }

    pub fn template(&mut self, src: &str) -> Pat {
        parse(self, src)
    }
}

impl Rewrite {
    pub fn new(lang: &mut Lang, src: &str, out: impl FnOnce(&mut Ctx) -> Out) -> Rewrite {
        let mut c = Ctx {
            lang,
            slots: HashMap::new(),
            filters: Vec::new(),
        };
        c.slot("ROOT");
        let pat = parse(&mut c, src);
        let out = out(&mut c);
        Rewrite {
            pat,
            out,
            nslots: c.slots.len(),
            filters: c.filters,
        }
    }
}

pub fn tokenize(s: &str) -> Vec<String> {
    let (mut out, mut cur, mut in_str) = (Vec::new(), String::new(), false);
    let mut pipe_depth: u32 = 0;
    for ch in s.chars() {
        if in_str {
            cur.push(ch);
            if ch == '"' {
                in_str = false;
                if pipe_depth > 0 {
                    continue;
                }
                out.push(std::mem::take(&mut cur));
            }
            continue;
        }
        if pipe_depth > 0 {
            match ch {
                '"' => {
                    in_str = true;
                    cur.push(ch);
                }
                '(' => {
                    pipe_depth += 1;
                    cur.push(ch);
                }
                ')' => {
                    pipe_depth -= 1;
                    cur.push(ch);
                }
                c if c.is_whitespace() && pipe_depth == 0 => {
                    unreachable!()
                }
                c => cur.push(c),
            }
            continue;
        }
        match ch {
            '"' => {
                in_str = true;
                cur.push(ch);
            }
            '(' => {
                if cur.starts_with("@$") && cur.contains('|') {
                    pipe_depth = 1;
                    cur.push(ch);
                } else {
                    if !cur.is_empty() {
                        out.push(std::mem::take(&mut cur));
                    }
                    out.push(ch.to_string());
                }
            }
            ')' => {
                if !cur.is_empty() {
                    out.push(std::mem::take(&mut cur));
                }
                out.push(ch.to_string());
            }
            c if c.is_whitespace() => {
                if !cur.is_empty() {
                    out.push(std::mem::take(&mut cur));
                }
            }
            c => cur.push(c),
        }
    }
    if !cur.is_empty() {
        out.push(cur);
    }
    out
}

fn parse(c: &mut Ctx, src: &str) -> Pat {
    let toks = tokenize(src);
    let mut pos = 0;
    let pat = item(c, &toks, &mut pos, 0);
    assert_eq!(pos, toks.len(), "trailing tokens in pattern: {src}");
    pat
}

/// Split on `|` at top level (not inside parens or quotes).
fn split_pipes(s: &str) -> Vec<&str> {
    let mut result = Vec::new();
    let mut start = 0;
    let mut depth = 0u32;
    let mut in_str = false;
    for (i, ch) in s.char_indices() {
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
            '|' if depth == 0 => {
                result.push(&s[start..i]);
                start = i + 1;
            }
            _ => {}
        }
    }
    result.push(&s[start..]);
    result
}

/// Parse quoted args from a function-call transform like `replace(".","/")`.
fn parse_tf_args(s: &str) -> Vec<&str> {
    let inner = s.trim_start_matches('(').trim_end_matches(')');
    let mut args = Vec::new();
    let mut in_str = false;
    let mut start = 0;
    let mut has_content = false;
    for (i, ch) in inner.char_indices() {
        match ch {
            '"' => {
                if !in_str {
                    in_str = true;
                    start = i + 1;
                } else {
                    in_str = false;
                    args.push(&inner[start..i]);
                    has_content = true;
                }
            }
            ',' if !in_str => {}
            _ if !in_str && !has_content => start = i,
            _ => {}
        }
    }
    if !has_content && !inner.is_empty() {
        args.push(inner.trim());
    }
    args
}

pub fn parse_single_tf(c: &mut Ctx, tf: &str) -> Tf {
    if let Some(paren_pos) = tf.find('(') {
        let name = &tf[..paren_pos];
        let args = parse_tf_args(&tf[paren_pos..]);
        match name {
            "replace" => {
                assert_eq!(
                    args.len(),
                    2,
                    "replace needs 2 args: replace(\"from\",\"to\")"
                );
                Tf::Replace(args[0].into(), args[1].into())
            }
            "strip_prefix" => {
                assert_eq!(args.len(), 1, "strip_prefix needs 1 arg");
                Tf::Strip(args[0].into())
            }
            "strip_suffix" => {
                assert_eq!(args.len(), 1, "strip_suffix needs 1 arg");
                Tf::StripSuffix(args[0].into())
            }
            "prepend" => {
                assert_eq!(args.len(), 1, "prepend needs 1 arg");
                Tf::Prepend(args[0].into())
            }
            "to_rel" => {
                assert_eq!(args.len(), 1, "to_rel needs 1 char arg");
                let ch = args[0].chars().next().expect("to_rel arg must be a char");
                Tf::ToRel(ch)
            }
            "regex" => {
                assert_eq!(args.len(), 2, "regex needs 2 args: regex(\"pat\",\"repl\")");
                todo!("regex transform not yet implemented")
            }
            _ => panic!("unknown transform: {name}"),
        }
    } else {
        match tf {
            "lowercase" => Tf::Lowercase,
            _ => {
                // Legacy syntax: strip=prefix, field=name
                match tf.split_once('=') {
                    Some(("strip", p)) => Tf::Strip(p.into()),
                    Some(("field", f)) => Tf::Field(c.field(f)),
                    _ => panic!("unknown transform: {tf}"),
                }
            }
        }
    }
}

pub fn parse_tf_chain(c: &mut Ctx, tf_str: &str) -> Tf {
    let parts = split_pipes(tf_str);
    if parts.len() == 1 {
        return parse_single_tf(c, parts[0]);
    }
    Tf::Pipeline(parts.iter().map(|p| parse_single_tf(c, p)).collect())
}

fn item(c: &mut Ctx, toks: &[String], pos: &mut usize, field: u16) -> Pat {
    let tok = toks[*pos].as_str();
    *pos += 1;
    if tok == "(" {
        let kind = c.kind(&toks[*pos]);
        *pos += 1;
        let (mut kids, mut text) = (Vec::new(), Text::Any);
        loop {
            let t = toks[*pos].as_str();
            if t == ")" {
                *pos += 1;
                break;
            }
            if let Some(lit) = t.strip_prefix('"') {
                *pos += 1;
                text = Text::Lit(c.lang.syms.get(&lit[..lit.len() - 1]));
            } else if let Some(rest) = t.strip_prefix("@$") {
                if rest.contains("->") || rest.contains("=>") {
                    kids.push(item(c, toks, pos, 0));
                } else {
                    *pos += 1;
                    let parts = split_pipes(rest);
                    let name = parts[0];
                    let tf = if parts.len() > 1 {
                        let tfs: Vec<Tf> =
                            parts[1..].iter().map(|p| parse_single_tf(c, p)).collect();
                        if tfs.len() == 1 {
                            tfs.into_iter().next().unwrap()
                        } else {
                            Tf::Pipeline(tfs)
                        }
                    } else {
                        Tf::Id
                    };
                    text = Text::From(c.slot(name), tf);
                }
            } else if let Some(f) = t.strip_suffix(':') {
                *pos += 1;
                let f = c.field(f);
                kids.push(item(c, toks, pos, f));
            } else {
                kids.push(item(c, toks, pos, 0));
            }
        }
        return Pat::Node {
            kind,
            field,
            text,
            kids,
        };
    }
    if let Some(rest) = tok.strip_prefix("$$$") {
        // $$$NAME=>__kind  (leaf rekind, discard children)
        // $$$NAME->__kind  (subtree rekind, keep children)
        // $$$NAME:filter   (no rekind)
        let (rest, rekind_str, leaf_only) = if let Some((before, after)) = rest.split_once("=>") {
            (before, Some(after), true)
        } else if let Some((before, after)) = rest.split_once("->") {
            (before, Some(after), false)
        } else {
            (rest, None, false)
        };
        let (n, kinds) = rest.split_once(':').map_or((rest, ""), |(n, k)| (n, k));
        let slot = c.slot(n);
        c.filters[slot as usize] = kinds
            .split('|')
            .filter(|k| !k.is_empty())
            .map(|k| c.kind(k))
            .collect();
        let rekind = rekind_str.map(|k| c.kind(k));
        return Pat::Var {
            slot,
            field,
            rekind,
            leaf_only,
        };
    }
    if let Some(rest) = tok.strip_prefix("@$") {
        // @$N->__name or @$N=>__name or @$N
        let (name, rekind) = if let Some((n, k)) = rest.split_once("=>") {
            (n, Some((c.kind(k), true)))
        } else if let Some((n, k)) = rest.split_once("->") {
            (n, Some((c.kind(k), false)))
        } else {
            (rest, None)
        };
        return Pat::Cap {
            slot: c.slot(name),
            field,
            kind: None,
            rekind: rekind.map(|(k, _)| k),
        };
    }
    if let Some(rest) = tok.strip_prefix('$') {
        let (n, k) = rest
            .split_once(':')
            .map_or((rest, None), |(n, k)| (n, Some(k)));
        return Pat::Cap {
            slot: c.slot(n),
            field,
            kind: k.map(|k| c.kind(k)),
            rekind: None,
        };
    }
    panic!("bad token {tok}")
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
                        flags: NAMED,
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
                flags: NAMED,
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
        if t.nodes[i as usize].flags & crate::lang::DEAD != 0 {
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
                                flags: NAMED,
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
