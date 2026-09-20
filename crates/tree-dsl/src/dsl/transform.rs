use std::borrow::Cow;
use std::str::FromStr;

use indextree::NodeId;

use crate::intern::Lang;
use crate::tree::{EdgeKind, Tree};

use super::types::{Ctx, EdgeCtx, EdgeDir, Tf};

fn child_sym(t: &Tree, id: NodeId, pred: impl Fn(&crate::tree::Node) -> bool) -> u32 {
    id.children(&t.arena)
        .find(|&c| pred(t.node(c)))
        .map_or(0, |c| t.node(c).sym)
}

fn nonempty(lang: &Lang, sym: u32) -> Option<u32> {
    if sym == 0 {
        return None;
    }
    if lang.syms.resolve(sym).is_empty() {
        return None;
    }
    Some(sym)
}

fn parse_nested_tf(spec: &str, ctx: Option<&mut Ctx>) -> Tf {
    if let Some((name, arg)) = spec.split_once(':') {
        Tf::from_func(name, &[arg], ctx)
    } else {
        Tf::from_func(spec, &[], ctx)
    }
}

impl Tf {
    pub(crate) fn from_func(name: &str, args: &[&str], mut ctx: Option<&mut Ctx>) -> Tf {
        let s = |i: usize| -> Box<str> { args[i].into() };
        let kind =
            |c: &mut Option<&mut Ctx>, a: &str| c.as_mut().expect("needs context").intern_kind(a);

        match name {
            "lowercase" | "stem" => match name {
                "lowercase" => Tf::Lowercase,
                _ => Tf::Stem,
            },
            "strip_prefix" | "strip" => Tf::Strip(s(0)),
            "strip_suffix" => Tf::StripSuffix(s(0)),
            "prepend" => Tf::Prepend(s(0)),
            "split_last" => Tf::SplitLast(s(0)),
            "split_first" => Tf::SplitFirst(s(0)),
            "replace" => Tf::Replace(s(0), s(1)),

            "collapse_index" => Tf::CollapseIndex(args.iter().map(|a| (*a).into()).collect()),
            "map" => Tf::Map(
                args.iter()
                    .filter_map(|a| a.split_once(':'))
                    .map(|(k, v)| (k.into(), v.into()))
                    .collect(),
            ),
            "field" => Tf::Field(ctx.as_mut().expect("needs context").intern_field(args[0])),
            "child_sym" => Tf::Child(kind(&mut ctx, args[0])),
            "parent_sym" => Tf::ParentSym(kind(&mut ctx, args[0])),
            "regex_replace" => {
                let re = regex::Regex::new(args[0]).expect("invalid regex");
                Tf::Regex(re, s(1))
            }
            "regex_first" => {
                let re = regex::Regex::new(args[0]).expect("invalid regex");
                Tf::RegexFirst(re, s(1))
            }
            "regex_loop" => {
                let re = regex::Regex::new(args[0]).expect("invalid regex");
                Tf::RegexLoop(re, s(1))
            }
            "regex_match" => {
                let re = regex::Regex::new(args[0]).expect("invalid regex");
                Tf::RegexMatch(re)
            }
            "ancestor_sym" => Tf::AncestorSym(kind(&mut ctx, args[0])),
            "ancestor_tag" => {
                let key = ctx
                    .as_mut()
                    .expect("needs context")
                    .lang
                    .syms
                    .intern(args[0]);
                Tf::AncestorTag(key)
            }
            "tag" => {
                let key = ctx
                    .as_mut()
                    .expect("needs context")
                    .lang
                    .syms
                    .intern(args[0]);
                Tf::Tag(key)
            }
            "has_incoming" | "has_outgoing" => {
                let ek = EdgeKind::from_str(args[0]).expect("unknown edge kind");
                let dir = if name == "has_incoming" {
                    EdgeDir::Incoming
                } else {
                    EdgeDir::Outgoing
                };
                Tf::HasEdge(ek, dir)
            }
            "concat" => {
                let a = parse_nested_tf(args[1], ctx.as_mut().map(|c| &mut **c));
                let b = parse_nested_tf(args[2], ctx);
                Tf::Concat(s(0), Box::new(a), Box::new(b))
            }
            _ => panic!("unknown transform: {name}"),
        }
    }

    pub(crate) fn apply_to_str<'a>(&self, s: &'a str) -> Cow<'a, str> {
        debug_assert!(
            !self.is_node_tf(),
            "node transform used as string transform"
        );
        match self {
            Tf::Id => Cow::Borrowed(s),
            Tf::Strip(p) => match s.strip_prefix(&**p) {
                Some(rest) => Cow::Borrowed(rest),
                None => Cow::Borrowed(s),
            },
            Tf::StripSuffix(p) => match s.strip_suffix(&**p) {
                Some(rest) => Cow::Borrowed(rest),
                None => Cow::Borrowed(s),
            },
            Tf::StripLeading(ch) => {
                let trimmed = s.trim_start_matches(*ch);
                if trimmed.len() == s.len() {
                    Cow::Borrowed(s)
                } else {
                    Cow::Borrowed(trimmed)
                }
            }
            Tf::SplitLast(sep) => match s.rsplit_once(&**sep) {
                Some((_, r)) => Cow::Borrowed(r),
                None => Cow::Borrowed(s),
            },
            Tf::SplitFirst(sep) => match s.split_once(&**sep) {
                Some((l, _)) => Cow::Borrowed(l),
                None => Cow::Borrowed(s),
            },
            Tf::Replace(from, to) => {
                if s.contains(&**from) {
                    Cow::Owned(s.replace(&**from, to))
                } else {
                    Cow::Borrowed(s)
                }
            }
            Tf::Prepend(p) => Cow::Owned(format!("{p}{s}")),
            Tf::Lowercase => Cow::Owned(s.to_lowercase()),

            Tf::Pipeline(steps) => {
                let mut owned = s.to_string();
                for step in steps {
                    owned = step.apply_to_str(&owned).into_owned();
                }
                Cow::Owned(owned)
            }
            Tf::Stem => {
                let p = std::path::Path::new(s);
                Cow::Owned(p.with_extension("").to_string_lossy().into_owned())
            }
            Tf::Map(entries) => {
                for (k, v) in entries {
                    if s == &**k {
                        return Cow::Owned(v.to_string());
                    }
                }
                Cow::Borrowed(s)
            }
            Tf::CollapseIndex(names) => {
                for name in names {
                    if let Some(prefix) = s.strip_suffix(&**name).and_then(|p| p.strip_suffix('/'))
                    {
                        return Cow::Owned(prefix.to_string());
                    }
                    if s == &**name {
                        return Cow::Owned(String::new());
                    }
                }
                Cow::Borrowed(s)
            }
            Tf::Regex(re, replacement) => re.replace_all(s, &**replacement),
            Tf::RegexFirst(re, replacement) => re.replace(s, &**replacement),
            Tf::RegexLoop(re, replacement) => {
                let mut cur = Cow::Borrowed(s);
                loop {
                    let next = re.replace_all(&cur, &**replacement);
                    if let Cow::Borrowed(_) = next {
                        break;
                    }
                    cur = Cow::Owned(next.into_owned());
                }
                cur
            }
            Tf::RegexMatch(re) => Cow::Borrowed(if re.is_match(s) { "true" } else { "false" }),
            _ => Cow::Borrowed(s),
        }
    }

    pub(crate) fn apply_sym(
        &self,
        t: &Tree,
        lang: &Lang,
        id: NodeId,
        edge_ctx: Option<&EdgeCtx>,
    ) -> u32 {
        match self {
            Tf::Id => t.node(id).sym,
            Tf::Field(f) => {
                let f = *f;
                let found = child_sym(t, id, |n| n.field == f);
                if found != 0 { found } else { t.node(id).sym }
            }
            Tf::Const(s) => lang.syms.intern(s),
            Tf::Child(k) => child_sym(t, id, |n| n.kind == *k),
            Tf::FieldChild(f, k) => {
                let (f, k) = (*f, *k);
                id.children(&t.arena)
                    .find(|&c| t.node(c).field == f)
                    .map_or(0, |n| child_sym(t, n, |n| n.kind == k))
            }
            Tf::ParentSym(k) => {
                let k = *k;
                id.parent(&t.arena)
                    .map_or(0, |p| child_sym(t, p, |n| n.kind == k))
            }
            Tf::AncestorSym(k) => {
                let k = *k;
                let mut cur = id;
                loop {
                    let sym = child_sym(t, cur, |n| n.kind == k);
                    if sym != 0 {
                        break sym;
                    }
                    match cur.parent(&t.arena) {
                        Some(p) => cur = p,
                        None => break 0,
                    }
                }
            }
            Tf::AncestorTag(key) => {
                let key = *key;
                let mut cur = id;
                loop {
                    match cur.parent(&t.arena) {
                        Some(p) => {
                            if let Some(v) = t.get_tag(Tree::to_raw(p), key) {
                                break v;
                            }
                            cur = p;
                        }
                        None => break 0,
                    }
                }
            }
            Tf::Tag(key) => t.get_tag(Tree::to_raw(id), *key).unwrap_or(0),
            Tf::LitSym(s) => *s,
            Tf::HasEdge(kind, dir) => {
                let raw = Tree::to_raw(id);
                let found = edge_ctx.is_some_and(|ctx| {
                    ctx.edges.iter().any(|e| {
                        e.kind == *kind
                            && match dir {
                                EdgeDir::Incoming => {
                                    e.to_tree == ctx.tree_index && e.to_node == raw
                                }
                                EdgeDir::Outgoing => {
                                    e.from_tree == ctx.tree_index && e.from_node == raw
                                }
                            }
                    })
                });
                lang.syms.intern(if found { "true" } else { "false" })
            }
            Tf::Concat(sep, a, b) => {
                let sa = nonempty(lang, a.apply_sym(t, lang, id, edge_ctx));
                let sb = nonempty(lang, b.apply_sym(t, lang, id, edge_ctx));
                match (sa, sb) {
                    (None, None) => 0,
                    (Some(a), None) => a,
                    (None, Some(b)) => b,
                    (Some(a), Some(b)) => lang.syms.intern(&format!(
                        "{}{sep}{}",
                        lang.syms.resolve(a),
                        lang.syms.resolve(b)
                    )),
                }
            }
            Tf::Pipeline(steps) => {
                let mut s = lang.syms.resolve(t.node(id).sym).to_string();
                for step in steps {
                    if step.is_node_tf() {
                        s = lang
                            .syms
                            .resolve(step.apply_sym(t, lang, id, edge_ctx))
                            .to_string();
                    } else {
                        s = step.apply_to_str(&s).into_owned();
                    }
                }
                lang.syms.intern(&s)
            }
            _ => {
                let sym = t.node(id).sym;
                if sym == 0 {
                    return 0;
                }
                let s = lang.syms.resolve(sym);
                let result = self.apply_to_str(s);
                lang.syms.intern(&result)
            }
        }
    }
}
