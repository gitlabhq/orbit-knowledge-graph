use std::str::FromStr;

use indextree::NodeId;

use crate::intern::Lang;
use crate::tree::{EdgeKind, Tree};

use super::types::{Ctx, EdgeCtx, EdgeDir, Tf};

fn parse_nested_tf(spec: &str, ctx: Option<&mut Ctx>) -> Tf {
    if let Some((name, arg)) = spec.split_once(':') {
        Tf::from_func(name, &[arg], ctx)
    } else {
        Tf::from_func(spec, &[], ctx)
    }
}

impl Tf {
    pub(crate) fn from_func(name: &str, args: &[&str], mut ctx: Option<&mut Ctx>) -> Tf {
        match name {
            "replace" => {
                assert_eq!(args.len(), 2, "replace needs 2 args");
                Tf::Replace(args[0].into(), args[1].into())
            }
            "strip_prefix" | "strip" => Tf::Strip(args[0].into()),
            "strip_suffix" => Tf::StripSuffix(args[0].into()),
            "prepend" => Tf::Prepend(args[0].into()),
            "to_rel" => Tf::ToRel(args[0].chars().next().expect("to_rel arg")),
            "split_last" => Tf::SplitLast(args[0].into()),
            "split_first" => Tf::SplitFirst(args[0].into()),
            "lowercase" => Tf::Lowercase,
            "field" => Tf::Field(ctx.expect("field needs context").intern_field(args[0])),
            "child_sym" => Tf::Child(ctx.expect("child_sym needs context").intern_kind(args[0])),
            "parent_sym" => {
                Tf::ParentSym(ctx.expect("parent_sym needs context").intern_kind(args[0]))
            }
            "ancestor_sym" => Tf::AncestorSym(
                ctx.expect("ancestor_sym needs context")
                    .intern_kind(args[0]),
            ),
            "concat" => {
                assert!(args.len() >= 3, "concat needs (sep, tf_a, tf_b)");
                let a = parse_nested_tf(args[1], ctx.as_mut().map(|c| &mut **c));
                let b = parse_nested_tf(args[2], ctx);
                Tf::Concat(args[0].into(), Box::new(a), Box::new(b))
            }
            "stem" => Tf::Stem,
            "collapse_index" => Tf::CollapseIndex(args.iter().map(|a| (*a).into()).collect()),
            "has_incoming" => {
                let kind = EdgeKind::from_str(args[0]).expect("unknown edge kind");
                Tf::HasEdge(kind, EdgeDir::Incoming)
            }
            "has_outgoing" => {
                let kind = EdgeKind::from_str(args[0]).expect("unknown edge kind");
                Tf::HasEdge(kind, EdgeDir::Outgoing)
            }
            _ => panic!("unknown transform: {name}"),
        }
    }

    pub(crate) fn apply_to_str(&self, s: &str) -> String {
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
            Tf::Stem => {
                let p = std::path::Path::new(s);
                p.with_extension("").to_string_lossy().to_string()
            }
            Tf::CollapseIndex(names) => {
                for name in names {
                    let suffix = format!("/{name}");
                    if s.ends_with(&suffix) {
                        return s.strip_suffix(&suffix).unwrap_or("").to_string();
                    }
                    if s == &**name {
                        return String::new();
                    }
                }
                s.to_string()
            }
            Tf::Field(_)
            | Tf::Child(_)
            | Tf::FieldChild(_, _)
            | Tf::Const(_)
            | Tf::ParentSym(_)
            | Tf::AncestorSym(_)
            | Tf::Concat(_, _, _)
            | Tf::HasEdge(_, _) => {
                unreachable!("tree-context transform used as string transform")
            }
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
                let fallback = t.node(id).sym;
                id.children(&t.arena)
                    .find(|&c| t.node(c).field == *f)
                    .map_or(fallback, |c| t.node(c).sym)
            }
            Tf::Const(s) => lang.syms.intern(s),
            Tf::Child(k) => id
                .children(&t.arena)
                .find(|&c| t.node(c).kind == *k)
                .map_or(0, |c| t.node(c).sym),
            Tf::FieldChild(f, k) => id
                .children(&t.arena)
                .find(|&c| t.node(c).field == *f)
                .and_then(|n| n.children(&t.arena).find(|&c| t.node(c).kind == *k))
                .map_or(0, |c| t.node(c).sym),
            Tf::ParentSym(k) => id
                .parent(&t.arena)
                .into_iter()
                .flat_map(|p| p.children(&t.arena))
                .find(|&c| t.node(c).kind == *k)
                .map_or(0, |c| t.node(c).sym),
            Tf::AncestorSym(k) => {
                let mut cur = id;
                loop {
                    let found = cur
                        .children(&t.arena)
                        .find(|&c| t.node(c).kind == *k)
                        .map(|c| t.node(c).sym);
                    if let Some(sym) = found {
                        break sym;
                    }
                    match cur.parent(&t.arena) {
                        Some(p) => cur = p,
                        None => break 0,
                    }
                }
            }
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
                let sa = a.apply_sym(t, lang, id, edge_ctx);
                let sb = b.apply_sym(t, lang, id, edge_ctx);
                let sa_empty = sa == 0 || lang.syms.resolve(sa).is_empty();
                let sb_empty = sb == 0 || lang.syms.resolve(sb).is_empty();
                if sa_empty && sb_empty {
                    return 0;
                }
                if sa_empty {
                    return sb;
                }
                if sb_empty {
                    return sa;
                }
                let result = format!("{}{sep}{}", lang.syms.resolve(sa), lang.syms.resolve(sb));
                lang.syms.intern(&result)
            }
            Tf::Pipeline(steps) => {
                let mut s = lang.syms.resolve(t.node(id).sym).to_string();
                for step in steps {
                    match step {
                        Tf::Child(_)
                        | Tf::FieldChild(_, _)
                        | Tf::Field(_)
                        | Tf::ParentSym(_)
                        | Tf::AncestorSym(_)
                        | Tf::Concat(_, _, _)
                        | Tf::HasEdge(_, _) => {
                            let sym = step.apply_sym(t, lang, id, edge_ctx);
                            s = lang.syms.resolve(sym).to_string();
                        }
                        _ => {
                            s = step.apply_to_str(&s);
                        }
                    }
                }
                lang.syms.intern(&s)
            }
            _ => {
                let sym = t.node(id).sym;
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
