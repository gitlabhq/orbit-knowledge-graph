// Pest-based parser for the pattern DSL.
//
// Grammar lives in pattern.pest. Pest produces the parse tree,
// the visitor below walks it and calls Ctx to intern kinds/slots/filters.

use super::pattern::*;

use pest_derive::Parser;

#[derive(Parser)]
#[grammar = "src/dsl/pattern.pest"]
struct PatParser;

#[pest_consume::parser]
impl PatParser {}

pub(crate) fn parse(c: &mut Ctx<'_>, src: &str) -> Pat {
    let root = <PatParser as pest_consume::Parser>::parse(Rule::Pattern, src)
        .unwrap_or_else(|e| panic!("pattern parse error: {e}"))
        .single()
        .expect("Pattern produces one pair");
    visit_element(c, root.into_children().next().unwrap(), 0)
}

type PNode<'i> = pest_consume::Node<'i, Rule, ()>;

fn visit_element(c: &mut Ctx<'_>, node: PNode<'_>, field: u16) -> Pat {
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

fn visit_node(c: &mut Ctx<'_>, node: PNode<'_>, field: u16) -> Pat {
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

fn visit_text_field(c: &mut Ctx<'_>, node: PNode<'_>) -> (u16, Tf) {
    let mut children = node.into_children();
    let slot = c.slot(children.next().unwrap().as_str());
    let tf = match children.next() {
        Some(chain) if chain.as_rule() == Rule::TfChain => visit_tf_chain(c, chain),
        _ => Tf::Id,
    };
    (slot, tf)
}

fn visit_text_field_as_cap(c: &mut Ctx<'_>, node: PNode<'_>, field: u16) -> Pat {
    let name = node.into_children().next().unwrap().as_str();
    Pat::Cap {
        slot: c.slot(name),
        field,
        kind: None,
        rekind: None,
        guard: None,
        optional: false,
        named_only: false,
    }
}

fn visit_variadic(c: &mut Ctx<'_>, node: PNode<'_>, field: u16) -> Pat {
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
        slot,
        field,
        rekind,
        leaf_only,
        guard,
        named_only,
    }
}

fn visit_spread(c: &mut Ctx<'_>, node: PNode<'_>) -> Pat {
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

fn visit_cap_ref(c: &mut Ctx<'_>, node: PNode<'_>, field: u16) -> Pat {
    let mut children = node.into_children();
    let name = children.next().unwrap().as_str();
    let _arrow = children.next();
    let rekind = c.intern_kind(children.next().unwrap().as_str());
    Pat::Cap {
        slot: c.slot(name),
        field,
        kind: None,
        rekind: Some(rekind),
        guard: None,
        optional: false,
        named_only: false,
    }
}

fn visit_capture(c: &mut Ctx<'_>, node: PNode<'_>, field: u16) -> Pat {
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
        slot: c.slot(name),
        field,
        kind,
        rekind: None,
        guard,
        named_only,
        optional,
    }
}

fn visit_tf_chain(c: &mut Ctx<'_>, node: PNode<'_>) -> Tf {
    let tfs: Vec<Tf> = node.into_children().map(|e| visit_tf_expr(c, e)).collect();
    if tfs.len() == 1 {
        tfs.into_iter().next().unwrap()
    } else {
        Tf::Pipeline(tfs)
    }
}

fn visit_tf_expr(c: &mut Ctx<'_>, node: PNode<'_>) -> Tf {
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
            Tf::from_func(func, &args, Some(c))
        }
        Rule::TfLegacy => {
            let mut ch = inner.into_children();
            let name = ch.next().unwrap().as_str();
            let val = ch.next().unwrap().as_str();
            Tf::from_func(name, &[val], Some(c))
        }
        Rule::TfBare => {
            let name = inner.into_children().next().unwrap().as_str();
            Tf::from_func(name, &[], Some(c))
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
