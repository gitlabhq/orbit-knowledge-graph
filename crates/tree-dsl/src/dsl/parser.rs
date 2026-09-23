use super::types::*;
use crate::error::LoadError;

use pest_derive::Parser;

#[derive(Parser)]
#[grammar = "src/dsl/pattern.pest"]
struct PatParser;

type PNode<'i> = pest_consume::Node<'i, Rule, ()>;
type R<T> = Result<T, LoadError>;

/// The next child the grammar promises; a missing one is a grammar bug and
/// still reported rather than unwound.
fn next<'i>(it: &mut impl Iterator<Item = PNode<'i>>, what: &str) -> R<PNode<'i>> {
    it.next()
        .ok_or_else(|| LoadError(format!("pattern grammar: missing {what}")))
}

fn unexpected<T>(what: &str, r: Rule) -> R<T> {
    Err(LoadError(format!(
        "pattern grammar: unexpected {r:?} in {what}"
    )))
}

fn root<'i>(rule: Rule, src: &'i str) -> R<PNode<'i>> {
    let nodes = <PatParser as pest_consume::Parser>::parse(rule, src)
        .map_err(|e| LoadError(format!("pattern {src:?}: {e}")))?;
    let root = nodes
        .single()
        .map_err(|e| LoadError(format!("pattern {src:?}: {e}")))?;
    next(&mut root.into_children(), "root element")
}

pub(crate) fn parse(c: &mut Ctx<'_>, src: &str) -> R<Pat> {
    visit_element(c, root(Rule::Pattern, src)?, 0)
}

pub(crate) fn parse_pipeline(c: &mut Ctx<'_>, src: &str) -> R<Tf> {
    visit_tf_chain(c, root(Rule::Pipeline, src)?)
}

#[pest_consume::parser]
impl PatParser {}

fn visit_element(c: &mut Ctx<'_>, node: PNode<'_>, field: u16) -> R<Pat> {
    match node.as_rule() {
        Rule::Node => visit_node(c, node, field),
        Rule::Variadic => visit_variadic(c, node, field),
        Rule::CapRef => visit_cap_ref(c, node, field),
        Rule::Capture => visit_capture(c, node, field),
        Rule::TextField => visit_text_field_as_cap(c, node, field),
        Rule::Spread => visit_spread(c, node),
        Rule::Negation => {
            let inner = next(&mut node.into_children(), "negated element")?;
            Ok(Pat::Not(Box::new(visit_element(c, inner, 0)?)))
        }
        Rule::Descendant => {
            let inner = next(&mut node.into_children(), "descendant element")?;
            Ok(Pat::Desc(Box::new(visit_element(c, inner, 0)?)))
        }
        r => unexpected("element", r),
    }
}

fn visit_node(c: &mut Ctx<'_>, node: PNode<'_>, field: u16) -> R<Pat> {
    let mut children = node.into_children();
    let name = next(&mut children, "node kind")?;
    let kind = c.intern_kind(if name.as_rule() == Rule::Quoted {
        quoted_inner(&name)
    } else {
        name.as_str()
    });
    let mut kids = Vec::new();
    let mut text = Text::Any;
    let mut optional = false;

    for child in children {
        match child.as_rule() {
            Rule::Opt => optional = true,
            Rule::Quoted => {
                let s = quoted_inner(&child);
                if let Some(pfx) = s.strip_prefix('^') {
                    text = Text::Prefix(c.lang.syms.intern(pfx));
                } else if let Some(re) = s.strip_prefix('/').and_then(|r| r.strip_suffix('/')) {
                    text = Text::Regex(regex::Regex::new(re)?);
                } else {
                    text = Text::Lit(c.lang.syms.intern(s));
                }
            }
            Rule::TextField => {
                let (slot, tf) = visit_text_field(c, child)?;
                text = Text::From(slot, tf);
            }
            Rule::FieldChild => {
                let mut fc = child.into_children();
                let f = c.intern_field(next(&mut fc, "field name")?.as_str());
                let after = next(&mut fc, "field element")?;
                let (opt, elem) = if after.as_rule() == Rule::Opt {
                    (true, next(&mut fc, "optional field element")?)
                } else {
                    (false, after)
                };
                let pat = visit_element(c, elem, f)?;
                kids.push(if opt { pat.with_optional() } else { pat });
            }
            _ => kids.push(visit_element(c, child, 0)?),
        }
    }

    Ok(Pat::Node {
        kind,
        field,
        text,
        kids,
        optional,
    })
}

fn visit_text_field(c: &mut Ctx<'_>, node: PNode<'_>) -> R<(u16, Tf)> {
    let mut children = node.into_children();
    let slot = c.slot(next(&mut children, "text field slot")?.as_str())?;
    let tf = match children.next() {
        Some(chain) if chain.as_rule() == Rule::TfChain => visit_tf_chain(c, chain)?,
        _ => Tf::Id,
    };
    Ok((slot, tf))
}

fn visit_text_field_as_cap(c: &mut Ctx<'_>, node: PNode<'_>, field: u16) -> R<Pat> {
    let name = next(&mut node.into_children(), "capture name")?;
    Ok(Pat::cap(c.slot(name.as_str())?, field))
}

fn visit_variadic(c: &mut Ctx<'_>, node: PNode<'_>, field: u16) -> R<Pat> {
    let mut children = node.into_children();
    let slot = c.slot(next(&mut children, "variadic slot")?.as_str())?;
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
            Rule::Node => guard = Some(Box::new(visit_node(c, child, 0)?)),
            Rule::Arrow => leaf_only = child.as_str() == "=>",
            Rule::Ident => rekind = Some(c.intern_kind(child.as_str())),
            r => return unexpected("variadic", r),
        }
    }

    Ok(Pat::Var {
        slot,
        field,
        rekind,
        leaf_only,
        guard,
        named_only,
    })
}

fn visit_spread(c: &mut Ctx<'_>, node: PNode<'_>) -> R<Pat> {
    let mut children = node.into_children();
    let slot = c.slot(next(&mut children, "spread slot")?.as_str())?;
    let inject = children
        .filter(|ch| {
            matches!(
                ch.as_rule(),
                Rule::Node | Rule::Variadic | Rule::CapRef | Rule::Capture | Rule::Spread
            )
        })
        .map(|ch| visit_element(c, ch, 0))
        .collect::<R<Vec<Pat>>>()?;
    Ok(Pat::Spread { slot, inject })
}

fn visit_cap_ref(c: &mut Ctx<'_>, node: PNode<'_>, field: u16) -> R<Pat> {
    let mut children = node.into_children();
    let name = next(&mut children, "capture name")?;
    let _arrow = children.next();
    let rekind = c.intern_kind(next(&mut children, "rekind")?.as_str());
    Ok(Pat::cap(c.slot(name.as_str())?, field).with_rekind(rekind))
}

fn visit_capture(c: &mut Ctx<'_>, node: PNode<'_>, field: u16) -> R<Pat> {
    let mut children = node.into_children();
    let name = next(&mut children, "capture name")?;
    let mut pat = Pat::cap(c.slot(name.as_str())?, field);
    for child in children {
        match child.as_rule() {
            Rule::Opt => pat = pat.with_optional(),
            Rule::Ident if child.as_str() == "_*_named" => pat = pat.with_named_only(),
            Rule::Ident => pat = pat.with_kind(c.intern_kind(child.as_str())),
            Rule::Node => pat = pat.with_guard(visit_node(c, child, 0)?),
            r => return unexpected("capture filter", r),
        }
    }
    Ok(pat)
}

fn visit_tf_chain(c: &mut Ctx<'_>, node: PNode<'_>) -> R<Tf> {
    let mut tfs = node
        .into_children()
        .map(|e| visit_tf_expr(c, e))
        .collect::<R<Vec<Tf>>>()?;
    Ok(if tfs.len() == 1 {
        tfs.remove(0)
    } else {
        Tf::Pipeline(tfs)
    })
}

fn visit_tf_expr(c: &mut Ctx<'_>, node: PNode<'_>) -> R<Tf> {
    let inner = next(&mut node.into_children(), "transform")?;
    match inner.as_rule() {
        Rule::TfFunc => {
            let mut ch = inner.into_children();
            let func = next(&mut ch, "transform name")?;
            let args: Vec<&str> = next(&mut ch, "transform args")?
                .into_children()
                .map(|q| quoted_inner(&q))
                .collect();
            Tf::from_func(func.as_str(), &args, Some(c))
        }
        Rule::TfLegacy => {
            let mut ch = inner.into_children();
            let name = next(&mut ch, "transform name")?;
            let val = next(&mut ch, "transform value")?;
            Tf::from_func(name.as_str(), &[val.as_str()], Some(c))
        }
        Rule::TfBare => {
            let name = next(&mut inner.into_children(), "transform name")?;
            Tf::from_func(name.as_str(), &[], None)
        }
        r => unexpected("transform", r),
    }
}

fn quoted_inner<'i>(node: &PNode<'i>) -> &'i str {
    node.clone()
        .into_children()
        .find(|c| c.as_rule() == Rule::Inner)
        .map(|c| c.as_str())
        .unwrap_or("")
}
