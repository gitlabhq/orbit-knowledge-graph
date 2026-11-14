//! The `(gql, combine)` stage phases: join the earlier stages' rows and order them, add the
//! columns redaction reads with the result contract, then page the joined rows. Every stage
//! column is read at its contract position.

use std::collections::BTreeMap;

use query_data_model::DataType;
use query_data_model::plan::combine::{CombineBuilder, CombineError, CombineOperation, Position};
use query_data_model::plan::tracking::Ref;

use super::{CompilerCtx, require};
use crate::constants::edge_column_prefix;
use crate::dag::{Combine, CombineEdge};
use crate::error::{QueryError, Result};
use crate::passes::result_contract::{EdgeMeta, ResultContext};
use crate::types::QueryType;

pub(super) fn plan(ctx: &mut impl CompilerCtx<Backend = Combine>) -> Result<()> {
    let query = require(ctx.combine_query().as_ref(), "combine_query")?;
    let mut used: BTreeMap<usize, Vec<usize>> = BTreeMap::new();
    let mut use_column = |position: &Position| {
        let columns = used.entry(position.stage).or_default();
        if !columns.contains(&position.column) {
            columns.push(position.column);
        }
    };
    let joined = query
        .joins
        .iter()
        .flat_map(|join| join.left.iter().chain(&join.right));
    joined.for_each(&mut use_column);
    query
        .columns
        .iter()
        .for_each(|(_, position)| use_column(position));
    query
        .order
        .iter()
        .for_each(|(position, _)| use_column(position));
    for node in &query.nodes {
        node.identity
            .iter()
            .for_each(|(position, _)| use_column(position));
        node.key.iter().for_each(&mut use_column);
    }
    for edge in &query.edges {
        edge_columns(edge).for_each(|(_, position)| use_column(&position));
    }
    let planned = CombineBuilder::build(ctx.data_model().id(), |q| {
        let mut stages = used.iter();
        let (first, columns) = stages.next().ok_or(CombineError::EmptyJoin)?;
        let mut rel = q.rows(*first, columns)?;
        let mut joined = vec![*first];
        for (stage, columns) in stages {
            let right = q.rows(*stage, columns)?;
            let mut on = Vec::new();
            for join in &query.joins {
                let pairs = join.left.iter().zip(&join.right).zip(&join.types);
                for ((left, other), data_type) in pairs {
                    let (near, far) = match (left.stage == *stage, other.stage == *stage) {
                        (false, true) => (left, other),
                        (true, false) => (other, left),
                        _ => continue,
                    };
                    if joined.contains(&near.stage) {
                        on.push((q.column(&rel, *near)?, q.column(&right, *far)?, *data_type));
                    }
                }
            }
            rel = q.join(rel, right, on)?;
            joined.push(*stage);
        }
        let keys = query
            .order
            .iter()
            .map(|(position, order)| Ok((q.column(&rel, *position)?, *order)))
            .collect::<std::result::Result<Vec<_>, CombineError>>()?;
        let rel = match keys.is_empty() {
            true => rel,
            false => q.order(rel, keys)?,
        };
        let columns = query
            .columns
            .iter()
            .map(|(name, position)| Ok((name.clone(), q.column(&rel, *position)?)))
            .collect::<std::result::Result<Vec<(String, Ref)>, CombineError>>()?;
        q.select(rel, columns)
    })?;
    if *ctx.trace() {
        ctx.set_planned(crate::dag::describe(&planned));
    }
    ctx.set_combine(planned);
    Ok(())
}

/// The suffix of each edge column a stage returns, with its position.
fn edge_columns(edge: &CombineEdge) -> impl Iterator<Item = (EdgeColumn, Position)> {
    let columns = match edge {
        CombineEdge::Crossing { .. } => Vec::new(),
        CombineEdge::Stage {
            kind,
            source,
            source_kind,
            target,
            target_kind,
            path,
            ..
        } => [
            (EdgeColumn::Kind, *kind),
            (EdgeColumn::Source, *source),
            (EdgeColumn::SourceKind, *source_kind),
            (EdgeColumn::Target, *target),
            (EdgeColumn::TargetKind, *target_kind),
        ]
        .into_iter()
        .chain(path.map(|path| (EdgeColumn::Path, path)))
        .collect(),
    };
    columns.into_iter()
}

#[derive(Clone, Copy)]
enum EdgeColumn {
    Kind,
    Source,
    SourceKind,
    Target,
    TargetKind,
    Path,
}

impl EdgeColumn {
    fn of(self, edge: &EdgeMeta) -> Option<&String> {
        match self {
            Self::Kind => Some(&edge.type_column),
            Self::Source => Some(&edge.src_column),
            Self::SourceKind => Some(&edge.src_type_column),
            Self::Target => Some(&edge.dst_column),
            Self::TargetKind => Some(&edge.dst_type_column),
            Self::Path => edge.path_column.as_ref(),
        }
    }
}

/// The result's nodes, whose id and type columns redaction reads, from each node's identity or
/// check key; each stage's own edges, under the pattern step they stand for; and each crossing's
/// edge, between its endpoint nodes.
pub(super) fn result_contract(ctx: &mut impl CompilerCtx<Backend = Combine>) -> Result<()> {
    let plan = require(ctx.take_combine(), "combine")?;
    let contexts = require(ctx.stage_contexts().as_ref(), "stage_contexts")?;
    let query = require(ctx.combine_query().as_ref(), "combine_query")?;
    let mut result = ResultContext::new().with_query_type(QueryType::Traversal);
    for (_, context) in contexts {
        for (entity, check) in context.entity_auth() {
            if !result.entity_checks(entity).contains(check) {
                result.add_entity_auth(entity, check.clone());
            }
        }
    }
    let proved = |stage: usize, alias: &str| {
        contexts
            .iter()
            .any(|(at, context)| *at == stage && context.stage_checked(alias))
    };
    let mut copied: Vec<(String, Position)> = Vec::new();
    let mut composites: Vec<(String, Vec<(Position, DataType)>)> = Vec::new();
    let mut tags = Vec::new();
    for node in &query.nodes {
        match node.checked || proved(node.stage, &node.alias) {
            true => result.add_stage_checked_node(&node.alias, &node.entity),
            false => result.add_node(&node.alias, &node.entity),
        }
        let added = result
            .get(&node.alias)
            .ok_or_else(|| QueryError::PipelineInvariant("a node was not added".into()))?;
        match (node.key, &node.identity[..]) {
            (Some(key), _) => copied.push((added.id_column.clone(), key)),
            (None, [(part, _)]) => copied.push((added.id_column.clone(), *part)),
            (None, parts) => composites.push((added.id_column.clone(), parts.to_vec())),
        }
        tags.push((added.type_column.clone(), node.entity.clone()));
    }
    let mut edges = Vec::new();
    for edge in &query.edges {
        match edge {
            CombineEdge::Crossing {
                step,
                relationship,
                source,
                target,
            } => {
                let (Some(from), Some(to)) = (result.get(source), result.get(target)) else {
                    continue;
                };
                let meta = EdgeMeta {
                    src_column: from.id_column.clone(),
                    src_type_column: from.type_column.clone(),
                    dst_column: to.id_column.clone(),
                    dst_type_column: to.type_column.clone(),
                    from_alias: source.clone(),
                    to_alias: target.clone(),
                    ..EdgeMeta::hop(
                        edge_column_prefix(*step, false),
                        false,
                        vec![relationship.clone()],
                        *step,
                    )
                };
                tags.push((meta.type_column.clone(), relationship.clone()));
                edges.push(meta);
            }
            CombineEdge::Stage {
                step,
                rel_types,
                path,
                ..
            } => {
                let variable_length = path.is_some();
                let meta = EdgeMeta::hop(
                    edge_column_prefix(*step, variable_length),
                    variable_length,
                    rel_types.clone(),
                    *step,
                );
                for (column, position) in edge_columns(edge) {
                    if let Some(name) = column.of(&meta) {
                        copied.push((name.clone(), position));
                    }
                }
                edges.push(meta);
            }
        }
    }
    edges.sort_by_key(|edge| edge.relationship);
    edges.into_iter().for_each(|edge| result.add_edge(edge));
    let plan = CombineBuilder::rebuild(ctx.data_model().id(), &plan, |q, step| {
        let CombineOperation::Select(selected) = &step.operation else {
            return step.keep(q);
        };
        let names = step
            .outputs()
            .into_iter()
            .map(|output| plan.name(output).unwrap_or_default());
        let mut columns: Vec<(String, Ref)> = names
            .map(str::to_string)
            .zip(selected.iter().copied())
            .collect();
        let input = step.inputs.remove(0);
        for (name, position) in &copied {
            columns.push((name.clone(), q.column(&input, *position)?));
        }
        let keys = composites
            .iter()
            .map(|(name, parts)| {
                let parts = parts
                    .iter()
                    .map(|(position, data_type)| Ok((q.column(&input, *position)?, *data_type)))
                    .collect::<std::result::Result<Vec<_>, CombineError>>()?;
                Ok((name.clone(), parts))
            })
            .collect::<std::result::Result<Vec<_>, CombineError>>()?;
        let rows = match keys.is_empty() {
            true => input,
            false => q.composite(input, keys)?,
        };
        let composed: Vec<(String, Ref)> = composites
            .iter()
            .map(|(name, _)| Ok((name.clone(), q.output(&rows, name)?)))
            .collect::<std::result::Result<_, CombineError>>()?;
        columns.extend(composed);
        let rows = q.select(rows, columns)?;
        match tags.is_empty() {
            true => Ok(rows),
            false => q.tag(rows, tags.clone()),
        }
    })?;
    ctx.set_combine(plan);
    ctx.set_result_ctx(result);
    Ok(())
}

/// Text columns of the joined rows keep as many characters as a single-stage response of the
/// same page would.
pub(super) fn response_policy(ctx: &mut impl CompilerCtx<Backend = Combine>) -> Result<()> {
    let plan = require(ctx.take_combine(), "combine")?;
    let query = require(ctx.combine_query().as_ref(), "combine_query")?;
    let rows = query.skip.unwrap_or(0) + query.limit + 1;
    let chars =
        crate::passes::response_policy::excerpt_chars(u32::try_from(rows).unwrap_or(u32::MAX));
    let suffix = crate::passes::response_policy::TEXT_TRUNCATION_SUFFIX;
    let root = plan.root();
    let plan = CombineBuilder::rebuild(ctx.data_model().id(), &plan, |q, step| {
        let rel = step.keep(q)?;
        if step.node() != root || query.excerpted.is_empty() {
            return Ok(rel);
        }
        let columns = query
            .excerpted
            .iter()
            .map(|name| q.output(&rel, name))
            .collect::<std::result::Result<Vec<_>, _>>()?;
        q.excerpt(rel, columns, chars, suffix)
    })?;
    ctx.set_combine(plan);
    Ok(())
}

pub(super) fn paginate(ctx: &mut impl CompilerCtx<Backend = Combine>) -> Result<()> {
    let plan = require(ctx.take_combine(), "combine")?;
    let query = require(ctx.combine_query().as_ref(), "combine_query")?;
    let (rows, skip) = (query.limit + 1, query.skip.unwrap_or(0));
    let root = plan.root();
    let plan = CombineBuilder::rebuild(ctx.data_model().id(), &plan, |q, step| {
        let rel = step.keep(q)?;
        match step.node() == root {
            true => q.limit(rel, rows, skip),
            false => Ok(rel),
        }
    })?;
    ctx.set_combine(plan);
    Ok(())
}
