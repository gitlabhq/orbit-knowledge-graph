//! The `(gql, engine)` stage phases: join the earlier stages' rows and order them, add the
//! columns redaction reads with the result contract, then page the joined rows.

use std::collections::BTreeMap;

use query_data_model::plan::engine::{EngineBuilder, EngineOperation};
use query_data_model::plan::tracking::Ref;

use super::{CompilerCtx, require};
use crate::dag::{Engine, EngineColumn};
use crate::error::Result;
use crate::passes::result_contract::ResultContext;
use crate::types::QueryType;

pub(super) fn plan(ctx: &mut impl CompilerCtx<Backend = Engine>) -> Result<()> {
    let query = require(ctx.engine_query().as_ref(), "engine_query")?;
    let contexts = require(ctx.stage_contexts().as_ref(), "stage_contexts")?;
    let mut used: BTreeMap<usize, Vec<String>> = BTreeMap::new();
    let mut use_column = |column: &EngineColumn| {
        let columns = used.entry(column.stage).or_default();
        if !columns.contains(&column.column) {
            columns.push(column.column.clone());
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
        .for_each(|(_, column)| use_column(column));
    query
        .order
        .iter()
        .for_each(|(column, _)| use_column(column));
    query.nodes.iter().for_each(|node| use_column(&node.id));
    for (stage, context) in contexts {
        for node in context.nodes() {
            use_column(&EngineColumn {
                stage: *stage,
                column: node.id_column.clone(),
            });
            use_column(&EngineColumn {
                stage: *stage,
                column: node.type_column.clone(),
            });
        }
    }
    let planned =
        EngineBuilder::build(ctx.data_model().id(), |q| {
            let mut stages = used.iter();
            let (first, columns) = stages
                .next()
                .ok_or(query_data_model::plan::engine::EngineError::EmptyJoin)?;
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
                            on.push((
                                q.column(&rel, near.stage, &near.column)?,
                                q.column(&right, far.stage, &far.column)?,
                                *data_type,
                            ));
                        }
                    }
                }
                rel = q.join(rel, right, on)?;
                joined.push(*stage);
            }
            let keys = query
            .order
            .iter()
            .map(|(column, order)| Ok((q.column(&rel, column.stage, &column.column)?, *order)))
            .collect::<std::result::Result<Vec<_>, query_data_model::plan::engine::EngineError>>()?;
            let rel = match keys.is_empty() {
                true => rel,
                false => q.order(rel, keys)?,
            };
            let columns = query
                .columns
                .iter()
                .map(|(name, column)| {
                    Ok((name.clone(), q.column(&rel, column.stage, &column.column)?))
                })
                .collect::<std::result::Result<
                    Vec<(String, Ref)>,
                    query_data_model::plan::engine::EngineError,
                >>()?;
            q.select(rel, columns)
        })?;
    if *ctx.trace() {
        ctx.set_planned(crate::dag::describe(&planned));
    }
    ctx.set_engine(planned);
    Ok(())
}

/// The relational stages' nodes, whose id and type columns the result carries for redaction.
pub(super) fn result_contract(ctx: &mut impl CompilerCtx<Backend = Engine>) -> Result<()> {
    let plan = require(ctx.take_engine(), "engine")?;
    let contexts = require(ctx.stage_contexts().as_ref(), "stage_contexts")?;
    let mut result = ResultContext::new().with_query_type(QueryType::Traversal);
    let mut redacted = Vec::new();
    for (stage, context) in contexts {
        for node in context.nodes() {
            result.add_node(&node.alias, &node.entity_type);
            for check in context.entity_checks(&node.entity_type) {
                result.add_entity_auth(node.entity_type.clone(), check.clone());
            }
            redacted.push((*stage, node.id_column.clone()));
            redacted.push((*stage, node.type_column.clone()));
        }
    }
    let query = require(ctx.engine_query().as_ref(), "engine_query")?;
    let mut remote = Vec::new();
    let mut tags = Vec::new();
    for node in &query.nodes {
        result.add_stage_checked_node(&node.alias, &node.entity);
        let added = result.get(&node.alias).ok_or_else(|| {
            crate::error::QueryError::PipelineInvariant("a node was not added".into())
        })?;
        remote.push((added.id_column.clone(), node.id.clone()));
        tags.push((added.type_column.clone(), node.entity.clone()));
    }
    let plan = EngineBuilder::rebuild(ctx.data_model().id(), &plan, |q, step| {
        let EngineOperation::Select(selected) = &step.operation else {
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
        for (stage, column) in &redacted {
            columns.push((column.clone(), q.column(&input, *stage, column)?));
        }
        for (name, id) in &remote {
            columns.push((name.clone(), q.column(&input, id.stage, &id.column)?));
        }
        let rows = q.select(input, columns)?;
        match tags.is_empty() {
            true => Ok(rows),
            false => q.tag(rows, tags.clone()),
        }
    })?;
    ctx.set_engine(plan);
    ctx.set_result_ctx(result);
    Ok(())
}

/// Text columns of the joined rows keep as many characters as a single-stage response of the
/// same page would.
pub(super) fn response_policy(ctx: &mut impl CompilerCtx<Backend = Engine>) -> Result<()> {
    let plan = require(ctx.take_engine(), "engine")?;
    let query = require(ctx.engine_query().as_ref(), "engine_query")?;
    let rows = query.skip.unwrap_or(0) + query.limit + 1;
    let chars =
        crate::passes::response_policy::excerpt_chars(u32::try_from(rows).unwrap_or(u32::MAX));
    let suffix = crate::passes::response_policy::TEXT_TRUNCATION_SUFFIX;
    let root = plan.root();
    let plan = EngineBuilder::rebuild(ctx.data_model().id(), &plan, |q, step| {
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
    ctx.set_engine(plan);
    Ok(())
}

pub(super) fn paginate(ctx: &mut impl CompilerCtx<Backend = Engine>) -> Result<()> {
    let plan = require(ctx.take_engine(), "engine")?;
    let query = require(ctx.engine_query().as_ref(), "engine_query")?;
    let (rows, skip) = (query.limit + 1, query.skip.unwrap_or(0));
    let root = plan.root();
    let plan = EngineBuilder::rebuild(ctx.data_model().id(), &plan, |q, step| {
        let rel = step.keep(q)?;
        match step.node() == root {
            true => q.limit(rel, rows, skip),
            false => Ok(rel),
        }
    })?;
    ctx.set_engine(plan);
    Ok(())
}
