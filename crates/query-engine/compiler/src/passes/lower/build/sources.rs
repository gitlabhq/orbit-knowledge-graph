use ontology::constants::{DELETED_COLUMN, EDGE_RESERVED_COLUMNS};
use query_data_model::implementations::relational::RelationalBackend;
use query_data_model::implementations::relational::plan::{Expr, Relation, RelationalBuilder};

use super::expr::{Filter, filter, live, tags};
use super::{Built, col, computed, filtered, scan, stored, versioning};
use crate::constants::union_arm_alias;
use crate::input::InputFilter;
use crate::passes::plan::BoundFilter;
use crate::passes::plan::filters::denorm_tag_values;
use crate::passes::plan::predicates::Column;

pub(crate) type Scope<'r> = [&'r Relation];

pub(crate) fn select<B: RelationalBackend>(
    q: &mut RelationalBuilder<'_, B>,
    rows: Relation,
    items: Vec<(String, Expr)>,
) -> Built<Relation> {
    let (rows, columns) = computed(q, rows, items)?;
    q.select(rows, columns)
}

pub(crate) fn columns<'n>(
    scope: &Scope<'_>,
    alias: &str,
    names: impl IntoIterator<Item = &'n str>,
    stored_rows: bool,
) -> Built<Vec<(String, Expr)>> {
    names
        .into_iter()
        .filter(|name| !stored_rows || !versioning(name))
        .map(|name| Ok((name.to_string(), col(scope, alias, name)?)))
        .collect()
}

fn edge_columns() -> impl Iterator<Item = &'static str> {
    EDGE_RESERVED_COLUMNS
        .iter()
        .copied()
        .chain([DELETED_COLUMN])
}

pub(crate) fn deleted<B: RelationalBackend>(scope: &Scope<'_>, alias: &str) -> Built<Option<Expr>> {
    match stored::<B>() {
        true => Ok(None),
        false => live::<B>(scope, alias).map(Some),
    }
}

pub(crate) fn edge_scan<B: RelationalBackend>(
    q: &mut RelationalBuilder<'_, B>,
    tables: &[String],
    alias: &str,
    arm_where: impl Fn(&mut RelationalBuilder<'_, B>, &Scope<'_>, &str) -> Built<Vec<Option<Expr>>>,
) -> Built<(Relation, Vec<Option<Expr>>)> {
    if let [table] = tables {
        let rows = scan(q, table, alias, false)?;
        let predicates = arm_where(q, &[&rows], alias)?;
        return Ok((rows, predicates));
    }
    let inner = union_arm_alias(alias);
    let body = q.subquery(alias, |q| {
        let mut arms = Vec::new();
        for table in tables {
            let rows = scan(q, table, &inner, false)?;
            let predicates = arm_where(q, &[&rows], &inner)?;
            let rows = filtered(q, rows, predicates)?;
            let items = columns(&[&rows], &inner, edge_columns(), stored::<B>())?;
            arms.push(select(q, rows, items)?);
        }
        q.union(arms)
    })?;
    Ok((q.read(&body, Some(alias))?, Vec::new()))
}

pub(crate) fn final_edge_scan<B: RelationalBackend>(
    q: &mut RelationalBuilder<'_, B>,
    tables: &[String],
    alias: &str,
) -> Built<Relation> {
    if let [table] = tables {
        return scan(q, table, alias, true);
    }
    let inner = union_arm_alias(alias);
    let body = q.subquery(alias, |q| {
        let mut arms = Vec::new();
        for table in tables {
            let rows = scan(q, table, &inner, true)?;
            let items = columns(&[&rows], &inner, edge_columns(), stored::<B>())?;
            arms.push(select(q, rows, items)?);
        }
        q.union(arms)
    })?;
    q.read(&body, Some(alias))
}

pub(crate) fn dedup<B: RelationalBackend>(
    q: &mut RelationalBuilder<'_, B>,
    alias: &str,
    table: &str,
    names: &[&str],
    scan_where: impl FnOnce(&Scope<'_>) -> Built<Vec<Expr>>,
) -> Built<(Relation, Option<Expr>)> {
    let body = q.subquery(alias, |q| {
        let rows = scan(q, table, alias, true)?;
        let predicates = scan_where(&[&rows])?.into_iter().map(Some).collect();
        let rows = filtered(q, rows, predicates)?;
        let items = columns(&[&rows], alias, names.iter().copied(), stored::<B>())?;
        select(q, rows, items)
    })?;
    let rows = q.read(&body, Some(alias))?;
    let deleted = deleted::<B>(&[&rows], alias)?;
    Ok((rows, deleted))
}

pub(crate) fn bound_filter<B: RelationalBackend>(
    scope: &Scope<'_>,
    alias: &str,
    property: &str,
    bound: &BoundFilter,
) -> Built<Expr> {
    filter::<B>(
        scope,
        Filter {
            column: &Column::new(alias, property),
            filter: &bound.filter,
            data_type: bound.data_type.as_ref(),
            in_sort_key: bound.in_sort_key,
            text_indexed: bound.text_indexed,
            null_as_epoch: bound.null_as_epoch,
            negated: false,
        },
    )
}

pub(crate) fn denorm_tags(
    scope: &Scope<'_>,
    alias: &str,
    column: &str,
    key: &str,
    filter: &InputFilter,
) -> Built<Option<Expr>> {
    match denorm_tag_values(key, filter) {
        Some(values) => Ok(Some(tags(col(scope, alias, column)?, &values))),
        None => Ok(None),
    }
}
