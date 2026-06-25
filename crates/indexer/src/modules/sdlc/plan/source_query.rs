use ontology::constants::TRAVERSAL_PATH_COLUMN;

use super::input::{EnrichmentSql, SourceColumn, SourceFrom, SourceQuerySpec};
use super::{Plan, TransformSpec};

const VERSION_ALIAS: &str = "_version";
const DELETED_ALIAS: &str = "_deleted";

pub(super) struct SourceQueryRenderer;

impl SourceQueryRenderer {
    pub(super) fn render_plan(input: SourceQuerySpec, batch_size: u64) -> Plan {
        let select_list = select_list(&input.columns, &input.watermark, &input.deleted);
        let from_sql = from_clause(&input.source);
        let traversal_predicate = scope_predicate(input.namespaced, &input.traversal_path_filter);

        let mut where_predicates: Vec<String> = Vec::new();
        if let Some(p) = traversal_predicate {
            where_predicates.push(p);
        }
        if let Some(p) = &input.additional_where {
            where_predicates.push(format!("({p})"));
        }
        let where_clause = if where_predicates.is_empty() {
            "1=1".to_string()
        } else {
            where_predicates.join(" AND ")
        };

        let order_by_sql = input.order_by.join(", ");

        let extract_template = match &input.enrichment {
            Some(enrichment) => render_cte_template(
                &select_list,
                &from_sql,
                &where_clause,
                &order_by_sql,
                &input.columns,
                enrichment,
            ),
            None => format!(
                "SELECT {select} FROM {from_sql} WHERE {where_clause} {{{{filters}}}} \
                 ORDER BY {order_by_sql} LIMIT {{{{batch_size}}}}",
                select = select_list.join(", "),
            ),
        };

        Plan {
            name: String::new(),
            extract_template,
            watermark_column: input.watermark,
            deleted_column: input.deleted,
            sort_key: input.order_by,
            batch_size,
            transform: TransformSpec::DataFusion(vec![]),
        }
    }
}

fn select_list(columns: &[SourceColumn], watermark: &str, deleted: &str) -> Vec<String> {
    let mut select_list: Vec<String> = columns.iter().map(column_expr).collect();
    select_list.push(format!("{watermark} AS {VERSION_ALIAS}"));
    select_list.push(format!("{deleted} AS {DELETED_ALIAS}"));
    select_list
}

fn from_clause(source: &SourceFrom) -> String {
    match source {
        SourceFrom::Table(table) => table.clone(),
        SourceFrom::Raw(raw) => raw.clone(),
    }
}

fn scope_predicate(namespaced: bool, custom_filter: &Option<String>) -> Option<String> {
    if !namespaced {
        return None;
    }
    Some(custom_filter.clone().unwrap_or_else(|| {
        format!("startsWith({TRAVERSAL_PATH_COLUMN}, {{traversal_path:String}})")
    }))
}

fn render_cte_template(
    select_list: &[String],
    from_sql: &str,
    where_clause: &str,
    order_by_sql: &str,
    columns: &[SourceColumn],
    enrichment: &EnrichmentSql,
) -> String {
    let outer_cols: Vec<String> = columns
        .iter()
        .map(|c| {
            let name = column_alias(c);
            format!("_batch.{name} AS {name}")
        })
        .chain([
            format!("_batch.{VERSION_ALIAS} AS {VERSION_ALIAS}"),
            format!("_batch.{DELETED_ALIAS} AS {DELETED_ALIAS}"),
        ])
        .chain(enrichment.select_exprs.iter().cloned())
        .collect();

    format!(
        "WITH _batch AS (\
         SELECT {select} FROM {from_sql} \
         WHERE {where_clause} {{{{filters}}}} \
         ORDER BY {order_by_sql} LIMIT {{{{batch_size}}}}\
         ), {cte_defs} \
         SELECT {outer_select} FROM _batch {joins}",
        select = select_list.join(", "),
        cte_defs = enrichment.cte_defs.join(", "),
        outer_select = outer_cols.join(", "),
        joins = enrichment.join_clauses.join(" "),
    )
}

fn column_alias(column: &SourceColumn) -> String {
    let raw = match column {
        SourceColumn::Bare(name) => name.as_str(),
        SourceColumn::ToString(name) => name.as_str(),
        SourceColumn::DateClamp(name) => name.as_str(),
    };
    raw.rsplit_once(" AS ")
        .map(|(_, alias)| alias.trim().to_string())
        .unwrap_or_else(|| raw.to_string())
}

fn column_expr(column: &SourceColumn) -> String {
    match column {
        SourceColumn::Bare(name) => name.clone(),
        SourceColumn::ToString(name) => format!("toString({name}) AS {name}"),
        SourceColumn::DateClamp(name) => format!(
            "if({name} >= toDate('1900-01-01') AND {name} <= toDate('2299-12-31'), {name}, NULL) AS {name}"
        ),
    }
}
