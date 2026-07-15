//! Generated-extract SQL rendering; all `DataType` → ClickHouse dialect rendering lives here.

use ontology::sql_template;
use ontology::{DataType, EtlScope, constants::TRAVERSAL_PATH_COLUMN};

use super::super::build::PlanError;
use super::lookup::PointLookupJoin;
use super::{ClickHouseExtractDeclaration, ClickHouseExtractPlan, SourceColumn};

struct SelectColumn {
    expression: String,
    output: String,
}

pub(in crate::modules::sdlc) fn compile_generated_extract(
    declaration: &ClickHouseExtractDeclaration,
    filter: Option<&str>,
) -> Result<ClickHouseExtractPlan, PlanError> {
    let filter = resolve_filter(declaration, filter)?;
    let sql = if declaration.lookup_joins.is_empty() {
        render_single_table_sql(
            declaration,
            &declaration
                .source_columns
                .iter()
                .map(SelectColumn::typed)
                .collect::<Vec<_>>(),
            filter.as_deref(),
        )
    } else {
        render_with_lookups(
            declaration,
            &declaration.source_columns,
            &declaration.lookup_joins,
            filter.as_deref(),
        )
    };
    declaration.build_spec(sql)
}

/// Substitutes `{{watermark_column}}`/`{{deleted_column}}` and rejects any other `{{marker}}`.
fn resolve_filter(
    declaration: &ClickHouseExtractDeclaration,
    filter: Option<&str>,
) -> Result<Option<String>, PlanError> {
    let Some(filter) = filter else {
        return Ok(None);
    };
    let rendered = sql_template::render(
        filter,
        sql_template::context! {
            watermark_column => declaration.watermark,
            deleted_column => declaration.deleted,
        },
    )
    .map_err(|e| {
        PlanError::MalformedTemplate(format!("filter for '{}': {e}", declaration.entity))
    })?;
    Ok(Some(rendered))
}

impl SelectColumn {
    fn typed(column: &SourceColumn) -> Self {
        let name = &column.name;
        let expression = match column.data_type {
            DataType::Uuid => format!("toString({name}) AS {name}"),
            // Postgres `date` exceeds ClickHouse `Date32` (1900..2299); one bad row poisons the Arrow batch.
            DataType::Date => format!(
                "if({name} >= toDate('1900-01-01') AND {name} <= toDate('2299-12-31'), {name}, NULL) AS {name}"
            ),
            // Postgres `bytea` can hold non-UTF8 bytes; one bad row poisons the Arrow batch, so hex-encode invalid values.
            _ if column.binary => format!("if(isValidUTF8({name}), {name}, hex({name})) AS {name}"),
            _ => name.clone(),
        };
        Self {
            expression,
            output: name.clone(),
        }
    }
}

fn render_single_table_sql(
    declaration: &ClickHouseExtractDeclaration,
    columns: &[SelectColumn],
    filter: Option<&str>,
) -> String {
    let mut select_columns = Vec::new();
    let mut seen = std::collections::HashSet::new();
    for column in columns {
        if seen.insert(column.output.clone()) {
            select_columns.push(column.expression.clone());
        }
    }
    for column in &declaration.order_by {
        if seen.insert(column.clone()) {
            select_columns.push(column.clone());
        }
    }
    select_columns.push(format!("{} AS _version", declaration.watermark));
    select_columns.push(format!("{} AS _deleted", declaration.deleted));

    let mut where_clause =
        if declaration.scope == EtlScope::Namespaced && seen.contains(TRAVERSAL_PATH_COLUMN) {
            "startsWith(traversal_path, {traversal_path:String})".to_string()
        } else {
            "1=1".to_string()
        };
    if let Some(filter) = filter {
        where_clause = format!("{where_clause} AND ({filter})");
    }

    format!(
        "SELECT {} FROM {} WHERE {} {{{{filters}}}} ORDER BY {} LIMIT {{{{batch_size}}}}",
        select_columns.join(", "),
        declaration.table,
        where_clause,
        declaration.order_by.join(", ")
    )
}

fn render_with_lookups(
    declaration: &ClickHouseExtractDeclaration,
    batch_columns: &[SourceColumn],
    joins: &[PointLookupJoin],
    filter: Option<&str>,
) -> String {
    let watermark = &declaration.watermark;
    let deleted = &declaration.deleted;
    let source = &declaration.table;

    let mut where_clause = "startsWith(traversal_path, {traversal_path:String})".to_string();
    if let Some(filter) = filter {
        where_clause.push_str(&format!(" AND ({filter})"));
    }

    let mut batch_select: Vec<String> = batch_columns
        .iter()
        .map(|c| SelectColumn::typed(c).expression)
        .collect();
    batch_select.push(format!("{watermark} AS _version"));
    batch_select.push(format!("{deleted} AS _deleted"));

    let mut ctes = vec![format!(
        "_batch AS (SELECT\n    {}\n  FROM {source}\n  WHERE {where_clause} {{{{filters}}}}\n  ORDER BY {}\n  LIMIT {{{{batch_size}}}})",
        batch_select.join(",\n    "),
        declaration.order_by.join(", "),
    )];

    let mut final_cols: Vec<String> = batch_columns
        .iter()
        .map(|c| {
            let name = &c.name;
            format!("_batch.{name} AS {name}")
        })
        .collect();
    final_cols.push("_batch._version AS _version".to_string());
    final_cols.push("_batch._deleted AS _deleted".to_string());

    let mut join_clauses: Vec<String> = Vec::new();
    for join in joins {
        let alias = &join.internal_alias;
        let table = &join.source_table;
        let key = &join.source_id_column;

        let mut select_cols = vec![format!("{key} AS id")];
        select_cols.extend(join.output_fields.iter().map(|field| {
            let source_column = &field.source_column;
            format!("argMax({source_column}, {watermark}) AS {source_column}")
        }));
        let path_scope = if join.has_traversal_path {
            "\n    AND startsWith(traversal_path, {traversal_path:String})"
        } else {
            ""
        };
        ctes.push(format!(
            "{alias} AS (SELECT\n    {}\n  FROM {table}\n  WHERE {key} IN (SELECT\n      DISTINCT {}\n    FROM _batch){path_scope}\n  GROUP BY {key}\n  HAVING argMax({deleted}, {watermark}) = false)",
            select_cols.join(",\n    "),
            join.batch_id_column,
        ));
        join_clauses.push(format!(
            "LEFT JOIN {alias} ON _batch.{} = {alias}.id",
            join.batch_id_column
        ));
        for field in &join.output_fields {
            let source_column = &field.source_column;
            let output_field = &field.output_field;
            final_cols.push(format!("{alias}.{source_column} AS {output_field}"));
        }
    }

    let from_clause = if join_clauses.is_empty() {
        "FROM _batch".to_string()
    } else {
        format!("FROM _batch\n{}", join_clauses.join("\n"))
    };
    let sql = format!(
        "WITH\n  {}\nSELECT\n  {}\n{from_clause}",
        ctes.join(",\n  "),
        final_cols.join(",\n  "),
    );
    sql
}
