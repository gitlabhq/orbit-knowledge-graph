//! Generated-extract SQL rendering; all `DataType` → ClickHouse dialect rendering lives here.

use ontology::sql_template;
use ontology::{DataType, EtlScope, constants::TRAVERSAL_PATH_COLUMN};

use super::super::build::PlanError;
use super::super::schema::{BatchSchema, EnrichedColumn};
use super::enrichment::EnrichmentJoin;
use super::{ExtractDecl, ExtractSpec, SourceColumn};

pub(in crate::modules::sdlc) enum Shape<'a> {
    Node {
        columns: &'a [SourceColumn],
        node_ref_columns: &'a [String],
    },
    SingleTable {
        columns: &'a [String],
    },
    Enriched {
        batch_columns: &'a [SourceColumn],
        joins: &'a [EnrichmentJoin],
    },
}

pub(in crate::modules::sdlc) fn build(
    decl: &ExtractDecl<'_>,
    shape: Shape<'_>,
    filter: Option<&str>,
) -> Result<(ExtractSpec, BatchSchema), PlanError> {
    let filter = resolve_filter(decl, filter)?;
    let (sql, schema) = match shape {
        Shape::Node {
            columns,
            node_ref_columns,
        } => (
            render_node(decl, columns, node_ref_columns, filter.as_deref()),
            BatchSchema::opaque(),
        ),
        Shape::SingleTable { columns } => (
            render_single_table(decl, columns, filter.as_deref()),
            BatchSchema::opaque(),
        ),
        Shape::Enriched {
            batch_columns,
            joins,
        } => render_enriched(decl, batch_columns, joins, filter.as_deref()),
    };
    Ok((decl.build_spec(sql)?, schema))
}

/// Substitutes `{{watermark_column}}`/`{{deleted_column}}` and rejects any other `{{marker}}`.
fn resolve_filter(
    decl: &ExtractDecl<'_>,
    filter: Option<&str>,
) -> Result<Option<String>, PlanError> {
    let Some(filter) = filter else {
        return Ok(None);
    };
    let rendered = sql_template::render(
        filter,
        sql_template::context! {
            watermark_column => decl.watermark,
            deleted_column => decl.deleted,
        },
    )
    .map_err(|e| PlanError::MalformedTemplate(format!("filter for '{}': {e}", decl.entity)))?;
    Ok(Some(rendered))
}

/// One SELECT-list entry; `output` is used to dedupe against later stragglers.
struct SelectColumn {
    expression: String,
    output: String,
}

impl SelectColumn {
    fn bare(name: &str) -> Self {
        Self {
            expression: name.to_string(),
            output: name.to_string(),
        }
    }

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

fn render_node(
    decl: &ExtractDecl<'_>,
    columns: &[SourceColumn],
    node_ref_columns: &[String],
    filter: Option<&str>,
) -> String {
    let mut select: Vec<SelectColumn> = columns.iter().map(SelectColumn::typed).collect();
    select.extend(node_ref_columns.iter().map(|name| SelectColumn::bare(name)));
    render_single_table_sql(decl, &select, filter)
}

fn render_single_table(decl: &ExtractDecl<'_>, columns: &[String], filter: Option<&str>) -> String {
    let select: Vec<SelectColumn> = columns.iter().map(|c| SelectColumn::bare(c)).collect();
    render_single_table_sql(decl, &select, filter)
}

fn render_single_table_sql(
    decl: &ExtractDecl<'_>,
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
    for column in decl.order_by {
        if seen.insert(column.clone()) {
            select_columns.push(column.clone());
        }
    }
    select_columns.push(format!("{} AS _version", decl.watermark));
    select_columns.push(format!("{} AS _deleted", decl.deleted));

    let mut where_clause =
        if decl.scope == EtlScope::Namespaced && seen.contains(TRAVERSAL_PATH_COLUMN) {
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
        decl.table,
        where_clause,
        decl.order_by.join(", ")
    )
}

/// `_batch` CTE + one `_eN` CTE per join; the per-consumer semantics ride in each [`EnrichmentJoin`].
fn render_enriched(
    decl: &ExtractDecl<'_>,
    batch_columns: &[SourceColumn],
    joins: &[EnrichmentJoin],
    filter: Option<&str>,
) -> (String, BatchSchema) {
    let watermark = decl.watermark;
    let deleted = decl.deleted;
    let source = decl.table;

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
        decl.order_by.join(", "),
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
    let mut enriched: Vec<EnrichedColumn> = Vec::new();
    for join in joins {
        let alias = &join.alias;
        let table = &join.table;
        let key = &join.key;

        let mut select_cols = vec![format!("{key} AS id")];
        select_cols.extend(
            join.columns
                .iter()
                .map(|c| format!("argMax({c}, {watermark}) AS {c}")),
        );
        let path_scope = if join.scope_to_path {
            "\n    AND startsWith(traversal_path, {traversal_path:String})"
        } else {
            ""
        };
        ctes.push(format!(
            "{alias} AS (SELECT\n    {}\n  FROM {table}\n  WHERE {key} IN (SELECT\n      DISTINCT {}\n    FROM _batch){path_scope}\n  GROUP BY {key}\n  HAVING argMax({deleted}, {watermark}) = false)",
            select_cols.join(",\n    "),
            join.batch_column,
        ));
        join_clauses.push(format!(
            "LEFT JOIN {alias} ON _batch.{} = {alias}.id",
            join.batch_column
        ));
        for c in &join.columns {
            let output = if join.prefix_output {
                format!("{alias}_{c}")
            } else {
                c.clone()
            };
            final_cols.push(format!("{alias}.{c} AS {output}"));
            enriched.push(EnrichedColumn {
                name: output,
                node_kind: join.node_kind.clone(),
                direction: join.direction.clone(),
                node_column: c.clone(),
            });
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
    (sql, BatchSchema::enriched(enriched))
}
