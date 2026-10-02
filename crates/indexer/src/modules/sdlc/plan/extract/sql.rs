use super::super::build::PlanError;
use ontology::constants::{DELETED_COLUMN, VERSION_COLUMN};
use ontology::sql_template;

use super::{
    BATCH_SIZE_MARKER, CHANGED_IDS_MARKER, CHANGED_IDS_VARIABLE, CHANGED_ROWS_MARKER,
    CHANGED_ROWS_VARIABLE, ClickHouseExtractDeclaration, ExtractSpec, ExtractTemplate,
    FILTERS_MARKER, watermark_sources,
};

pub(in crate::modules::sdlc) fn compile_authored_extract(
    declaration: &ClickHouseExtractDeclaration,
    raw: &str,
) -> Result<ExtractSpec, PlanError> {
    let uses_changed_rows = !declaration.watermark_sources.is_empty();
    // Re-emit the per-page markers unchanged so `PreparedQuery::to_sql` renders them at extraction time.
    let mut context = std::collections::BTreeMap::from([
        ("version_column", declaration.version.as_str()),
        ("watermark_column", declaration.watermark.as_str()),
        ("deleted_column", declaration.deleted.as_str()),
        ("filters", FILTERS_MARKER),
        ("batch_size", BATCH_SIZE_MARKER),
    ]);
    if uses_changed_rows {
        context.insert(CHANGED_ROWS_VARIABLE, CHANGED_ROWS_MARKER);
        context.insert(CHANGED_IDS_VARIABLE, CHANGED_IDS_MARKER);
    }
    let rendered = sql_template::render(raw, context).map_err(|e| {
        PlanError::MalformedTemplate(format!("authored SQL for '{}': {e}", declaration.entity))
    })?;

    let version = aliased_expression(&rendered, VERSION_COLUMN).ok_or_else(|| {
        PlanError::MalformedTemplate(format!(
            "authored SQL for '{}' must select a version expression AS {VERSION_COLUMN}",
            declaration.entity
        ))
    })?;
    let watermark =
        qualified_sibling_column(&version, &declaration.version, &declaration.watermark)
            .ok_or_else(|| {
                PlanError::MalformedTemplate(format!(
                    "authored SQL for '{}' must select [qualifier.]{} AS {VERSION_COLUMN}",
                    declaration.entity, declaration.version
                ))
            })?;
    let deleted = aliased_expression(&rendered, DELETED_COLUMN)
        .unwrap_or_else(|| declaration.deleted.clone());

    let template = if uses_changed_rows {
        ExtractTemplate::with_changed_rows(rendered)?
    } else {
        ExtractTemplate::new(rendered)?
    };
    Ok(ExtractSpec {
        template,
        watermark,
        deleted,
        watermark_sources: watermark_sources(declaration),
    })
}

/// The SELECT-list expression written `AS {alias}`, so filters/cursors target the source expression.
fn aliased_expression(sql: &str, alias: &str) -> Option<String> {
    let marker = format!(" AS {alias}");
    let end = sql.find(&marker)?;
    let prefix = &sql[..end];
    let start = prefix
        .rfind(',')
        .map(|idx| idx + 1)
        .or_else(|| prefix.rfind("SELECT ").map(|idx| idx + "SELECT ".len()))?;
    Some(prefix[start..].trim().to_string())
}

fn qualified_sibling_column(expression: &str, column: &str, sibling: &str) -> Option<String> {
    if expression == column {
        return Some(sibling.to_string());
    }
    expression
        .strip_suffix(&format!(".{column}"))
        .map(|qualifier| format!("{qualifier}.{sibling}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use ontology::EtlScope;

    fn group_decl() -> ClickHouseExtractDeclaration {
        ClickHouseExtractDeclaration {
            entity: "Group".to_string(),
            scope: EtlScope::Namespaced,
            table: "t".to_string(),
            source_columns: vec![],
            version: "_siphon_replicated_at".to_string(),
            watermark: "_siphon_watermark".to_string(),
            deleted: "_siphon_deleted".to_string(),
            order_by: vec!["traversal_path".to_string(), "id".to_string()],
            query: ontology::ExtractQuery::Sql(String::new()),
            lookup_joins: vec![],
            watermark_sources: vec![],
        }
    }

    #[test]
    fn markers_are_substituted_and_aliases_recovered() {
        let spec = compile_authored_extract(
            &group_decl(),
            "SELECT namespace.{{version_column}} AS _version, (namespace.{{deleted_column}} OR namespace.type != 'Group') AS _deleted FROM t WHERE 1=1 {{filters}} LIMIT {{batch_size}}",
        )
        .expect("valid authored SQL");
        assert!(!spec.template.as_str().contains("{{version_column}}"));
        assert!(
            spec.template
                .as_str()
                .contains("namespace._siphon_replicated_at AS _version")
        );
        assert_eq!(spec.watermark, "namespace._siphon_watermark");
        assert_eq!(
            spec.deleted,
            "(namespace._siphon_deleted OR namespace.type != 'Group')"
        );
    }

    #[test]
    fn unresolved_marker_is_rejected() {
        let err = compile_authored_extract(
            &group_decl(),
            "SELECT {{typo_column}} AS _version, x AS _deleted FROM t WHERE 1=1 {{filters}} LIMIT {{batch_size}}",
        )
        .expect_err("unresolved marker should be rejected");
        assert!(
            err.to_string().contains("authored SQL for 'Group'"),
            "got: {err}"
        );
    }

    #[test]
    fn complex_version_expression_is_rejected() {
        let err = compile_authored_extract(
            &group_decl(),
            "SELECT greatest({{version_column}}, now64(6)) AS _version, x AS _deleted FROM t WHERE 1=1 {{filters}} LIMIT {{batch_size}}",
        )
        .expect_err("complex version expression should be rejected");
        assert!(
            err.to_string()
                .contains("[qualifier.]_siphon_replicated_at")
        );
    }
}
