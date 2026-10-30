use super::super::build::PlanError;
use super::{ClickHouseExtractDeclaration, ExtractSpec, ExtractTemplate};
use ontology::constants::{DELETED_COLUMN, VERSION_COLUMN};

pub(in crate::modules::sdlc) fn compile_authored_extract(
    declaration: &ClickHouseExtractDeclaration,
    raw: &str,
) -> Result<ExtractSpec, PlanError> {
    let template = ExtractTemplate::new(raw, declaration.template_columns()).map_err(|e| {
        PlanError::MalformedTemplate(format!("authored SQL for '{}': {e}", declaration.entity))
    })?;
    let rendered = template.incremental();

    let version = aliased_expression(rendered, VERSION_COLUMN).ok_or_else(|| {
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
    let deleted =
        aliased_expression(rendered, DELETED_COLUMN).unwrap_or_else(|| declaration.deleted.clone());

    Ok(ExtractSpec {
        template,
        watermark,
        deleted,
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
        }
    }

    #[test]
    fn markers_are_substituted_and_aliases_recovered() {
        let spec = compile_authored_extract(
            &group_decl(),
            "SELECT namespace.{{version_column}} AS _version, (namespace.{{deleted_column}} OR namespace.type != 'Group') AS _deleted FROM t {{final}} WHERE 1=1 {{filters}} LIMIT {{batch_size}}",
        )
        .expect("valid authored SQL");
        assert!(!spec.template.incremental().contains("{{version_column}}"));
        assert!(
            spec.template
                .incremental()
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
            "SELECT {{typo_column}} AS _version, x AS _deleted FROM t {{final}} WHERE 1=1 {{filters}} LIMIT {{batch_size}}",
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
            "SELECT greatest({{version_column}}, now64(6)) AS _version, x AS _deleted FROM t {{final}} WHERE 1=1 {{filters}} LIMIT {{batch_size}}",
        )
        .expect_err("complex version expression should be rejected");
        assert!(
            err.to_string()
                .contains("[qualifier.]_siphon_replicated_at")
        );
    }
}
