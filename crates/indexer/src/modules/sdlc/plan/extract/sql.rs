use super::super::build::PlanError;
use ontology::constants::{DELETED_COLUMN, VERSION_COLUMN};
use ontology::sql_template;

use super::{
    BATCH_SIZE_MARKER, ClickHouseExtractDeclaration, ExtractSpec, ExtractTemplate, FILTERS_MARKER,
    FINAL_MARKER,
};

pub(in crate::modules::sdlc) fn compile_authored_extract(
    declaration: &ClickHouseExtractDeclaration,
    raw: &str,
) -> Result<ExtractSpec, PlanError> {
    let rendered = sql_template::render(
        raw,
        sql_template::context! {
            version_column => declaration.version,
            watermark_column => declaration.watermark,
            deleted_column => declaration.deleted,
            // Re-emit the per-page markers unchanged so `PreparedQuery::to_sql` renders them at extraction time.
            filters => FILTERS_MARKER,
            batch_size => BATCH_SIZE_MARKER,
            final => FINAL_MARKER,
        },
    )
    .map_err(|e| {
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

    Ok(ExtractSpec {
        template: ExtractTemplate::new(rendered)?,
        watermark,
        deleted,
    })
}

/// The SELECT-list expression written `AS {alias}`, so filters/cursors target the source expression.
fn aliased_expression(sql: &str, alias: &str) -> Option<String> {
    let marker = format!(" AS {alias}");
    let prefix = &sql[..sql.find(&marker)?];
    let mut depth = 0usize;
    let start = prefix.char_indices().rev().find_map(|(idx, ch)| {
        match ch {
            ')' => depth += 1,
            '(' => depth = depth.saturating_sub(1),
            ',' if depth == 0 => return Some(idx + 1),
            _ if depth == 0 && ch.is_whitespace() && prefix[..idx].ends_with("SELECT") => {
                return Some(idx);
            }
            _ => {}
        }
        None
    })?;
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
    fn deleted_expression_keeps_commas_inside_parentheses() {
        let spec = compile_authored_extract(
            &group_decl(),
            "SELECT\n  id,\n  {{version_column}} AS _version,\n  ({{deleted_column}}\n    OR dictGetOrDefault('d', 'traversal_path', toUInt64(id), '0/') != traversal_path) AS _deleted\nFROM t {{final}} WHERE 1=1 {{filters}} LIMIT {{batch_size}}",
        )
        .expect("valid authored SQL");
        assert_eq!(
            spec.deleted,
            "(_siphon_deleted\n    OR dictGetOrDefault('d', 'traversal_path', toUInt64(id), '0/') != traversal_path)"
        );
        assert_eq!(spec.watermark, "_siphon_watermark");
    }

    #[test]
    fn full_load_deleted_filter_keeps_a_function_call_with_commas() {
        use crate::modules::sdlc::plan::{DeletedFilter, Filter};
        let spec = compile_authored_extract(
            &group_decl(),
            "SELECT id, {{version_column}} AS _version, ({{deleted_column}} OR dictGetOrDefault('d', 'traversal_path', toUInt64(id), '0/') != traversal_path) AS _deleted FROM t {{final}} WHERE 1=1 {{filters}} LIMIT {{batch_size}}",
        )
        .expect("valid authored SQL");
        assert_eq!(
            DeletedFilter {
                column: &spec.deleted
            }
            .condition(),
            "(_siphon_deleted OR dictGetOrDefault('d', 'traversal_path', toUInt64(id), '0/') != traversal_path) = false"
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
