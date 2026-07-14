//! Authored-SQL strategy. Marker conformance (no hardcoded watermark/deleted
//! column) is a build-time gate in `gkg-server`'s build script via
//! `ontology::etl_sql::validate_authored_etl_sql`, not a runtime check here.

use super::super::build::PlanError;
use super::super::schema::BatchSchema;
use ontology::constants::{DELETED_COLUMN, VERSION_COLUMN};
use ontology::sql_template;

use super::{BATCH_SIZE_MARKER, ExtractDecl, ExtractSpec, ExtractTemplate, FILTERS_MARKER};

pub(in crate::modules::sdlc) fn build(
    decl: &ExtractDecl,
    raw: &str,
) -> Result<ExtractSpec, PlanError> {
    let rendered = sql_template::render(
        raw,
        sql_template::context! {
            watermark_column => decl.watermark,
            deleted_column => decl.deleted,
            // Re-emit the per-page markers unchanged so `PreparedQuery::to_sql` renders them at extraction time.
            filters => FILTERS_MARKER,
            batch_size => BATCH_SIZE_MARKER,
        },
    )
    .map_err(|e| {
        PlanError::MalformedTemplate(format!("authored SQL for '{}': {e}", decl.entity))
    })?;

    let watermark =
        aliased_expression(&rendered, VERSION_COLUMN).unwrap_or_else(|| decl.watermark.clone());
    let deleted =
        aliased_expression(&rendered, DELETED_COLUMN).unwrap_or_else(|| decl.deleted.clone());

    Ok(ExtractSpec {
        template: ExtractTemplate::new(rendered)?,
        watermark,
        deleted,
        batch_schema: BatchSchema::opaque(),
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

#[cfg(test)]
mod tests {
    use super::*;
    use ontology::EtlScope;

    fn group_decl() -> ExtractDecl {
        ExtractDecl {
            entity: "Group".to_string(),
            scope: EtlScope::Namespaced,
            table: "t".to_string(),
            watermark: "_siphon_watermark".to_string(),
            deleted: "_siphon_deleted".to_string(),
            order_by: vec!["traversal_path".to_string(), "id".to_string()],
        }
    }

    #[test]
    fn markers_are_substituted_and_aliases_recovered() {
        let spec = build(
            &group_decl(),
            "SELECT namespace.{{watermark_column}} AS _version, (namespace.{{deleted_column}} OR namespace.type != 'Group') AS _deleted FROM t WHERE 1=1 {{filters}} LIMIT {{batch_size}}",
        )
        .expect("valid authored SQL");
        assert!(!spec.template.as_str().contains("{{watermark_column}}"));
        assert_eq!(spec.watermark, "namespace._siphon_watermark");
        assert_eq!(
            spec.deleted,
            "(namespace._siphon_deleted OR namespace.type != 'Group')"
        );
    }

    #[test]
    fn unresolved_marker_is_rejected() {
        let err = build(
            &group_decl(),
            "SELECT {{typo_column}} AS _version, x AS _deleted FROM t WHERE 1=1 {{filters}} LIMIT {{batch_size}}",
        )
        .expect_err("unresolved marker should be rejected");
        assert!(
            err.to_string().contains("authored SQL for 'Group'"),
            "got: {err}"
        );
    }
}
