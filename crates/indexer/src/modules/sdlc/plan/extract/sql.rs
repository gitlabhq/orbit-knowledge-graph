//! Authored-SQL strategy; declared-column conformance is enforced by `crates/ontology/build.rs`, not here.

use super::super::build::PlanError;
use super::super::schema::BatchSchema;
use ontology::constants::{DELETED_COLUMN, VERSION_COLUMN};

use super::{ExtractDecl, ExtractSpec, ExtractTemplate};

const WATERMARK_MARKER: &str = "{{watermark_column}}";
const DELETED_MARKER: &str = "{{deleted_column}}";

pub(in crate::modules::sdlc) fn build(
    decl: &ExtractDecl<'_>,
    raw: &str,
) -> Result<(ExtractSpec, BatchSchema), PlanError> {
    for (what, column, marker) in [
        ("watermark", decl.watermark, WATERMARK_MARKER),
        ("deleted", decl.deleted, DELETED_MARKER),
    ] {
        if raw.contains(column) {
            return Err(PlanError::MalformedTemplate(format!(
                "authored SQL for '{}' hardcodes {what} column '{column}'; use {marker} instead",
                decl.entity
            )));
        }
    }

    let rendered = raw
        .replace(WATERMARK_MARKER, decl.watermark)
        .replace(DELETED_MARKER, decl.deleted);

    let watermark =
        aliased_expression(&rendered, VERSION_COLUMN).unwrap_or_else(|| decl.watermark.to_string());
    let deleted =
        aliased_expression(&rendered, DELETED_COLUMN).unwrap_or_else(|| decl.deleted.to_string());

    Ok((
        ExtractSpec {
            template: ExtractTemplate::new(rendered)?,
            watermark,
            deleted,
            order_by: decl.order_by.to_vec(),
        },
        BatchSchema::opaque(),
    ))
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

    fn group_decl(order_by: &[String]) -> ExtractDecl<'_> {
        ExtractDecl {
            entity: "Group",
            scope: EtlScope::Namespaced,
            table: "t",
            watermark: "_siphon_watermark",
            deleted: "_siphon_deleted",
            order_by,
        }
    }

    fn order_by() -> Vec<String> {
        ["traversal_path", "id"]
            .iter()
            .map(|s| s.to_string())
            .collect()
    }

    #[test]
    fn hardcoded_watermark_is_rejected() {
        let order_by = order_by();
        let err = build(
            &group_decl(&order_by),
            "SELECT _siphon_watermark AS _version, _siphon_deleted AS _deleted FROM t WHERE 1=1 {{filters}} LIMIT {{batch_size}}",
        )
        .expect_err("hardcoded watermark should be rejected");
        assert!(
            err.to_string().contains("hardcodes watermark column"),
            "got: {err}"
        );
    }

    #[test]
    fn markers_are_substituted_and_aliases_recovered() {
        let order_by = order_by();
        let (spec, _) = build(
            &group_decl(&order_by),
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
        let order_by = order_by();
        let err = build(
            &group_decl(&order_by),
            "SELECT {{typo_column}} AS _version, x AS _deleted FROM t WHERE 1=1 {{filters}} LIMIT {{batch_size}}",
        )
        .expect_err("unresolved marker should be rejected");
        assert!(err.to_string().contains("unresolved marker"), "got: {err}");
    }
}
