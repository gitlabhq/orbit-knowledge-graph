//! Entity-level channel gating (ADR 013).
//!
//! Runs alongside [`crate::passes::security`]. For each `gl_*` alias, resolves
//! the target entity's `channel_allowlist` and checks the caller's channel
//! (derived by Rails from the request's auth mechanism) against it. When the
//! channel is not in the allowlist — including the fail-closed case where the
//! ontology declares an empty allowlist — the alias's scan is short-circuited
//! by ANDing `Bool(false)` into the query's `WHERE`.
//!
//! `Bool(false)` is the same primitive [`crate::passes::security`] uses when a
//! caller has no eligible traversal paths for an entity. That's deliberate:
//! it lets [`crate::passes::check`] treat both mechanisms uniformly — an
//! alias whose `WHERE` AND-chains a `Bool(false)` conjunct trivially scopes
//! to zero rows and needs no per-alias `startsWith`.
//!
//! # Fail-closed
//!
//! An entity whose ontology has no `channel_allowlist` (or an empty one)
//! resolves to no allowed channels — every channel, including
//! `core_feature`, sees zero rows. Ontology authors must opt in explicitly,
//! and the ontology-crate build script fails the build if any node lacks a
//! resolved allowlist.

use serde_json::Value;

use ontology::{Channel, Ontology};

use crate::ast::{Expr, Node, Query, TableRef};
use crate::error::Result;
use crate::passes::security::collect_aliased_tables;
use crate::types::SecurityContext;

/// Apply channel gating across the query tree. Idempotent w.r.t. already-
/// applied SecurityPass output, since it composes by AND.
///
/// When `ctx.channel` is `None`, this pass is a no-op — see the
/// [`SecurityContext::channel`] docs for the (test-only) fixture posture.
pub fn apply_channel_context(
    node: &mut Node,
    ctx: &SecurityContext,
    ontology: &Ontology,
) -> Result<()> {
    let Some(channel) = ctx.channel else {
        return Ok(());
    };
    match node {
        Node::Query(q) => {
            for cte in &mut q.ctes {
                apply_to_query(&mut cte.query, channel, ontology);
            }
            apply_to_query(q, channel, ontology);
            Ok(())
        }
        Node::Insert(_) => Ok(()),
    }
}

fn apply_to_query(q: &mut Query, channel: Channel, ontology: &Ontology) {
    let mut denied_any = false;
    for (_alias, table) in collect_aliased_tables(&q.from) {
        if channel_denied(&table, channel, ontology) {
            denied_any = true;
            break;
        }
    }
    if denied_any {
        // AND-in Bool(false) so CheckPass's dead-alias detection accepts
        // every aliased scan in the FROM without a per-alias startsWith.
        // Using a Literal (not a Param) keeps ClickHouse's constant folder
        // free to fold the whole predicate at plan time.
        let false_lit = Expr::Literal(Value::Bool(false));
        q.where_clause = Expr::and_all(
            std::iter::once(Some(false_lit)).chain(std::iter::once(q.where_clause.take())),
        );
    }

    apply_to_from(&mut q.from, channel, ontology);

    if let Some(where_clause) = &mut q.where_clause {
        apply_to_expr(where_clause, channel, ontology);
    }

    for arm in &mut q.union_all {
        apply_to_query(arm, channel, ontology);
    }
}

fn apply_to_from(table_ref: &mut TableRef, channel: Channel, ontology: &Ontology) {
    match table_ref {
        TableRef::Union { queries, .. } => {
            for arm in queries {
                apply_to_query(arm, channel, ontology);
            }
        }
        TableRef::Subquery { query, .. } => {
            apply_to_query(query, channel, ontology);
        }
        TableRef::Join { left, right, .. } => {
            apply_to_from(left, channel, ontology);
            apply_to_from(right, channel, ontology);
        }
        TableRef::Scan { .. } => {}
    }
}

fn apply_to_expr(expr: &mut Expr, channel: Channel, ontology: &Ontology) {
    match expr {
        Expr::InSelect { query, .. } => apply_to_query(query, channel, ontology),
        Expr::BinaryOp { left, right, .. } => {
            apply_to_expr(left, channel, ontology);
            apply_to_expr(right, channel, ontology);
        }
        Expr::UnaryOp { expr, .. }
        | Expr::Lambda { body: expr, .. }
        | Expr::InSubquery { expr, .. } => apply_to_expr(expr, channel, ontology),
        Expr::FuncCall { args, .. } => {
            for arg in args {
                apply_to_expr(arg, channel, ontology);
            }
        }
        Expr::Column { .. }
        | Expr::Identifier(_)
        | Expr::Literal(_)
        | Expr::Param { .. }
        | Expr::Star => {}
    }
}

/// Whether `channel` is *not* in the resolved allowlist for the node backing
/// `table`. Tables without an ontology-mapped node (edge tables, CTEs) return
/// false — same posture as [`Ontology::min_access_level_for_table`] returning
/// `None`, i.e. "no gate applies here."
fn channel_denied(table: &str, channel: Channel, ontology: &Ontology) -> bool {
    match ontology.channel_allowlist_for_table(table) {
        Some(set) => !set.contains(&channel),
        None => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ast::{JoinType, SelectExpr};
    use ontology::{ChannelAllowlist, ChannelAllowlistEntry, ChannelGroup};

    fn ontology_with(entity: &str, allowlist: ChannelAllowlist) -> Ontology {
        Ontology::new()
            .with_nodes([entity])
            .with_redaction(entity, "dummy", "id")
            .with_redaction_channels(entity, allowlist)
    }

    fn project_query(alias: &str, table: &str) -> Node {
        Node::Query(Box::new(Query {
            select: vec![SelectExpr {
                expr: Expr::col(alias, "id"),
                alias: None,
            }],
            from: TableRef::scan(table, alias),
            where_clause: None,
            limit: Some(10),
            ..Default::default()
        }))
    }

    fn ctx_with_channel(channel: Channel) -> SecurityContext {
        SecurityContext::new(1, vec!["1/".into()])
            .unwrap()
            .with_channel(channel)
    }

    // An entity opened up to every channel via the `all_interfaces` group
    // must never gain a `Bool(false)` — otherwise the fail-closed default
    // would leak into intentionally-unrestricted entities.
    #[test]
    fn all_interfaces_allows_every_channel() {
        let ontology = ontology_with(
            "Project",
            ChannelAllowlist::from_entries(vec![ChannelAllowlistEntry::Group(
                ChannelGroup::AllInterfaces,
            )]),
        );
        for channel in [
            Channel::ExternalAgent,
            Channel::DapInternal,
            Channel::CoreFeature,
            Channel::Frontend,
        ] {
            let mut node = project_query("p", "gl_project");
            apply_channel_context(&mut node, &ctx_with_channel(channel), &ontology).unwrap();
            let Node::Query(q) = &node else {
                unreachable!()
            };
            assert!(
                q.where_clause.is_none(),
                "{channel:?} on all_interfaces must not add a filter; got {:?}",
                q.where_clause
            );
        }
    }

    // Fail-closed default: an ontology that declares no allowlist blocks
    // every channel, including `core_feature`. This is the whole point of
    // ADR 013's stricter posture vs. `required_role`.
    #[test]
    fn empty_allowlist_blocks_every_channel_including_core_feature() {
        let ontology = ontology_with("Project", ChannelAllowlist::default());
        for channel in [
            Channel::ExternalAgent,
            Channel::DapInternal,
            Channel::CoreFeature,
            Channel::Frontend,
        ] {
            let mut node = project_query("p", "gl_project");
            apply_channel_context(&mut node, &ctx_with_channel(channel), &ontology).unwrap();
            let Node::Query(q) = &node else {
                unreachable!()
            };
            let sql = format!("{:?}", q.where_clause);
            assert!(
                sql.contains("Bool") && sql.contains("false"),
                "empty allowlist must Bool(false)-gate {channel:?}, got: {sql}"
            );
        }
    }

    // `dap_internal` alone is a single-raw-channel allowlist. External
    // agents must be denied even though the user is fully authenticated —
    // the point of the ADR is that the channel gate is independent of role.
    #[test]
    fn dap_only_denies_external_agent() {
        let ontology = ontology_with(
            "Vulnerability",
            ChannelAllowlist::from_entries(vec![ChannelAllowlistEntry::Channel(
                Channel::DapInternal,
            )]),
        );

        let mut node = project_query("v", "gl_vulnerability");
        apply_channel_context(
            &mut node,
            &ctx_with_channel(Channel::ExternalAgent),
            &ontology,
        )
        .unwrap();
        let Node::Query(q) = &node else {
            unreachable!()
        };
        let sql = format!("{:?}", q.where_clause);
        assert!(
            sql.contains("Bool") && sql.contains("false"),
            "external_agent must be denied on a DAP-only entity, got: {sql}"
        );
    }

    #[test]
    fn dap_only_allows_dap_internal() {
        let ontology = ontology_with(
            "Vulnerability",
            ChannelAllowlist::from_entries(vec![ChannelAllowlistEntry::Channel(
                Channel::DapInternal,
            )]),
        );

        let mut node = project_query("v", "gl_vulnerability");
        apply_channel_context(
            &mut node,
            &ctx_with_channel(Channel::DapInternal),
            &ontology,
        )
        .unwrap();
        let Node::Query(q) = &node else {
            unreachable!()
        };
        assert!(
            q.where_clause.is_none(),
            "dap_internal must pass on a DAP-only entity"
        );
    }

    // A join between a fully-open entity and a gated one must Bool(false) the
    // whole query — the compiler can't emit rows for the gated table, so no
    // combination of the two survives.
    #[test]
    fn join_with_denied_alias_bool_false_gates_whole_query() {
        let ontology = ontology_with(
            "Project",
            ChannelAllowlist::from_entries(vec![ChannelAllowlistEntry::Group(
                ChannelGroup::AllInterfaces,
            )]),
        )
        .with_nodes(["Vulnerability"])
        .with_redaction("Vulnerability", "vulnerability", "id")
        .with_redaction_channels(
            "Vulnerability",
            ChannelAllowlist::from_entries(vec![ChannelAllowlistEntry::Channel(
                Channel::DapInternal,
            )]),
        );

        let mut node = Node::Query(Box::new(Query {
            select: vec![SelectExpr {
                expr: Expr::col("v", "id"),
                alias: None,
            }],
            from: TableRef::join(
                JoinType::Inner,
                TableRef::scan("gl_project", "p"),
                TableRef::scan("gl_vulnerability", "v"),
                Expr::eq(Expr::col("p", "id"), Expr::col("v", "project_id")),
            ),
            limit: Some(10),
            ..Default::default()
        }));

        apply_channel_context(
            &mut node,
            &ctx_with_channel(Channel::ExternalAgent),
            &ontology,
        )
        .unwrap();
        let Node::Query(q) = &node else {
            unreachable!()
        };
        let sql = format!("{:?}", q.where_clause);
        assert!(
            sql.contains("Bool") && sql.contains("false"),
            "an external_agent query joining a DAP-only entity must be gated to zero rows, got: {sql}"
        );
    }

    // Missing channel on the SecurityContext (e.g. a legacy test fixture)
    // disables the pass entirely so pre-ADR-013 unit tests continue to
    // compile without change.
    #[test]
    fn no_channel_on_context_is_pass_noop() {
        let ontology = ontology_with("Project", ChannelAllowlist::default());
        let ctx = SecurityContext::new(1, vec!["1/".into()]).unwrap();
        let mut node = project_query("p", "gl_project");
        apply_channel_context(&mut node, &ctx, &ontology).unwrap();
        let Node::Query(q) = &node else {
            unreachable!()
        };
        assert!(
            q.where_clause.is_none(),
            "no channel on context must skip gating, got {:?}",
            q.where_clause
        );
    }

    // Edge tables and CTEs aren't node-backed so channel_allowlist_for_table
    // returns None. The pass must leave them alone regardless of the caller's
    // channel — gating rides on the node scans, not the edge scans.
    #[test]
    fn edge_and_cte_scans_are_untouched() {
        let ontology = Ontology::new().with_nodes(["Project"]);
        let mut node = project_query("e", ontology::constants::EDGE_TABLE);
        apply_channel_context(
            &mut node,
            &ctx_with_channel(Channel::ExternalAgent),
            &ontology,
        )
        .unwrap();
        let Node::Query(q) = &node else {
            unreachable!()
        };
        assert!(q.where_clause.is_none());
    }
}
