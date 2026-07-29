//! Manual end-to-end validation for ADR 013.
//!
//! Not part of the normal test surface — the `#[ignore]` attribute keeps it out
//! of `cargo test` / `mise test:fast`. Run explicitly with:
//!
//! ```
//! cargo test --package compiler --test manual_adr013 -- --ignored --nocapture --test-threads=1
//! ```
//!
//! Each scenario is a separate `#[test]` so the harness reports pass/fail per
//! scenario and prints reproduction detail through the standard test writer
//! rather than a bespoke logger.

use std::sync::OnceLock;

use compiler::SecurityContext;
use ontology::{
    Channel, ChannelAllowlist, ChannelAllowlistEntry, ChannelGroup, Ontology,
    introspection::{IntrospectionScope, build_schema_response},
};

fn ontology() -> &'static Ontology {
    static ONT: OnceLock<Ontology> = OnceLock::new();
    ONT.get_or_init(|| Ontology::load_embedded().expect("embedded ontology loads"))
}

fn traversal_ctx(channel: Channel) -> SecurityContext {
    SecurityContext::new_with_roles(
        1,
        vec![compiler::TraversalPath::with_access_levels(
            "1/",
            vec![compiler::AccessLevel::Owner as u32],
        )],
    )
    .expect("SecurityContext must build")
    .with_channel(channel)
}

fn compile_project_at(channel: Channel, ontology: &Ontology) -> String {
    let query = r#"{
        "query_type": "traversal",
        "nodes": [{"id": "p", "entity": "Project", "node_ids": [1]}],
        "limit": 10
    }"#;
    compiler::compile(query, ontology, &traversal_ctx(channel))
        .expect("compile should succeed even under a gated alias")
        .base
        .render()
}

/// True iff the emitted SQL carries a ChannelPass-injected `Bool(false)`
/// short-circuit as a top-level AND conjunct. Distinguishing this from the
/// routine `_deleted = false` column filter is what makes this manual test
/// meaningful — a pure `contains("false")` check trips on the column filter
/// and reports every query as gated. The actual ChannelPass output shape is
/// `WHERE (false AND ...)` because `Expr::and_all` builds a left-leaning AND
/// tree and the `false` literal always lands on the left. If the codegen for
/// `Expr::Literal(Value::Bool(false))` ever changes, adjust this heuristic
/// AND write the change up in ADR 013's implementation notes.
fn has_bool_false_gate(sql: &str) -> bool {
    let lc = sql.to_lowercase();
    lc.contains("(false and") || lc.contains(" false and ") || lc.starts_with("false and ")
}

// -----------------------------------------------------------------------------
// Scenario 1: with the shipped ontology (every node = [all_interfaces]) every
// channel compiles a plain query — no Bool(false) is injected.
// -----------------------------------------------------------------------------
#[test]
#[ignore]
fn scenario_1_shipped_ontology_gates_nobody() {
    let ont = ontology();
    for channel in [
        Channel::ExternalAgent,
        Channel::DapInternal,
        Channel::CoreFeature,
        Channel::Frontend,
    ] {
        let sql = compile_project_at(channel, ont);
        println!(
            "[scenario 1] channel={channel:?} sql len={} contains 'false'? {}",
            sql.len(),
            sql.to_lowercase().contains("false")
        );
        assert!(
            !has_bool_false_gate(&sql),
            "[scenario 1] shipped [all_interfaces] must not gate {channel:?}, got:\n{sql}"
        );
    }
    println!(
        "[scenario 1] PASS: all four channels compile without Bool(false) under the shipped ontology"
    );
}

// -----------------------------------------------------------------------------
// Scenario 2: narrow Vulnerability to [dap_internal]. external_agent, frontend,
// and core_feature see Bool(false); dap_internal doesn't.
// -----------------------------------------------------------------------------
#[test]
#[ignore]
fn scenario_2_narrow_allowlist_gates_denied_channels() {
    let ont = ontology().clone().with_redaction_channels(
        "Vulnerability",
        ChannelAllowlist::from_entries(vec![ChannelAllowlistEntry::Channel(Channel::DapInternal)]),
    );

    let vuln_query = r#"{
        "query_type": "traversal",
        "nodes": [{"id": "v", "entity": "Vulnerability", "node_ids": [1]}],
        "limit": 10
    }"#;

    for (channel, should_gate) in [
        (Channel::ExternalAgent, true),
        (Channel::Frontend, true),
        (Channel::CoreFeature, true),
        (Channel::DapInternal, false),
    ] {
        // Vulnerability needs security_manager (25); Owner (50) on `1/` clears
        // the role floor so we can observe channel gating in isolation.
        let sql = compiler::compile(vuln_query, &ont, &traversal_ctx(channel))
            .expect("compile should succeed")
            .base
            .render();
        let gated = has_bool_false_gate(&sql);
        println!("[scenario 2] channel={channel:?} gated={gated} expected_gated={should_gate}");
        assert_eq!(
            gated, should_gate,
            "[scenario 2] channel={channel:?} produced unexpected gating\n{sql}"
        );
    }
    println!(
        "[scenario 2] PASS: [dap_internal] gates external_agent/frontend/core_feature; admits dap_internal"
    );
}

// -----------------------------------------------------------------------------
// Scenario 3: schema-discovery hides gated entities. An external_agent asking
// for the schema does NOT see Vulnerability once it's narrowed to
// [dap_internal]; a dap_internal caller DOES.
// -----------------------------------------------------------------------------
#[test]
#[ignore]
fn scenario_3_schema_discovery_omits_gated_entities() {
    let ont = ontology().clone().with_redaction_channels(
        "Vulnerability",
        ChannelAllowlist::from_entries(vec![ChannelAllowlistEntry::Channel(Channel::DapInternal)]),
    );

    let names_for = |channel: Option<Channel>| -> Vec<String> {
        build_schema_response(&ont, IntrospectionScope::All, &[], channel)
            .domains
            .iter()
            .flat_map(|d| d.nodes.iter())
            .map(|n| match n {
                ontology::introspection::SchemaNode::Name(s) => s.clone(),
                ontology::introspection::SchemaNode::Expanded { name, .. } => name.clone(),
            })
            .collect()
    };

    let ea = names_for(Some(Channel::ExternalAgent));
    let dap = names_for(Some(Channel::DapInternal));
    let none = names_for(None);

    println!(
        "[scenario 3] ExternalAgent sees {} entities (Vulnerability present? {})",
        ea.len(),
        ea.contains(&"Vulnerability".to_string())
    );
    println!(
        "[scenario 3] DapInternal sees {} entities (Vulnerability present? {})",
        dap.len(),
        dap.contains(&"Vulnerability".to_string())
    );
    println!(
        "[scenario 3] None (no channel filter) sees {} entities (Vulnerability present? {})",
        none.len(),
        none.contains(&"Vulnerability".to_string())
    );

    assert!(
        !ea.contains(&"Vulnerability".to_string()),
        "[scenario 3] external_agent must NOT see Vulnerability in the schema, got: {ea:?}"
    );
    assert!(
        dap.contains(&"Vulnerability".to_string()),
        "[scenario 3] dap_internal MUST see Vulnerability in the schema, got: {dap:?}"
    );
    assert!(
        none.contains(&"Vulnerability".to_string()),
        "[scenario 3] channel=None disables the gate (build scripts / tests), got: {none:?}"
    );
    // The counts should differ by exactly one — the gated entity.
    assert_eq!(
        dap.len(),
        ea.len() + 1,
        "[scenario 3] only Vulnerability should differ; ExternalAgent seen: {ea:?}, DapInternal seen: {dap:?}"
    );
    println!("[scenario 3] PASS: schema-discovery omits Vulnerability for external_agent");
}

// -----------------------------------------------------------------------------
// Scenario 4: internal_only gates every agent-facing channel including DAP,
// while core_feature still passes. Also verifies the *group* form resolves
// the same as the equivalent raw-channel list.
// -----------------------------------------------------------------------------
#[test]
#[ignore]
fn scenario_4_internal_only_group_gates_dap_but_not_core() {
    let ont = ontology().clone().with_redaction_channels(
        "Project",
        ChannelAllowlist::from_entries(vec![ChannelAllowlistEntry::Group(
            ChannelGroup::InternalOnly,
        )]),
    );

    for (channel, should_gate) in [
        (Channel::ExternalAgent, true),
        (Channel::DapInternal, true),
        (Channel::Frontend, true),
        (Channel::CoreFeature, false),
    ] {
        let sql = compile_project_at(channel, &ont);
        let gated = has_bool_false_gate(&sql);
        println!(
            "[scenario 4 group] channel={channel:?} gated={gated} expected_gated={should_gate}"
        );
        assert_eq!(
            gated, should_gate,
            "[scenario 4] internal_only channel={channel:?}\n{sql}"
        );
    }

    // Raw-channel-list form should behave identically.
    let raw = ontology().clone().with_redaction_channels(
        "Project",
        ChannelAllowlist::from_entries(vec![ChannelAllowlistEntry::Channel(Channel::CoreFeature)]),
    );
    for channel in [
        Channel::ExternalAgent,
        Channel::DapInternal,
        Channel::Frontend,
    ] {
        let sql = compile_project_at(channel, &raw);
        assert!(
            has_bool_false_gate(&sql),
            "[scenario 4] raw [core_feature] must gate {channel:?}"
        );
    }
    let ok = compile_project_at(Channel::CoreFeature, &raw);
    assert!(
        !has_bool_false_gate(&ok),
        "[scenario 4] raw [core_feature] must admit CoreFeature"
    );

    println!("[scenario 4] PASS: internal_only group and raw [core_feature] behave identically");
}

// -----------------------------------------------------------------------------
// Scenario 5: named-query current_channel binding renders the caller's channel
// into the query without accepting a client-supplied override.
// -----------------------------------------------------------------------------
#[test]
#[ignore]
fn scenario_5_named_query_current_channel_binding_substitutes() {
    // NamedQuery::from_yaml is pub(crate), so route through the loader by
    // writing the template into a scratch dir.
    let tmp = std::env::temp_dir().join(format!("adr013-manual-nq-{}", std::process::id()));
    std::fs::create_dir_all(&tmp).expect("mkdir");
    std::fs::write(
        tmp.join("whoami.yaml"),
        r#"
name: whoami
description: Reflect the caller's channel back through the DSL.
bindings: [current_user_id, current_channel]
query:
  user_id: { $binding: current_user_id }
  channel: { $binding: current_channel }
"#,
    )
    .expect("write");

    let queries = named_queries::NamedQueries::load_from_dir(&tmp).expect("valid template");
    let q = queries.get("whoami").expect("template registered");

    for channel in ["external_agent", "dap_internal", "core_feature", "frontend"] {
        let values = named_queries::BindingValues {
            current_user_id: 42,
            current_channel: Some(channel.to_string()),
        };
        let rendered = q
            .render(&values, &Default::default())
            .expect("render should succeed with a channel present");
        println!("[scenario 5] channel={channel} → {rendered}");
        assert!(
            rendered.contains(&format!("\"channel\":\"{channel}\"")),
            "[scenario 5] rendered payload must contain the resolved channel, got: {rendered}"
        );
        assert!(
            rendered.contains("\"user_id\":42"),
            "[scenario 5] rendered payload must contain the resolved user id, got: {rendered}"
        );
    }

    // Without a channel present, a template that references current_channel
    // must fail cleanly rather than silently substituting a default.
    let values = named_queries::BindingValues {
        current_user_id: 42,
        current_channel: None,
    };
    let err = q
        .render(&values, &Default::default())
        .expect_err("render without channel must fail when template references it");
    println!("[scenario 5] no-channel error: {err}");
    assert!(
        err.to_string().contains("current_channel"),
        "[scenario 5] the error must name the missing binding, got: {err}"
    );

    let _ = std::fs::remove_dir_all(&tmp);
    println!(
        "[scenario 5] PASS: current_channel substitutes correctly, and its absence fails loudly"
    );
}

// -----------------------------------------------------------------------------
// Scenario 6: mixed allowlist (group + raw channel) resolves to the union.
// -----------------------------------------------------------------------------
#[test]
#[ignore]
fn scenario_6_mixed_group_and_raw_resolves_to_union() {
    let ont = ontology().clone().with_redaction_channels(
        "Project",
        ChannelAllowlist::from_entries(vec![
            ChannelAllowlistEntry::Group(ChannelGroup::InternalOnly), // → core_feature
            ChannelAllowlistEntry::Channel(Channel::DapInternal),
        ]),
    );

    for (channel, should_gate) in [
        (Channel::ExternalAgent, true),
        (Channel::Frontend, true),
        (Channel::CoreFeature, false),
        (Channel::DapInternal, false),
    ] {
        let sql = compile_project_at(channel, &ont);
        let gated = has_bool_false_gate(&sql);
        println!("[scenario 6] channel={channel:?} gated={gated} expected_gated={should_gate}");
        assert_eq!(
            gated, should_gate,
            "[scenario 6] union of {{internal_only, dap_internal}} misbehaved for {channel:?}"
        );
    }
    println!("[scenario 6] PASS: mixed allowlist admits union {{core_feature, dap_internal}}");
}
