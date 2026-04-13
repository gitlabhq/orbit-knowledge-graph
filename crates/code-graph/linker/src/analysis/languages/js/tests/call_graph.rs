use super::helpers::{process_fixture_file, setup_js_fixture_pipeline};
use crate::analysis::languages::js::{JsAnalyzer, JsCallConfidence, JsCallTarget};
use tracing_test::traced_test;

#[test]
fn jsx_component_creates_call_edge() {
    let processed = process_fixture_file("analysis/jsx-component-call", "src/main.tsx");
    let analysis = processed.js_analysis.expect("should produce JS analysis");
    assert!(
        analysis.calls.iter().any(|c| matches!(
            &c.callee,
            JsCallTarget::Direct { fqn, .. } if fqn == "Button"
        )),
        "JSX <Button /> should create a call edge"
    );
}

#[test]
fn direct_function_call() {
    let analysis = JsAnalyzer::analyze_file(
        "function foo() {}\nfunction bar() { foo(); }",
        "test.ts",
        "test.ts",
    )
    .unwrap();
    assert!(analysis.calls.iter().any(|c| matches!(
        &c.callee,
        JsCallTarget::Direct { fqn, .. } if fqn == "foo"
    ) && c.confidence == JsCallConfidence::Known));
}

#[test]
fn function_valued_variable_creates_call_edge() {
    let analysis = JsAnalyzer::analyze_file(
        "const foo = () => {};\nfunction bar() { foo(); }",
        "test.ts",
        "test.ts",
    )
    .unwrap();
    assert!(analysis.calls.iter().any(|c| matches!(
        &c.callee,
        JsCallTarget::Direct { fqn, .. } if fqn == "foo"
    )));
}

#[test]
fn direct_call_to_const_is_not_emitted() {
    let analysis = JsAnalyzer::analyze_file(
        "const HTTP_STATUS_OK = 200;\nfunction f() { HTTP_STATUS_OK(); }",
        "test.ts",
        "test.ts",
    )
    .unwrap();
    assert!(!analysis.calls.iter().any(|c| matches!(
        &c.callee,
        JsCallTarget::Direct { fqn, .. } if fqn == "HTTP_STATUS_OK"
    )));
}

#[test]
fn new_expression_creates_call_edge() {
    let analysis = JsAnalyzer::analyze_file(
        "class Foo {}\nfunction bar() { new Foo(); }",
        "test.ts",
        "test.ts",
    )
    .unwrap();
    assert!(analysis.calls.iter().any(|c| matches!(
        &c.callee,
        JsCallTarget::Direct { fqn, .. } if fqn == "Foo"
    )));
}

#[test]
fn this_method_call_resolution() {
    let analysis = JsAnalyzer::analyze_file(
        r#"
class Widget {
    render() { return "hi"; }
    init() { this.render(); }
}
"#,
        "test.ts",
        "test.ts",
    )
    .unwrap();
    assert!(analysis.calls.iter().any(|c| matches!(
        &c.callee,
        JsCallTarget::ThisMethod { method_name, resolved_fqn: Some(fqn), .. }
        if method_name == "render" && fqn == "Widget::render"
    )));
}

#[test]
fn super_method_call_resolution() {
    let analysis = JsAnalyzer::analyze_file(
        r#"
class Base { speak() {} }
class Child extends Base { talk() { super.speak(); } }
"#,
        "test.ts",
        "test.ts",
    )
    .unwrap();
    assert!(analysis.calls.iter().any(|c| matches!(
        &c.callee,
        JsCallTarget::SuperMethod { method_name, resolved_fqn: Some(fqn), .. }
        if method_name == "speak" && fqn == "Base::speak"
    )));
}

#[test]
fn variable_method_call_inferred_confidence() {
    let analysis = process_fixture_file("cross-file/variable-and-static-calls", "src/consumer.ts");
    let js = analysis.js_analysis.expect("should produce JS analysis");
    assert!(
        js.calls.iter().any(|c| matches!(
            &c.callee,
            JsCallTarget::Direct { fqn, .. } if fqn == "Parser::parse"
        ) && c.confidence == JsCallConfidence::Inferred),
        "p.parse() should resolve with Inferred confidence"
    );
}

#[test]
fn static_method_call_known_confidence() {
    let analysis = process_fixture_file("cross-file/variable-and-static-calls", "src/consumer.ts");
    let js = analysis.js_analysis.expect("should produce JS analysis");
    assert!(
        js.calls.iter().any(|c| matches!(
            &c.callee,
            JsCallTarget::Direct { fqn, .. } if fqn == "Parser::fromConfig"
        ) && c.confidence == JsCallConfidence::Known),
        "Parser.fromConfig() should resolve with Known confidence"
    );
}

#[test]
fn param_type_method_call_annotated_confidence() {
    let analysis = process_fixture_file("cross-file/variable-and-static-calls", "src/consumer.ts");
    let js = analysis.js_analysis.expect("should produce JS analysis");
    assert!(
        js.calls.iter().any(|c| matches!(
            &c.callee,
            JsCallTarget::Direct { fqn, .. } if fqn == "Parser::parse"
        ) && c.confidence == JsCallConfidence::Annotated),
        "svc.parse() with type annotation should have Annotated confidence"
    );
}

#[test]
fn callback_argument_does_not_emit_call_edge() {
    let analysis = JsAnalyzer::analyze_file(
        r#"
function transform(value) { return value; }
const items = [];
items.map(transform);
"#,
        "test.ts",
        "test.ts",
    )
    .unwrap();
    assert!(
        !analysis.calls.iter().any(|c| matches!(
            &c.callee,
            JsCallTarget::Direct { fqn, .. } if fqn == "transform"
        )),
        "passing transform as an argument should not create a CALLS edge"
    );
}

#[test]
fn member_property_reads_inside_arguments_do_not_emit_member_calls() {
    let analysis = JsAnalyzer::analyze_file(
        r#"
class TOCHeading {
  text() { return 'text'; }
  href() { return 'href'; }
}

function addSubHeading(text, href) {
  return `${text}:${href}`;
}

function toTree(heading: TOCHeading) {
  return addSubHeading(heading.text, heading.href);
}
"#,
        "test.ts",
        "test.ts",
    )
    .unwrap();

    assert!(
        analysis.calls.iter().any(|c| matches!(
            &c.callee,
            JsCallTarget::Direct { fqn, .. } if fqn == "addSubHeading"
        )),
        "the outer function call should still be recorded"
    );
    assert!(
        !analysis.calls.iter().any(|c| matches!(
            &c.callee,
            JsCallTarget::Direct { fqn, .. } if fqn == "TOCHeading::text" || fqn == "TOCHeading::href"
        )),
        "property reads used as call arguments must not be treated as member calls"
    );
}

#[test]
fn local_function_valued_variables_keep_scoped_call_targets() {
    let analysis = JsAnalyzer::analyze_file(
        r#"
class Widget {
  render() {
    const helper = () => 'ok';
    return helper();
  }
}
"#,
        "test.ts",
        "test.ts",
    )
    .unwrap();

    assert!(
        analysis.calls.iter().any(|c| matches!(
            &c.callee,
            JsCallTarget::Direct { fqn, .. } if fqn.starts_with("Widget::render::helper@")
        )),
        "calls to local function-valued variables should resolve to the scoped local definition"
    );
}

#[test]
fn local_function_valued_variables_keep_scoped_call_owners() {
    let analysis = JsAnalyzer::analyze_file(
        r#"
function utility() { return 'ok'; }

class Widget {
  render() {
    const helper = () => utility();
    return helper();
  }
}
"#,
        "test.ts",
        "test.ts",
    )
    .unwrap();

    assert!(
        analysis.calls.iter().any(|c| {
            matches!(
                &c.caller,
                crate::analysis::languages::js::JsCallSite::Definition { fqn, .. }
                if fqn.starts_with("Widget::render::helper@")
            ) && matches!(
                &c.callee,
                JsCallTarget::Direct { fqn, .. } if fqn == "utility"
            )
        }),
        "calls inside local function-valued variables should keep the local function as the caller"
    );
}

#[test]
fn tagged_template_creates_call_edge() {
    let analysis = JsAnalyzer::analyze_file(
        r#"function html(strings: TemplateStringsArray) { return ""; }
const x = html`<div/>`;
"#,
        "test.ts",
        "test.ts",
    )
    .unwrap();
    assert!(analysis.calls.iter().any(|c| matches!(
        &c.callee,
        JsCallTarget::Direct { fqn, .. } if fqn == "html"
    )));
}

#[test]
fn inherited_method_walks_hierarchy() {
    let analysis = JsAnalyzer::analyze_file(
        r#"
class A { helper() {} }
class B extends A {}
class C extends B { run() { this.helper(); } }
"#,
        "test.ts",
        "test.ts",
    )
    .unwrap();
    assert!(
        analysis.calls.iter().any(|c| matches!(
            &c.callee,
            JsCallTarget::ThisMethod { resolved_fqn: Some(fqn), .. } if fqn == "A::helper"
        )),
        "should walk A <- B <- C hierarchy to find A::helper"
    );
}

#[test]
fn module_level_call_site() {
    let analysis =
        JsAnalyzer::analyze_file("function setup() {}\nsetup();", "test.ts", "test.ts").unwrap();
    assert!(analysis.calls.iter().any(|c| matches!(
        &c.caller,
        crate::analysis::languages::js::JsCallSite::ModuleLevel
    ) && matches!(&c.callee, JsCallTarget::Direct { fqn, .. } if fqn == "setup")));
}

#[traced_test]
#[tokio::test]
async fn cross_file_reexport_chain() {
    let setup = setup_js_fixture_pipeline("cross-file/reexport-resolution").await;
    let import_targets = setup.imported_definition_targets_from("src/consumer.ts");
    let calls = setup.find_calls_from_method("run");

    assert!(
        import_targets
            .iter()
            .any(|(path, fqn)| path == "src/direct.ts" && fqn == "normalize"),
        "import should resolve through re-export to originating definition"
    );
    assert!(
        calls.iter().any(|fqn| fqn == "normalize"),
        "run should call normalize across files through a re-export"
    );
}

#[traced_test]
#[tokio::test]
async fn cross_file_multi_import_statement_preserves_symbol_identity() {
    let setup = setup_js_fixture_pipeline("cross-file/import-statement-identity").await;
    let import_targets = setup.imported_definition_targets_by_local_name_from("src/consumer.ts");

    assert_eq!(
        import_targets.get("dateFormats"),
        Some(&vec![(
            "src/constants.ts".to_string(),
            "dateFormats".to_string()
        )]),
    );
    assert_eq!(
        import_targets.get("metrics"),
        Some(&vec![(
            "src/constants.ts".to_string(),
            "FLOW_METRICS".to_string()
        )]),
    );
    assert_eq!(
        import_targets.get("formatPrecision"),
        Some(&vec![(
            "src/constants.ts".to_string(),
            "formatPrecision".to_string()
        )]),
    );
}

#[traced_test]
#[tokio::test]
async fn cross_file_default_import() {
    let setup = setup_js_fixture_pipeline("cross-file/default-import-resolution").await;
    let import_targets = setup.imported_definition_targets_from("src/consumer.ts");
    let calls = setup.find_calls_from_method("run");

    assert!(
        import_targets
            .iter()
            .any(|(path, fqn)| path == "src/default_formatter.ts" && fqn == "defaultFormat"),
    );
    assert!(calls.iter().any(|fqn| fqn == "defaultFormat"));
}

#[traced_test]
#[tokio::test]
async fn cross_file_import_provenance_survives_rebinding() {
    let setup = setup_js_fixture_pipeline("cross-file/import-provenance-calls").await;
    let import_targets = setup.imported_definition_targets_from("src/consumer.ts");

    assert!(
        import_targets
            .iter()
            .any(|(path, fqn)| path == "src/api.ts" && fqn == "api"),
        "default and named object imports should resolve to the exported object binding"
    );
    assert!(
        import_targets
            .iter()
            .any(|(path, fqn)| path == "src/api.ts" && fqn == "fetchData"),
        "direct function imports should resolve to their exported definition"
    );
    assert!(
        setup
            .find_calls_from_method("runDefault")
            .iter()
            .any(|fqn| fqn == "fetchData")
    );
    assert!(
        setup
            .find_calls_from_method("runNamed")
            .iter()
            .any(|fqn| fqn == "fetchData")
    );
    assert!(
        setup
            .find_calls_from_method("runAliasObject")
            .iter()
            .any(|fqn| fqn == "fetchData")
    );
    assert!(
        setup
            .find_calls_from_method("runAliasFunction")
            .iter()
            .any(|fqn| fqn == "fetchData")
    );
}

#[traced_test]
#[tokio::test]
async fn cross_file_namespace_import() {
    let setup = setup_js_fixture_pipeline("cross-file/namespace-import-calls").await;
    let import_targets = setup.imported_file_targets_by_local_name_from("src/consumer.ts");
    let process_calls = setup.find_calls_from_method("process");
    assert_eq!(
        import_targets.get("utils"),
        Some(&vec!["src/utils.ts".to_string()]),
        "namespace imports should retain file-level provenance"
    );
    assert!(process_calls.iter().any(|fqn| fqn == "validate"));
    assert!(process_calls.iter().any(|fqn| fqn == "normalize"));
}

#[traced_test]
#[tokio::test]
async fn cross_file_graphql_import_resolves_to_file() {
    let setup = setup_js_fixture_pipeline("cross-file/graphql-import-resolution").await;
    let import_targets = setup.imported_file_targets_by_local_name_from("src/consumer.ts");

    assert_eq!(
        import_targets.get("viewerQuery"),
        Some(&vec!["src/query.graphql".to_string()]),
        "GraphQL document imports should resolve to the underlying file"
    );
    assert!(
        setup
            .graph_data
            .file_nodes
            .iter()
            .any(|file| file.path == "src/query.graphql" && file.language == "GraphQL"),
        "GraphQL files should be indexed as file nodes"
    );
}

#[traced_test]
#[tokio::test]
async fn cross_file_namespace_reexport_member_chain() {
    let setup = setup_js_fixture_pipeline("cross-file/namespace-reexport-member-chain").await;
    let import_targets = setup.imported_file_targets_by_local_name_from("src/consumer.ts");
    let calls = setup.find_calls_from_method("run");

    assert_eq!(
        import_targets.get("extensions"),
        Some(&vec!["src/barrel.ts".to_string()]),
        "namespace import should resolve to the barrel file"
    );
    assert!(
        calls.iter().any(|fqn| fqn == "Suggestions::configure"),
        "extensions.Suggestions.configure() should resolve through the barrel to the originating static method"
    );
}

#[traced_test]
#[tokio::test]
async fn cross_file_imported_constants_are_not_called() {
    let setup = setup_js_fixture_pipeline("cross-file/imported-constants-not-called").await;
    let file_calls = setup.find_calls_from_file("src/consumer.ts");
    let run_calls = setup.find_calls_from_method("run");
    let attempt_calls = setup.find_calls_from_method("attempt");

    assert!(
        !file_calls.iter().any(|fqn| fqn == "HTTP_STATUS_OK"),
        "module-level argument passing should not create a file-to-definition CALLS edge to constants"
    );
    assert!(
        run_calls.iter().any(|fqn| fqn == "formatter"),
        "direct imported function calls should still resolve across files"
    );
    assert!(
        !attempt_calls.iter().any(|fqn| fqn == "HTTP_STATUS_OK"),
        "imported constants invoked as functions should not resolve as cross-file CALLS"
    );
}

#[traced_test]
#[tokio::test]
async fn cross_file_reexport_default_and_namespace_calls() {
    let setup = setup_js_fixture_pipeline("cross-file/reexport-default-and-namespace").await;
    let import_targets = setup.imported_definition_targets_from("src/consumer.ts");

    assert!(
        import_targets
            .iter()
            .any(|(path, fqn)| path == "src/direct.ts" && fqn == "defaultFormat"),
        "default re-exports should resolve to the originating default export"
    );
    assert!(
        setup
            .find_calls_from_method("runFormat")
            .iter()
            .any(|fqn| fqn == "defaultFormat")
    );
    assert!(
        setup
            .find_calls_from_method("runToolkit")
            .iter()
            .any(|fqn| fqn == "normalize")
    );
}

#[traced_test]
#[tokio::test]
async fn cross_file_conditional_exports_respect_import_mode() {
    let setup = setup_js_fixture_pipeline("cross-file/conditional-exports-resolution").await;
    let import_targets = setup.imported_definition_targets_from("src/import-consumer.ts");
    let require_targets = setup.imported_definition_targets_from("src/require-consumer.js");

    assert!(
        import_targets
            .iter()
            .any(|(path, fqn)| path == "node_modules/dual-pkg/esm.js" && fqn == "loadImport"),
        "ESM imports should resolve through the import export condition"
    );
    assert!(
        require_targets
            .iter()
            .any(|(path, fqn)| path == "node_modules/dual-pkg/cjs.cjs" && fqn == "loadRequire"),
        "require() should resolve through the require export condition"
    );
    assert!(
        setup
            .find_calls_from_method("runImport")
            .iter()
            .any(|fqn| fqn == "loadImport")
    );
    assert!(
        setup
            .find_calls_from_method("runRequire")
            .iter()
            .any(|fqn| fqn == "loadRequire")
    );
}

#[traced_test]
#[tokio::test]
async fn cross_file_inheritance() {
    let setup = setup_js_fixture_pipeline("cross-file/inheritance-calls").await;

    let child_calls = setup.find_calls_from_method("Child::run");
    assert!(child_calls.iter().any(|fqn| fqn == "Base::helper"));

    let dog_calls = setup.find_calls_from_method("Dog::speak");
    assert!(dog_calls.iter().any(|fqn| fqn == "Animal::speak"));
}

#[traced_test]
#[tokio::test]
async fn webpack_config_alias_resolution_prefers_project_alias_target() {
    let setup = setup_js_fixture_pipeline("cross-file/webpack-config-alias-resolution").await;
    let import_targets =
        setup.imported_definition_targets_from("app/assets/javascripts/consumer.ts");
    let calls = setup.find_calls_from_method("run");

    assert!(
        import_targets
            .iter()
            .any(|(path, fqn)| path == "ee/app/assets/javascripts/utils.ts" && fqn == "normalize"),
        "import should resolve through webpack alias config to the EE target"
    );
    assert!(
        calls.iter().any(|fqn| fqn == "normalize"),
        "run should call normalize across files through webpack alias resolution"
    );
}

#[traced_test]
#[tokio::test]
async fn definition_ids_unique_per_file() {
    let setup = setup_js_fixture_pipeline("cross-file/duplicate-definition-ids").await;
    let foo_defs: Vec<_> = setup
        .graph_data
        .definition_nodes
        .iter()
        .filter(|n| n.fqn.to_string() == "foo")
        .collect();
    assert_eq!(foo_defs.len(), 2);
    assert_ne!(foo_defs[0].id, foo_defs[1].id);
}
