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
fn callback_argument_guessed_confidence() {
    let analysis = process_fixture_file("cross-file/variable-and-static-calls", "src/consumer.ts");
    let js = analysis.js_analysis.expect("should produce JS analysis");
    assert!(
        js.calls.iter().any(|c| matches!(
            &c.callee,
            JsCallTarget::Direct { fqn, .. } if fqn == "transform"
        ) && c.confidence == JsCallConfidence::Guessed),
        "items.map(transform) should have Guessed confidence"
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
async fn cross_file_namespace_import() {
    let setup = setup_js_fixture_pipeline("cross-file/namespace-import-calls").await;
    let process_calls = setup.find_calls_from_method("process");
    assert!(process_calls.iter().any(|fqn| fqn == "validate"));
    assert!(process_calls.iter().any(|fqn| fqn == "normalize"));
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
