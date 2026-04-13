use super::helpers::{process_fixture_file, setup_js_fixture_pipeline};
use crate::analysis::languages::js::{ImportedName, JsAnalyzer, JsCallTarget, JsImportKind};
use tracing_test::traced_test;

#[test]
fn cjs_require_bindings() {
    let processed = process_fixture_file("analysis/commonjs-require-bindings", "src/main.js");
    let analysis = processed.js_analysis.expect("should produce JS analysis");

    assert!(
        analysis.imports.iter().any(|i| {
            matches!(
                &i.kind,
                JsImportKind::CjsRequire {
                    imported_name: None
                }
            ) && i.local_name == "fs"
        }),
        "default require should produce CjsRequire with None"
    );
    assert!(
        analysis.imports.iter().any(|i| {
            matches!(&i.kind, JsImportKind::CjsRequire { imported_name: Some(n) } if n == "join")
                && i.local_name == "join"
        }),
        "destructured require should keep member name"
    );
    assert!(
        analysis.imports.iter().any(|i| {
            matches!(&i.kind, JsImportKind::CjsRequire { imported_name: Some(n) } if n == "resolve")
                && i.local_name == "presolve"
        }),
        "aliased require should preserve both names"
    );
}

#[test]
fn cjs_module_exports_default() {
    let analysis = JsAnalyzer::analyze_file(
        "function main() {}\nmodule.exports = main;",
        "test.js",
        "test.js",
    )
    .unwrap();
    assert_eq!(analysis.module_info.cjs_exports.len(), 1);
    assert!(matches!(
        &analysis.module_info.cjs_exports[0],
        crate::analysis::languages::js::CjsExport::Default { .. }
    ));
}

#[test]
fn cjs_exports_named() {
    let analysis = JsAnalyzer::analyze_file(
        "exports.foo = function() {};\nexports.bar = 42;",
        "test.js",
        "test.js",
    )
    .unwrap();
    assert_eq!(analysis.module_info.cjs_exports.len(), 2);
}

#[test]
fn cjs_module_exports_named() {
    let analysis = JsAnalyzer::analyze_file(
        "module.exports.helper = function() {};",
        "test.js",
        "test.js",
    )
    .unwrap();
    assert!(analysis.module_info.cjs_exports.iter().any(|e| matches!(
        e,
        crate::analysis::languages::js::CjsExport::Named { name, .. } if name == "helper"
    )));
}

#[test]
fn cjs_require_namespace_call_edge() {
    let analysis = JsAnalyzer::analyze_file(
        r#"var utils = require('./utils');
function run() { utils.doStuff(); }"#,
        "test.js",
        "test.js",
    )
    .unwrap();
    assert!(
        analysis.calls.iter().any(|c| matches!(
            &c.callee,
            JsCallTarget::ImportedCall {
                imported_call,
            } if imported_call.binding.specifier == "./utils"
                && imported_call.binding.imported_name == ImportedName::Named("doStuff".to_string())
                && imported_call.member_path.is_empty()
        )),
        "utils.doStuff() via require should produce ImportedCall"
    );
}

#[traced_test]
#[tokio::test]
async fn cjs_cross_file_imports_present() {
    let setup = setup_js_fixture_pipeline("cross-file/cjs-cross-file").await;
    let consumer_imports: Vec<_> = setup
        .graph_data
        .imported_symbol_nodes
        .iter()
        .filter(|n| n.location.file_path == "src/consumer.js")
        .collect();
    assert!(
        !consumer_imports.is_empty(),
        "consumer.js should have CJS imports"
    );
}

#[traced_test]
#[tokio::test]
async fn cjs_destructured_require_resolves_named_exports_to_definitions() {
    let setup = setup_js_fixture_pipeline("cross-file/cjs-local-require-resolution").await;
    let definition_targets = setup.imported_definition_targets_by_local_name_from("src/main.js");

    assert_eq!(
        definition_targets.get("ROOT_PATH"),
        Some(&vec![(
            "src/config.js".to_string(),
            "ROOT_PATH".to_string()
        )]),
        "destructured require should resolve named exports to their originating definitions"
    );
    assert_eq!(
        definition_targets.get("IS_EE"),
        Some(&vec![("src/config.js".to_string(), "IS_EE".to_string())]),
        "shorthand object exports should preserve definition-backed provenance"
    );
}

#[traced_test]
#[tokio::test]
async fn cjs_namespace_require_falls_back_to_module_file() {
    let setup = setup_js_fixture_pipeline("cross-file/cjs-local-require-resolution").await;
    let file_targets = setup.imported_file_targets_by_local_name_from("src/main.js");

    assert_eq!(
        file_targets.get("cfg"),
        Some(&vec!["src/config.js".to_string()]),
        "namespace-style require should keep file-backed provenance when the module exports an object surface"
    );
}
