use gitalisk_core::repository::testing::local::LocalGitRepository;
use std::{
    fs,
    path::{Path, PathBuf},
};

use crate::analysis::types::GraphData;
use crate::graph::RelationshipType;
use crate::indexer::{IndexingConfig, RepositoryIndexer};
use crate::loading::DirectoryFileSource;
use crate::parsing::processor::{FileProcessingResult, FileProcessor, ProcessingResult};

fn init_js_fixture_repository(relative_fixture: &str) -> LocalGitRepository {
    let mut local_repo = LocalGitRepository::new(None);
    let fixtures_path = fixture_root(relative_fixture);
    local_repo.copy_dir(&fixtures_path);
    local_repo
        .add_all()
        .commit("Initial commit with JS fixture examples");
    local_repo
}

fn fixture_root(relative_fixture: &str) -> PathBuf {
    Path::new(env!("FIXTURES_DIR"))
        .join("code")
        .join("typescript")
        .join(relative_fixture)
}

fn fixture_file_path(relative_fixture: &str, file_path: &str) -> PathBuf {
    fixture_root(relative_fixture).join(file_path)
}

fn read_fixture_file(relative_fixture: &str, file_path: &str) -> String {
    fs::read_to_string(fixture_file_path(relative_fixture, file_path))
        .expect("Should read JS fixture file")
}

fn process_fixture_file(relative_fixture: &str, file_path: &str) -> Box<FileProcessingResult> {
    let source = read_fixture_file(relative_fixture, file_path);
    let result = FileProcessor::new(file_path.to_string(), &source).process();
    match result {
        ProcessingResult::Success(result) => result,
        ProcessingResult::Skipped(skipped) => {
            panic!(
                "Fixture {file_path} was skipped unexpectedly: {}",
                skipped.reason
            )
        }
        ProcessingResult::Error(error) => {
            panic!(
                "Fixture {file_path} failed unexpectedly: {}",
                error.error_message
            )
        }
    }
}

fn collect_discovered_paths(root_dir: &Path) -> Vec<String> {
    fn walk(current: &Path, root: &Path, paths: &mut Vec<String>) {
        let mut entries = fs::read_dir(current)
            .expect("Should read fixture directory")
            .map(|entry| entry.expect("Should read fixture dir entry"))
            .collect::<Vec<_>>();
        entries.sort_by_key(|entry| entry.path());

        for entry in entries {
            let path = entry.path();
            if path.is_dir() {
                walk(&path, root, paths);
            } else if path.is_file() {
                let relative = path
                    .strip_prefix(root)
                    .expect("Fixture path should be under root")
                    .to_string_lossy()
                    .replace('\\', "/");
                paths.push(relative);
            }
        }
    }

    let mut paths = Vec::new();
    walk(root_dir, root_dir, &mut paths);
    paths
}

pub struct JsFixtureTestSetup {
    pub _local_repo: LocalGitRepository,
    pub graph_data: GraphData,
}

impl JsFixtureTestSetup {
    fn get_definition_fqn_by_id(&self, id: u32) -> Option<String> {
        self.graph_data
            .definition_nodes
            .get(id as usize)
            .map(|node| node.fqn.to_string())
    }

    fn imported_definition_targets_from(&self, file_path: &str) -> Vec<(String, String)> {
        self.graph_data
            .relationships
            .iter()
            .filter(|rel| rel.relationship_type == RelationshipType::ImportedSymbolToDefinition)
            .filter(|rel| rel.source_path.as_ref().map(|p| p.as_ref().as_str()) == Some(file_path))
            .filter_map(|rel| {
                Some((
                    rel.target_path.as_ref()?.to_string(),
                    self.get_definition_fqn_by_id(rel.target_id?)?,
                ))
            })
            .collect()
    }

    fn find_calls_from_method(&self, method_fqn: &str) -> Vec<String> {
        self.graph_data
            .relationships
            .iter()
            .filter(|rel| rel.relationship_type == RelationshipType::Calls)
            .filter_map(|rel| {
                let source_fqn = self.get_definition_fqn_by_id(rel.source_id?)?;
                if source_fqn == method_fqn {
                    self.get_definition_fqn_by_id(rel.target_id?)
                } else {
                    None
                }
            })
            .collect()
    }
}

pub async fn setup_js_fixture_pipeline(relative_fixture: &str) -> JsFixtureTestSetup {
    let local_repo = init_js_fixture_repository(relative_fixture);
    let repo_path_str = local_repo.path.to_str().unwrap();

    let indexer = RepositoryIndexer::with_graph_identity(
        "js-fixture-test".to_string(),
        repo_path_str.to_string(),
        1,
        "main".to_string(),
    );
    let file_source = DirectoryFileSource::new(repo_path_str.to_string());

    let config = IndexingConfig {
        worker_threads: 1,
        max_file_size: 5_000_000,
        respect_gitignore: false,
    };

    let indexing_result = indexer
        .index_files(file_source, &config)
        .await
        .expect("Failed to index repository");
    let graph_data = indexing_result.graph_data.expect("Should have graph data");

    JsFixtureTestSetup {
        _local_repo: local_repo,
        graph_data,
    }
}

#[cfg(test)]
mod integration_tests {
    use super::{
        collect_discovered_paths, fixture_root, process_fixture_file, read_fixture_file,
        setup_js_fixture_pipeline,
    };
    use crate::analysis::languages::js::{
        ImportedName, JsCallConfidence, JsCallTarget, JsDirective, JsImportKind, detect_workspaces,
        extract_scripts, is_bun_project,
    };
    use tracing_test::traced_test;

    #[test]
    fn test_js_use_server_directive_uses_fixture_repo() {
        let processed = process_fixture_file("directives/use-server", "src/action.ts");
        assert_eq!(
            processed
                .js_analysis
                .expect("use-server fixture should produce JS analysis")
                .directive,
            Some(JsDirective::UseServer)
        );
    }

    #[test]
    fn test_js_use_client_directive_uses_fixture_repo() {
        let processed = process_fixture_file("directives/use-client", "src/page.tsx");
        assert_eq!(
            processed
                .js_analysis
                .expect("use-client fixture should produce JS analysis")
                .directive,
            Some(JsDirective::UseClient)
        );
    }

    #[test]
    fn test_js_non_framework_directive_uses_fixture_repo() {
        let processed = process_fixture_file("directives/no-directive", "src/value.ts");
        assert_eq!(
            processed
                .js_analysis
                .expect("no-directive fixture should produce JS analysis")
                .directive,
            None
        );
    }

    #[test]
    fn test_js_default_export_binding_uses_fixture_repo() {
        let processed = process_fixture_file("analysis/default-export-class", "src/main.ts");
        let analysis = processed
            .js_analysis
            .expect("default-export fixture should produce JS analysis");
        let bar = analysis
            .defs
            .iter()
            .find(|def| def.fqn == "Bar")
            .expect("Should extract Bar definition");
        let default_binding = analysis
            .module_info
            .exports
            .get("default")
            .expect("Should track default export binding");

        assert_eq!(default_binding.definition_range, Some(bar.range));
    }

    #[test]
    fn test_js_typed_variable_uses_fixture_repo() {
        let processed = process_fixture_file("analysis/typed-variable", "src/main.ts");
        let analysis = processed
            .js_analysis
            .expect("typed-variable fixture should produce JS analysis");
        let x = analysis
            .defs
            .iter()
            .find(|def| def.name == "x")
            .expect("Should extract typed variable");

        assert_eq!(x.type_annotation.as_deref(), Some("string"));
    }

    #[test]
    fn test_js_jsx_component_call_uses_fixture_repo() {
        let processed = process_fixture_file("analysis/jsx-component-call", "src/main.tsx");
        let analysis = processed
            .js_analysis
            .expect("jsx-component fixture should produce JS analysis");

        assert!(
            analysis.calls.iter().any(|call| matches!(
                &call.callee,
                JsCallTarget::Direct { fqn, .. } if fqn == "Button"
            )),
            "JSX fixture should create a call edge to Button"
        );
    }

    #[test]
    fn test_js_commonjs_require_bindings_use_fixture_repo() {
        let processed = process_fixture_file("analysis/commonjs-require-bindings", "src/main.js");
        let analysis = processed
            .js_analysis
            .expect("CommonJS fixture should produce JS analysis");

        assert!(
            analysis.defs.iter().any(|def| def.name == "fs"),
            "Should keep default require binding"
        );
        assert!(
            analysis.imports.iter().any(|import| {
                matches!(
                    &import.kind,
                    JsImportKind::CjsRequire {
                        imported_name: None
                    }
                ) && import.local_name == "fs"
            }),
            "Should represent default require as a CJS import"
        );
        assert!(
            analysis.imports.iter().any(|import| {
                matches!(
                    &import.kind,
                    JsImportKind::CjsRequire { imported_name: Some(name) } if name == "join"
                ) && import.local_name == "join"
            }),
            "Should keep destructured require member names"
        );
        assert!(
            analysis.imports.iter().any(|import| {
                matches!(
                    &import.kind,
                    JsImportKind::CjsRequire { imported_name: Some(name) } if name == "resolve"
                ) && import.local_name == "presolve"
            }),
            "Should keep aliased require member names"
        );
    }

    #[test]
    fn test_js_vue_sfc_uses_fixture_repo() {
        let processed = process_fixture_file("sfc/vue-merged-scripts", "src/App.vue");
        let analysis = processed
            .js_analysis
            .expect("Vue fixture should produce JS analysis");

        assert!(
            analysis.module_info.exports.contains_key("serverOnly"),
            "Vue fixture should preserve exports from the regular script block"
        );
        assert!(
            analysis
                .module_info
                .imports
                .iter()
                .any(|import| import.specifier == "vue"),
            "Vue fixture should merge setup-script imports"
        );
    }

    #[test]
    fn test_js_svelte_sfc_uses_fixture_repo() {
        let processed = process_fixture_file("sfc/svelte-module-instance", "src/Widget.svelte");
        let analysis = processed
            .js_analysis
            .expect("Svelte fixture should produce JS analysis");

        assert!(
            analysis.defs.iter().any(|def| def.name == "prerender"),
            "Svelte fixture should include module script definitions"
        );
        assert!(
            analysis
                .imports
                .iter()
                .any(|import| import.specifier == "svelte/store"),
            "Svelte fixture should include instance script imports"
        );
    }

    #[test]
    fn test_js_astro_frontmatter_uses_fixture_repo() {
        let astro = read_fixture_file("sfc/astro-frontmatter", "src/Page.astro");
        let blocks = extract_scripts(&astro, "astro");

        assert_eq!(
            blocks.len(),
            2,
            "Astro fixture should expose frontmatter and script"
        );
        assert!(
            blocks[0].source_text.contains("const title = \"Hello\""),
            "Astro fixture should keep frontmatter content"
        );
    }

    #[test]
    fn test_js_pnpm_workspace_fixture_repo() {
        let root = fixture_root("workspace-cases/pnpm");
        let paths = collect_discovered_paths(&root);
        let packages = detect_workspaces(&root, &paths);

        assert_eq!(packages.len(), 2);
        let core = packages
            .iter()
            .find(|pkg| pkg.name == "@myapp/core")
            .expect("Should detect @myapp/core package");
        assert_eq!(core.version.as_deref(), Some("1.0.0"));
        assert_eq!(core.path, "packages/core");
    }

    #[test]
    fn test_js_package_array_workspace_fixture_repo() {
        let root = fixture_root("workspace-cases/package-array");
        let paths = collect_discovered_paths(&root);
        assert_eq!(detect_workspaces(&root, &paths).len(), 2);
    }

    #[test]
    fn test_js_package_object_workspace_fixture_repo() {
        let root = fixture_root("workspace-cases/package-object");
        let paths = collect_discovered_paths(&root);
        assert_eq!(detect_workspaces(&root, &paths).len(), 2);
    }

    #[test]
    fn test_js_pnpm_priority_workspace_fixture_repo() {
        let root = fixture_root("workspace-cases/pnpm-priority");
        let paths = collect_discovered_paths(&root);
        let packages = detect_workspaces(&root, &paths);

        assert_eq!(packages.len(), 1);
        assert_eq!(packages[0].name, "@myapp/web");
    }

    #[test]
    fn test_js_empty_workspace_fixture_repo() {
        let root = fixture_root("workspace-cases/none");
        let paths = collect_discovered_paths(&root);
        assert!(detect_workspaces(&root, &paths).is_empty());
    }

    #[test]
    fn test_js_bun_lock_fixture_repo() {
        let root = fixture_root("workspace-cases/bun-lock");
        let paths = collect_discovered_paths(&root);
        assert!(is_bun_project(&root, &paths));
    }

    #[test]
    fn test_js_bunfig_fixture_repo() {
        let root = fixture_root("workspace-cases/bunfig");
        let paths = collect_discovered_paths(&root);
        assert!(is_bun_project(&root, &paths));
    }

    #[test]
    fn test_js_types_bun_fixture_repo() {
        let root = fixture_root("workspace-cases/types-bun");
        let paths = collect_discovered_paths(&root);
        assert!(is_bun_project(&root, &paths));
    }

    #[traced_test]
    #[tokio::test]
    async fn test_js_reexport_resolution_uses_fixture_repo() {
        let setup = setup_js_fixture_pipeline("cross-file/reexport-resolution").await;
        let import_targets = setup.imported_definition_targets_from("src/consumer.ts");
        let imported_calls =
            process_fixture_file("cross-file/reexport-resolution", "src/consumer.ts");
        let imported_call_analysis = imported_calls
            .js_analysis
            .expect("consumer.ts should produce JS analysis");
        let calls = setup.find_calls_from_method("run");

        assert!(
            import_targets
                .iter()
                .any(|(path, fqn)| path == "src/direct.ts" && fqn == "normalize"),
            "Named import through a re-export should resolve to the originating definition"
        );
        assert!(
            imported_call_analysis.calls.iter().any(|call| matches!(
                &call.callee,
                JsCallTarget::ImportedCall {
                    local_name,
                    specifier,
                    imported_name: ImportedName::Named(name),
                } if local_name == "normalize" && specifier == "./reexports" && name == "normalize"
            )),
            "consumer.ts should emit ImportedCall edges for named imports"
        );
        assert!(
            calls.iter().any(|fqn| fqn == "normalize"),
            "run should call normalize across files through a re-export"
        );
    }

    #[traced_test]
    #[tokio::test]
    async fn test_js_default_import_resolution_uses_fixture_repo() {
        let setup = setup_js_fixture_pipeline("cross-file/default-import-resolution").await;
        let import_targets = setup.imported_definition_targets_from("src/consumer.ts");
        let imported_calls =
            process_fixture_file("cross-file/default-import-resolution", "src/consumer.ts");
        let imported_call_analysis = imported_calls
            .js_analysis
            .expect("consumer.ts should produce JS analysis");
        let calls = setup.find_calls_from_method("run");

        assert!(
            import_targets
                .iter()
                .any(|(path, fqn)| path == "src/default_formatter.ts" && fqn == "defaultFormat"),
            "Default import should resolve to the exported definition"
        );
        assert!(
            imported_call_analysis.calls.iter().any(|call| matches!(
                &call.callee,
                JsCallTarget::ImportedCall {
                    local_name,
                    imported_name: ImportedName::Default,
                    ..
                } if local_name == "defaultFormat"
            )),
            "consumer.ts should emit ImportedCall edges for default imports"
        );
        assert!(
            calls.iter().any(|fqn| fqn == "defaultFormat"),
            "run should call a default-imported function across files"
        );
    }

    #[traced_test]
    #[tokio::test]
    async fn test_js_namespace_import_calls_use_fixture_repo() {
        let setup = setup_js_fixture_pipeline("cross-file/namespace-import-calls").await;
        let imported_calls =
            process_fixture_file("cross-file/namespace-import-calls", "src/consumer.ts");
        let analysis = imported_calls
            .js_analysis
            .expect("consumer.ts should produce JS analysis");

        // Verify namespace member calls produce ImportedCall edges with Named(method_name)
        assert!(
            analysis.calls.iter().any(|call| matches!(
                &call.callee,
                JsCallTarget::ImportedCall {
                    specifier,
                    imported_name: ImportedName::Named(name),
                    ..
                } if specifier == "./utils" && name == "validate"
            )),
            "utils.validate() should emit ImportedCall with Named('validate')"
        );
        assert!(
            analysis.calls.iter().any(|call| matches!(
                &call.callee,
                JsCallTarget::ImportedCall {
                    specifier,
                    imported_name: ImportedName::Named(name),
                    ..
                } if specifier == "./utils" && name == "normalize"
            )),
            "utils.normalize() should emit ImportedCall with Named('normalize')"
        );

        // Verify cross-file CALLS edges resolve to the target definitions
        let process_calls = setup.find_calls_from_method("process");
        assert!(
            process_calls.iter().any(|fqn| fqn == "validate"),
            "process should call validate across files via namespace import"
        );
        assert!(
            process_calls.iter().any(|fqn| fqn == "normalize"),
            "process should call normalize across files via namespace import"
        );
    }

    #[traced_test]
    #[tokio::test]
    async fn test_js_variable_and_static_calls_use_fixture_repo() {
        let analysis =
            process_fixture_file("cross-file/variable-and-static-calls", "src/consumer.ts");
        let js = analysis
            .js_analysis
            .expect("consumer.ts should produce JS analysis");

        // P1: const p = new Parser(); p.parse(input) → Parser::parse
        assert!(
            js.calls.iter().any(|call| matches!(
                &call.callee,
                JsCallTarget::Direct { fqn, .. } if fqn == "Parser::parse"
            ) && call.confidence == JsCallConfidence::Inferred),
            "p.parse() should resolve to Parser::parse with Inferred confidence"
        );

        // P3: Parser.fromConfig("default") → Parser::fromConfig
        assert!(
            js.calls.iter().any(|call| matches!(
                &call.callee,
                JsCallTarget::Direct { fqn, .. } if fqn == "Parser::fromConfig"
            ) && call.confidence == JsCallConfidence::Known),
            "Parser.fromConfig() should resolve to Parser::fromConfig with Known confidence"
        );

        // P2: function runWithService(svc: Parser) { svc.parse(...) } → Parser::parse
        assert!(
            js.calls.iter().any(|call| matches!(
                &call.callee,
                JsCallTarget::Direct { fqn, .. } if fqn == "Parser::parse"
            ) && call.confidence == JsCallConfidence::Annotated),
            "svc.parse() with typed param should resolve to Parser::parse with Annotated confidence"
        );

        // P4: items.map(transform) → transform is called as callback
        assert!(
            js.calls.iter().any(|call| matches!(
                &call.callee,
                JsCallTarget::Direct { fqn, .. } if fqn == "transform"
            ) && call.confidence == JsCallConfidence::Guessed),
            "items.map(transform) should produce a Guessed call edge to transform"
        );
    }

    #[traced_test]
    #[tokio::test]
    async fn test_js_inheritance_calls_use_fixture_repo() {
        let setup = setup_js_fixture_pipeline("cross-file/inheritance-calls").await;

        let child_calls = setup.find_calls_from_method("Child::run");
        assert!(
            child_calls.iter().any(|fqn| fqn == "Base::helper"),
            "Child::run should resolve this.helper() to Base::helper"
        );

        let dog_calls = setup.find_calls_from_method("Dog::speak");
        assert!(
            dog_calls.iter().any(|fqn| fqn == "Animal::speak"),
            "Dog::speak should resolve super.speak() to Animal::speak"
        );
    }

    #[traced_test]
    #[tokio::test]
    async fn test_js_definition_ids_are_unique_per_file_in_fixture_repo() {
        let setup = setup_js_fixture_pipeline("cross-file/duplicate-definition-ids").await;
        let foo_defs: Vec<_> = setup
            .graph_data
            .definition_nodes
            .iter()
            .filter(|node| node.fqn.to_string() == "foo")
            .collect();

        assert_eq!(
            foo_defs.len(),
            2,
            "Fixture should include duplicate top-level names"
        );
        assert_ne!(foo_defs[0].id, foo_defs[1].id);
    }
}
