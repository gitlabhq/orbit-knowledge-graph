use std::path::{Component, Path, PathBuf};

use crate::utils::Range as SourceRange;
use crate::v2::config::Language;
use crate::v2::types::{
    CanonicalDefinition, CanonicalImport, DefKind, DefinitionMetadata, Fqn, ImportBindingKind,
    ImportMode, Position as GraphPosition, Range as GraphRange,
};
use rayon::prelude::*;
use rustc_hash::{FxHashMap, FxHashSet};

use crate::v2::pipeline::PipelineError;

use super::{
    CjsExport, ExportedBinding, ImportedName, JsAnalyzer, JsDef, JsDefKind, JsExportName,
    JsFileAnalysis, JsImport, JsImportKind, JsModuleBindingInput, JsModuleBindingTargetInput,
    JsModuleInfo, JsPhase1File, JsStarReexport,
};

#[derive(Debug, Clone)]
pub struct AnalyzedJsFile {
    pub relative_path: String,
    pub analysis: JsFileAnalysis,
    pub phase1: JsPhase1File,
}

#[derive(Debug, Clone)]
pub struct ResolvedJsFile {
    pub relative_path: String,
    pub analysis: JsFileAnalysis,
}

pub fn analyze_files(
    files: &[String],
    root_path: &str,
) -> (Vec<AnalyzedJsFile>, Vec<PipelineError>) {
    // `catch_unwind` isolates per-file panics: a malformed input that trips
    // an OXC invariant takes down that file's analysis, not the pipeline.
    let results: Vec<_> = files
        .par_iter()
        .map(|relative_path| {
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                analyze_file(relative_path, root_path)
            }))
            .unwrap_or_else(|panic_payload| {
                let message = panic_message(&panic_payload);
                Err(PipelineError {
                    file_path: relative_path.clone(),
                    error: format!("panic during analysis: {message}"),
                })
            })
        })
        .collect();

    let mut analyzed = Vec::with_capacity(results.len());
    let mut errors = Vec::new();

    for result in results {
        match result {
            Ok(file) => analyzed.push(file),
            Err(error) => errors.push(error),
        }
    }

    (analyzed, errors)
}

/// Reject relative paths that could escape the repo clone.
///
/// A malicious repository is *not* expected to produce absolute paths or `..`
/// components through the walker, but we defend in depth: the symlink check
/// also catches a committed `link -> /etc/...` whose target the walker would
/// happily hand us as a "file".
fn safe_repo_join(root_path: &str, relative_path: &str) -> Result<PathBuf, String> {
    if relative_path.contains('\0') || relative_path.contains('\n') {
        return Err("relative path contains NUL or newline".to_string());
    }
    let input = Path::new(relative_path);
    // Callers sometimes hand us absolute paths that already live under
    // `root_path` (the v2 pipeline walker and the integration-test
    // harness both do). Strip the root so the rest of the check sees a
    // clean relative form; anything else is refused.
    let rel = if input.is_absolute() {
        input.strip_prefix(root_path).map_err(|_| {
            format!("absolute path outside root: {relative_path} (root: {root_path})")
        })?
    } else {
        input
    };
    for component in rel.components() {
        match component {
            Component::ParentDir => {
                return Err(format!("refusing `..` in path: {relative_path}"));
            }
            Component::Prefix(_) | Component::RootDir => {
                return Err(format!("refusing rooted path: {relative_path}"));
            }
            _ => {}
        }
    }
    let joined = Path::new(root_path).join(rel);
    let meta = std::fs::symlink_metadata(&joined)
        .map_err(|err| format!("stat {}: {err}", joined.display()))?;
    if !meta.file_type().is_file() {
        return Err(format!("refusing non-regular file: {}", joined.display()));
    }
    if meta.len() > MAX_FILE_BYTES {
        return Err(format!(
            "refusing oversize file: {} ({} bytes, max {MAX_FILE_BYTES})",
            joined.display(),
            meta.len()
        ));
    }
    Ok(joined)
}

/// Per-file ceiling on star-reexport specifiers. Star re-export chains
/// drive repeated resolver work inside `resolve_star_reexport_target`,
/// so an attacker-crafted module with 100 k `export * from '...'`
/// statements would amplify the cost of every import that touches it.
const MAX_STAR_REEXPORTS: usize = 64;

fn dedup_and_cap_star_reexports(specifiers: &[String]) -> Vec<JsStarReexport> {
    let mut seen: FxHashSet<String> = FxHashSet::default();
    let mut out = Vec::with_capacity(specifiers.len().min(MAX_STAR_REEXPORTS));
    for specifier in specifiers {
        if out.len() >= MAX_STAR_REEXPORTS {
            break;
        }
        if seen.insert(specifier.clone()) {
            out.push(JsStarReexport {
                specifier: specifier.clone(),
                mode: ImportMode::Declarative,
            });
        }
    }
    out
}

/// Per-file ceiling for the JS pipeline. The walker also applies a cap,
/// but callers that bypass the walker (`WorkspaceProbe`, SFC block
/// recombination) rely on this constant to stay bounded.
pub const MAX_FILE_BYTES: u64 = 2 * 1024 * 1024;

fn panic_message(payload: &Box<dyn std::any::Any + Send>) -> String {
    if let Some(s) = payload.downcast_ref::<&'static str>() {
        (*s).to_string()
    } else if let Some(s) = payload.downcast_ref::<String>() {
        s.clone()
    } else {
        "unknown panic".to_string()
    }
}

fn analyze_file(relative_path: &str, root_path: &str) -> Result<AnalyzedJsFile, PipelineError> {
    let absolute_path =
        safe_repo_join(root_path, relative_path).map_err(|error| PipelineError {
            file_path: relative_path.to_string(),
            error: error.to_string(),
        })?;
    let source = std::fs::read_to_string(&absolute_path).map_err(|error| PipelineError {
        file_path: relative_path.to_string(),
        error: error.to_string(),
    })?;
    let relative_path = normalize_relative_path(relative_path, root_path);
    let extension = extension_for(&relative_path);
    let language = language_for_extension(extension.as_str());
    if let Some(stub) = file_backed_module(
        &relative_path,
        extension.as_str(),
        language,
        source.len() as u64,
    ) {
        return Ok(stub);
    }
    let (virtual_path, source_text) = prepared_source(&relative_path, &extension, &source)
        .map_err(|error| PipelineError {
            file_path: relative_path.clone(),
            error,
        })?;

    // One analyzer call per file. SFCs with multiple `<script>` blocks
    // go through `frameworks::combine_scripts` first, so the analyzer
    // never sees the N-blocks-to-merge shape that silently dropped
    // colliding bindings in the old multi-pass path.
    let mut analysis = JsAnalyzer::analyze_file(&source_text, &virtual_path, &relative_path)
        .map_err(|error| PipelineError {
            file_path: relative_path.clone(),
            error,
        })?;
    analysis.relative_path = relative_path.clone();

    let phase1 = JsPhase1File {
        path: relative_path.clone(),
        extension,
        language,
        size: source.len() as u64,
        definitions: canonical_definitions(&analysis),
        imports: canonical_imports(&analysis.imports),
        bindings: module_bindings(&analysis),
        star_reexports: dedup_and_cap_star_reexports(&analysis.module_info.star_export_sources),
    };

    Ok(AnalyzedJsFile {
        relative_path,
        analysis,
        phase1,
    })
}

fn file_backed_module(
    relative_path: &str,
    extension: &str,
    language: Language,
    size: u64,
) -> Option<AnalyzedJsFile> {
    let is_file_backed = super::constants::ASSET_EXTENSIONS.contains(&extension)
        || super::constants::DATA_EXTENSIONS.contains(&extension);
    let primary_binding = is_file_backed.then(|| JsModuleBindingInput {
        export_name: JsExportName::Primary,
        binding: ExportedBinding::primary(None, SourceRange::empty()),
        target: JsModuleBindingTargetInput::File {
            path: relative_path.to_string(),
        },
    })?;

    Some(AnalyzedJsFile {
        relative_path: relative_path.to_string(),
        analysis: JsFileAnalysis {
            relative_path: relative_path.to_string(),
            defs: Vec::new(),
            imports: Vec::new(),
            local_calls: Vec::new(),
            calls: Vec::new(),
            classes: Vec::new(),
            module_info: JsModuleInfo::default(),
        },
        phase1: JsPhase1File {
            path: relative_path.to_string(),
            extension: extension.to_string(),
            language,
            size,
            definitions: Vec::new(),
            imports: Vec::new(),
            bindings: vec![primary_binding],
            star_reexports: Vec::new(),
        },
    })
}

/// Pick the single (virtual_path, source) tuple the analyzer runs on.
///
/// For SFCs, every `<script>` block is concatenated into one buffer so
/// the analyzer, OXC parser, and SSA pass each run once per file.
fn prepared_source(
    relative_path: &str,
    extension: &str,
    source: &str,
) -> Result<(String, String), String> {
    if !super::frameworks::has_embedded_scripts(extension) {
        return Ok((relative_path.to_string(), source.to_string()));
    }
    let combined = super::frameworks::combine_scripts(source, extension)?;
    if combined.block_count == 0 {
        // No `<script>` block found — treat the raw SFC as empty source
        // so the analyzer emits no defs rather than tripping on markup.
        return Ok((format!("{relative_path}.js"), String::new()));
    }
    let virtual_ext = if combined.is_typescript { "ts" } else { "js" };
    Ok((format!("{relative_path}.{virtual_ext}"), combined.source))
}

fn extension_for(path: &str) -> String {
    Path::new(path)
        .extension()
        .and_then(|ext| ext.to_str())
        .unwrap_or("js")
        .to_string()
}

fn language_for_extension(extension: &str) -> Language {
    if super::constants::is_ts_extension(extension) {
        Language::TypeScript
    } else {
        Language::JavaScript
    }
}

fn normalize_relative_path(path: &str, root_path: &str) -> String {
    Path::new(path)
        .strip_prefix(root_path)
        .map(|value| value.to_string_lossy().to_string())
        .unwrap_or_else(|_| path.to_string())
}

fn canonical_definitions(analysis: &JsFileAnalysis) -> Vec<CanonicalDefinition> {
    let extends_by_fqn: FxHashMap<_, _> = analysis
        .classes
        .iter()
        .filter_map(|class| {
            class
                .extends
                .as_ref()
                .map(|extends| (class.fqn.as_str(), extends.clone()))
        })
        .collect();

    analysis
        .defs
        .iter()
        .map(|definition| {
            canonical_definition(definition, extends_by_fqn.get(definition.fqn.as_str()))
        })
        .collect()
}

fn canonical_definition(definition: &JsDef, extends: Option<&String>) -> CanonicalDefinition {
    let mut metadata = DefinitionMetadata {
        type_annotation: definition.type_annotation.clone(),
        is_exported: definition.is_exported,
        ..DefinitionMetadata::default()
    };
    if let Some(extends) = extends {
        metadata.super_types.push(extends.clone());
    }

    CanonicalDefinition {
        definition_type: definition.kind.as_str(),
        kind: canonical_def_kind(&definition.kind),
        name: definition.name.clone(),
        fqn: Fqn::from_parts(&[definition.fqn.as_str()], "::"),
        range: to_range(definition.range),
        is_top_level: !definition.fqn.contains("::"),
        metadata: Some(Box::new(metadata)),
    }
}

fn canonical_def_kind(kind: &JsDefKind) -> DefKind {
    // Arms are explicit (no catch-all) so adding a new `JsDefKind`
    // variant triggers an exhaustiveness error instead of silently
    // collapsing to a wrong graph `DefKind`.
    match kind {
        JsDefKind::Class => DefKind::Class,
        JsDefKind::Interface => DefKind::Interface,
        JsDefKind::Namespace => DefKind::Module,
        JsDefKind::Function => DefKind::Function,
        JsDefKind::Method { .. } => DefKind::Method,
        JsDefKind::LifecycleHook { .. } => DefKind::Method,
        JsDefKind::Watcher { .. } => DefKind::Method,
        JsDefKind::ComputedProperty { .. } => DefKind::Property,
        JsDefKind::Variable => DefKind::Property,
        JsDefKind::EnumMember => DefKind::EnumEntry,
        JsDefKind::TypeAlias => DefKind::Other,
        JsDefKind::Enum => DefKind::Other,
    }
}

fn canonical_imports(imports: &[JsImport]) -> Vec<CanonicalImport> {
    imports.iter().map(canonical_import).collect()
}

fn canonical_import(import_entry: &JsImport) -> CanonicalImport {
    let (import_type, binding_kind, mode, name, alias) = match &import_entry.kind {
        JsImportKind::Named { imported_name } => (
            "NamedImport",
            ImportBindingKind::Named,
            ImportMode::Declarative,
            Some(imported_name.clone()),
            (import_entry.local_name != *imported_name).then(|| import_entry.local_name.clone()),
        ),
        JsImportKind::Default => (
            "DefaultImport",
            ImportBindingKind::Primary,
            ImportMode::Declarative,
            Some("default".to_string()),
            (import_entry.local_name != "default").then(|| import_entry.local_name.clone()),
        ),
        JsImportKind::Namespace => (
            "NamespaceImport",
            ImportBindingKind::Namespace,
            ImportMode::Declarative,
            None,
            Some(import_entry.local_name.clone()),
        ),
        JsImportKind::CjsRequire { imported_name } => (
            "CjsRequire",
            if imported_name.is_some() {
                ImportBindingKind::Named
            } else {
                ImportBindingKind::Primary
            },
            ImportMode::Runtime,
            Some(
                imported_name
                    .clone()
                    .unwrap_or_else(|| "default".to_string()),
            ),
            imported_name.as_ref().map_or_else(
                || (import_entry.local_name != "default").then(|| import_entry.local_name.clone()),
                |name| (import_entry.local_name != *name).then(|| import_entry.local_name.clone()),
            ),
        ),
    };

    CanonicalImport {
        import_type,
        binding_kind,
        mode,
        path: import_entry.specifier.clone(),
        name,
        alias,
        scope_fqn: None,
        range: to_range(import_entry.range),
        is_type_only: import_entry.is_type,
        wildcard: false,
    }
}

fn module_bindings(analysis: &JsFileAnalysis) -> Vec<JsModuleBindingInput> {
    let local_fqns: FxHashSet<_> = analysis.defs.iter().map(|def| def.fqn.as_str()).collect();
    let local_definition_ranges = &analysis.module_info.definition_fqns;
    let mut bindings = Vec::new();

    let mut named_exports: Vec<_> = analysis.module_info.exports.iter().collect();
    named_exports.sort_by_key(|(name, _)| name.as_str());
    for (export_name, binding) in named_exports {
        bindings.push(module_binding(export_name, binding, &local_fqns));
    }

    let mut seen_exports: FxHashSet<_> = bindings
        .iter()
        .map(|binding| binding.export_name.clone())
        .collect();
    for export in &analysis.module_info.cjs_exports {
        let Some(binding) = cjs_binding(export, &local_fqns, local_definition_ranges) else {
            continue;
        };
        if seen_exports.insert(binding.export_name.clone()) {
            bindings.push(binding);
        }
    }

    bindings
}

fn module_binding(
    export_name: &str,
    binding: &ExportedBinding,
    local_fqns: &FxHashSet<&str>,
) -> JsModuleBindingInput {
    let export_name = export_name_to_variant(export_name);
    let target = binding_target_input(binding, &export_name, local_fqns);
    JsModuleBindingInput {
        export_name,
        binding: binding.clone(),
        target,
    }
}

fn cjs_binding(
    export: &CjsExport,
    local_fqns: &FxHashSet<&str>,
    local_definition_ranges: &std::collections::HashMap<String, crate::utils::Range>,
) -> Option<JsModuleBindingInput> {
    match export {
        CjsExport::Default {
            local_fqn,
            range,
            invocation_support,
        } => Some(JsModuleBindingInput {
            export_name: JsExportName::Primary,
            binding: ExportedBinding::primary(local_fqn.clone(), *range)
                .with_definition_range(
                    local_fqn
                        .as_ref()
                        .and_then(|fqn| local_definition_ranges.get(fqn).copied()),
                )
                .with_invocation_support(*invocation_support),
            target: cjs_local_target(local_fqn, local_fqns),
        }),
        CjsExport::Named {
            name,
            local_fqn,
            range,
            invocation_support,
        } => Some(JsModuleBindingInput {
            export_name: JsExportName::Named(name.clone()),
            binding: ExportedBinding::local(
                local_fqn.clone().unwrap_or_else(|| name.clone()),
                *range,
            )
            .with_definition_range(
                local_fqn
                    .as_ref()
                    .and_then(|fqn| local_definition_ranges.get(fqn).copied()),
            )
            .with_invocation_support(*invocation_support),
            target: cjs_local_target(local_fqn, local_fqns),
        }),
    }
}

fn cjs_local_target(
    local_fqn: &Option<String>,
    local_fqns: &FxHashSet<&str>,
) -> JsModuleBindingTargetInput {
    local_fqn
        .as_ref()
        .filter(|fqn| local_fqns.contains(fqn.as_str()))
        .map(|fqn| JsModuleBindingTargetInput::LocalDefinition { fqn: fqn.clone() })
        .unwrap_or(JsModuleBindingTargetInput::Unresolved)
}

fn binding_target_input(
    binding: &ExportedBinding,
    export_name: &JsExportName,
    local_fqns: &FxHashSet<&str>,
) -> JsModuleBindingTargetInput {
    if let Some(specifier) = &binding.reexport_source {
        return JsModuleBindingTargetInput::Reexport {
            specifier: specifier.clone(),
            export_name: binding
                .reexport_imported_name
                .as_ref()
                .map(imported_name_to_export_name)
                .unwrap_or_else(|| {
                    if binding.is_default {
                        JsExportName::Primary
                    } else {
                        export_name.clone()
                    }
                }),
        };
    }

    if local_fqns.contains(binding.local_fqn.as_str()) {
        return JsModuleBindingTargetInput::LocalDefinition {
            fqn: binding.local_fqn.clone(),
        };
    }

    JsModuleBindingTargetInput::Unresolved
}

fn export_name_to_variant(name: &str) -> JsExportName {
    if name == "default" {
        JsExportName::Primary
    } else {
        JsExportName::Named(name.to_string())
    }
}

fn imported_name_to_export_name(name: &ImportedName) -> JsExportName {
    match name {
        ImportedName::Named(name) => JsExportName::Named(name.clone()),
        ImportedName::Default | ImportedName::Namespace => JsExportName::Primary,
    }
}

fn to_range(range: crate::utils::Range) -> GraphRange {
    GraphRange::new(
        GraphPosition::new(range.start.line, range.start.column),
        GraphPosition::new(range.end.line, range.end.column),
        range.byte_offset,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    #[test]
    fn analyze_files_accepts_extended_typescript_extensions() {
        let temp = tempfile::tempdir().expect("temp dir");
        let root = temp.path();

        for (path, content) in [
            (
                "tsx/component.tsx",
                "export function renderPanel(): string { return 'panel'; }\n",
            ),
            (
                "tsx/consumer.ts",
                "import { renderPanel } from './component.tsx';\nexport function runPanel(): string { return renderPanel(); }\n",
            ),
            (
                "mts/component.mts",
                "export function formatMts(value: string): string { return value.trim(); }\n",
            ),
            (
                "mts/consumer.ts",
                "import { formatMts } from './component.mts';\nexport function runMts(value: string): string { return formatMts(value); }\n",
            ),
            (
                "cts/component.cts",
                "export function formatCts(value: string): string { return value.toLowerCase(); }\n",
            ),
            (
                "cts/consumer.ts",
                "import { formatCts } from './component.cts';\nexport function runCts(value: string): string { return formatCts(value); }\n",
            ),
        ] {
            let file_path = root.join(path);
            fs::create_dir_all(
                file_path
                    .parent()
                    .expect("test fixture path should have a parent"),
            )
            .expect("create fixture directories");
            fs::write(file_path, content).expect("write fixture");
        }

        let files = vec![
            "tsx/component.tsx".to_string(),
            "tsx/consumer.ts".to_string(),
            "mts/component.mts".to_string(),
            "mts/consumer.ts".to_string(),
            "cts/component.cts".to_string(),
            "cts/consumer.ts".to_string(),
        ];

        let (analyzed, errors) = analyze_files(&files, root.to_str().expect("utf8 root path"));

        assert!(
            errors.is_empty(),
            "expected no phase1 errors, got: {errors:#?}"
        );
        assert_eq!(analyzed.len(), files.len());
    }
}
