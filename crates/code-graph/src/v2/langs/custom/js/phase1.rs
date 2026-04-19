use std::path::Path;

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
    JsModuleInfo, JsPhase1File, JsStarReexport, extract_scripts,
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
    let results: Vec<_> = files
        .par_iter()
        .map(|relative_path| analyze_file(relative_path, root_path))
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

fn analyze_file(relative_path: &str, root_path: &str) -> Result<AnalyzedJsFile, PipelineError> {
    let absolute_path = Path::new(root_path).join(relative_path);
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
    let sources =
        source_variants(&relative_path, &extension, &source).map_err(|error| PipelineError {
            file_path: relative_path.clone(),
            error,
        })?;

    let mut all_defs = Vec::new();
    let mut all_imports = Vec::new();
    let mut all_local_calls = Vec::new();
    let mut all_calls = Vec::new();
    let mut all_classes = Vec::new();
    let mut directive = None;
    let mut module_info = JsModuleInfo::default();

    for (virtual_path, source_text) in sources {
        let analysis = JsAnalyzer::analyze_file(&source_text, &virtual_path, &relative_path)
            .map_err(|error| PipelineError {
                file_path: relative_path.clone(),
                error,
            })?;

        if directive.is_none() {
            directive = analysis.directive;
        }
        module_info.merge(analysis.module_info);
        all_defs.extend(analysis.defs);
        all_imports.extend(analysis.imports);
        all_local_calls.extend(analysis.local_calls);
        all_calls.extend(analysis.calls);
        all_classes.extend(analysis.classes);
    }

    let analysis = JsFileAnalysis {
        relative_path: relative_path.clone(),
        defs: all_defs,
        imports: all_imports,
        local_calls: all_local_calls,
        calls: all_calls,
        classes: all_classes,
        directive,
        module_info,
    };

    let phase1 = JsPhase1File {
        path: relative_path.clone(),
        extension,
        language,
        size: source.len() as u64,
        definitions: canonical_definitions(&analysis),
        imports: canonical_imports(&analysis.imports),
        bindings: module_bindings(&analysis),
        star_reexports: analysis
            .module_info
            .star_export_sources
            .iter()
            .cloned()
            .map(|specifier| JsStarReexport {
                specifier,
                mode: ImportMode::Declarative,
            })
            .collect(),
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
    let primary_binding = matches!(
        extension,
        "graphql" | "gql" | "svg" | "png" | "jpg" | "jpeg" | "gif" | "webp" | "avif" | "json"
    )
    .then(|| JsModuleBindingInput {
        export_name: JsExportName::Primary,
        binding: ExportedBinding {
            local_fqn: "default".to_string(),
            range: SourceRange::empty(),
            definition_range: None,
            invocation_support: None,
            member_bindings: Default::default(),
            is_type: false,
            is_default: true,
            reexport_source: None,
            reexport_imported_name: None,
        },
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
            directive: None,
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

fn source_variants(
    relative_path: &str,
    extension: &str,
    source: &str,
) -> Result<Vec<(String, String)>, String> {
    match extension {
        "vue" | "svelte" | "astro" => extract_scripts(source, extension).map(|blocks| {
            blocks
                .into_iter()
                .map(|block| {
                    let virtual_ext = if block.source_type.is_typescript() {
                        "ts"
                    } else {
                        "js"
                    };
                    (
                        format!("{relative_path}.{virtual_ext}"),
                        block.source_text.to_string(),
                    )
                })
                .collect()
        }),
        _ => Ok(vec![(relative_path.to_string(), source.to_string())]),
    }
}

fn extension_for(path: &str) -> String {
    Path::new(path)
        .extension()
        .and_then(|ext| ext.to_str())
        .unwrap_or("js")
        .to_string()
}

fn language_for_extension(extension: &str) -> Language {
    match extension {
        "ts" | "tsx" | "mts" | "cts" => Language::TypeScript,
        _ => Language::JavaScript,
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
    match kind {
        JsDefKind::Class => DefKind::Class,
        JsDefKind::Interface => DefKind::Interface,
        JsDefKind::Namespace => DefKind::Module,
        JsDefKind::Function => DefKind::Function,
        JsDefKind::Method { .. }
        | JsDefKind::LifecycleHook { .. }
        | JsDefKind::Watcher { .. }
        | JsDefKind::Getter { .. }
        | JsDefKind::Setter { .. } => DefKind::Method,
        JsDefKind::ComputedProperty { .. } | JsDefKind::Variable => DefKind::Property,
        JsDefKind::EnumMember => DefKind::EnumEntry,
        JsDefKind::TypeAlias | JsDefKind::Enum => DefKind::Other,
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
    named_exports.sort_by(|(left, _), (right, _)| left.cmp(right));
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
            binding: ExportedBinding {
                local_fqn: local_fqn.clone().unwrap_or_else(|| "default".to_string()),
                range: *range,
                definition_range: local_fqn
                    .as_ref()
                    .and_then(|fqn| local_definition_ranges.get(fqn).copied()),
                invocation_support: *invocation_support,
                member_bindings: Default::default(),
                is_type: false,
                is_default: true,
                reexport_source: None,
                reexport_imported_name: None,
            },
            target: local_fqn
                .as_ref()
                .filter(|fqn| local_fqns.contains(fqn.as_str()))
                .map(|fqn| JsModuleBindingTargetInput::LocalDefinition { fqn: fqn.clone() })
                .unwrap_or(JsModuleBindingTargetInput::Unresolved),
        }),
        CjsExport::Named {
            name,
            local_fqn,
            range,
            invocation_support,
        } => Some(JsModuleBindingInput {
            export_name: JsExportName::Named(name.clone()),
            binding: ExportedBinding {
                local_fqn: local_fqn.clone().unwrap_or_else(|| name.clone()),
                range: *range,
                definition_range: local_fqn
                    .as_ref()
                    .and_then(|fqn| local_definition_ranges.get(fqn).copied()),
                invocation_support: *invocation_support,
                member_bindings: Default::default(),
                is_type: false,
                is_default: false,
                reexport_source: None,
                reexport_imported_name: None,
            },
            target: local_fqn
                .as_ref()
                .filter(|fqn| local_fqns.contains(fqn.as_str()))
                .map(|fqn| JsModuleBindingTargetInput::LocalDefinition { fqn: fqn.clone() })
                .unwrap_or(JsModuleBindingTargetInput::Unresolved),
        }),
    }
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
