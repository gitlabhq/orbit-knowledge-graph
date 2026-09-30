use std::path::Path;

use crate::utils::Range as SourceRange;
use crate::v2::config::Language;
use crate::v2::error::{AbortPhase, AnalyzerError, FileFault, FileSkip};
use crate::v2::pipeline::{FamilyFileInput, PipelineContext, Unread};
use crate::v2::types::{
    CanonicalDefinition, CanonicalImport, DefKind, DefinitionMetadata, Fqn, ImportBindingKind,
    ImportMode, Position as GraphPosition, Range as GraphRange,
};
use orbit_utils::files::SkipReason;
use rayon::prelude::*;
use rustc_hash::{FxHashMap, FxHashSet};

use super::{
    CjsExport, ExportedBinding, ImportedName, JsAnalyzer, JsCallEdge, JsDef, JsDefKind,
    JsDefSupport, JsExportName, JsFileAnalysis, JsImport, JsImportKind, JsModuleBindingInput,
    JsModuleBindingTargetInput, JsModuleInfo, JsPendingLocalCall, JsPhase1File, JsStarReexport,
};

#[derive(Debug, Clone)]
pub struct AnalyzedJsFile {
    pub relative_path: String,
    pub analysis: JsFileAnalysis,
    pub phase1: JsPhase1File,
    /// Wall-clock time for OXC parse + semantic + extraction.
    pub parse_ms: f64,
}

/// What resolution needs from a parsed file. The module graph builder is the last
/// reader of the rest of [`JsFileAnalysis`], so it is dropped instead of carried.
#[derive(Debug, Clone)]
pub struct ResolvedJsFile {
    pub relative_path: String,
    pub calls: Vec<JsCallEdge>,
    pub local_calls: Vec<JsPendingLocalCall>,
    pub def_support: Vec<JsDefSupport>,
}

impl ResolvedJsFile {
    pub fn from_analysis(relative_path: String, analysis: JsFileAnalysis) -> Self {
        Self {
            relative_path,
            calls: analysis.calls,
            local_calls: analysis.local_calls,
            def_support: analysis
                .defs
                .into_iter()
                .map(|def| JsDefSupport {
                    fqn: def.fqn,
                    byte_offset: def.range.byte_offset,
                    invocation_support: def.invocation_support,
                })
                .collect(),
        }
    }
}

pub type FailedJsFile = (String, AnalyzerError);

pub fn analyze_files(
    files: &[FamilyFileInput],
    ctx: &PipelineContext,
    sentinel: Option<&crate::v2::sentinel::SentinelHandle>,
    progress: &dyn crate::v2::pipeline::ProgressObserver,
) -> (Vec<AnalyzedJsFile>, Vec<FailedJsFile>) {
    // `catch_unwind` isolates per-file panics: a malformed input that trips
    // an OXC invariant takes down that file's analysis, not the pipeline.
    let results: Vec<_> = files
        .par_iter()
        .filter_map(|file| {
            if ctx.is_cancelled() {
                return None;
            }
            let guard = sentinel.map(|s| s.file_start(&file.path));
            let t_file = std::time::Instant::now();
            let outcome =
                std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| analyze_file(file, ctx)))
                    .unwrap_or_else(|panic_payload| {
                        let message = panic_message(&panic_payload);
                        Err(AnalyzerError::fault(
                            FileFault::AnalyzerPanic,
                            format!("panic during analysis: {message}"),
                        ))
                    });
            let parse_ms = t_file.elapsed().as_secs_f64() * 1000.0;
            progress.files_advanced(crate::v2::pipeline::ProgressPhase::Parse, 1);
            // If the sentinel killed this file while OXC was running,
            // convert whatever result we got into a timeout skip.
            if guard.as_ref().is_some_and(|g| g.is_killed()) {
                return Some((
                    file.path.clone(),
                    Err(AnalyzerError::skip(
                        FileSkip::Timeout(AbortPhase::Sentinel),
                        "per-file watchdog killed analysis",
                    )),
                ));
            }
            Some((
                file.path.clone(),
                outcome.map(|mut f| {
                    f.parse_ms = parse_ms;
                    f
                }),
            ))
        })
        .collect();

    let mut analyzed = Vec::with_capacity(results.len());
    let mut errors = Vec::new();

    for (path, result) in results {
        match result {
            Ok(file) => analyzed.push(file),
            Err(error) => errors.push((path, error)),
        }
    }

    (analyzed, errors)
}

/// A path is a graph identity, so it has to be a clean repository-relative
/// one; the walker never produces anything else, but we defend in depth.
fn safe_relative_path(relative_path: &str) -> Result<(), AnalyzerError> {
    if relative_path.contains('\0') || relative_path.contains('\n') {
        return Err(AnalyzerError::skip(
            FileSkip::UnsafePath,
            "relative path contains NUL or newline",
        ));
    }
    if !orbit_utils::fs::is_safe_relative_path(Path::new(relative_path)) {
        return Err(AnalyzerError::skip(
            FileSkip::UnsafePath,
            format!("refusing unsafe path: {relative_path}"),
        ));
    }
    Ok(())
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
pub const MAX_FILE_BYTES: u64 = 1024 * 1024;

fn panic_message(payload: &Box<dyn std::any::Any + Send>) -> String {
    let raw = if let Some(s) = payload.downcast_ref::<&'static str>() {
        (*s).to_string()
    } else if let Some(s) = payload.downcast_ref::<String>() {
        s.clone()
    } else {
        "unknown panic".to_string()
    };
    sanitize_panic_message(&raw)
}

/// Panic payloads from OXC can embed source snippets (byte ranges of
/// the file that triggered the panic). The surrounding pipeline writes
/// this into a `PipelineError` that lands in logs and downstream
/// telemetry, so a hostile repository could steer arbitrary repository
/// bytes into observability pipelines. Keep only printable ASCII and
/// cap the length so the message remains useful for debugging but
/// cannot exfiltrate arbitrary content.
fn sanitize_panic_message(raw: &str) -> String {
    const MAX: usize = 256;
    let mut out = String::with_capacity(MAX.min(raw.len()));
    for ch in raw.chars() {
        if out.len() >= MAX {
            out.push('…');
            break;
        }
        if ch == ' ' || (ch.is_ascii_graphic()) {
            out.push(ch);
        } else {
            out.push('?');
        }
    }
    out
}

fn analyze_file(
    file: &FamilyFileInput,
    ctx: &PipelineContext,
) -> Result<AnalyzedJsFile, AnalyzerError> {
    safe_relative_path(&file.path)?;
    if file.size > MAX_FILE_BYTES {
        return Err(AnalyzerError::skip(
            FileSkip::Oversize,
            format!("{} ({} bytes, max {MAX_FILE_BYTES})", file.path, file.size),
        ));
    }
    let source = ctx.read_source(file).map_err(|unread| match unread {
        Unread::Skip(kind, detail) => AnalyzerError::skip(kind, detail),
        Unread::Fault(kind, detail) => AnalyzerError::fault(kind, detail),
    })?;
    let source = std::str::from_utf8(&source)
        .map_err(|_| AnalyzerError::skip(FileSkip::Filter(SkipReason::NotUtf8), "not utf-8"))?;
    let relative_path = file.path.clone();
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
    let (virtual_path, source_text) = prepared_source(&relative_path, &extension, source)?;

    // One analyzer call per file. SFCs with multiple `<script>` blocks
    // go through `frameworks::combine_scripts` first, so the analyzer
    // never sees the N-blocks-to-merge shape that silently dropped
    // colliding bindings in the old multi-pass path.
    let mut analysis = JsAnalyzer::analyze_file(&source_text, &virtual_path, &relative_path)?;
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
        parse_ms: 0.0,
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
        parse_ms: 0.0,
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

/// For SFCs, every `<script>` block is concatenated into one buffer so
/// the analyzer, OXC parser, and SSA pass each run once per file.
fn prepared_source(
    relative_path: &str,
    extension: &str,
    source: &str,
) -> Result<(String, String), AnalyzerError> {
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
        type_annotation: definition
            .type_annotation
            .as_deref()
            .map(truncate_identifier),
        is_exported: definition.is_exported,
        ..DefinitionMetadata::default()
    };
    if let Some(extends) = extends {
        metadata.super_types.push(truncate_identifier(extends));
    }

    let fqn_truncated = truncate_identifier(&definition.fqn);
    CanonicalDefinition {
        definition_type: definition.kind.as_str(),
        kind: canonical_def_kind(&definition.kind),
        name: truncate_identifier(&definition.name),
        fqn: Fqn::from_parts(&[fqn_truncated.as_str()], "::"),
        range: to_range(definition.range),
        is_top_level: !fqn_truncated.contains("::"),
        metadata: Some(Box::new(metadata)),
    }
}

/// JS identifiers are attacker-controlled and carry into the graph as
/// hash keys and display strings. A source with 100 MB of contiguous
/// identifier text can hold the whole thing in memory for the life of
/// the pipeline. Cap at 1 KiB, which is still two orders of magnitude
/// past any realistic human-written name.
fn truncate_identifier(value: &str) -> String {
    const MAX: usize = 1024;
    if value.len() <= MAX {
        value.to_string()
    } else {
        let mut end = MAX;
        while end > 0 && !value.is_char_boundary(end) {
            end -= 1;
        }
        format!("{}…", &value[..end])
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
        JsDefKind::Property { .. } => DefKind::Property,
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
        path: truncate_identifier(&import_entry.specifier),
        name: name.as_deref().map(truncate_identifier),
        alias: alias.as_deref().map(truncate_identifier),
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
    use crate::v2::pipeline::testing;

    #[test]
    fn analyze_files_accepts_extended_typescript_extensions() {
        let (ctx, files) = testing::repo(&[
            (
                "tsx/component.tsx",
                b"export function renderPanel(): string { return 'panel'; }\n",
            ),
            (
                "tsx/consumer.ts",
                b"import { renderPanel } from './component.tsx';\nexport function runPanel(): string { return renderPanel(); }\n",
            ),
            (
                "mts/component.mts",
                b"export function formatMts(value: string): string { return value.trim(); }\n",
            ),
            (
                "mts/consumer.ts",
                b"import { formatMts } from './component.mts';\nexport function runMts(value: string): string { return formatMts(value); }\n",
            ),
            (
                "cts/component.cts",
                b"export function formatCts(value: string): string { return value.toLowerCase(); }\n",
            ),
            (
                "cts/consumer.ts",
                b"import { formatCts } from './component.cts';\nexport function runCts(value: string): string { return formatCts(value); }\n",
            ),
        ]);

        let (analyzed, errors) =
            analyze_files(&files, &ctx, None, &crate::v2::pipeline::SilentProgress);

        assert!(
            errors.is_empty(),
            "expected no phase1 errors, got: {errors:#?}"
        );
        assert_eq!(analyzed.len(), files.len());
    }

    #[test]
    fn an_oversize_file_is_a_typed_skip() {
        let big = vec![b'a'; (MAX_FILE_BYTES + 16) as usize];
        let (ctx, files) = testing::repo(&[("big.js", &big)]);
        let result = analyze_file(&files[0], &ctx);
        assert!(matches!(
            result,
            Err(AnalyzerError::Skip {
                kind: FileSkip::Oversize,
                ..
            })
        ));
    }

    #[test]
    fn a_cancelled_analysis_reads_no_files() {
        let (ctx, files) = testing::repo(&[
            ("a.js", b"export const x = 1;\n"),
            ("b.js", b"export const y = 2;\n"),
        ]);
        ctx.config.cancel.cancel();

        let (analyzed, errors) =
            analyze_files(&files, &ctx, None, &crate::v2::pipeline::SilentProgress);

        assert!(analyzed.is_empty(), "got {} analyzed files", analyzed.len());
        assert!(
            errors.is_empty(),
            "a dropped file is not an error: {errors:?}"
        );
    }

    #[test]
    fn a_path_that_climbs_out_is_an_unsafe_path_skip() {
        assert!(matches!(
            safe_relative_path("../etc/passwd"),
            Err(AnalyzerError::Skip {
                kind: FileSkip::UnsafePath,
                ..
            })
        ));
    }
}
