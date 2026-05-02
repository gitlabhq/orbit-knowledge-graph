//! Framework-specific hooks plugged into the JS pipeline.
//!
//! Anything that requires knowing about Vue/Svelte/Astro before running
//! the standard JS analyzer lives here. The rest of the pipeline only
//! calls `has_embedded_scripts` / `extract_scripts` / `combine_scripts`
//! and the per-framework analyzer hooks re-exported below.

mod vue;

use oxc_linter::loader::{JavaScriptSource, PartialLoader};

pub(in crate::v2::langs::custom::js) use vue::{
    extract_vue_options_api, is_vue_like_path, vue_default_component_def,
};

use crate::v2::error::{AnalyzerError, FileFault, FileSkip};

use super::constants::is_sfc_extension;

pub fn has_embedded_scripts(extension: &str) -> bool {
    is_sfc_extension(extension)
}

/// Split a Single-File Component (Vue/Svelte/Astro) into its embedded
/// `<script>` blocks.
pub fn extract_scripts<'a>(
    source: &'a str,
    extension: &str,
) -> Result<Vec<JavaScriptSource<'a>>, AnalyzerError> {
    PartialLoader::parse(extension, source).ok_or_else(|| {
        AnalyzerError::fault(
            FileFault::EmbeddedScriptParse,
            format!("failed to parse embedded scripts for .{extension} file"),
        )
    })
}

/// Source text derived from an SFC's `<script>` blocks, flattened into
/// one buffer so the analyzer sees a single module instead of N.
pub struct CombinedScripts {
    /// Concatenation of every `<script>` block, separated by a newline
    /// so two blocks never merge into one statement.
    pub source: String,
    /// `true` iff at least one source block was TypeScript; the
    /// combined source is analysed as TS in that case.
    pub is_typescript: bool,
    /// Number of script blocks combined. Zero means "no `<script>`
    /// block was found at all"; callers fall back to the raw source.
    pub block_count: usize,
}

/// Fold every `<script>` block in an SFC into one analyzer input.
///
/// A single OXC parse + SemanticBuilder run covers every block,
/// eliminating the per-block merge path (and the silent-overwrite
/// bug that merge hid for SFCs with both `<script>` and
/// `<script setup>`). Block scopes flatten into one module, which
/// is fine for def / import / call extraction — Vue's runtime
/// scoping rules are not what we model.
pub fn combine_scripts(source: &str, extension: &str) -> Result<CombinedScripts, AnalyzerError> {
    let blocks = extract_scripts(source, extension)?;
    let mut combined = String::new();
    let mut is_typescript = false;
    for block in &blocks {
        if block.source_type.is_typescript() {
            is_typescript = true;
        }
        if !combined.is_empty() {
            // `;\n` instead of `\n` so ASI-defeating tokens at the end
            // of one block (open paren, arithmetic operator, template
            // literal) cannot statement-merge with the start of the
            // next block. Misleading merges silently corrupt downstream
            // def/range data and can mask whole assignments.
            combined.push_str(";\n");
        }
        if combined.len().saturating_add(block.source_text.len()) as u64
            > super::extract::MAX_FILE_BYTES
        {
            return Err(AnalyzerError::skip(
                FileSkip::OversizeCombined,
                format!(
                    "combined script blocks exceed per-file cap ({} bytes)",
                    super::extract::MAX_FILE_BYTES
                ),
            ));
        }
        combined.push_str(block.source_text);
    }
    Ok(CombinedScripts {
        source: combined,
        is_typescript,
        block_count: blocks.len(),
    })
}
