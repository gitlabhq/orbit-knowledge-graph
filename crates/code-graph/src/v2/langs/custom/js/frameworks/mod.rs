pub(in crate::v2::langs::custom::js) mod react;
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

pub struct CombinedScripts {
    pub source: String,
    pub is_typescript: bool,
    pub block_count: usize,
}

pub fn combine_scripts(source: &str, extension: &str) -> Result<CombinedScripts, AnalyzerError> {
    let blocks = extract_scripts(source, extension)?;
    let mut combined = String::new();
    let mut is_typescript = false;
    for block in &blocks {
        if block.source_type.is_typescript() {
            is_typescript = true;
        }
        if !combined.is_empty() {
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
