//! Framework-specific hooks plugged into the JS pipeline.
//!
//! Anything that requires knowing about Vue/Svelte/Astro/React-RSC before
//! running the standard JS analyzer lives here. The rest of the pipeline
//! only calls `has_embedded_scripts` / `extract_scripts` / `detect_directive`
//! and the per-framework analyzer hooks re-exported below.

pub mod directives;
pub mod vue;

use oxc_linter::loader::{JavaScriptSource, PartialLoader};

pub use directives::{JsDirective, detect_directive};
pub(in crate::v2::langs::custom::js) use vue::extract_vue_options_api;

use super::constants::is_sfc_extension;

pub fn has_embedded_scripts(extension: &str) -> bool {
    is_sfc_extension(extension)
}

/// Split a Single-File Component (Vue/Svelte/Astro) into its embedded
/// `<script>` blocks. Returns an error message describing the parse
/// failure rather than silently dropping the file.
pub fn extract_scripts<'a>(
    source: &'a str,
    extension: &str,
) -> Result<Vec<JavaScriptSource<'a>>, String> {
    PartialLoader::parse(extension, source)
        .ok_or_else(|| format!("failed to parse embedded scripts for .{extension} file"))
}
