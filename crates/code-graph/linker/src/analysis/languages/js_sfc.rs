use oxc_linter::loader::PartialLoader;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScriptBlock {
    pub source_text: String,
    pub is_typescript: bool,
    pub start_byte: u32,
}

/// Extract `<script>` blocks from Vue/Svelte single-file components.
/// Delegates to OXC's `PartialLoader` which handles Vue, Svelte, and Astro.
pub fn extract_scripts(source: &str, extension: &str) -> Vec<ScriptBlock> {
    let Some(sources) = PartialLoader::parse(extension, source) else {
        return vec![];
    };

    sources
        .into_iter()
        .map(|js_source| ScriptBlock {
            source_text: js_source.source_text.to_string(),
            is_typescript: js_source.source_type.is_typescript(),
            start_byte: js_source.start,
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn vue_script_setup_lang_ts() {
        let source = r#"<script setup lang="ts">
const msg = ref("hello");
</script>"#;
        let blocks = extract_scripts(source, "vue");
        assert_eq!(blocks.len(), 1);
        assert!(blocks[0].is_typescript);
        assert!(blocks[0].source_text.contains("const msg"));
    }

    #[test]
    fn vue_dual_script_blocks() {
        let source = "<script>aaa</script><script setup>bbb</script>";
        let blocks = extract_scripts(source, "vue");
        assert_eq!(blocks.len(), 2);
    }

    #[test]
    fn svelte_script_lang_ts() {
        let source = r#"<script lang="ts">
let count = 0;
</script>"#;
        let blocks = extract_scripts(source, "svelte");
        assert_eq!(blocks.len(), 1);
        assert!(blocks[0].is_typescript);
        assert!(blocks[0].source_text.contains("let count"));
    }

    #[test]
    fn svelte_module_script() {
        let source = r#"<script context="module">
export const prerender = true;
</script>
<script>
let name = "world";
</script>"#;
        let blocks = extract_scripts(source, "svelte");
        assert_eq!(blocks.len(), 2);
    }

    #[test]
    fn unknown_extension_returns_empty() {
        let blocks = extract_scripts("<script>hello</script>", "html");
        assert!(blocks.is_empty());
    }

    #[test]
    fn astro_also_works() {
        let source = r#"---
const title = "Hello";
---
<html><body>{title}</body></html>"#;
        let blocks = extract_scripts(source, "astro");
        // Astro frontmatter is extracted as a script block
        assert!(!blocks.is_empty());
    }

    #[test]
    fn start_byte_offset_correct() {
        let source = r#"<template><div>hi</div></template>
<script lang="ts">
const x = 1;
</script>"#;
        let blocks = extract_scripts(source, "vue");
        assert_eq!(blocks.len(), 1);
        // start_byte should point to the content after <script lang="ts">\n
        let start = blocks[0].start_byte as usize;
        assert!(
            source[start..].starts_with('\n') || source[start..].starts_with("const"),
            "start_byte should point to script content, got: {:?}",
            &source[start..start + 20.min(source.len() - start)]
        );
    }
}
