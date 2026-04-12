use super::helpers::{process_fixture_file, read_fixture_file};
use crate::analysis::languages::js::extract_scripts;

#[test]
fn vue_merged_script_blocks() {
    let processed = process_fixture_file("sfc/vue-merged-scripts", "src/App.vue");
    let analysis = processed.js_analysis.expect("should produce JS analysis");

    assert!(
        analysis.module_info.exports.contains_key("serverOnly"),
        "should preserve exports from the regular script block"
    );
    assert!(
        analysis
            .module_info
            .imports
            .iter()
            .any(|i| i.specifier == "vue"),
        "should merge setup-script imports"
    );
}

#[test]
fn svelte_module_and_instance_scripts() {
    let processed = process_fixture_file("sfc/svelte-module-instance", "src/Widget.svelte");
    let analysis = processed.js_analysis.expect("should produce JS analysis");

    assert!(
        analysis.defs.iter().any(|def| def.name == "prerender"),
        "should include module script definitions"
    );
    assert!(
        analysis
            .imports
            .iter()
            .any(|i| i.specifier == "svelte/store"),
        "should include instance script imports"
    );
}

#[test]
fn astro_frontmatter_extraction() {
    let astro = read_fixture_file("sfc/astro-frontmatter", "src/Page.astro");
    let blocks = extract_scripts(&astro, "astro");

    assert_eq!(blocks.len(), 2, "should expose frontmatter and script");
    assert!(
        blocks[0].source_text.contains("const title = \"Hello\""),
        "should keep frontmatter content"
    );
}

#[test]
fn vue_empty_script_block() {
    let blocks = extract_scripts("<template><div/></template>", "vue");
    assert!(
        blocks.is_empty(),
        "template-only Vue file should yield no script blocks"
    );
}

#[test]
fn vue_script_setup_with_lang_ts() {
    let blocks = extract_scripts(
        r#"<script setup lang="ts">const x: number = 1;</script>"#,
        "vue",
    );
    assert_eq!(blocks.len(), 1);
    assert!(blocks[0].source_type.is_typescript());
}

#[test]
fn svelte_no_script_block() {
    let blocks = extract_scripts("<div>just html</div>", "svelte");
    assert!(blocks.is_empty());
}
