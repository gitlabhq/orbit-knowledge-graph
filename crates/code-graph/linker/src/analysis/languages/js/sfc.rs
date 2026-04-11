use oxc_linter::loader::{JavaScriptSource, PartialLoader};

pub fn extract_scripts<'a>(source: &'a str, extension: &str) -> Vec<JavaScriptSource<'a>> {
    PartialLoader::parse(extension, source).unwrap_or_default()
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
        assert!(blocks[0].source_type.is_typescript());
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
        assert!(blocks[0].source_type.is_typescript());
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
    fn astro_frontmatter() {
        let source = r#"---
const title = "Hello";
---
<html><body>{title}</body></html>"#;
        let blocks = extract_scripts(source, "astro");
        assert!(!blocks.is_empty());
    }

    #[test]
    fn vue_script_in_comment_is_skipped() {
        let source = r"
        <!-- <script>a</script> -->
        <!-- <script> -->
        <script>b</script>
        ";
        let blocks = extract_scripts(source, "vue");
        assert_eq!(blocks.len(), 1);
        assert_eq!(blocks[0].source_text, "b");
    }

    #[test]
    fn vue_script_view_tag_ignored() {
        let source = r"
        <template><script-view /></template>
        <script>a</script>
        ";
        let blocks = extract_scripts(source, "vue");
        assert_eq!(blocks.len(), 1);
        assert_eq!(blocks[0].source_text, "a");
    }

    #[test]
    fn vue_closing_angle_in_attribute() {
        let source = r"
        <script description='PI > 5'>a</script>
        ";
        let blocks = extract_scripts(source, "vue");
        assert_eq!(blocks.len(), 1);
        assert_eq!(blocks[0].source_text, "a");
    }

    #[test]
    fn vue_generic_with_nested_angles() {
        let source = r#"
        <script lang="ts" setup generic="T extends Record<string, string>">
            1/1
        </script>
        "#;
        let blocks = extract_scripts(source, "vue");
        assert_eq!(blocks.len(), 1);
        assert!(blocks[0].source_type.is_typescript());
        assert_eq!(blocks[0].source_text.trim(), "1/1");
    }

    #[test]
    fn svelte_ts_with_generic() {
        let source = r#"
        <script lang="ts" generics="T extends Record<string, unknown>">
          console.log("hi");
        </script>
        "#;
        let blocks = extract_scripts(source, "svelte");
        assert_eq!(blocks.len(), 1);
        assert!(blocks[0].source_type.is_typescript());
    }

    #[test]
    fn astro_with_frontmatter_and_script() {
        let source = r#"
        ---
            const msg = 'hello';
        ---
        <script>
            console.log("Hi");
        </script>
        "#;
        let blocks = extract_scripts(source, "astro");
        assert_eq!(blocks.len(), 2);
        assert!(blocks[0].source_text.contains("const msg"));
    }

    #[test]
    fn astro_self_closing_script() {
        let source = r#"
        <script is:inline src="https://example.com/script.js" />
        <script>
            console.log("Hi");
        </script>
        "#;
        let blocks = extract_scripts(source, "astro");
        assert_eq!(blocks.len(), 2);
        assert!(blocks[0].source_text.is_empty());
    }
}
