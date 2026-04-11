use memchr::memmem::{Finder, FinderRev};

const SCRIPT_START: &str = "<script";
const SCRIPT_END: &str = "</script>";
const COMMENT_START: &str = "<!--";
const COMMENT_END: &str = "-->";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScriptBlock {
    pub source_text: String,
    pub is_typescript: bool,
    pub start_byte: u32,
}

pub fn extract_scripts(source: &str, extension: &str) -> Vec<ScriptBlock> {
    match extension {
        "vue" => parse_scripts(source, true),
        "svelte" => parse_scripts(source, false),
        _ => vec![],
    }
}

fn parse_scripts(source: &str, is_vue: bool) -> Vec<ScriptBlock> {
    let mut pointer = 0;
    let Some(first) = parse_script(source, &mut pointer, is_vue) else {
        return vec![];
    };
    let Some(second) = parse_script(source, &mut pointer, is_vue) else {
        return vec![first];
    };
    vec![first, second]
}

fn parse_script(source: &str, pointer: &mut usize, is_vue: bool) -> Option<ScriptBlock> {
    let script_start_finder = Finder::new(SCRIPT_START);
    let comment_start_finder = FinderRev::new(COMMENT_START);
    let comment_end_finder = Finder::new(COMMENT_END);

    *pointer += find_script_start(
        source,
        *pointer,
        &script_start_finder,
        &comment_start_finder,
        &comment_end_finder,
    )?;

    if is_vue && !source[*pointer..].starts_with([' ', '>']) {
        return parse_script(source, pointer, is_vue);
    }

    let offset = find_script_closing_angle(source, *pointer)?;
    let attrs = &source[*pointer..*pointer + offset];

    let is_typescript = detect_typescript(attrs);

    *pointer += offset + 1;
    #[expect(clippy::cast_possible_truncation)]
    let js_start = *pointer as u32;

    let script_end_finder = Finder::new(SCRIPT_END);
    let end_offset = script_end_finder.find(&source.as_bytes()[*pointer..])?;
    let js_end = *pointer + end_offset;
    *pointer += end_offset + SCRIPT_END.len();

    Some(ScriptBlock {
        source_text: source[js_start as usize..js_end].to_string(),
        is_typescript,
        start_byte: js_start,
    })
}

fn detect_typescript(attrs: &str) -> bool {
    let trimmed = attrs.trim();
    let Some(lang_idx) = trimmed.find("lang") else {
        return false;
    };
    let mut rest = trimmed[lang_idx + 4..].trim_start();
    if !rest.starts_with('=') {
        return false;
    }
    rest = rest[1..].trim_start();

    let value = match rest.chars().next() {
        Some(q @ ('"' | '\'')) => {
            rest = &rest[1..];
            rest.find(q).map(|end| &rest[..end])
        }
        Some(_) => {
            let end = rest
                .find(|c: char| c.is_whitespace() || c == '>')
                .unwrap_or(rest.len());
            Some(&rest[..end])
        }
        None => None,
    };

    matches!(value, Some("ts" | "tsx"))
}

/// Finds the closing `>` of a `<script ...>` tag, handling `>` inside quoted
/// attribute values (e.g. `generic="T extends Record<string, string>"`).
fn find_script_closing_angle(source: &str, pointer: usize) -> Option<usize> {
    let mut in_quote: Option<char> = None;

    for (offset, c) in source[pointer..].char_indices() {
        match c {
            '"' | '\'' => {
                if let Some(q) = in_quote {
                    if q == c {
                        in_quote = None;
                    }
                } else {
                    in_quote = Some(c);
                }
            }
            '>' if in_quote.is_none() => return Some(offset),
            _ => {}
        }
    }

    None
}

/// Advances past `<script` while skipping occurrences inside HTML comments.
fn find_script_start(
    source: &str,
    pointer: usize,
    script_start_finder: &Finder<'_>,
    comment_start_finder: &FinderRev<'_>,
    comment_end_finder: &Finder<'_>,
) -> Option<usize> {
    let mut new_pointer = pointer;

    loop {
        new_pointer +=
            script_start_finder.find(&source.as_bytes()[new_pointer..])? + SCRIPT_START.len();

        if let Some(offset) = comment_start_finder.rfind(&source.as_bytes()[..new_pointer]) {
            if comment_end_finder
                .find(&source.as_bytes()[offset + COMMENT_START.len()..new_pointer])
                .is_some()
            {
                break;
            }
        } else {
            break;
        }
    }

    Some(new_pointer - pointer)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn vue_script_setup_lang_ts() {
        let source = r#"
<template><div>hi</div></template>
<script setup lang="ts">
const x: number = 1;
</script>
"#;
        let blocks = extract_scripts(source, "vue");
        assert_eq!(blocks.len(), 1);
        assert!(blocks[0].is_typescript);
        assert_eq!(blocks[0].source_text.trim(), "const x: number = 1;");
    }

    #[test]
    fn vue_dual_script_blocks() {
        let source = r#"
<template><div /></template>
<script lang="ts">
export default { name: 'Foo' }
</script>
<script setup lang="ts">
const msg = 'hello'
</script>
"#;
        let blocks = extract_scripts(source, "vue");
        assert_eq!(blocks.len(), 2);
        assert!(blocks[0].is_typescript);
        assert_eq!(
            blocks[0].source_text.trim(),
            "export default { name: 'Foo' }"
        );
        assert!(blocks[1].is_typescript);
        assert_eq!(blocks[1].source_text.trim(), "const msg = 'hello'");
    }

    #[test]
    fn svelte_script_lang_ts() {
        let source = r#"
<script lang="ts">
  let count: number = 0;
</script>
<h1>Hello</h1>
"#;
        let blocks = extract_scripts(source, "svelte");
        assert_eq!(blocks.len(), 1);
        assert!(blocks[0].is_typescript);
        assert_eq!(blocks[0].source_text.trim(), "let count: number = 0;");
    }

    #[test]
    fn svelte_module_script() {
        let source = r#"
<script module>
  export const PI = 3.14;
</script>
<script>
  let x = 1;
</script>
"#;
        let blocks = extract_scripts(source, "svelte");
        assert_eq!(blocks.len(), 2);
        assert!(!blocks[0].is_typescript);
        assert_eq!(blocks[0].source_text.trim(), "export const PI = 3.14;");
        assert!(!blocks[1].is_typescript);
        assert_eq!(blocks[1].source_text.trim(), "let x = 1;");
    }

    #[test]
    fn start_byte_offsets_are_correct() {
        let source = "<script>abc</script>";
        let blocks = extract_scripts(source, "vue");
        assert_eq!(blocks.len(), 1);
        assert_eq!(blocks[0].start_byte, 8);
        assert_eq!(&source[blocks[0].start_byte as usize..][..3], "abc");
    }

    #[test]
    fn start_byte_offsets_dual_blocks() {
        let source = "<script>aaa</script><script setup>bbb</script>";
        let blocks = extract_scripts(source, "vue");
        assert_eq!(blocks.len(), 2);
        assert_eq!(&source[blocks[0].start_byte as usize..][..3], "aaa");
        assert_eq!(&source[blocks[1].start_byte as usize..][..3], "bbb");
    }

    #[test]
    fn typescript_detection_variants() {
        let cases = [
            (r#"<script lang="ts">x</script>"#, true),
            (r#"<script lang="tsx">x</script>"#, true),
            ("<script lang='ts'>x</script>", true),
            (r#"<script lang="js">x</script>"#, false),
            ("<script>x</script>", false),
            (r#"<script lang = "ts" >x</script>"#, true),
        ];
        for (source, expected_ts) in cases {
            let blocks = extract_scripts(source, "vue");
            assert_eq!(blocks.len(), 1, "expected 1 block for: {source}");
            assert_eq!(
                blocks[0].is_typescript, expected_ts,
                "is_typescript mismatch for: {source}"
            );
        }
    }

    #[test]
    fn generic_attribute_with_angle_brackets() {
        let source =
            r#"<script lang="ts" setup generic="T extends Record<string, string>">code</script>"#;
        let blocks = extract_scripts(source, "vue");
        assert_eq!(blocks.len(), 1);
        assert!(blocks[0].is_typescript);
        assert_eq!(blocks[0].source_text, "code");
    }

    #[test]
    fn script_inside_html_comment_is_skipped() {
        let source = r#"
<!-- <script>a</script> -->
<!-- <script> -->
<script>b</script>
"#;
        let blocks = extract_scripts(source, "vue");
        assert_eq!(blocks.len(), 1);
        assert_eq!(blocks[0].source_text, "b");
    }

    #[test]
    fn no_script_tag() {
        let source = "<template><div /></template>";
        let blocks = extract_scripts(source, "vue");
        assert!(blocks.is_empty());
    }

    #[test]
    fn unknown_extension_returns_empty() {
        let source = "<script>x</script>";
        let blocks = extract_scripts(source, "html");
        assert!(blocks.is_empty());
    }

    #[test]
    fn script_like_tag_in_template_is_skipped_vue() {
        let source = r#"
<template><script-view /></template>
<script>a</script>
"#;
        let blocks = extract_scripts(source, "vue");
        assert_eq!(blocks.len(), 1);
        assert_eq!(blocks[0].source_text, "a");
    }

    #[test]
    fn quoted_angle_bracket_in_attribute() {
        let source = r#"<script description='PI > 5'>a</script>"#;
        let blocks = extract_scripts(source, "vue");
        assert_eq!(blocks.len(), 1);
        assert_eq!(blocks[0].source_text, "a");
    }
}
