pub const NAME_CAP: usize = 512;
pub const FQN_CAP: usize = 1024;
pub const SIGNATURE_CAP: usize = 2048;
pub const PARAMETER_CAP: usize = 512;
pub const PARAMETERS_MAX: usize = 64;
pub const RETURN_TYPE_CAP: usize = 512;
pub const DECLARED_TYPE_CAP: usize = 1024;
pub const DOC_COMMENT_CAP: usize = 1024;
pub const GENERATED_BY_CAP: usize = 256;
pub const IMPORT_ALIAS_CAP: usize = 256;
pub const LINES_MAX: usize = 64;

const MARKER: char = '…';

pub fn collapse_whitespace(text: &str) -> String {
    let mut out = String::with_capacity(text.len().min(SIGNATURE_CAP + 8));
    for word in text.split_whitespace() {
        if !out.is_empty() {
            out.push(' ');
        }
        out.push_str(word);
    }
    out
}

fn floor_boundary(text: &str, max: usize) -> usize {
    let mut end = max.min(text.len());
    while end > 0 && !text.is_char_boundary(end) {
        end -= 1;
    }
    end
}

pub fn cap(mut text: String, max: usize) -> String {
    if text.len() <= max {
        return text;
    }
    let end = floor_boundary(&text, max - MARKER.len_utf8());
    text.truncate(end);
    text.push(MARKER);
    text
}

pub fn cap_at_word(mut text: String, max: usize) -> String {
    if text.len() <= max {
        return text;
    }
    let end = floor_boundary(&text, max - MARKER.len_utf8());
    let end = text[..end].rfind(' ').filter(|&i| i > 0).unwrap_or(end);
    text.truncate(end);
    text.push(MARKER);
    text
}

pub fn signature(text: &str) -> Option<String> {
    let text =
        collapse_whitespace(text.trim_end_matches(|c: char| {
            c.is_whitespace() || matches!(c, '{' | ':' | '=' | ';' | ',')
        }));
    (!text.is_empty()).then(|| cap(text, SIGNATURE_CAP))
}

pub fn parameters<'a>(items: impl IntoIterator<Item = &'a str>) -> Vec<String> {
    items
        .into_iter()
        .map(collapse_whitespace)
        .filter(|p| !p.is_empty())
        .take(PARAMETERS_MAX)
        .map(|p| cap(p, PARAMETER_CAP))
        .collect()
}

pub fn type_text(text: &str, max: usize) -> Option<String> {
    let text =
        collapse_whitespace(text.trim_start_matches(|c: char| c == ':' || c.is_whitespace()));
    (!text.is_empty()).then(|| cap(text, max))
}

pub fn doc_paragraph<'a>(lines: impl IntoIterator<Item = &'a str>) -> Option<String> {
    let mut out = String::new();
    for line in lines {
        let line = line.trim();
        if line.is_empty() {
            if out.is_empty() {
                continue;
            }
            break;
        }
        if !out.is_empty() {
            out.push(' ');
        }
        out.push_str(line);
        if out.len() > DOC_COMMENT_CAP * 2 {
            break;
        }
    }
    let out = collapse_whitespace(&out);
    (!out.is_empty()).then(|| cap_at_word(out, DOC_COMMENT_CAP))
}

pub fn strip_comment_markers<'a>(
    text: &'a str,
    markers: &'a [&'a str],
) -> impl Iterator<Item = &'a str> + 'a {
    text.lines().map(move |line| {
        let mut line = line.trim();
        for _ in 0..2 {
            for marker in markers {
                if let Some(rest) = line.strip_prefix(marker) {
                    line = rest.trim_start();
                }
                if let Some(rest) = line.strip_suffix(marker) {
                    line = rest.trim_end();
                }
            }
        }
        line
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cap_cuts_on_a_char_boundary_with_a_marker() {
        let text = "é".repeat(20);
        let capped = cap(text, 9);
        assert!(capped.len() <= 9);
        assert!(capped.ends_with('…'));
    }

    #[test]
    fn cap_at_word_cuts_after_a_word() {
        let capped = cap_at_word("alpha beta gamma".to_string(), 12);
        assert_eq!(capped, "alpha…");
    }

    #[test]
    fn signature_collapses_whitespace() {
        assert_eq!(
            signature("def  f(\n  a,\n  b\n):").as_deref(),
            Some("def f( a, b )")
        );
    }

    #[test]
    fn doc_paragraph_keeps_the_first_paragraph() {
        let lines = strip_comment_markers(
            "/**\n * Parses a date.\n * Strict.\n *\n * More.\n */",
            &["/**", "*/", "*"],
        );
        assert_eq!(
            doc_paragraph(lines).as_deref(),
            Some("Parses a date. Strict.")
        );
    }
}
