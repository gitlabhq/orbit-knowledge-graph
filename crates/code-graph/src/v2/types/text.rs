use orbit_utils::strings::truncate_bytes;

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

pub const CUT: &str = "…";

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

pub fn signature(text: &str) -> Option<String> {
    let text =
        collapse_whitespace(text.trim_end_matches(|c: char| {
            c.is_whitespace() || matches!(c, '{' | ':' | '=' | ';' | ',')
        }));
    (!text.is_empty()).then(|| truncate_bytes(&text, SIGNATURE_CAP, CUT, false).into_owned())
}

pub fn parameters<'a>(items: impl IntoIterator<Item = &'a str>) -> Vec<String> {
    items
        .into_iter()
        .map(collapse_whitespace)
        .filter(|p| !p.is_empty())
        .take(PARAMETERS_MAX)
        .map(|p| truncate_bytes(&p, PARAMETER_CAP, CUT, false).into_owned())
        .collect()
}

pub fn type_text(text: &str, max: usize) -> Option<String> {
    let text =
        collapse_whitespace(text.trim_start_matches(|c: char| c == ':' || c.is_whitespace()));
    (!text.is_empty()).then(|| truncate_bytes(&text, max, CUT, false).into_owned())
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
    (!out.is_empty()).then(|| truncate_bytes(&out, DOC_COMMENT_CAP, CUT, true).into_owned())
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
