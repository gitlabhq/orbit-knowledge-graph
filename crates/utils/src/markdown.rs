//! Markdown document structure shared by both code-graph engines, so the same
//! file yields the same sections, names, FQN segments and byte ranges in each.

use comrak::nodes::{AstNode, LineColumn, NodeValue};
use comrak::{Arena, Options, parse_document};
use rustc_hash::{FxHashMap, FxHashSet};

/// Separator between FQN segments. A `#` or `\` inside heading text is
/// escaped with `\` in the segment, so the FQN splits unambiguously.
pub const FQN_SEPARATOR: &str = "#";

pub const MAX_SECTIONS_PER_FILE: usize = 10_000;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Section {
    /// Plain heading text: inline markup and closing `#`s removed.
    pub name: String,
    /// The section's FQN segment: the escaped name, suffixed `-N` when a
    /// sibling already uses it.
    pub segment: String,
    pub parent: Option<usize>,
    pub start: usize,
    /// Byte offset of the line after the heading (setext underline included).
    pub heading_end: usize,
    /// Byte offset where the next heading of the same or a higher level
    /// starts, or the end of the file.
    pub end: usize,
}

/// The sections of a Markdown document. Only headings at the top level of the
/// document count: a heading inside a block quote or list item is quoted
/// content, not document structure. A leading `---` front matter block is
/// skipped, and headings with no text produce no section.
pub fn sections(source: &str) -> Vec<Section> {
    let arena = Arena::new();
    let mut options = Options::default();
    options.extension.front_matter_delimiter = Some("---".to_string());
    let root = parse_document(&arena, source, &options);
    let lines = LineIndex::new(source);

    let mut sections: Vec<Section> = Vec::new();
    let mut open: Vec<(u8, usize)> = Vec::new();
    let mut child_segments: Vec<SiblingSegments> = Vec::new();
    let mut root_segments = SiblingSegments::default();

    for node in root.children() {
        let data = node.data.borrow();
        let NodeValue::Heading(heading) = &data.value else {
            continue;
        };
        let name = inline_text(node);
        if name.is_empty() {
            continue;
        }
        if sections.len() == MAX_SECTIONS_PER_FILE {
            break;
        }
        let start = lines.byte(data.sourcepos.start);
        while let Some(&(level, index)) = open.last()
            && level >= heading.level
        {
            sections[index].end = start;
            open.pop();
        }
        let parent = open.last().map(|&(_, index)| index);
        let siblings = parent.map_or(&mut root_segments, |index| &mut child_segments[index]);
        let segment = siblings.claim(escape_segment(&name));
        open.push((heading.level, sections.len()));
        child_segments.push(SiblingSegments::default());
        sections.push(Section {
            name,
            segment,
            parent,
            start,
            heading_end: lines.next_line_start(data.sourcepos.end.line),
            end: source.len(),
        });
    }
    sections
}

pub fn segment_path(sections: &[Section], index: usize) -> Vec<&str> {
    let mut path = Vec::new();
    let mut current = Some(index);
    while let Some(i) = current {
        path.push(sections[i].segment.as_str());
        current = sections[i].parent;
    }
    path.reverse();
    path
}

fn escape_segment(name: &str) -> String {
    name.replace('\\', "\\\\").replace('#', "\\#")
}

#[derive(Default)]
struct SiblingSegments {
    used: FxHashSet<String>,
    next_suffix: FxHashMap<String, usize>,
}

impl SiblingSegments {
    fn claim(&mut self, segment: String) -> String {
        if self.used.insert(segment.clone()) {
            return segment;
        }
        let next = self.next_suffix.entry(segment.clone()).or_insert(1);
        loop {
            let candidate = format!("{segment}-{next}");
            *next += 1;
            if self.used.insert(candidate.clone()) {
                return candidate;
            }
        }
    }
}

fn inline_text<'a>(node: &'a AstNode<'a>) -> String {
    let mut text = String::new();
    for child in node.descendants().skip(1) {
        match &child.data.borrow().value {
            NodeValue::Text(t) => text.push_str(t),
            NodeValue::Code(code) => text.push_str(&code.literal),
            NodeValue::SoftBreak | NodeValue::LineBreak => text.push(' '),
            _ => {}
        }
    }
    text.trim().to_string()
}

/// Line starts of a source, for converting between byte offsets and
/// zero-based (line, byte column) positions.
pub struct LineIndex {
    starts: Vec<usize>,
    len: usize,
}

impl LineIndex {
    pub fn new(source: &str) -> Self {
        let mut starts = vec![0];
        starts.extend(memchr::memchr_iter(b'\n', source.as_bytes()).map(|i| i + 1));
        Self {
            starts,
            len: source.len(),
        }
    }

    fn byte(&self, position: LineColumn) -> usize {
        self.starts
            .get(position.line.saturating_sub(1))
            .map_or(0, |start| start + position.column.saturating_sub(1))
            .min(self.len)
    }

    fn next_line_start(&self, line: usize) -> usize {
        self.starts.get(line).copied().unwrap_or(self.len)
    }

    pub fn position(&self, offset: usize) -> (usize, usize) {
        let line = self
            .starts
            .partition_point(|start| *start <= offset)
            .saturating_sub(1);
        (line, offset - self.starts[line])
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fqns(source: &str) -> Vec<String> {
        let sections = sections(source);
        (0..sections.len())
            .map(|i| segment_path(&sections, i).join(FQN_SEPARATOR))
            .collect()
    }

    fn names(source: &str) -> Vec<String> {
        sections(source).into_iter().map(|s| s.name).collect()
    }

    #[test]
    fn setext_headings_open_sections_like_atx_ones() {
        let source = "Title\n=====\n\nIntro.\n\nPart\n----\n\nBody.\n\n# Next\n";
        assert_eq!(fqns(source), vec!["Title", "Title#Part", "Next"]);
        let parsed = sections(source);
        assert_eq!(parsed[0].heading_end, "Title\n=====\n".len());
        assert_eq!(parsed[1].end, source.find("# Next").unwrap());
    }

    #[test]
    fn names_drop_inline_markup_and_closing_hashes() {
        assert_eq!(
            names("## **Bold** `code` [link](https://example.com) heading\n\n# Closed ##\n"),
            vec!["Bold code link heading", "Closed"]
        );
    }

    #[test]
    fn hashes_and_backslashes_in_heading_text_are_escaped_in_segments() {
        let source = "# Guide\n\n## C# Basics\n\n### a\\\\#b\n";
        assert_eq!(names(source), vec!["Guide", "C# Basics", "a\\#b"]);
        assert_eq!(
            fqns(source),
            vec!["Guide", "Guide#C\\# Basics", "Guide#C\\# Basics#a\\\\\\#b"]
        );
    }

    #[test]
    fn headings_in_block_quotes_and_list_items_are_not_sections() {
        let source = "# Top\n\n> ## Quoted\n\n- ## Listed\n\n## Child\n";
        assert_eq!(fqns(source), vec!["Top", "Top#Child"]);
        assert_eq!(sections(source)[0].end, source.len());
    }

    #[test]
    fn duplicate_siblings_get_suffixed_segments_and_keep_their_name() {
        let source = "# A\n\n## X\n\n## X\n\n# A\n\n## X\n";
        assert_eq!(fqns(source), vec!["A", "A#X", "A#X-1", "A-1", "A-1#X"]);
        assert_eq!(names(source), vec!["A", "X", "X", "A", "X"]);
    }

    #[test]
    fn ten_thousand_duplicate_siblings_get_sequential_suffixes() {
        let parsed = sections(&"## X\n".repeat(MAX_SECTIONS_PER_FILE));
        assert_eq!(parsed.len(), MAX_SECTIONS_PER_FILE);
        assert_eq!(parsed[0].segment, "X");
        assert_eq!(parsed[MAX_SECTIONS_PER_FILE - 1].segment, "X-9999");
    }

    #[test]
    fn front_matter_and_fenced_code_are_not_headings() {
        let source = "---\ntitle: Doc\n---\n\n# Real\n\n```md\n# not a heading\n```\n";
        assert_eq!(names(source), vec!["Real"]);
    }

    #[test]
    fn ranges_stay_on_char_boundaries() {
        for source in [
            "\u{feff}# BOM lead\n",
            "Setext \u{e9}\n===\n\ntext\n",
            "# No trailing newline",
            "  ## Indented \u{4f60}\u{597d}\r\nbody\r\n",
        ] {
            for section in sections(source) {
                for offset in [section.start, section.heading_end, section.end] {
                    assert!(source.is_char_boundary(offset), "{offset} in {source:?}");
                }
                assert!(section.start <= section.heading_end && section.heading_end <= section.end);
            }
        }
    }

    #[test]
    fn positions_are_zero_based_lines_and_byte_columns() {
        let lines = LineIndex::new("ab\n\u{e9}c\n");
        assert_eq!(lines.position(0), (0, 0));
        assert_eq!(lines.position(3), (1, 0));
        assert_eq!(lines.position(5), (1, 2));
    }
}
