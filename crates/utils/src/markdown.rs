use std::borrow::Cow;
use std::collections::BTreeMap;

use comrak::nodes::{AstNode, LineColumn, NodeValue};
use comrak::{Arena, Options, parse_document};
use rustc_hash::{FxHashMap, FxHashSet};
use serde_saphyr::granit_parser::{Event, Parser, ScalarStyle};

/// Separator between FQN segments. A `#` or `\` inside heading text is
/// escaped with `\` in the segment, so the FQN splits unambiguously.
pub const FQN_SEPARATOR: &str = "#";

pub const MAX_SECTIONS_PER_FILE: usize = 10_000;

pub const FRONT_MATTER_NAME: &str = "front_matter";

const FRONT_MATTER_DELIMITER: &str = "---";

const MAX_FRONT_MATTER_YAML_BYTES: usize = 64 * 1024;

const MAX_FRONT_MATTER_VALUE_BYTES: usize = 4 * 1024;

const METADATA_KEY: &str = "metadata";

#[derive(Debug)]
pub struct FrontMatterField {
    pub key: &'static str,
    pub column: &'static str,
}

const fn field(key: &'static str, column: &'static str) -> FrontMatterField {
    FrontMatterField { key, column }
}

pub const FRONT_MATTER_FIELDS: [FrontMatterField; 12] = [
    field("name", "front_matter_name"),
    field("description", "front_matter_description"),
    field("license", "front_matter_license"),
    field("compatibility", "front_matter_compatibility"),
    field("allowed-tools", "front_matter_allowed_tools"),
    field(METADATA_KEY, "front_matter_metadata"),
    field("title", "front_matter_title"),
    field("stage", "front_matter_stage"),
    field("group", "front_matter_group"),
    field("info", "front_matter_info"),
    field("version", "front_matter_version"),
    field("category", "front_matter_category"),
];

pub type FrontMatterValues = [String; FRONT_MATTER_FIELDS.len()];

#[derive(Debug)]
pub struct Document {
    pub front_matter: Option<FrontMatter>,
    pub sections: Vec<Section>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FrontMatter {
    pub start: usize,
    pub end: usize,
    pub values: FrontMatterValues,
}

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
    pub end: usize,
}

/// Only headings at the top level of the document count as sections: a heading
/// inside a block quote or list item is quoted content, not document structure.
pub fn parse(source: &str) -> Document {
    let arena = Arena::new();
    let mut options = Options::default();
    options.extension.front_matter_delimiter = Some(FRONT_MATTER_DELIMITER.to_string());
    let root = parse_document(&arena, source, &options);
    let lines = LineIndex::new(source);

    let mut front_matter = None;
    let mut sections: Vec<Section> = Vec::new();
    let mut open: Vec<(u8, usize)> = Vec::new();

    for node in root.children() {
        let data = node.data.borrow();
        let heading = match &data.value {
            NodeValue::Heading(heading) => heading,
            NodeValue::FrontMatter(block) => {
                front_matter = Some(parse_front_matter(source, block));
                continue;
            }
            _ => continue,
        };
        let name = inline_text(node);
        if name.is_empty() {
            continue;
        }
        let start = lines.byte(data.sourcepos.start);
        while let Some(&(level, index)) = open.last()
            && level >= heading.level
        {
            sections[index].end = start;
            open.pop();
        }
        if sections.len() == MAX_SECTIONS_PER_FILE {
            continue;
        }
        let parent = open.last().map(|&(_, index)| index);
        open.push((heading.level, sections.len()));
        sections.push(Section {
            segment: escape_segment(&name),
            name,
            parent,
            start,
            heading_end: lines.next_line_start(data.sourcepos.end.line),
            end: source.len(),
        });
    }
    let reserved = front_matter.as_ref().map(|_| FRONT_MATTER_NAME);
    assign_unique_segments(&mut sections, reserved);
    Document {
        front_matter,
        sections,
    }
}

/// `block` is comrak's front matter slice of `source`: it starts after any
/// byte order mark and runs to the end of the blank line after the closing
/// delimiter, when there is one.
fn parse_front_matter(source: &str, block: &str) -> FrontMatter {
    let start = source.len() - source.strip_prefix('\u{feff}').unwrap_or(source).len();
    let through_delimiter = block.trim_end_matches(['\r', '\n']);
    let line_ending = &block[through_delimiter.len()..];
    let line_ending = if line_ending.starts_with("\r\n") {
        2
    } else {
        usize::from(line_ending.starts_with('\n'))
    };
    let yaml = through_delimiter
        .strip_suffix(FRONT_MATTER_DELIMITER)
        .and_then(|inner| inner.split_once('\n'))
        .map_or("", |(_, yaml)| yaml);
    FrontMatter {
        start,
        end: start + through_delimiter.len() + line_ending,
        values: front_matter_values(yaml),
    }
}

fn front_matter_values(yaml: &str) -> FrontMatterValues {
    if yaml.len() > MAX_FRONT_MATTER_YAML_BYTES {
        return FrontMatterValues::default();
    }
    let mut events = YamlEvents(Parser::new_from_str(yaml));
    read_values(&mut events)
        .filter(|_| events.all_valid())
        .map(|values| values.map(truncate_value))
        .unwrap_or_default()
}

fn read_values(events: &mut YamlEvents<'_>) -> Option<FrontMatterValues> {
    let mut values = FrontMatterValues::default();
    let Event::StreamStart = events.next()? else {
        return None;
    };
    match events.next()? {
        Event::StreamEnd => return Some(values),
        Event::DocumentStart(..) => {}
        _ => return None,
    }
    let Event::MappingStart(..) = events.next()? else {
        return None;
    };
    while let Entry::Pair(key, value) = events.entry()? {
        let slot = key
            .as_deref()
            .and_then(|key| FRONT_MATTER_FIELDS.iter().position(|f| f.key == key));
        let Some(slot) = slot else {
            events.skip(value)?;
            continue;
        };
        values[slot] = if FRONT_MATTER_FIELDS[slot].key == METADATA_KEY {
            events.metadata_json(value)?
        } else {
            events.scalar_text(value)?
        };
    }
    Some(values)
}

fn truncate_value(mut value: String) -> String {
    value.truncate(value.floor_char_boundary(MAX_FRONT_MATTER_VALUE_BYTES));
    value
}

enum Entry<'a> {
    End,
    Pair(Option<Cow<'a, str>>, Event<'a>),
}

struct YamlEvents<'a>(Parser<'a, serde_saphyr::granit_parser::StrInput<'a>>);

impl<'a> YamlEvents<'a> {
    fn next(&mut self) -> Option<Event<'a>> {
        loop {
            match self.0.next()? {
                Ok((Event::Comment(..), _)) => continue,
                Ok((event, _)) => return Some(event),
                Err(_) => return None,
            }
        }
    }

    fn all_valid(&mut self) -> bool {
        self.0.all(|event| event.is_ok())
    }

    fn entry(&mut self) -> Option<Entry<'a>> {
        let key = match self.next()? {
            Event::MappingEnd => return Some(Entry::End),
            Event::Scalar(key, ..) => Some(key),
            other => {
                self.skip(other)?;
                None
            }
        };
        Some(Entry::Pair(key, self.next()?))
    }

    fn skip(&mut self, first: Event<'a>) -> Option<()> {
        let mut depth = 0usize;
        let mut event = first;
        loop {
            match event {
                Event::SequenceStart(..) | Event::MappingStart(..) => depth += 1,
                Event::SequenceEnd | Event::MappingEnd => depth -= 1,
                _ => {}
            }
            if depth == 0 {
                return Some(());
            }
            event = self.next()?;
        }
    }

    fn scalar_text(&mut self, value: Event<'a>) -> Option<String> {
        match value {
            Event::Scalar(text, style, ..) if !is_null(&text, style) => Some(text.into_owned()),
            other => self.skip(other).map(|()| String::new()),
        }
    }

    fn metadata_json(&mut self, value: Event<'a>) -> Option<String> {
        let Event::MappingStart(..) = value else {
            return self.skip(value).map(|()| String::new());
        };
        let mut metadata = BTreeMap::new();
        while let Entry::Pair(key, value) = self.entry()? {
            let text = self.scalar_text(value)?;
            if let Some(key) = key
                && !text.is_empty()
            {
                metadata.insert(key.into_owned(), text);
            }
        }
        if metadata.is_empty() {
            return Some(String::new());
        }
        serde_json::to_string(&metadata).ok()
    }
}

fn is_null(text: &str, style: ScalarStyle) -> bool {
    style == ScalarStyle::Plain && matches!(text, "" | "~" | "null" | "Null" | "NULL")
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

fn assign_unique_segments(sections: &mut [Section], reserved_top_level: Option<&str>) {
    let mut siblings: FxHashMap<Option<usize>, SiblingSegments> = FxHashMap::default();
    for section in sections.iter() {
        siblings
            .entry(section.parent)
            .or_default()
            .used
            .insert(section.segment.clone());
    }
    if let Some(segment) = reserved_top_level {
        siblings.entry(None).or_default().claim(segment.to_string());
    }
    for section in sections.iter_mut() {
        let segment = std::mem::take(&mut section.segment);
        section.segment = siblings.entry(section.parent).or_default().claim(segment);
    }
}

#[derive(Default)]
struct SiblingSegments {
    used: FxHashSet<String>,
    next_suffix: FxHashMap<String, usize>,
}

impl SiblingSegments {
    fn claim(&mut self, segment: String) -> String {
        let Some(next) = self.next_suffix.get_mut(&segment) else {
            self.next_suffix.insert(segment.clone(), 1);
            return segment;
        };
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

/// Zero-based (line, byte column) positions. A lone `\r` ends a line, as in
/// CommonMark, so line numbers agree with comrak's.
pub struct LineIndex {
    starts: Vec<usize>,
    len: usize,
}

impl LineIndex {
    pub fn new(source: &str) -> Self {
        let bytes = source.as_bytes();
        let mut starts = vec![0];
        starts.extend(
            memchr::memchr2_iter(b'\n', b'\r', bytes)
                .filter(|&i| bytes[i] == b'\n' || bytes.get(i + 1) != Some(&b'\n'))
                .map(|i| i + 1),
        );
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

    /// The position just past the last character of `source[..end]`, before
    /// its trailing line ending, so a range ends on its own last line.
    pub fn end_position(&self, source: &str, end: usize) -> (usize, usize) {
        let content = source.get(..end).unwrap_or(source);
        let content = content
            .strip_suffix("\r\n")
            .or_else(|| content.strip_suffix(['\n', '\r']))
            .unwrap_or(content);
        self.position(content.len())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sections(source: &str) -> Vec<Section> {
        parse(source).sections
    }

    fn front_matter(source: &str) -> FrontMatter {
        parse(source).front_matter.expect("front matter")
    }

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
    fn generated_suffixes_skip_real_segments_that_come_later() {
        assert_eq!(fqns("# X\n\n# X\n\n# X-1\n"), vec!["X", "X-2", "X-1"]);
    }

    #[test]
    fn ten_thousand_duplicate_siblings_get_sequential_suffixes() {
        let parsed = sections(&"## X\n".repeat(MAX_SECTIONS_PER_FILE));
        assert_eq!(parsed.len(), MAX_SECTIONS_PER_FILE);
        assert_eq!(parsed[0].segment, "X");
        assert_eq!(parsed[MAX_SECTIONS_PER_FILE - 1].segment, "X-9999");
    }

    #[test]
    fn headings_past_the_cap_still_end_the_sections_before_them() {
        let capped = "## X\n".repeat(MAX_SECTIONS_PER_FILE);
        let source = format!("# Top\n{capped}# Past\n");
        let parsed = sections(&source);
        assert_eq!(parsed.len(), MAX_SECTIONS_PER_FILE);
        assert_eq!(parsed[0].end, source.find("# Past").unwrap());
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
            "a\rb\r# H\n",
            "x\r   # H\n\u{e9}\u{e9}\n",
            "x\r# A\n   ## B\n\u{e9}\u{e9}\n",
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
    fn lone_carriage_returns_end_lines() {
        let source = "intro\r\r# One\n\n## Two\n";
        let parsed = sections(source);
        assert_eq!(parsed[0].start, source.find("# One").unwrap());
        assert_eq!(parsed[1].start, source.find("## Two").unwrap());
    }

    #[test]
    fn positions_are_zero_based_lines_and_byte_columns() {
        let lines = LineIndex::new("ab\n\u{e9}c\n");
        assert_eq!(lines.position(0), (0, 0));
        assert_eq!(lines.position(3), (1, 0));
        assert_eq!(lines.position(5), (1, 2));
    }

    #[test]
    fn end_positions_stay_on_the_last_line_of_the_range() {
        let source = "# A\r\nbody\r\n# B\nend";
        let lines = LineIndex::new(source);
        assert_eq!(
            lines.end_position(source, source.find("# B").unwrap()),
            (1, 4)
        );
        assert_eq!(lines.end_position(source, source.len()), (3, 3));
    }

    #[test]
    fn front_matter_ends_at_its_closing_delimiter_line() {
        let block = "---\ntitle: Doc\n---\n";
        for source in [
            format!("{block}# Real\n"),
            format!("{block}\n# Real\n"),
            format!("{block}\n\n\n# Real\n"),
        ] {
            let parsed = parse(&source);
            let front_matter = parsed.front_matter.expect("front matter");
            assert_eq!((front_matter.start, front_matter.end), (0, block.len()));
            assert_eq!(parsed.sections[0].start, source.find("# Real").unwrap());
        }
    }

    #[test]
    fn front_matter_ranges_handle_crlf_eof_and_a_byte_order_mark() {
        let source = "---\r\ntitle: Doc\r\n---\r\n\r\n# Real\r\n";
        let crlf = front_matter(source);
        assert_eq!(crlf.end, "---\r\ntitle: Doc\r\n---\r\n".len());

        let at_eof = "---\ntitle: Doc\n---";
        assert_eq!(front_matter(at_eof).end, at_eof.len());

        let with_bom = "\u{feff}---\ntitle: Doc\n---\n";
        let bom = front_matter(with_bom);
        assert_eq!((bom.start, bom.end), (3, with_bom.len()));
    }

    #[test]
    fn a_blank_front_matter_block_counts_and_adjacent_delimiters_do_not() {
        let blank = "---\n\n---\n\nBody\n";
        assert_eq!(front_matter(blank).end, "---\n\n---\n".len());
        assert!(parse("---\n---\n\nBody\n").front_matter.is_none());
    }

    #[test]
    fn a_top_level_heading_named_like_the_front_matter_is_suffixed() {
        let source = "---\ntitle: Doc\n---\n\n# front_matter\n\n## front_matter\n";
        assert_eq!(
            fqns(source),
            vec!["front_matter-1", "front_matter-1#front_matter"]
        );
        assert_eq!(fqns("# front_matter\n"), vec!["front_matter"]);
    }

    #[test]
    fn front_matter_ranges_stay_on_char_boundaries() {
        for source in [
            "\u{feff}---\nt\u{e9}tle: \u{4f60}\n---\n",
            "---\n\u{e9}: x\n  \u{4f60}\n---",
            "---\r\n\"\u{e9}\": 1\r\n---\r\n\r\n",
        ] {
            let FrontMatter { start, end, .. } = front_matter(source);
            assert!(
                start <= end && end <= source.len(),
                "{start}..{end} in {source:?}"
            );
            assert!(
                source.get(start..end).is_some(),
                "{start}..{end} in {source:?}"
            );
        }
    }

    fn values(source: &str) -> Vec<(&'static str, String)> {
        let values = front_matter(source).values;
        FRONT_MATTER_FIELDS
            .iter()
            .zip(values)
            .filter(|(_, value)| !value.is_empty())
            .map(|(field, value)| (field.column, value))
            .collect()
    }

    fn owned(pairs: &[(&'static str, &str)]) -> Vec<(&'static str, String)> {
        pairs.iter().map(|&(c, v)| (c, v.to_string())).collect()
    }

    #[test]
    fn agent_skills_fields_are_read_verbatim() {
        let source = "---\nname: review-mr\ndescription: >-\n  Review a merge\n  request.\nlicense: MIT\ncompatibility: Requires git\nallowed-tools: Bash(git:*) Read\nmetadata:\n  author: example-org\n  version: 1.0\n  tags: [a, b]\n  nested: {k: v}\n  empty: ~\nversion: 2.1.0\ncategory: Development Workflow\nunknown: ignored\n---\n# Review\n";
        assert_eq!(
            values(source),
            owned(&[
                ("front_matter_name", "review-mr"),
                ("front_matter_description", "Review a merge request."),
                ("front_matter_license", "MIT"),
                ("front_matter_compatibility", "Requires git"),
                ("front_matter_allowed_tools", "Bash(git:*) Read"),
                (
                    "front_matter_metadata",
                    r#"{"author":"example-org","version":"1.0"}"#
                ),
                ("front_matter_version", "2.1.0"),
                ("front_matter_category", "Development Workflow"),
            ])
        );
    }

    #[test]
    fn gitlab_docs_fields_are_read_verbatim() {
        let source = "---\r\nstage: Verify\r\ngroup: Pipeline Authoring\r\ninfo: To determine the technical writer, see https://example.com\r\ntitle: 'CI/CD: \"YAML\" syntax'\r\ndescription: Configure pipelines.\r\n---\r\n";
        assert_eq!(
            values(source),
            owned(&[
                ("front_matter_description", "Configure pipelines."),
                ("front_matter_title", "CI/CD: \"YAML\" syntax"),
                ("front_matter_stage", "Verify"),
                ("front_matter_group", "Pipeline Authoring"),
                (
                    "front_matter_info",
                    "To determine the technical writer, see https://example.com"
                ),
            ])
        );
    }

    #[test]
    fn non_string_scalars_keep_their_source_text_and_collections_are_skipped() {
        let source = "---\nversion: 1.10\nname: 0x1F\nstage: true\ngroup: null\ntitle: [a, b]\ninfo:\n  k: v\ncategory: '~'\nmetadata: [a]\n---\n";
        assert_eq!(
            values(source),
            owned(&[
                ("front_matter_name", "0x1F"),
                ("front_matter_stage", "true"),
                ("front_matter_version", "1.10"),
                ("front_matter_category", "~"),
            ])
        );
        assert!(values("---\nmetadata: plain\n---\n").is_empty());
        assert!(values("---\nmetadata: {list: [1]}\n---\n").is_empty());
    }

    #[test]
    fn long_values_are_truncated_on_a_char_boundary() {
        let long = "\u{e9}".repeat(MAX_FRONT_MATTER_VALUE_BYTES);
        let source = format!("---\ndescription: a{long}\n---\n");
        let [(_, value)] = values(&source).try_into().unwrap();
        assert_eq!(value.len(), MAX_FRONT_MATTER_VALUE_BYTES - 1);
        assert!(value.starts_with("a\u{e9}"));
    }

    #[test]
    fn invalid_or_non_mapping_yaml_stores_no_values() {
        for source in [
            "---\nname: [unclosed\n---\n",
            "---\nname: a\nbad: \"unterminated\n---\n",
            "---\n- name\n- title\n---\n",
            "---\njust text\n---\n",
            "---\n\n---\n",
        ] {
            assert!(values(source).is_empty(), "{source:?}");
            assert!(parse(source).front_matter.is_some(), "{source:?}");
        }
        let huge = format!(
            "---\nname: a\nfiller: {}\n---\n",
            "x".repeat(MAX_FRONT_MATTER_YAML_BYTES)
        );
        assert!(values(&huge).is_empty());
    }
}
