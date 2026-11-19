use std::borrow::Cow;
use std::collections::BTreeMap;

use comrak::html::collect_text;
use comrak::nodes::{AstNode, LineColumn, NodeValue};
use comrak::{Anchorizer, Arena, Options, parse_document};
use percent_encoding::percent_decode_str;
use rustc_hash::{FxHashMap, FxHashSet};
use serde_saphyr::granit_parser::{Event, Parser, ScalarStyle};

/// Separator between FQN segments. A `#` or `\` inside heading text or the
/// file stem is escaped with `\` in the segment, so the FQN splits
/// unambiguously.
pub const FQN_SEPARATOR: &str = "#";

pub const MAX_SECTIONS_PER_FILE: usize = 10_000;

pub const MAX_LINKS_PER_FILE: usize = 10_000;

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
    pub links: Vec<Link>,
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
    /// GitLab's rendered fragment, whose duplicate suffixes also count
    /// headings in block quotes and list items.
    pub anchor: String,
    pub parent: Option<usize>,
    pub start: usize,
    /// Byte offset of the line after the heading (setext underline included).
    pub heading_end: usize,
    pub end: usize,
}

/// Only headings at the top level of the document count as sections: a heading
/// inside a block quote or list item is quoted content, not document structure.
/// Agent instruction files also turn their `@path` imports into links.
pub fn parse(source: &str, agent_file: Option<AgentFile>) -> Document {
    let arena = Arena::new();
    let mut options = Options::default();
    options.extension.front_matter_delimiter = Some(FRONT_MATTER_DELIMITER.to_string());
    let root = parse_document(&arena, source, &options);
    let lines = LineIndex::new(source);

    let mut front_matter = None;
    let imports = agent_file == Some(AgentFile::Instructions);
    let (top_level_anchors, links) = anchors_and_links(root, &lines, imports);
    let mut top_level_anchors = top_level_anchors.into_iter();
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
        let anchor = top_level_anchors.next().unwrap_or_default();
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
            anchor,
            parent,
            start,
            heading_end: lines.next_line_start(data.sourcepos.end.line),
            end: source.len(),
        });
    }
    let reserved = front_matter.as_ref().map(|_| FRONT_MATTER_NAME);
    assign_unique_segments(&mut sections, reserved);
    let links = links
        .into_iter()
        .map(|(start, link)| Link {
            section: innermost_section(&sections, start),
            ..link
        })
        .collect();
    Document {
        front_matter,
        sections,
        links,
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

fn innermost_section(sections: &[Section], offset: usize) -> Option<usize> {
    let mut index = sections
        .partition_point(|section| section.start <= offset)
        .checked_sub(1)?;
    while sections[index].end <= offset {
        index = sections[index].parent?;
    }
    Some(index)
}

fn anchors_and_links<'a>(
    root: &'a AstNode<'a>,
    lines: &LineIndex,
    imports: bool,
) -> (Vec<String>, Vec<(usize, Link)>) {
    let mut anchorizer = UniqueAnchors::default();
    let mut top_level_anchors = Vec::new();
    let mut links = Vec::new();
    let link = |url, image| Link {
        url,
        image,
        section: None,
    };
    for node in root.descendants() {
        let data = node.data.borrow();
        let room = MAX_LINKS_PER_FILE - links.len();
        let found = match &data.value {
            NodeValue::Heading(_) => {
                let anchor = anchorizer.anchorize(&collect_text(node));
                if node
                    .parent()
                    .is_some_and(|parent| std::ptr::eq(parent, root))
                {
                    top_level_anchors.push(anchor);
                }
                continue;
            }
            NodeValue::Link(target) => vec![link(target.url.clone(), false)],
            NodeValue::Image(target) => vec![link(target.url.clone(), true)],
            NodeValue::Text(text) if imports && room > 0 => import_paths(text, room)
                .into_iter()
                .map(|path| link(path, false))
                .collect(),
            _ => continue,
        };
        let start = lines.byte(data.sourcepos.start);
        links.extend(found.into_iter().take(room).map(|found| (start, found)));
    }
    (top_level_anchors, links)
}

/// Claude Code's import syntax: `@` at the start of a text run or after
/// whitespace, then non-space characters or `\ `, with any `#` fragment
/// dropped. Only file-relative paths are kept: `~/` and `/` paths point
/// outside the repository.
fn import_paths(text: &str, limit: usize) -> Vec<String> {
    let mut paths = Vec::new();
    let mut offset = 0;
    while paths.len() < limit
        && let Some(at) = text[offset..].find('@').map(|index| offset + index)
    {
        offset = at + 1;
        let after_space = text[..at]
            .chars()
            .next_back()
            .is_none_or(char::is_whitespace);
        if !after_space {
            continue;
        }
        let length = import_length(&text[offset..]);
        let path = &text[offset..offset + length];
        offset += length;
        let path = path.split_once('#').map_or(path, |(path, _)| path);
        let relative = path
            .chars()
            .next()
            .is_some_and(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-'));
        if relative {
            paths.push(path.replace("\\ ", " "));
        }
    }
    paths
}

fn import_length(text: &str) -> usize {
    let mut chars = text.char_indices().peekable();
    while let Some((index, c)) = chars.next() {
        if c == '\\' && chars.next_if(|&(_, next)| next == ' ').is_some() {
            continue;
        }
        if c == '\\' || c.is_whitespace() {
            return index;
        }
    }
    text.len()
}

/// Same anchors as comrak's `Anchorizer`, in linear time: each base resumes
/// after its last suffix instead of probing from zero.
#[derive(Default)]
struct UniqueAnchors {
    taken: FxHashSet<String>,
    next_suffix: FxHashMap<String, usize>,
}

impl UniqueAnchors {
    fn anchorize(&mut self, heading: &str) -> String {
        let base = Anchorizer::new().anchorize(heading);
        let suffix = self.next_suffix.entry(base.clone()).or_insert(0);
        let anchor = loop {
            let candidate = match *suffix {
                0 => base.clone(),
                n => format!("{base}-{n}"),
            };
            *suffix += 1;
            if !self.taken.contains(&candidate) {
                break candidate;
            }
        };
        self.taken.insert(anchor.clone());
        anchor
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Link {
    pub url: String,
    pub image: bool,
    /// Index of the innermost section containing the link; `None` before the
    /// first heading.
    pub section: Option<usize>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct LinkTarget {
    pub path: String,
    pub anchor: Option<String>,
}

impl Link {
    pub fn target(&self, source_path: &str) -> Option<LinkTarget> {
        let url = self.url.trim();
        if url.starts_with("//") || has_scheme(url) {
            return None;
        }
        let (path, fragment) = url
            .split_once('#')
            .map_or((url, None), |(path, fragment)| (path, Some(fragment)));
        let path = path.split_once('?').map_or(path, |(path, _)| path);
        if path.is_empty() {
            return None;
        }
        let path = percent_decode(path)?;
        let directory = match path.starts_with('/') {
            true => "",
            false => source_path
                .rsplit_once('/')
                .map_or("", |(directory, _)| directory),
        };
        let path = normalize_path(directory, &path)?;
        if path == source_path {
            return None;
        }
        let anchor = match fragment.filter(|fragment| !self.image && !fragment.is_empty()) {
            Some(fragment) => Some(percent_decode(fragment)?),
            None => None,
        };
        Some(LinkTarget { path, anchor })
    }
}

/// RFC 3986: a scheme is a letter, then letters, digits, `+`, `-` or `.`, then `:`.
fn has_scheme(url: &str) -> bool {
    let Some((scheme, _)) = url.split_once(':') else {
        return false;
    };
    let mut chars = scheme.chars();
    chars.next().is_some_and(|c| c.is_ascii_alphabetic())
        && chars.all(|c| c.is_ascii_alphanumeric() || matches!(c, '+' | '-' | '.'))
}

fn percent_decode(text: &str) -> Option<String> {
    percent_decode_str(text)
        .decode_utf8()
        .ok()
        .map(|decoded| decoded.into_owned())
}

fn normalize_path(directory: &str, path: &str) -> Option<String> {
    let mut segments: Vec<&str> = directory.split('/').filter(|s| !s.is_empty()).collect();
    for segment in path.split('/') {
        match segment {
            "" | "." => {}
            ".." => {
                segments.pop()?;
            }
            segment => segments.push(segment),
        }
    }
    (!segments.is_empty()).then(|| segments.join("/"))
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

pub fn escape_segment(name: &str) -> String {
    name.replace('\\', "\\\\").replace('#', "\\#")
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AgentFile {
    Instructions,
    Skill,
}

impl AgentFile {
    pub fn classify(path: &str) -> Option<Self> {
        match split_file_name(path).1 {
            "AGENTS.md" | "CLAUDE.md" | "CLAUDE.local.md" => Some(Self::Instructions),
            "SKILL.md" => Some(Self::Skill),
            _ => None,
        }
    }

    pub fn definition_type(self) -> &'static str {
        match self {
            Self::Instructions => "AgentInstructions",
            Self::Skill => "Skill",
        }
    }

    /// A skill without a front matter `name` is named after its directory, as
    /// skill loaders do.
    pub fn name<'a>(self, path: &'a str, front_matter: Option<&'a FrontMatter>) -> &'a str {
        let (directory, file_name) = split_file_name(path);
        if self == Self::Instructions {
            return file_name;
        }
        let declared = front_matter.and_then(|block| {
            FRONT_MATTER_FIELDS
                .iter()
                .zip(&block.values)
                .find(|(field, _)| field.key == "name")
                .map(|(_, value)| value.as_str())
        });
        match declared {
            Some(name) if !name.is_empty() => name,
            _ => directory
                .and_then(|directory| directory.rsplit('/').next())
                .unwrap_or(file_name),
        }
    }
}

fn split_file_name(path: &str) -> (Option<&str>, &str) {
    path.rsplit_once('/')
        .map_or((None, path), |(directory, file_name)| {
            (Some(directory), file_name)
        })
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
        parse(source, None).sections
    }

    fn front_matter(source: &str) -> FrontMatter {
        parse(source, None).front_matter.expect("front matter")
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

    fn anchors(source: &str) -> Vec<String> {
        sections(source).into_iter().map(|s| s.anchor).collect()
    }

    /// Expected anchors come from GitLab 19.6's renderer (`POST /api/v4/markdown`).
    #[test]
    fn anchors_match_gitlab_including_quoted_and_listed_headings() {
        let source = "# Edge cases\n\n> ## Install\n\n- ## Install\n\n## Install\n\n\
            ## Caf\u{e9} \u{fc}ber\n\n## *Emph* and `code`\n\n## What's new?\n\n## A -- B\n\n\
            ## 123\n\n## Install 1\n\n## Install\n\nSetext Heading\n--------------\n\n\
            ## C++ & C#\n\n## <kbd>Ctrl</kbd> key\n\n## :smile: Emoji\n\n## Trailing -\n\n\
            ## Link [text](x.md)\n";
        assert_eq!(
            anchors(source),
            vec![
                "edge-cases",
                "install-2",
                "caf\u{e9}-\u{fc}ber",
                "emph-and-code",
                "whats-new",
                "a----b",
                "123",
                "install-1-1",
                "install-3",
                "setext-heading",
                "c--c",
                "ctrl-key",
                "smile-emoji",
                "trailing--",
                "link-text",
            ]
        );
    }

    #[test]
    fn many_repeated_headings_keep_gitlab_numbering() {
        let quoted = 100_000;
        let source = format!("## X-1\n{}## X\n## X\n", "> ## X\n".repeat(quoted));
        let anchors = anchors(&source);
        assert_eq!(anchors[0], "x-1");
        assert_eq!(anchors[1], format!("x-{}", quoted + 1));
        assert_eq!(anchors[2], format!("x-{}", quoted + 2));
    }

    #[test]
    fn empty_headings_still_take_an_anchor() {
        assert_eq!(anchors("##\n\n## ?\n\n## !\n"), vec!["-1", "-2"]);
    }

    fn target(url: &str) -> Option<(String, Option<String>)> {
        let link = Link {
            url: url.to_string(),
            image: false,
            section: None,
        };
        link.target("doc/user/guide.md")
            .map(|target| (target.path, target.anchor))
    }

    fn some(path: &str, anchor: Option<&str>) -> Option<(String, Option<String>)> {
        Some((path.to_string(), anchor.map(str::to_string)))
    }

    #[test]
    fn links_resolve_against_the_file_directory_or_the_repository_root() {
        assert_eq!(target("install.md"), some("doc/user/install.md", None));
        assert_eq!(target("./a/b.md#c"), some("doc/user/a/b.md", Some("c")));
        assert_eq!(target("../../README.md"), some("README.md", None));
        assert_eq!(target("/src/main.rs"), some("src/main.rs", None));
        assert_eq!(
            target("../api//x.md?plain=1#top"),
            some("doc/api/x.md", Some("top"))
        );
        assert_eq!(target("../../../outside.md"), None);
    }

    #[test]
    fn external_protocol_relative_and_same_document_links_have_no_target() {
        for url in [
            "https://example.com/a.md",
            "mailto:someone@example.com",
            "//cdn.example.com/a.png",
            "#section",
            "guide.md#section",
            "",
        ] {
            assert_eq!(target(url), None, "{url}");
        }
        assert_eq!(target("dir/a:b.md"), some("doc/user/dir/a:b.md", None));
    }

    #[test]
    fn link_paths_and_fragments_are_percent_decoded() {
        assert_eq!(
            target("my%20page.md#caf%C3%A9"),
            some("doc/user/my page.md", Some("caf\u{e9}"))
        );
        assert_eq!(target("bad%FF.md"), None);
    }

    #[test]
    fn images_target_the_file_and_never_a_heading() {
        let image = Link {
            url: "img/shot.png#frag".to_string(),
            image: true,
            section: None,
        };
        assert_eq!(
            image.target("doc/guide.md"),
            Some(LinkTarget {
                path: "doc/img/shot.png".to_string(),
                anchor: None,
            })
        );
    }

    #[test]
    fn links_and_images_are_collected_from_the_whole_document() {
        let source = "# A [one](one.md)\n\n> [two](two.md)\n\n- ![three](three.png)\n\n\
            [four][ref]\n\n```\n[not](code.md)\n```\n\n[ref]: <four four.md>\n";
        let links: Vec<(String, bool)> = parse(source, None)
            .links
            .into_iter()
            .map(|link| (link.url, link.image))
            .collect();
        assert_eq!(
            links,
            vec![
                ("one.md".to_string(), false),
                ("two.md".to_string(), false),
                ("three.png".to_string(), true),
                ("four four.md".to_string(), false),
            ]
        );
    }

    fn link_urls(source: &str, agent_file: Option<AgentFile>) -> Vec<String> {
        parse(source, agent_file)
            .links
            .into_iter()
            .map(|link| link.url)
            .collect()
    }

    #[test]
    fn agent_instruction_files_turn_at_imports_into_links() {
        let source = "@AGENTS.md\n\n# Rules\n\nRead @docs/x.md#setup, then @./my\\ notes.md.\n\
            Ask @someone or mail a@example.com.\n\
            [@linked.md](target.md) and *@emphasis.md*\n";
        assert_eq!(
            link_urls(source, Some(AgentFile::Instructions)),
            vec![
                "AGENTS.md",
                "docs/x.md",
                "./my notes.md.",
                "someone",
                "target.md",
                "linked.md",
                "emphasis.md",
            ]
        );
    }

    #[test]
    fn at_imports_skip_code_and_paths_outside_the_repository() {
        let source = "Use `@code.md` here.\n\n```\n@fenced.md\n```\n\n    @indented.md\n\n\
            @~/home.md @/etc/abs.md (@paren.md) x@glued.md @#only @@double.md @\n";
        assert!(link_urls(source, Some(AgentFile::Instructions)).is_empty());
    }

    #[test]
    fn at_imports_stop_at_the_limit() {
        assert_eq!(import_paths("@a @b @c @d", 2), vec!["a", "b"]);
    }

    #[test]
    fn at_imports_only_count_in_agent_instruction_files() {
        let source = "@AGENTS.md and [link](a.md)\n";
        assert_eq!(link_urls(source, Some(AgentFile::Skill)), vec!["a.md"]);
        assert_eq!(link_urls(source, None), vec!["a.md"]);
    }

    #[test]
    fn at_imports_belong_to_the_innermost_section() {
        let document = parse(
            "@top.md\n\n# One\n\n@one.md\n",
            Some(AgentFile::Instructions),
        );
        let sections: Vec<Option<usize>> = document.links.iter().map(|link| link.section).collect();
        assert_eq!(sections, vec![None, Some(0)]);
    }

    fn link_sections(source: &str) -> Vec<(String, Option<String>)> {
        let document = parse(source, None);
        document
            .links
            .into_iter()
            .map(|link| {
                let section = link.section.map(|i| document.sections[i].name.clone());
                (link.url, section)
            })
            .collect()
    }

    #[test]
    fn links_belong_to_the_innermost_section_containing_them() {
        let source = "Intro [a](a.md)\n\n# One [b](b.md)\n\n## Two\n\n> [c](c.md)\n\n\
            - item\n  ![d](d.png)\n\n# Three\n\nSetext\n---\n\n[e][ref]\n\n[ref]: e.md\n";
        let named =
            |url: &str, section: Option<&str>| (url.to_string(), section.map(str::to_string));
        assert_eq!(
            link_sections(source),
            vec![
                named("a.md", None),
                named("b.md", Some("One b")),
                named("c.md", Some("Two")),
                named("d.png", Some("Two")),
                named("e.md", Some("Setext")),
            ]
        );
    }

    #[test]
    fn links_under_headings_without_a_section_belong_to_the_enclosing_one() {
        let source = "---\ntitle: x\n---\n[a](a.md)\n\n# One\n\n## Two\n\n#\n\n[b](b.md)\n";
        assert_eq!(
            link_sections(source),
            vec![
                ("a.md".to_string(), None),
                ("b.md".to_string(), Some("Two".to_string()))
            ]
        );
        let mut capped = "# Root\n".to_string();
        capped.extend((1..MAX_SECTIONS_PER_FILE).map(|i| format!("## h{i}\n")));
        capped.push_str("## Uncounted\n\n[c](c.md)\n");
        assert_eq!(
            link_sections(&capped),
            vec![("c.md".to_string(), Some("Root".to_string()))]
        );
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
            let parsed = parse(&source, None);
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
        assert!(parse("---\n---\n\nBody\n", None).front_matter.is_none());
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
            assert!(parse(source, None).front_matter.is_some(), "{source:?}");
        }
        let huge = format!(
            "---\nname: a\nfiller: {}\n---\n",
            "x".repeat(MAX_FRONT_MATTER_YAML_BYTES)
        );
        assert!(values(&huge).is_empty());
    }

    #[test]
    fn agent_files_are_classified_by_exact_file_name() {
        let classified = [
            "AGENTS.md",
            "a/CLAUDE.md",
            "a/b/CLAUDE.local.md",
            "skills/x/SKILL.md",
            "agents.md",
            "Skill.md",
            "README.md",
        ]
        .map(AgentFile::classify);
        assert_eq!(
            classified,
            [
                Some(AgentFile::Instructions),
                Some(AgentFile::Instructions),
                Some(AgentFile::Instructions),
                Some(AgentFile::Skill),
                None,
                None,
                None,
            ]
        );
    }

    #[test]
    fn a_skill_is_named_by_front_matter_then_directory_then_file() {
        let named = parse("---\nname: review\n---\n", None);
        let unnamed = parse("---\nname: ''\n---\n", None);
        let skill = AgentFile::Skill;
        assert_eq!(
            skill.name("a/b/SKILL.md", named.front_matter.as_ref()),
            "review"
        );
        assert_eq!(
            skill.name("a/b/SKILL.md", unnamed.front_matter.as_ref()),
            "b"
        );
        assert_eq!(skill.name("a/b/SKILL.md", None), "b");
        assert_eq!(skill.name("SKILL.md", None), "SKILL.md");
        assert_eq!(
            AgentFile::Instructions.name("a/AGENTS.md", named.front_matter.as_ref()),
            "AGENTS.md"
        );
    }
}
