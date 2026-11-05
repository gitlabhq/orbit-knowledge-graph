use std::fmt;

use serde::de::{self, Deserialize, Deserializer, MapAccess, SeqAccess, Visitor};
use serde::ser::{Serialize, SerializeMap, SerializeSeq, Serializer};
use serde_saphyr::{FlowMap, FlowSeq};

use crate::DataModelError;
use crate::spec::Fragment;

const WIDTH: usize = 100;
const INDENT: usize = 2;

pub(super) fn render(fragment: &Fragment) -> Result<String, DataModelError> {
    let block = serde_saphyr::to_string(fragment).map_err(invalid)?;
    let node: Node = orbit_utils::yaml::from_str(&block).map_err(invalid)?;
    let Node::Map(entries) = &node else {
        return Err(DataModelError::Invalid(
            "a fragment is not a mapping".into(),
        ));
    };
    let yaml = serde_saphyr::to_string(&children(entries, 0)).map_err(invalid)?;
    Ok(separate_sections(&yaml))
}

fn invalid(error: impl fmt::Display) -> DataModelError {
    DataModelError::Invalid(error.to_string())
}

fn children(entries: &[(Node, Node)], indent: usize) -> Styled<'_> {
    Styled::Map(
        entries
            .iter()
            .map(|(key, value)| {
                let column = indent + flow_width(key) + 2;
                (key, layout(value, indent, column))
            })
            .collect(),
    )
}

fn layout(node: &Node, indent: usize, column: usize) -> Styled<'_> {
    match node {
        Node::Seq(_) | Node::Map(_) if fits(node, column) => Styled::Flow(node),
        Node::Seq(items) => {
            let item_column = indent + INDENT;
            let uniform = items.iter().all(|item| fits(item, item_column));
            Styled::Seq(
                items
                    .iter()
                    .map(|item| match item {
                        Node::Map(entries) if !uniform => children(entries, item_column),
                        item => layout(item, indent, item_column),
                    })
                    .collect(),
            )
        }
        Node::Map(entries) => children(entries, indent + INDENT),
        _ => Styled::Scalar(node),
    }
}

fn fits(node: &Node, column: usize) -> bool {
    !node.has_multiline() && column + flow_width(node) <= WIDTH
}

fn flow_width(node: &Node) -> usize {
    let separators = |count: usize| 2 * count.saturating_sub(1);
    match node {
        Node::Seq(items) => {
            2 + items.iter().map(flow_width).sum::<usize>() + separators(items.len())
        }
        Node::Map(entries) => {
            let pairs: usize = entries
                .iter()
                .map(|(key, value)| flow_width(key) + 2 + flow_width(value))
                .sum();
            2 + pairs + separators(entries.len())
        }
        scalar => serde_saphyr::to_string(&FlowSeq([scalar])).map_or(WIDTH, |text| {
            text.trim_end().chars().count().saturating_sub(2)
        }),
    }
}

fn separate_sections(yaml: &str) -> String {
    let mut separated = String::with_capacity(yaml.len());
    let mut entry_lines = 0;
    for line in yaml.lines() {
        let section_entry = line.starts_with("  ") && !line[2..].starts_with([' ', '-']);
        if section_entry && entry_lines > 1 {
            separated.push('\n');
        }
        if section_entry || !line.starts_with(' ') {
            entry_lines = 0;
        }
        entry_lines += 1;
        separated.push_str(line);
        separated.push('\n');
    }
    separated
}

#[derive(Debug)]
enum Node {
    Null,
    Bool(bool),
    Int(i64),
    UInt(u64),
    Float(f64),
    Str(String),
    Seq(Vec<Node>),
    Map(Vec<(Node, Node)>),
}

impl Node {
    fn has_multiline(&self) -> bool {
        match self {
            Self::Str(text) => text.contains('\n'),
            Self::Seq(items) => items.iter().any(Self::has_multiline),
            Self::Map(entries) => entries
                .iter()
                .any(|(key, value)| key.has_multiline() || value.has_multiline()),
            _ => false,
        }
    }
}

enum Styled<'a> {
    Scalar(&'a Node),
    Flow(&'a Node),
    Seq(Vec<Styled<'a>>),
    Map(Vec<(&'a Node, Styled<'a>)>),
}

struct Flow<'a>(&'a Node);

impl Serialize for Styled<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self {
            Self::Scalar(node) => node.serialize(serializer),
            Self::Flow(node) => Flow(node).serialize(serializer),
            Self::Seq(items) => {
                let mut seq = serializer.serialize_seq(Some(items.len()))?;
                for item in items {
                    seq.serialize_element(item)?;
                }
                seq.end()
            }
            Self::Map(entries) => {
                let mut map = serializer.serialize_map(Some(entries.len()))?;
                for (key, value) in entries {
                    map.serialize_entry(key, value)?;
                }
                map.end()
            }
        }
    }
}

impl Serialize for Flow<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self.0 {
            node @ Node::Seq(_) => FlowSeq(node).serialize(serializer),
            node @ Node::Map(_) => FlowMap(node).serialize(serializer),
            scalar => scalar.serialize(serializer),
        }
    }
}

impl Serialize for Node {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self {
            Self::Null => serializer.serialize_unit(),
            Self::Bool(value) => serializer.serialize_bool(*value),
            Self::Int(value) => serializer.serialize_i64(*value),
            Self::UInt(value) => serializer.serialize_u64(*value),
            Self::Float(value) => serializer.serialize_f64(*value),
            Self::Str(value) => serializer.serialize_str(value),
            Self::Seq(items) => {
                let mut seq = serializer.serialize_seq(Some(items.len()))?;
                for item in items {
                    seq.serialize_element(item)?;
                }
                seq.end()
            }
            Self::Map(entries) => {
                let mut map = serializer.serialize_map(Some(entries.len()))?;
                for (key, value) in entries {
                    map.serialize_entry(key, value)?;
                }
                map.end()
            }
        }
    }
}

impl<'de> Deserialize<'de> for Node {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        deserializer.deserialize_any(NodeVisitor)
    }
}

struct NodeVisitor;

impl<'de> Visitor<'de> for NodeVisitor {
    type Value = Node;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a YAML value")
    }

    fn visit_unit<E: de::Error>(self) -> Result<Node, E> {
        Ok(Node::Null)
    }

    fn visit_none<E: de::Error>(self) -> Result<Node, E> {
        Ok(Node::Null)
    }

    fn visit_bool<E: de::Error>(self, value: bool) -> Result<Node, E> {
        Ok(Node::Bool(value))
    }

    fn visit_i64<E: de::Error>(self, value: i64) -> Result<Node, E> {
        Ok(Node::Int(value))
    }

    fn visit_u64<E: de::Error>(self, value: u64) -> Result<Node, E> {
        Ok(Node::UInt(value))
    }

    fn visit_f64<E: de::Error>(self, value: f64) -> Result<Node, E> {
        Ok(Node::Float(value))
    }

    fn visit_str<E: de::Error>(self, value: &str) -> Result<Node, E> {
        Ok(Node::Str(value.to_string()))
    }

    fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<Node, A::Error> {
        let mut items = Vec::new();
        while let Some(item) = seq.next_element()? {
            items.push(item);
        }
        Ok(Node::Seq(items))
    }

    fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Node, A::Error> {
        let mut entries = Vec::new();
        while let Some(entry) = map.next_entry()? {
            entries.push(entry);
        }
        Ok(Node::Map(entries))
    }
}
