//! Structure-level memory accounting for the v2 pipeline.
//!
//! Emits sizes on the `codegraph_mem` tracing target at DEBUG. Every call site
//! is behind [`enabled`], so a production build that does not enable the target
//! pays one `Interest` check per phase boundary and never walks the graph.

use std::mem::{align_of, size_of};

use crate::v2::linker::CodeGraph;
use crate::v2::linker::graph::GraphNode;

pub const TARGET: &str = "codegraph_mem";

/// Two `EdgeIndex<u32>` link pointers stored alongside every petgraph node.
const PETGRAPH_NODE_LINKS: usize = 8;
/// Two `EdgeIndex<u32>` link pointers plus two `NodeIndex<u32>` endpoints.
const PETGRAPH_EDGE_LINKS: usize = 16;

/// petgraph stores the weight next to `u32` indices in one struct, so the slot is
/// padded to the wider of the weight's alignment and the index's. A five-byte edge
/// weight occupies 24 bytes, not 21.
fn petgraph_slot<T>(links: usize) -> usize {
    let align = align_of::<T>().max(align_of::<u32>());
    (size_of::<T>() + links).next_multiple_of(align)
}

#[inline]
pub fn enabled() -> bool {
    tracing::enabled!(target: "codegraph_mem", tracing::Level::DEBUG)
}

#[derive(Debug, Default, Clone, Copy)]
pub struct GraphBytes {
    pub node_count: usize,
    pub edge_count: usize,
    /// Slots petgraph has allocated. A single push past a reservation doubles
    /// these, so the spread against the counts is the memory a mis-sized
    /// `reserve` is holding for nothing.
    pub node_capacity: usize,
    pub edge_capacity: usize,
    pub def_count: usize,
    pub import_count: usize,
    pub string_count: usize,
    /// petgraph node array, excluding the `String`s the weights point at.
    pub nodes: usize,
    /// petgraph edge array.
    pub edges: usize,
    /// `String` payloads owned by Directory and File node weights.
    pub node_strings: usize,
    pub defs: usize,
    /// Boxed `GraphDefMeta` plus its SmallVec spills.
    pub def_metadata: usize,
    pub imports: usize,
    pub strings: usize,
    pub index_by_fqn: usize,
    pub index_by_name: usize,
    pub index_nested: usize,
    pub index_ancestors: usize,
    pub index_definition_ranges: usize,
    pub index_construction: usize,
}

impl GraphBytes {
    pub fn total(&self) -> usize {
        self.nodes
            + self.edges
            + self.node_strings
            + self.defs
            + self.def_metadata
            + self.imports
            + self.strings
            + self.index_by_fqn
            + self.index_by_name
            + self.index_nested
            + self.index_ancestors
            + self.index_definition_ranges
            + self.index_construction
    }
}

pub fn graph_bytes(graph: &CodeGraph) -> GraphBytes {
    use std::mem::size_of;

    let (node_cap, edge_cap) = graph.graph.capacity();
    let mut out = GraphBytes {
        node_count: graph.graph.node_count(),
        edge_count: graph.graph.edge_count(),
        node_capacity: node_cap,
        edge_capacity: edge_cap,
        def_count: graph.defs.len(),
        import_count: graph.imports.len(),
        string_count: graph.strings.len(),
        nodes: node_cap * petgraph_slot::<GraphNode>(PETGRAPH_NODE_LINKS),
        edges: edge_cap * petgraph_slot::<crate::v2::linker::graph::GraphEdge>(PETGRAPH_EDGE_LINKS),
        defs: graph.defs.capacity() * size_of::<crate::v2::linker::GraphDef>(),
        imports: graph.imports.capacity() * size_of::<crate::v2::linker::GraphImport>(),
        strings: graph.strings.heap_bytes(),
        ..Default::default()
    };

    for node in graph.graph.node_weights() {
        out.node_strings += match node {
            GraphNode::Directory(d) => d.path.capacity() + d.name.capacity(),
            GraphNode::File(f) => f.path.capacity() + f.name.capacity() + f.extension.capacity(),
            // `Arc<str>` is shared with the file node, so only the pointer is
            // charged here and it already sits inside `size_of::<GraphNode>()`.
            GraphNode::Definition { .. } | GraphNode::Import { .. } => 0,
        };
    }

    for def in &graph.defs {
        if let Some(meta) = &def.metadata {
            out.def_metadata += size_of::<crate::v2::linker::GraphDefMeta>()
                + meta.super_types.spilled_bytes()
                + meta.decorators.spilled_bytes();
        }
    }

    out.index_by_fqn = graph.indexes.by_fqn.heap_bytes();
    out.index_by_name = graph.indexes.by_name.heap_bytes();
    out.index_nested = graph.indexes.nested.heap_bytes();
    out.index_ancestors = map_bytes(
        graph.indexes.ancestors.capacity(),
        size_of::<petgraph::graph::NodeIndex>()
            + size_of::<smallvec::SmallVec<[petgraph::graph::NodeIndex; 8]>>(),
    ) + graph
        .indexes
        .ancestors
        .values()
        .map(spilled_node_indexes)
        .sum::<usize>();
    out.index_definition_ranges = map_bytes(
        graph.indexes.definition_ranges.capacity(),
        size_of::<String>() + size_of::<crate::v2::linker::state::DefinitionRangeIndex>(),
    ) + graph
        .indexes
        .definition_ranges
        .iter()
        .map(|(path, idx)| path.capacity() + idx.heap_bytes())
        .sum::<usize>();
    out.index_construction = graph
        .indexes
        .dir_index
        .as_ref()
        .map(|m| {
            map_bytes(
                m.capacity(),
                size_of::<String>() + size_of::<petgraph::graph::NodeIndex>(),
            ) + m.keys().map(|k| k.capacity()).sum::<usize>()
        })
        .unwrap_or(0)
        + graph
            .indexes
            .file_index
            .as_ref()
            .map(|m| {
                map_bytes(
                    m.capacity(),
                    size_of::<String>() + size_of::<petgraph::graph::NodeIndex>(),
                ) + m.keys().map(|k| k.capacity()).sum::<usize>()
            })
            .unwrap_or(0);

    out
}

/// hashbrown allocates the next power of two at or above `capacity * 8 / 7`,
/// and stores one control byte per bucket next to the entry array. Charging
/// `capacity` buckets instead understates a map by between 14% and a factor of
/// two, depending on where its capacity sits relative to the next power of two.
pub fn map_bytes(capacity: usize, entry_size: usize) -> usize {
    if capacity == 0 {
        return 0;
    }
    let buckets = capacity.saturating_mul(8).div_ceil(7).next_power_of_two();
    buckets * (entry_size + 1)
}

fn spilled_node_indexes(v: &smallvec::SmallVec<[petgraph::graph::NodeIndex; 8]>) -> usize {
    if v.spilled() {
        v.capacity() * std::mem::size_of::<petgraph::graph::NodeIndex>()
    } else {
        0
    }
}

pub trait SpilledBytes {
    fn spilled_bytes(&self) -> usize;
}

impl<A: smallvec::Array> SpilledBytes for smallvec::SmallVec<A> {
    fn spilled_bytes(&self) -> usize {
        if self.spilled() {
            self.capacity() * std::mem::size_of::<A::Item>()
        } else {
            0
        }
    }
}

#[derive(Debug, Default, Clone, Copy)]
pub struct ParseResultBytes {
    pub definitions: usize,
    pub imports: usize,
    /// Inferred return types and unresolved aliases, both keyed by index into
    /// the definitions and refs the graph build is about to consume.
    pub tail: usize,
}

pub fn parse_result_bytes(r: &crate::v2::dsl::engine::ParseFullResult) -> ParseResultBytes {
    use std::mem::size_of;
    ParseResultBytes {
        definitions: r.definitions.capacity() * size_of::<crate::v2::types::CanonicalDefinition>()
            + r.definitions.iter().map(definition_bytes).sum::<usize>(),
        imports: r.imports.capacity() * size_of::<crate::v2::types::CanonicalImport>()
            + r.imports.iter().map(import_bytes).sum::<usize>(),
        tail: r.inferred_returns.capacity() * size_of::<(u32, String)>()
            + r.inferred_returns
                .iter()
                .map(|(_, s)| s.capacity())
                .sum::<usize>()
            + r.unresolved_aliases.capacity() * size_of::<(usize, String)>()
            + r.unresolved_aliases
                .iter()
                .map(|(_, s)| s.capacity())
                .sum::<usize>(),
    }
}

/// What the parse barrier holds for the whole family.
#[derive(Debug, Default, Clone, Copy)]
pub struct ParseBarrierBytes {
    pub files_held: usize,
    pub def_count: usize,
    pub import_count: usize,
    pub definitions: usize,
    pub imports: usize,
    pub refpack_and_tail: usize,
}

impl ParseBarrierBytes {
    pub fn total(&self) -> usize {
        self.definitions + self.imports + self.refpack_and_tail
    }

    pub fn max(self, other: Self) -> Self {
        if other.total() > self.total() {
            other
        } else {
            self
        }
    }

    pub fn log(&self, family: &str, file_count: usize, parse_ms: f64) {
        tracing::debug!(
            target: "codegraph_mem",
            family,
            stage = "parse_barrier",
            file_count,
            files_held = self.files_held,
            def_count = self.def_count,
            import_count = self.import_count,
            parse_ms,
            bytes_definitions = self.definitions,
            bytes_imports = self.imports,
            bytes_refpack = self.refpack_and_tail,
            bytes_total = self.total(),
            "parse barrier"
        );
    }
}

pub fn parse_barrier_bytes(
    files: impl Iterator<Item = (ParseResultBytes, usize, usize, usize)>,
) -> ParseBarrierBytes {
    let mut out = ParseBarrierBytes::default();
    for (bytes, refpack_and_tail, defs, imports) in files {
        out.files_held += 1;
        out.def_count += defs;
        out.import_count += imports;
        out.definitions += bytes.definitions;
        out.imports += bytes.imports;
        out.refpack_and_tail += refpack_and_tail + bytes.tail;
    }
    out
}

pub fn definition_bytes(d: &crate::v2::types::CanonicalDefinition) -> usize {
    use std::mem::size_of;
    let meta = d.metadata.as_ref().map_or(0, |m| {
        size_of::<crate::v2::types::DefinitionMetadata>()
            + m.super_types.capacity() * size_of::<String>()
            + m.super_types.iter().map(String::capacity).sum::<usize>()
            + m.decorators.capacity() * size_of::<String>()
            + m.decorators.iter().map(String::capacity).sum::<usize>()
            + opt_str_bytes(&m.return_type)
            + opt_str_bytes(&m.type_annotation)
            + opt_str_bytes(&m.receiver_type)
            + opt_str_bytes(&m.companion_of)
    });
    d.name.capacity() + d.fqn.capacity() + meta
}

pub fn import_bytes(i: &crate::v2::types::CanonicalImport) -> usize {
    i.path.capacity()
        + opt_str_bytes(&i.name)
        + opt_str_bytes(&i.alias)
        + i.scope_fqn.as_ref().map_or(0, |f| f.capacity())
}

fn opt_str_bytes(s: &Option<String>) -> usize {
    s.as_ref().map_or(0, String::capacity)
}

pub fn log_graph(family: &str, stage: &str, graph: &CodeGraph) {
    if !enabled() {
        return;
    }
    let b = graph_bytes(graph);
    tracing::debug!(
        target: "codegraph_mem",
        family,
        stage,
        node_count = b.node_count,
        edge_count = b.edge_count,
        node_capacity = b.node_capacity,
        edge_capacity = b.edge_capacity,
        def_count = b.def_count,
        import_count = b.import_count,
        string_count = b.string_count,
        bytes_nodes = b.nodes,
        bytes_edges = b.edges,
        bytes_node_strings = b.node_strings,
        bytes_defs = b.defs,
        bytes_def_metadata = b.def_metadata,
        bytes_graph_imports = b.imports,
        bytes_strings = b.strings,
        bytes_index_by_fqn = b.index_by_fqn,
        bytes_index_by_name = b.index_by_name,
        bytes_index_nested = b.index_nested,
        bytes_index_ancestors = b.index_ancestors,
        bytes_index_definition_ranges = b.index_definition_ranges,
        bytes_index_construction = b.index_construction,
        bytes_total = b.total(),
        "graph"
    );
}
