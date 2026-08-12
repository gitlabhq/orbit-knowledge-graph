//! Arrow Row types for graph serialization, and shared indexes
//! (IncludeIndex, ReexportIndex) used by the resolver.

use gkg_utils::arrow::{AsRecordBatch, BatchBuilder, ColumnSpec, ColumnType};
use gkg_utils::strings::StringPool;

use super::concurrent_graph::{ConcurrentGraph, GraphDef, GraphImport, NodeId};
use crate::v2::types::Range;

// ── IncludeIndex ─────────────────────────────────────────────────────────────

/// Precomputed include-graph lookup tables for C/C++ resolution.
/// Built once before Phase 2 resolve, shared across all file resolvers.
#[derive(Default)]
pub struct IncludeIndex {
    /// file_path -> list of normalized include paths for that file
    pub include_map: rustc_hash::FxHashMap<String, Vec<String>>,
    /// suffix -> list of file NodeId values whose path ends with that suffix
    pub suffix_map: rustc_hash::FxHashMap<String, Vec<NodeId>>,
    /// file NodeId -> file path
    pub path_by_idx: rustc_hash::FxHashMap<NodeId, String>,
}

impl IncludeIndex {
    pub fn build(graph: &ConcurrentGraph) -> Self {
        let mut idx = Self::default();

        for (_node_id, file_path, imp) in graph.iter_imports() {
            let raw = graph.str(imp.path);
            let cleaned = raw.trim_matches('"').trim_matches('<').trim_matches('>');
            let normalized = cleaned.trim_start_matches("../").trim_start_matches("./");
            idx.include_map
                .entry(file_path.to_string())
                .or_default()
                .push(normalized.to_string());
        }

        for (file_id, path, _name, _ext, _lang, _size, _reason) in graph.iter_files() {
            idx.path_by_idx.insert(file_id, path.to_string());
            let mut start = 0;
            loop {
                idx.suffix_map
                    .entry(path[start..].to_string())
                    .or_default()
                    .push(file_id);
                match path[start..].find('/') {
                    Some(pos) => start += pos + 1,
                    None => break,
                }
            }
        }

        idx
    }

    pub fn is_empty(&self) -> bool {
        self.include_map.is_empty()
    }
}

// ── ReexportIndex ────────────────────────────────────────────────────────────

/// Language-agnostic re-export lookup; a language hook decides what counts as a
/// re-export. `named`: `module -> { bound_name -> (target_module, target_name) }`.
/// `wildcard`: `module -> [starred source modules]`.
#[derive(Default)]
pub struct ReexportIndex {
    named: rustc_hash::FxHashMap<String, rustc_hash::FxHashMap<String, (String, String)>>,
    wildcard: rustc_hash::FxHashMap<String, Vec<String>>,
}

impl ReexportIndex {
    pub fn add_named(
        &mut self,
        module: String,
        bound: String,
        target_module: String,
        target_name: String,
    ) {
        self.named
            .entry(module)
            .or_default()
            .insert(bound, (target_module, target_name));
    }

    pub fn add_wildcard(&mut self, module: String, source_module: String) {
        self.wildcard.entry(module).or_default().push(source_module);
    }

    pub fn named(&self, module: &str, name: &str) -> Option<(&str, &str)> {
        self.named
            .get(module)
            .and_then(|names| names.get(name))
            .map(|(module, name)| (module.as_str(), name.as_str()))
    }

    pub fn wildcard_sources(&self, module: &str) -> &[String] {
        self.wildcard
            .get(module)
            .map(|v| v.as_slice())
            .unwrap_or(&[])
    }
}

// ── Arrow Row types ──────────────────────────────────────────────────────────

pub struct RowContext<'a> {
    pub project_id: i64,
    pub branch: &'a str,
    pub commit_sha: &'a str,
}

impl<'a> RowContext<'a> {
    pub fn empty() -> Self {
        Self {
            project_id: 0,
            branch: "",
            commit_sha: "",
        }
    }
}

impl gkg_utils::arrow::RowEnvelope for RowContext<'_> {
    fn write_header(&self, b: &mut BatchBuilder, id: i64) -> Result<(), arrow::error::ArrowError> {
        b.col("id")?.push_int(id)?;
        b.col("project_id")?.push_int(self.project_id)?;
        b.col("branch")?.push_str(self.branch)?;
        b.col("commit_sha")?.push_str(self.commit_sha)?;
        b.col("traversal_path")?.push_str("")?;
        Ok(())
    }

    fn header_specs(&self) -> Vec<ColumnSpec> {
        vec![
            ColumnSpec {
                name: "id".into(),
                col_type: ColumnType::Int,
                nullable: false,
            },
            ColumnSpec {
                name: "project_id".into(),
                col_type: ColumnType::Int,
                nullable: false,
            },
            ColumnSpec {
                name: "branch".into(),
                col_type: ColumnType::Str,
                nullable: false,
            },
            ColumnSpec {
                name: "commit_sha".into(),
                col_type: ColumnType::Str,
                nullable: false,
            },
            ColumnSpec {
                name: "traversal_path".into(),
                col_type: ColumnType::Str,
                nullable: false,
            },
        ]
    }
}

fn write_range(b: &mut BatchBuilder, range: &Range) -> Result<(), arrow::error::ArrowError> {
    b.col("start_line")?.push_int(range.start.line as i64 + 1)?;
    b.col("end_line")?.push_int(range.end.line as i64 + 1)?;
    b.col("start_byte")?.push_int(range.byte_offset.0 as i64)?;
    b.col("end_byte")?.push_int(range.byte_offset.1 as i64)?;
    b.col("start_char")?
        .push_int(range.start.column as i64 + 1)?;
    b.col("end_char")?.push_int(range.end.column as i64 + 1)?;
    Ok(())
}

pub struct DirectoryRow<'a> {
    pub path: &'a str,
    pub name: &'a str,
    pub id: i64,
}
impl<C: gkg_utils::arrow::RowEnvelope> AsRecordBatch<C> for DirectoryRow<'_> {
    fn write_row(&self, b: &mut BatchBuilder, ctx: &C) -> Result<(), arrow::error::ArrowError> {
        ctx.write_header(b, self.id)?;
        b.col("path")?.push_str(self.path)?;
        b.col("name")?.push_str(self.name)?;
        Ok(())
    }
}

pub struct FileRow<'a> {
    pub path: &'a str,
    pub name: &'a str,
    pub extension: &'a str,
    pub language: &'a str,
    pub size: u64,
    pub reason: &'a str,
    pub id: i64,
}
impl<C: gkg_utils::arrow::RowEnvelope> AsRecordBatch<C> for FileRow<'_> {
    fn write_row(&self, b: &mut BatchBuilder, ctx: &C) -> Result<(), arrow::error::ArrowError> {
        ctx.write_header(b, self.id)?;
        b.col("path")?.push_str(self.path)?;
        b.col("name")?.push_str(self.name)?;
        b.col("extension")?.push_str(self.extension)?;
        b.col("language")?.push_str(self.language)?;
        b.col("size_bytes")?.push_int(self.size as i64)?;
        b.col("reason")?.push_str(self.reason)?;
        Ok(())
    }
}

pub struct DefinitionRow<'a> {
    pub file_path: &'a str,
    pub def: &'a GraphDef,
    pub pool: &'a StringPool,
    pub id: i64,
}
impl<C: gkg_utils::arrow::RowEnvelope> AsRecordBatch<C> for DefinitionRow<'_> {
    fn write_row(&self, b: &mut BatchBuilder, ctx: &C) -> Result<(), arrow::error::ArrowError> {
        ctx.write_header(b, self.id)?;
        b.col("file_path")?.push_str(self.file_path)?;
        b.col("fqn")?.push_str(self.pool.get(self.def.fqn))?;
        b.col("name")?.push_str(self.pool.get(self.def.name))?;
        b.col("definition_type")?
            .push_str(self.def.definition_type)?;
        write_range(b, &self.def.range)?;
        Ok(())
    }
}

pub struct ImportRow<'a> {
    pub file_path: &'a str,
    pub import: &'a GraphImport,
    pub pool: &'a StringPool,
    pub id: i64,
}
impl<C: gkg_utils::arrow::RowEnvelope> AsRecordBatch<C> for ImportRow<'_> {
    fn write_row(&self, b: &mut BatchBuilder, ctx: &C) -> Result<(), arrow::error::ArrowError> {
        ctx.write_header(b, self.id)?;
        b.col("file_path")?.push_str(self.file_path)?;
        b.col("import_type")?.push_str(self.import.import_type)?;
        b.col("import_path")?
            .push_str(self.pool.get(self.import.path))?;
        b.col("identifier_name")?
            .push_str(self.import.name.map(|id| self.pool.get(id)).unwrap_or(""))?;
        b.col("identifier_alias")?
            .push_str(self.import.alias.map(|id| self.pool.get(id)).unwrap_or(""))?;
        write_range(b, &self.import.range)?;
        Ok(())
    }
}

pub struct EdgeRow<'a> {
    pub source_id: i64,
    pub target_id: i64,
    pub edge_kind: &'a str,
    pub source_node_kind: &'a str,
    pub target_node_kind: &'a str,
}

impl AsRecordBatch for EdgeRow<'_> {
    fn write_row(&self, b: &mut BatchBuilder, _ctx: &()) -> Result<(), arrow::error::ArrowError> {
        b.col("source_id")?.push_int(self.source_id)?;
        b.col("source_kind")?.push_str(self.source_node_kind)?;
        b.col("relationship_kind")?.push_str(self.edge_kind)?;
        b.col("target_id")?.push_int(self.target_id)?;
        b.col("target_kind")?.push_str(self.target_node_kind)?;
        b.col("traversal_path")?.push_str("")?;
        Ok(())
    }
}
