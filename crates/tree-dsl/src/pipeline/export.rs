use std::hash::{Hash, Hasher};

use arrow::error::ArrowError;
use arrow::record_batch::RecordBatch;
use ontology::{DataType, Ontology};
use orbit_utils::arrow::{BatchBuilder, ColumnSpec, ColumnType};
use rustc_hash::{FxHashMap, FxHashSet, FxHasher};

use crate::canonical::Canonical as C;
use crate::intern::Lang;
use crate::pipeline::State;
use crate::tree::{Cursor, EdgeKind, Tree};

/// Header columns every local row starts with, and the seed for stable ids;
/// the same envelope code-graph writes, so both pipelines land in the same
/// tables with the same ids.
pub struct Envelope<'a> {
    pub project_id: i64,
    pub branch: &'a str,
    pub commit_sha: &'a str,
}

impl Envelope<'_> {
    fn write_header(&self, b: &mut BatchBuilder, id: i64) -> Result<(), ArrowError> {
        b.col("id")?.push_int(id)?;
        b.col("project_id")?.push_int(self.project_id)?;
        b.col("branch")?.push_str(self.branch)?;
        b.col("commit_sha")?.push_str(self.commit_sha)?;
        b.col("traversal_path")?.push_str("")?;
        Ok(())
    }

    fn id(&self, kind: &str, parts: &[&str]) -> i64 {
        let mut hasher = FxHasher::default();
        self.project_id.to_string().hash(&mut hasher);
        self.branch.hash(&mut hasher);
        kind.hash(&mut hasher);
        parts.hash(&mut hasher);
        (hasher.finish() & 0x7FFF_FFFF_FFFF_FFFF) as i64
    }
}

/// One `(table, batch)` per local entity, then the edge table.
pub fn export(
    state: &State,
    lang: &Lang,
    ontology: &Ontology,
    envelope: &Envelope,
) -> Result<Vec<(String, RecordBatch)>, ArrowError> {
    let mut tables = Vec::new();
    let mut nodes = Nodes::default();

    for entity in ontology.local_entity_names() {
        let table = ontology
            .get_node(entity)
            .ok_or_else(|| schema_error(format!("no node '{entity}' in ontology")))?
            .destination_table
            .clone();
        let mut b = BatchBuilder::new(&entity_specs(ontology, entity)?, 0)?;
        match entity {
            "Directory" => write_directories(&mut b, state, envelope, &mut nodes)?,
            "File" => write_files(&mut b, state, lang, envelope, &mut nodes)?,
            "Definition" => write_definitions(&mut b, state, lang, envelope, &mut nodes)?,
            "ImportedSymbol" => write_imports(&mut b, state, lang, envelope, &mut nodes)?,
            other => {
                return Err(schema_error(format!(
                    "tree-dsl has no rows for local entity '{other}'"
                )));
            }
        }
        tables.push((table, b.finish()?));
    }

    let edge_table = ontology
        .local_edge_table_name()
        .ok_or_else(|| schema_error("ontology has no local edge table".into()))?
        .to_string();
    tables.push((edge_table, write_edges(state, ontology, &nodes)?));
    Ok(tables)
}

fn schema_error(message: String) -> ArrowError {
    ArrowError::SchemaError(message)
}

fn entity_specs(ontology: &Ontology, entity: &str) -> Result<Vec<ColumnSpec>, ArrowError> {
    let fields = ontology
        .local_entity_fields(entity)
        .ok_or_else(|| schema_error(format!("'{entity}' is not a local entity")))?;
    Ok(fields
        .iter()
        .map(|f| ColumnSpec {
            name: f.name.clone(),
            col_type: column_type(&f.data_type),
            nullable: f.nullable,
        })
        .collect())
}

fn column_type(data_type: &DataType) -> ColumnType {
    match data_type {
        DataType::Int => ColumnType::Int,
        _ => ColumnType::Str,
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Entity {
    File,
    Definition,
    ImportedSymbol,
}

impl Entity {
    fn name(self) -> &'static str {
        match self {
            Entity::File => "File",
            Entity::Definition => "Definition",
            Entity::ImportedSymbol => "ImportedSymbol",
        }
    }
}

/// Exported ids keyed by `(tree, node)`, plus the directory tree.
#[derive(Default)]
struct Nodes {
    ids: FxHashMap<(u32, u32), (i64, Entity)>,
    directories: FxHashMap<String, i64>,
}

impl Nodes {
    fn record(&mut self, tree: usize, node: Cursor, id: i64, entity: Entity) {
        self.ids.insert((tree as u32, node.index()), (id, entity));
    }

    fn get(&self, tree: u32, node: u32) -> Option<(i64, Entity)> {
        self.ids.get(&(tree, node)).copied()
    }
}

fn parent_dir(path: &str) -> Option<&str> {
    path.rsplit_once('/').map(|(dir, _)| dir)
}

fn basename(path: &str) -> &str {
    path.rsplit('/').next().unwrap_or(path)
}

fn write_directories(
    b: &mut BatchBuilder,
    state: &State,
    envelope: &Envelope,
    nodes: &mut Nodes,
) -> Result<(), ArrowError> {
    let mut dirs = FxHashSet::default();
    for tree in &state.trees {
        let mut dir = parent_dir(&tree.label);
        while let Some(d) = dir {
            dirs.insert(d);
            dir = parent_dir(d);
        }
    }
    let mut dirs: Vec<&str> = dirs.into_iter().collect();
    dirs.sort_unstable();
    for dir in dirs {
        let id = envelope.id("dir", &[dir]);
        envelope.write_header(b, id)?;
        b.col("path")?.push_str(dir)?;
        b.col("name")?.push_str(basename(dir))?;
        nodes.directories.insert(dir.to_string(), id);
    }
    Ok(())
}

fn write_files(
    b: &mut BatchBuilder,
    state: &State,
    lang: &Lang,
    envelope: &Envelope,
    nodes: &mut Nodes,
) -> Result<(), ArrowError> {
    let language = lang.syms.intern("language");
    for (fi, tree) in state.trees.iter().enumerate() {
        let root = tree.root();
        if !root.is(C::SourceFile) {
            continue;
        }
        let path = tree.label.as_str();
        let name = basename(path);
        let id = envelope.id("file", &[path]);
        envelope.write_header(b, id)?;
        b.col("path")?.push_str(path)?;
        b.col("name")?.push_str(name)?;
        b.col("extension")?
            .push_str(name.rsplit_once('.').map_or("", |(_, ext)| ext))?;
        b.col("language")?
            .push_str(tag_text(tree, root, language, lang).unwrap_or(""))?;
        b.col("size_bytes")?.push_int(root.end() as i64)?;
        b.col("reason")?.push_str("")?;
        nodes.record(fi, root, id, Entity::File);
    }
    Ok(())
}

fn write_definitions(
    b: &mut BatchBuilder,
    state: &State,
    lang: &Lang,
    envelope: &Envelope,
    nodes: &mut Nodes,
) -> Result<(), ArrowError> {
    let fqn_key = lang.syms.intern("fqn");
    let def_type = lang.syms.intern("def_type");
    let definition_span = lang.syms.intern("definition_span");
    for (fi, tree) in state.trees.iter().enumerate() {
        let file_path = tree.label.as_str();
        for def in tree.root().descendants().filter(|c| c.is(C::Def)) {
            let fqn = tag_text(tree, def, fqn_key, lang).unwrap_or("");
            let span = if def.has_tag(definition_span) {
                def
            } else {
                def.child(C::DefName).unwrap_or(def)
            };
            let range = format!("{}:{}", span.start(), span.end());
            let id = envelope.id("def", &[file_path, fqn, &range]);
            envelope.write_header(b, id)?;
            b.col("file_path")?.push_str(file_path)?;
            b.col("fqn")?.push_str(fqn)?;
            b.col("name")?.push_str(
                def.child_sym(C::DefName)
                    .map_or("", |s| lang.syms.resolve(s)),
            )?;
            b.col("definition_type")?
                .push_str(tag_text(tree, def, def_type, lang).unwrap_or(""))?;
            write_range(b, span)?;
            nodes.record(fi, def, id, Entity::Definition);
        }
    }
    Ok(())
}

fn write_imports(
    b: &mut BatchBuilder,
    state: &State,
    lang: &Lang,
    envelope: &Envelope,
    nodes: &mut Nodes,
) -> Result<(), ArrowError> {
    let source_path = lang.syms.intern("source_path");
    let import_type = lang.syms.intern("import_type");
    for (fi, tree) in state.trees.iter().enumerate() {
        let file_path = tree.label.as_str();
        let imports = tree
            .root()
            .descendants()
            .filter(|c| c.is(C::Import) || c.is(C::ImportType))
            .filter(|c| !c.parent().is_some_and(|p| p.is(C::ModuleExport)));
        for import in imports {
            let path = tag_text(tree, import, source_path, lang).unwrap_or("");
            let source_sym = import.child_sym(C::Source).unwrap_or(0);
            let range = format!("{}:{}", import.start(), import.end());
            let mut names = import
                .children()
                .filter(|c| c.is(C::Name) && c.sym() != 0)
                .peekable();
            let whole_import = names.peek().is_none();
            let rows: Vec<(Cursor, Option<Cursor>)> = if whole_import {
                vec![(import, None)]
            } else {
                names.map(|n| (n, Some(n))).collect()
            };
            for (node, name) in rows {
                let name_text = name.map_or("", |n| lang.syms.resolve(n.sym()));
                let alias = name
                    .and_then(|n| n.child_sym(C::Alias))
                    .or_else(|| import.child_sym(C::Alias))
                    .map_or("", |s| lang.syms.resolve(s));
                let kind = name
                    .and_then(|n| tree.get_tag(n.index(), import_type))
                    .or_else(|| tree.get_tag(import.index(), import_type))
                    .map(|v| lang.syms.resolve(v))
                    .unwrap_or(if name.is_some_and(|n| n.sym() == source_sym) {
                        "Import"
                    } else {
                        "NamedImport"
                    });
                let id_name = if name_text.is_empty() { "*" } else { name_text };
                let id = envelope.id("import", &[file_path, path, id_name, &range]);
                envelope.write_header(b, id)?;
                b.col("file_path")?.push_str(file_path)?;
                b.col("import_type")?.push_str(kind)?;
                b.col("import_path")?.push_str(path)?;
                b.col("identifier_name")?.push_str(name_text)?;
                b.col("identifier_alias")?.push_str(alias)?;
                write_range(b, import)?;
                nodes.record(fi, node, id, Entity::ImportedSymbol);
            }
        }
    }
    Ok(())
}

fn write_range(b: &mut BatchBuilder, c: Cursor) -> Result<(), ArrowError> {
    b.col("start_line")?.push_int(c.start_row() as i64 + 1)?;
    b.col("end_line")?.push_int(c.end_row() as i64 + 1)?;
    b.col("start_byte")?.push_int(c.start() as i64)?;
    b.col("end_byte")?.push_int(c.end() as i64)?;
    b.col("start_char")?.push_int(c.start_col() as i64 + 1)?;
    b.col("end_char")?.push_int(c.end_col() as i64 + 1)?;
    Ok(())
}

fn tag_text<'a>(tree: &Tree, c: Cursor, key: u32, lang: &'a Lang) -> Option<&'a str> {
    std::iter::once(c)
        .chain(c.ancestors())
        .find_map(|n| tree.get_tag(n.index(), key))
        .map(|v| lang.syms.resolve(v))
}

struct EdgeRow<'a> {
    source: (i64, &'a str),
    kind: &'a str,
    target: (i64, &'a str),
}

fn write_edges(
    state: &State,
    ontology: &Ontology,
    nodes: &Nodes,
) -> Result<RecordBatch, ArrowError> {
    let mut rows: Vec<EdgeRow> = Vec::new();

    let mut dirs: Vec<(&String, &i64)> = nodes.directories.iter().collect();
    dirs.sort_unstable();
    for (dir, id) in dirs {
        if let Some(parent) = parent_dir(dir) {
            rows.push(EdgeRow {
                source: (nodes.directories[parent], "Directory"),
                kind: "CONTAINS",
                target: (*id, "Directory"),
            });
        }
    }

    for (fi, tree) in state.trees.iter().enumerate() {
        let root = tree.root();
        let Some((file_id, Entity::File)) = nodes.get(fi as u32, root.index()) else {
            continue;
        };
        if let Some(dir) = parent_dir(&tree.label) {
            rows.push(EdgeRow {
                source: (nodes.directories[dir], "Directory"),
                kind: "CONTAINS",
                target: (file_id, "File"),
            });
        }
        for node in root.descendants() {
            let Some((id, entity)) = nodes.get(fi as u32, node.index()) else {
                continue;
            };
            let kind = match entity {
                Entity::Definition => "DEFINES",
                Entity::ImportedSymbol => "IMPORTS",
                Entity::File => continue,
            };
            rows.push(EdgeRow {
                source: (file_id, "File"),
                kind,
                target: (id, entity.name()),
            });
        }
    }

    let has_calls: FxHashSet<(u32, u32)> = state
        .edges
        .iter()
        .filter(|e| e.kind == EdgeKind::Calls)
        .map(|e| (e.from_tree, e.from_node))
        .collect();
    let mut seen_cross_file = FxHashSet::default();

    for e in &state.edges {
        let Some((from, from_entity)) = nodes.get(e.from_tree, e.from_node) else {
            continue;
        };
        let Some((to, to_entity)) = nodes.get(e.to_tree, e.to_node) else {
            continue;
        };
        let kind = match (e.kind, from_entity, to_entity) {
            (EdgeKind::Imports, Entity::Definition, Entity::ImportedSymbol) => {
                if has_calls.contains(&(e.from_tree, e.from_node)) {
                    continue;
                }
                "CALLS"
            }
            (EdgeKind::Imports, Entity::ImportedSymbol, Entity::Definition) => "IMPORTS",
            (EdgeKind::Calls, _, Entity::Definition) => "CALLS",
            (EdgeKind::Defines, Entity::Definition, Entity::Definition) => "DEFINES",
            (EdgeKind::Extends, Entity::Definition, Entity::Definition) => "EXTENDS",
            _ => continue,
        };
        if e.from_tree != e.to_tree && !seen_cross_file.insert((from, to, kind)) {
            continue;
        }
        rows.push(EdgeRow {
            source: (from, from_entity.name()),
            kind,
            target: (to, to_entity.name()),
        });
    }

    let specs: Vec<ColumnSpec> = ontology
        .local_edge_columns()
        .iter()
        .map(|c| ColumnSpec {
            name: c.name.clone(),
            col_type: column_type(&c.data_type),
            nullable: false,
        })
        .collect();
    BatchBuilder::new(&specs, rows.len())?.build(&rows, |row, b| {
        b.col("source_id")?.push_int(row.source.0)?;
        b.col("source_kind")?.push_str(row.source.1)?;
        b.col("relationship_kind")?.push_str(row.kind)?;
        b.col("target_id")?.push_int(row.target.0)?;
        b.col("target_kind")?.push_str(row.target.1)?;
        b.col("traversal_path")?.push_str("")?;
        Ok(())
    })
}
