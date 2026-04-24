//! Custom Ruby pipeline using Prism parser.
//!
//! Demonstrates the custom pipeline pattern: parses Ruby files with Prism,
//! builds Arrow RecordBatches directly without going through CodeGraph.

use std::path::Path;
use std::sync::Arc;

use arrow::array::{Int64Builder, StringBuilder};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use ruby_prism::Visit;

use crate::v2::pipeline::{BatchTx, FileInput, LanguagePipeline, PipelineContext, PipelineError};

pub struct RubyPipeline;

impl LanguagePipeline for RubyPipeline {
    fn process_files(
        files: &[FileInput],
        ctx: &Arc<PipelineContext>,
        btx: &BatchTx<'_>,
    ) -> Result<(), Vec<PipelineError>> {
        let root_path = ctx.root_path.as_str();
        let mut defs: Vec<DefEntry> = Vec::new();
        let mut file_entries: Vec<FileEntry> = Vec::new();
        let mut edges: Vec<EdgeEntry> = Vec::new();
        let mut errors: Vec<PipelineError> = Vec::new();
        let mut next_id: i64 = 1;

        for file_path in files {
            let abs_path = if Path::new(file_path.as_str()).starts_with(root_path) {
                file_path.clone()
            } else {
                format!("{root_path}/{file_path}")
            };

            let source = match std::fs::read_to_string(&abs_path) {
                Ok(s) => s,
                Err(e) => {
                    errors.push(PipelineError {
                        file_path: file_path.clone(),
                        error: e.to_string(),
                    });
                    continue;
                }
            };

            let relative = file_path
                .strip_prefix(root_path)
                .unwrap_or(file_path)
                .trim_start_matches('/');

            let file_name = Path::new(relative)
                .file_name()
                .map(|n| n.to_string_lossy().to_string())
                .unwrap_or_default();

            let file_id = next_id;
            next_id += 1;

            file_entries.push(FileEntry {
                id: file_id,
                path: relative.to_string(),
                name: file_name,
            });

            let parse_result = ruby_prism::parse(source.as_bytes());
            let mut visitor = PrismVisitor {
                source: &source,
                file_path: relative,
                file_id,
                scope_stack: Vec::new(),
                defs: &mut defs,
                edges: &mut edges,
                next_id: &mut next_id,
            };
            visitor.visit(&parse_result.node());
        }

        if !errors.is_empty() && file_entries.is_empty() {
            return Err(errors);
        }
        if !errors.is_empty() {
            for e in &errors {
                eprintln!("[ruby_prism] skipped {}: {}", e.file_path, e.error);
            }
        }

        let file_batch = build_file_batch(&file_entries)?;
        let def_batch = build_def_batch(&defs)?;
        let edge_batch = build_edge_batch(&edges)?;

        btx.send_raw("File".to_string(), file_batch);
        btx.send_raw("Definition".to_string(), def_batch);
        btx.send_raw("DefinitionToDefinition".to_string(), edge_batch);

        Ok(())
    }
}

// ── Internal types ──────────────────────────────────────────────

struct FileEntry {
    id: i64,
    path: String,
    name: String,
}

struct DefEntry {
    id: i64,
    file_path: String,
    fqn: String,
    name: String,
    definition_type: &'static str,
    start_line: i64,
    end_line: i64,
}

struct EdgeEntry {
    source_id: i64,
    target_id: i64,
    edge_kind: &'static str,
    source_kind: &'static str,
    target_kind: &'static str,
}

// ── Prism visitor ───────────────────────────────────────────────

struct PrismVisitor<'a> {
    source: &'a str,
    file_path: &'a str,
    file_id: i64,
    scope_stack: Vec<(String, i64)>,
    defs: &'a mut Vec<DefEntry>,
    edges: &'a mut Vec<EdgeEntry>,
    next_id: &'a mut i64,
}

impl PrismVisitor<'_> {
    fn add_def(&mut self, name: &str, def_type: &'static str, start: usize, end: usize) {
        let def_id = *self.next_id;
        *self.next_id += 1;

        let fqn = if self.scope_stack.is_empty() {
            name.to_string()
        } else {
            let prefix: Vec<&str> = self.scope_stack.iter().map(|(n, _)| n.as_str()).collect();
            format!("{}::{name}", prefix.join("::"))
        };

        let start_line = self.source[..start].lines().count() as i64 + 1;
        let end_line = self.source[..end].lines().count() as i64 + 1;

        self.defs.push(DefEntry {
            id: def_id,
            file_path: self.file_path.to_string(),
            fqn,
            name: name.to_string(),
            definition_type: def_type,
            start_line,
            end_line,
        });

        let parent_id = self
            .scope_stack
            .last()
            .map(|(_, id)| *id)
            .unwrap_or(self.file_id);
        let parent_kind = if self.scope_stack.is_empty() {
            "File"
        } else {
            "Definition"
        };
        self.edges.push(EdgeEntry {
            source_id: parent_id,
            target_id: def_id,
            edge_kind: "Defines",
            source_kind: parent_kind,
            target_kind: "Definition",
        });
    }
}

impl<'pr> Visit<'pr> for PrismVisitor<'_> {
    fn visit_class_node(&mut self, node: &ruby_prism::ClassNode) {
        let loc = node.location();
        let name = std::str::from_utf8(node.name().as_slice()).unwrap_or("?");
        self.add_def(name, "Class", loc.start_offset(), loc.end_offset());

        let def_id = self.defs.last().unwrap().id;
        self.scope_stack.push((name.to_string(), def_id));
        if let Some(body) = node.body() {
            self.visit(&body);
        }
        self.scope_stack.pop();
    }

    fn visit_module_node(&mut self, node: &ruby_prism::ModuleNode) {
        let loc = node.location();
        let name = std::str::from_utf8(node.name().as_slice()).unwrap_or("?");
        self.add_def(name, "Module", loc.start_offset(), loc.end_offset());

        let def_id = self.defs.last().unwrap().id;
        self.scope_stack.push((name.to_string(), def_id));
        if let Some(body) = node.body() {
            self.visit(&body);
        }
        self.scope_stack.pop();
    }

    fn visit_def_node(&mut self, node: &ruby_prism::DefNode) {
        let loc = node.location();
        let name = std::str::from_utf8(node.name().as_slice()).unwrap_or("?");
        self.add_def(name, "Method", loc.start_offset(), loc.end_offset());
    }
}

// ── Arrow batch builders ────────────────────────────────────────

fn build_file_batch(files: &[FileEntry]) -> Result<RecordBatch, Vec<PipelineError>> {
    let n = files.len();
    let mut id_b = Int64Builder::with_capacity(n);
    let mut path_b = StringBuilder::with_capacity(n, n * 32);
    let mut name_b = StringBuilder::with_capacity(n, n * 16);

    for f in files {
        id_b.append_value(f.id);
        path_b.append_value(&f.path);
        name_b.append_value(&f.name);
    }

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("path", DataType::Utf8, false),
        Field::new("name", DataType::Utf8, false),
    ]));

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(id_b.finish()),
            Arc::new(path_b.finish()),
            Arc::new(name_b.finish()),
        ],
    )
    .map_err(|e| {
        vec![PipelineError {
            file_path: String::new(),
            error: e.to_string(),
        }]
    })
}

fn build_def_batch(defs: &[DefEntry]) -> Result<RecordBatch, Vec<PipelineError>> {
    let n = defs.len();
    let mut id_b = Int64Builder::with_capacity(n);
    let mut fp_b = StringBuilder::with_capacity(n, n * 32);
    let mut fqn_b = StringBuilder::with_capacity(n, n * 32);
    let mut name_b = StringBuilder::with_capacity(n, n * 16);
    let mut dt_b = StringBuilder::with_capacity(n, n * 8);
    let mut sl_b = Int64Builder::with_capacity(n);
    let mut el_b = Int64Builder::with_capacity(n);

    for d in defs {
        id_b.append_value(d.id);
        fp_b.append_value(&d.file_path);
        fqn_b.append_value(&d.fqn);
        name_b.append_value(&d.name);
        dt_b.append_value(d.definition_type);
        sl_b.append_value(d.start_line);
        el_b.append_value(d.end_line);
    }

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("file_path", DataType::Utf8, false),
        Field::new("fqn", DataType::Utf8, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("definition_type", DataType::Utf8, false),
        Field::new("start_line", DataType::Int64, false),
        Field::new("end_line", DataType::Int64, false),
    ]));

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(id_b.finish()),
            Arc::new(fp_b.finish()),
            Arc::new(fqn_b.finish()),
            Arc::new(name_b.finish()),
            Arc::new(dt_b.finish()),
            Arc::new(sl_b.finish()),
            Arc::new(el_b.finish()),
        ],
    )
    .map_err(|e| {
        vec![PipelineError {
            file_path: String::new(),
            error: e.to_string(),
        }]
    })
}

fn build_edge_batch(edges: &[EdgeEntry]) -> Result<RecordBatch, Vec<PipelineError>> {
    let n = edges.len();
    let mut src_b = Int64Builder::with_capacity(n);
    let mut tgt_b = Int64Builder::with_capacity(n);
    let mut ek_b = StringBuilder::with_capacity(n, n * 8);
    let mut sk_b = StringBuilder::with_capacity(n, n * 12);
    let mut tk_b = StringBuilder::with_capacity(n, n * 12);

    for e in edges {
        src_b.append_value(e.source_id);
        tgt_b.append_value(e.target_id);
        ek_b.append_value(e.edge_kind);
        sk_b.append_value(e.source_kind);
        tk_b.append_value(e.target_kind);
    }

    let schema = Arc::new(Schema::new(vec![
        Field::new("source_id", DataType::Int64, false),
        Field::new("target_id", DataType::Int64, false),
        Field::new("relationship_kind", DataType::Utf8, false),
        Field::new("source_kind", DataType::Utf8, false),
        Field::new("target_kind", DataType::Utf8, false),
    ]));

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(src_b.finish()),
            Arc::new(tgt_b.finish()),
            Arc::new(ek_b.finish()),
            Arc::new(sk_b.finish()),
            Arc::new(tk_b.finish()),
        ],
    )
    .map_err(|e| {
        vec![PipelineError {
            file_path: String::new(),
            error: e.to_string(),
        }]
    })
}
