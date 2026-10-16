use std::path::Path;

use arrow::record_batch::RecordBatch;
use code_graph_incremental::pipeline::{Display, Emit, Export, Exported};
use code_graph_incremental::treesitter::SupportLang;
use code_graph_incremental::{Context, Env, Envelope, Limits, Scalar, templates};
mod common;
use ontology::Ontology;

const UTILS: &str = "\
def helper(x):
    return x

class Store:
    def get(self):
        pass
";

const MAIN: &str = "\
from utils import helper, Store
import missing

def run():
    helper(1)
    s = Store()
    s.get()
";

fn write_all(root: &Path, files: &[(&str, &[u8])]) {
    for (path, content) in files {
        std::fs::create_dir_all(root.join(path).parent().unwrap()).unwrap();
        std::fs::write(root.join(path), content).unwrap();
    }
}

fn envelope() -> Envelope<'static> {
    Envelope::new([
        ("project_id", Scalar::Int(7)),
        ("branch", Scalar::Str("main")),
        ("commit_sha", Scalar::Str("abc")),
    ])
}

#[test]
fn export_config_emits_typed_metadata_and_envelope_overrides() {
    use code_graph_incremental::export::ExportConfig;
    use orbit_arrow::{ColumnSpec, ColumnType};
    let repo = tempfile::tempdir().unwrap();
    write_all(repo.path(), &[("main.py", MAIN.as_bytes())]);
    let sources = common::walk(repo.path());
    let mut env = Env::with_limits(SupportLang::Python, Limits::UNLIMITED).unwrap();
    let ontology = Ontology::load_embedded().unwrap();
    let mut config = ExportConfig::embedded().clone();
    let specs = ontology
        .local_entity_fields("File")
        .unwrap()
        .iter()
        .map(|field| ColumnSpec {
            name: field.name.clone(),
            col_type: if field.data_type == ontology::DataType::Int {
                ColumnType::Int
            } else {
                ColumnType::DictStr
            },
            nullable: field.nullable,
        })
        .chain([
            ColumnSpec {
                name: "_version".into(),
                col_type: ColumnType::TimestampMicros,
                nullable: false,
            },
            ColumnSpec {
                name: "_deleted".into(),
                col_type: ColumnType::Bool,
                nullable: false,
            },
        ])
        .collect();
    config.schemas.insert("gl_file".into(), specs);
    env.export_config = config;
    let error = templates::index(Context::new(&env, sources.clone()))
        .unwrap()
        .then(Display)
        .unwrap()
        .then(Export {
            ontology: &ontology,
            envelope: envelope(),
        })
        .err()
        .unwrap();
    assert!(error.to_string().contains("version"));
    for traversal_path in [None, Some("1/2/")] {
        let mut values = vec![
            ("project_id", Scalar::Int(7)),
            ("branch", Scalar::Str("main")),
            ("commit_sha", Scalar::Str("abc")),
            ("_version", Scalar::Int(123456)),
            ("_deleted", Scalar::Bool(false)),
        ];
        if let Some(path) = traversal_path {
            values.push(("traversal_path", Scalar::Str(path)));
        }
        let exported = templates::index(Context::new(&env, sources.clone()))
            .unwrap()
            .then(Display)
            .unwrap()
            .then(Export {
                ontology: &ontology,
                envelope: Envelope::new(values),
            })
            .unwrap()
            .into_value();
        let batch = table(&exported, "gl_file");
        assert_eq!(batch.num_rows(), 1);
        let versions = batch
            .column_by_name("_version")
            .unwrap()
            .as_any()
            .downcast_ref::<arrow::array::TimestampMicrosecondArray>()
            .unwrap();
        assert_eq!(versions.value(0), 123456);
        assert_eq!(column(batch, "_deleted"), ["false"]);
        assert_eq!(
            column(batch, "traversal_path"),
            [traversal_path.unwrap_or("")]
        );
    }
}

#[test]
fn each_pipeline_uses_its_own_export_config() {
    let repo = tempfile::tempdir().unwrap();
    write_all(repo.path(), &[("main.py", MAIN.as_bytes())]);
    let sources = common::walk(repo.path());
    let mut env = Env::with_limits(SupportLang::Python, Limits::UNLIMITED).unwrap();
    let ontology = Ontology::load_embedded().unwrap();
    for marker in ["first", "second", ""] {
        let yaml = include_str!("../config/export.yaml").replace(
            "name: traversal_path, type: Utf8, const: \"\"",
            &format!("name: traversal_path, type: Utf8, const: \"{marker}\""),
        );
        env.export_config = orbit_utils::yaml::from_str(&yaml).unwrap();
        let exported = templates::index(Context::new(&env, sources.clone()))
            .unwrap()
            .then(Display)
            .unwrap()
            .then(Export {
                ontology: &ontology,
                envelope: envelope(),
            })
            .unwrap()
            .into_value();
        assert!(
            exported
                .tables
                .iter()
                .any(|(_, batch)| batch.num_rows() > 0)
        );
        for (_, batch) in &exported.tables {
            assert!(
                column(batch, "traversal_path")
                    .iter()
                    .all(|value| value == marker)
            );
        }
    }
}

#[test]
fn custom_export_configs_still_validate_against_the_ontology() {
    let repo = tempfile::tempdir().unwrap();
    write_all(repo.path(), &[("main.py", MAIN.as_bytes())]);
    let mut env = Env::with_limits(SupportLang::Python, Limits::UNLIMITED).unwrap();
    let ontology = Ontology::load_embedded().unwrap();
    let yaml = include_str!("../config/export.yaml").replace("name: traversal_path", "name: typo");
    env.export_config = orbit_utils::yaml::from_str(&yaml).unwrap();
    let error = templates::index(Context::new(&env, common::walk(repo.path())))
        .unwrap()
        .then(Display)
        .unwrap()
        .then(Export {
            ontology: &ontology,
            envelope: envelope(),
        })
        .err()
        .expect("unknown columns must fail");
    assert!(error.to_string().contains("traversal_path"));
}

fn export_repo(root: &Path) -> Exported {
    let env = Env::with_limits(SupportLang::Python, Limits::UNLIMITED).unwrap();
    let ontology = Ontology::load_embedded().unwrap();
    templates::index(Context::new(&env, common::walk(root)))
        .unwrap()
        .then(Display)
        .unwrap()
        .then(Export {
            ontology: &ontology,
            envelope: envelope(),
        })
        .unwrap()
        .into_value()
}

fn table<'a>(exported: &'a Exported, name: &str) -> &'a RecordBatch {
    &exported
        .tables
        .iter()
        .find(|(table, _)| table == name)
        .unwrap_or_else(|| panic!("no table {name}"))
        .1
}

fn column(batch: &RecordBatch, name: &str) -> Vec<String> {
    let array = batch
        .column_by_name(name)
        .unwrap_or_else(|| panic!("no column {name}"));
    (0..batch.num_rows())
        .map(|row| arrow::util::display::array_value_to_string(array.as_ref(), row).unwrap())
        .collect()
}

fn rows(batch: &RecordBatch, names: &[&str]) -> Vec<Vec<String>> {
    let columns: Vec<_> = names.iter().map(|n| column(batch, n)).collect();
    let mut rows: Vec<Vec<String>> = (0..batch.num_rows())
        .map(|row| columns.iter().map(|c| c[row].clone()).collect())
        .collect();
    rows.sort();
    rows
}

fn strs(row: &[&str]) -> Vec<String> {
    row.iter().map(|s| s.to_string()).collect()
}

fn options() -> templates::Options {
    templates::Options {
        admission: std::sync::Arc::new(()),
        limits: Limits::UNLIMITED,
        worker_threads: 0,
        concurrent_families: 2,
        small_family_files: 0,
        batch_bytes: orbit_arrow::BATCH_BYTES,
        cancelled: std::sync::Arc::new(|| false),
        family_finished: std::sync::Arc::new(|| {}),
        phase_started: std::sync::Arc::new(|_, _, _| {}),
    }
}

const FAMILIES: &[(&str, &[u8])] = &[
    ("main.py", b"def python_function():\n    pass\n"),
    ("main.ts", b"export function typescriptFunction() {}"),
    ("main.rb", b"def ruby_method; end\n"),
    ("main.go", b"package main\nfunc goFunction() {}\n"),
];

#[test]
fn families_follow_the_worker_budget_and_the_family_cap() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Condvar, Mutex};
    let repo = tempfile::tempdir().unwrap();
    write_all(repo.path(), FAMILIES);
    let ontology = Ontology::load_embedded().unwrap();
    let main = std::thread::current().id();
    for (worker_threads, small_family_files, cap, pool) in [(3, 0, 1, 3), (8, 1, 2, 2)] {
        let finished = Arc::new(AtomicUsize::new(0));
        let arrivals = Arc::new(Mutex::new((std::collections::HashSet::new(), Vec::new())));
        let state = Mutex::new((0, 0, 0, Vec::new()));
        let changed = Condvar::new();
        templates::repository(
            common::walk(repo.path()),
            templates::Options {
                worker_threads,
                concurrent_families: cap,
                small_family_files,
                cancelled: Arc::new({
                    let (finished, arrivals) = (finished.clone(), arrivals.clone());
                    move || {
                        let thread = std::thread::current().id();
                        if thread != main && rayon::current_thread_index().is_none() {
                            let mut arrivals = arrivals.lock().unwrap();
                            if arrivals.0.insert(thread) {
                                arrivals.1.push(finished.load(Ordering::SeqCst));
                            }
                        }
                        false
                    }
                }),
                family_finished: Arc::new({
                    let finished = finished.clone();
                    move || {
                        finished.fetch_add(1, Ordering::SeqCst);
                    }
                }),
                phase_started: std::sync::Arc::new(|_, _, _| {}),
                ..options()
            },
            &envelope(),
            &ontology,
            code_graph_incremental::export::ExportConfig::embedded(),
            |_| -> Result<(), String> {
                let mut guard = state.lock().unwrap();
                let (active, _, peak, pools) = &mut *guard;
                *active += 1;
                *peak = (*peak).max(*active);
                pools.push(rayon::current_num_threads());
                changed.notify_all();
                let mut guard = changed
                    .wait_while(guard, |(active, done, _, _)| {
                        *active < cap && *active + *done < FAMILIES.len()
                    })
                    .unwrap();
                guard.0 -= 1;
                guard.1 += 1;
                changed.notify_all();
                Ok(())
            },
        )
        .unwrap();
        let (_, done, peak, pools) = state.into_inner().unwrap();
        assert_eq!(done, FAMILIES.len());
        assert_eq!(peak, cap);
        assert_eq!(pools, [pool; 4]);
        let started = &arrivals.lock().unwrap().1;
        assert_eq!(started.len(), FAMILIES.len());
        for (order, finished) in started.iter().enumerate() {
            assert!(
                finished + cap > order,
                "family {order} started after {finished} finished"
            );
        }
    }
}

#[test]
fn a_family_panic_ends_the_run_with_a_typed_error() {
    let repo = tempfile::tempdir().unwrap();
    write_all(repo.path(), FAMILIES);
    let ontology = Ontology::load_embedded().unwrap();
    let emitted = std::sync::atomic::AtomicUsize::new(0);
    let result = templates::repository(
        common::walk(repo.path()),
        templates::Options {
            concurrent_families: 1,
            ..options()
        },
        &envelope(),
        &ontology,
        code_graph_incremental::export::ExportConfig::embedded(),
        |_| -> Result<(), String> {
            emitted.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            panic!("emit failed");
        },
    );
    let Err(code_graph_incremental::Error::Panic { message, .. }) = result else {
        panic!("expected a family panic");
    };
    assert_eq!(message, "emit failed");
    assert_eq!(emitted.into_inner(), 1);
}

#[test]
fn tables_over_the_batch_byte_budget_split_into_batches() {
    let repo = tempfile::tempdir().unwrap();
    write_all(
        repo.path(),
        &[("utils.py", UTILS.as_bytes()), ("main.py", MAIN.as_bytes())],
    );
    let ontology = Ontology::load_embedded().unwrap();
    let export = |batch_bytes| {
        let tables = std::sync::Mutex::new(std::collections::BTreeMap::<_, Vec<_>>::new());
        templates::repository(
            common::walk(repo.path()),
            templates::Options {
                batch_bytes,
                ..options()
            },
            &envelope(),
            &ontology,
            code_graph_incremental::export::ExportConfig::embedded(),
            |batches| -> Result<(), String> {
                let mut tables = tables.lock().unwrap();
                for (name, batch) in batches {
                    tables.entry(name).or_default().push(batch);
                }
                Ok(())
            },
        )
        .unwrap();
        tables.into_inner().unwrap()
    };
    let whole = export(orbit_arrow::BATCH_BYTES);
    let split = export(1);
    assert_eq!(
        whole.keys().collect::<Vec<_>>(),
        split.keys().collect::<Vec<_>>()
    );
    for (name, batches) in &whole {
        let [batch] = batches.as_slice() else {
            panic!("{name} came in {} batches", batches.len());
        };
        let pieces = &split[name];
        assert!(pieces.iter().all(|piece| piece.num_rows() <= 1), "{name}");
        assert_eq!(
            pieces.iter().filter(|piece| piece.num_rows() > 0).count(),
            batch.num_rows(),
            "{name}"
        );
        let names: Vec<_> = batch
            .schema()
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect();
        let names: Vec<_> = names.iter().map(String::as_str).collect();
        let joined = arrow::compute::concat_batches(&batch.schema(), pieces).unwrap();
        assert_eq!(rows(&joined, &names), rows(batch, &names), "{name}");
    }
}

#[test]
fn cross_file_timeout_still_exports_every_family() {
    let repo = tempfile::tempdir().unwrap();
    write_all(
        repo.path(),
        &[
            ("main.py", b"def python_function():\n    pass\n"),
            ("main.ts", b"export function typescriptFunction() {}"),
        ],
    );
    let ontology = Ontology::load_embedded().unwrap();
    let definitions = std::sync::Mutex::new(Vec::new());
    let report = templates::repository(
        common::walk(repo.path()),
        templates::Options {
            limits: Limits {
                cross_file_resolve_ms: 0,
                ..Limits::UNLIMITED
            },
            ..options()
        },
        &envelope(),
        &ontology,
        code_graph_incremental::export::ExportConfig::embedded(),
        |batches| -> Result<(), String> {
            for (name, batch) in batches {
                if name == "gl_definition" {
                    definitions.lock().unwrap().extend(column(&batch, "name"));
                }
            }
            Ok(())
        },
    )
    .unwrap();
    let mut definitions = definitions.into_inner().unwrap();
    definitions.sort();
    assert_eq!(definitions, ["python_function", "typescriptFunction"]);
    assert_eq!(report.skipped.len(), 2);
    assert!(
        report
            .skipped
            .iter()
            .all(|skip| skip.label == "resolve" && !skip.cancelled)
    );
}

#[test]
fn definition_coordinates_cover_the_same_span_as_bytes() {
    for (language, path, source, expected) in [
        (
            SupportLang::Ruby,
            "main.rb",
            "class User\n  def run; end\nend\n",
            vec![strs(&["User", "1", "3", "1", "4", "0", "29"])],
        ),
        (
            SupportLang::Python,
            "main.py",
            "def run():\n    pass\n",
            vec![strs(&["run", "1", "2", "1", "9", "0", "19"])],
        ),
    ] {
        let repo = tempfile::tempdir().unwrap();
        write_all(repo.path(), &[(path, source.as_bytes())]);
        let env = Env::with_limits(language, Limits::UNLIMITED).unwrap();
        let ontology = Ontology::load_embedded().unwrap();
        let exported = templates::index(Context::new(&env, common::walk(repo.path())))
            .unwrap()
            .then(Display)
            .unwrap()
            .then(Export {
                ontology: &ontology,
                envelope: envelope(),
            })
            .unwrap()
            .into_value();
        let actual: Vec<_> = rows(
            table(&exported, "gl_definition"),
            &[
                "name",
                "start_line",
                "end_line",
                "start_char",
                "end_char",
                "start_byte",
                "end_byte",
            ],
        )
        .into_iter()
        .filter(|row| row[0] == expected[0][0])
        .collect();
        assert_eq!(actual, expected, "{language:?}");
    }
}

#[test]
fn every_file_becomes_a_row_with_its_reason() {
    let repo = tempfile::tempdir().unwrap();
    write_all(
        repo.path(),
        &[
            ("src/main.py", MAIN.as_bytes()),
            ("README.md", b"# hi\n"),
            ("logo.png", b"\x89PNG\x00\x00"),
        ],
    );

    let exported = export_repo(repo.path());

    let files = table(&exported, "gl_file");
    assert_eq!(
        rows(
            files,
            &[
                "path",
                "name",
                "extension",
                "size_bytes",
                "reason",
                "project_id",
                "branch"
            ]
        ),
        [
            strs(&["README.md", "README.md", "md", "5", "", "7", "main"]),
            strs(&[
                "logo.png",
                "logo.png",
                "png",
                "6",
                "skip_excluded_extension",
                "7",
                "main"
            ]),
            strs(&[
                "src/main.py",
                "main.py",
                "py",
                &MAIN.len().to_string(),
                "",
                "7",
                "main"
            ]),
        ]
    );
    assert_eq!(
        column(table(&exported, "gl_directory"), "path"),
        [".", "src"]
    );
    assert_eq!(
        column(table(&exported, "gl_directory"), "name"),
        [".", "src"]
    );
    let root_id = column(table(&exported, "gl_directory"), "id")[0].clone();
    let edges = rows(
        table(&exported, "gl_edge"),
        &["source_id", "relationship_kind", "target_id"],
    );
    let root_children = edges
        .iter()
        .filter(|edge| edge[0] == root_id && edge[1] == "CONTAINS")
        .count();
    assert_eq!(root_children, 3);
}

#[test]
fn definitions_imports_and_edges_export_as_ontology_rows() {
    let repo = tempfile::tempdir().unwrap();
    write_all(
        repo.path(),
        &[("utils.py", UTILS.as_bytes()), ("main.py", MAIN.as_bytes())],
    );

    let exported = export_repo(repo.path());

    assert_eq!(
        rows(
            table(&exported, "gl_definition"),
            &["fqn", "name", "definition_type", "file_path", "start_line"]
        ),
        [
            strs(&["main.run", "run", "Function", "main.py", "4"]),
            strs(&["utils.Store", "Store", "Class", "utils.py", "4"]),
            strs(&["utils.Store.get", "get", "Method", "utils.py", "5"]),
            strs(&["utils.helper", "helper", "Function", "utils.py", "1"]),
        ]
    );
    assert_eq!(
        rows(
            table(&exported, "gl_imported_symbol"),
            &["identifier_name", "import_path", "import_type", "file_path"]
        ),
        [
            strs(&["Store", "utils", "NamedImport", "main.py"]),
            strs(&["helper", "utils", "NamedImport", "main.py"]),
            strs(&["missing", "missing", "Import", "main.py"]),
        ]
    );
    let edges = table(&exported, "gl_edge");
    let import_to_def = rows(edges, &["source_kind", "relationship_kind", "target_kind"])
        .iter()
        .filter(|r| r == &&strs(&["ImportedSymbol", "IMPORTS", "Definition"]))
        .count();
    assert_eq!(
        import_to_def, 2,
        "both names of `from utils import helper, Store` resolve"
    );
    let mut kinds: Vec<(String, String, String)> =
        rows(edges, &["source_kind", "relationship_kind", "target_kind"])
            .into_iter()
            .map(|r| (r[0].clone(), r[1].clone(), r[2].clone()))
            .collect();
    kinds.dedup();
    assert_eq!(
        kinds,
        [
            ("Definition".into(), "CALLS".into(), "Definition".into()),
            ("Definition".into(), "DEFINES".into(), "Definition".into()),
            ("Directory".into(), "CONTAINS".into(), "File".into()),
            ("File".into(), "DEFINES".into(), "Definition".into()),
            ("File".into(), "IMPORTS".into(), "ImportedSymbol".into()),
            (
                "ImportedSymbol".into(),
                "IMPORTS".into(),
                "Definition".into()
            ),
        ]
    );
}

#[test]
fn ids_depend_on_content_not_on_the_run() {
    let repo = tempfile::tempdir().unwrap();
    write_all(
        repo.path(),
        &[("utils.py", UTILS.as_bytes()), ("main.py", MAIN.as_bytes())],
    );

    let first = export_repo(repo.path());
    let second = export_repo(repo.path());

    for name in ["gl_file", "gl_definition", "gl_imported_symbol", "gl_edge"] {
        let columns: &[&str] = if name == "gl_edge" {
            &["source_id", "target_id"]
        } else {
            &["id"]
        };
        assert_eq!(
            rows(table(&first, name), columns),
            rows(table(&second, name), columns),
            "{name}"
        );
    }
}

#[test]
fn emit_hands_every_table_to_the_sink_and_keeps_the_graph() {
    let repo = tempfile::tempdir().unwrap();
    write_all(
        repo.path(),
        &[("utils.py", UTILS.as_bytes()), ("main.py", MAIN.as_bytes())],
    );
    let env = Env::with_limits(SupportLang::Python, Limits::UNLIMITED).unwrap();
    let ontology = Ontology::load_embedded().unwrap();
    let mut seen: Vec<(String, usize)> = Vec::new();

    let displayed = templates::index(Context::new(&env, common::walk(repo.path())))
        .unwrap()
        .then(Display)
        .unwrap()
        .then(Export {
            ontology: &ontology,
            envelope: envelope(),
        })
        .unwrap()
        .then(Emit(
            |table: &str, batch: RecordBatch| -> Result<(), String> {
                seen.push((table.to_string(), batch.num_rows()));
                Ok(())
            },
        ))
        .unwrap()
        .into_value();

    seen.sort();
    let names: Vec<_> = seen.iter().map(|(t, _)| t.as_str()).collect();
    assert_eq!(
        names,
        [
            "gl_definition",
            "gl_directory",
            "gl_edge",
            "gl_file",
            "gl_imported_symbol"
        ]
    );
    let empty: Vec<_> = seen
        .iter()
        .filter(|(_, n)| *n == 0)
        .map(|(t, _)| t.as_str())
        .collect();
    assert!(empty.is_empty());
    assert_eq!(
        seen.iter()
            .find(|(table, _)| table == "gl_directory")
            .unwrap()
            .1,
        1
    );
    assert_eq!(displayed.state.trees.len(), 2);
}

#[test]
fn exporting_deeply_nested_definitions_grows_linearly() {
    use std::time::Instant;

    fn export_ms(levels: usize) -> f64 {
        let mut source = String::new();
        for i in 0..levels {
            source += &format!("function h{i}() {{\n");
        }
        source += &"}\n".repeat(levels);
        let repo = tempfile::tempdir().unwrap();
        write_all(repo.path(), &[("deep.js", source.as_bytes())]);
        let env = Env::with_limits(SupportLang::JavaScript, Limits::UNLIMITED).unwrap();
        let ontology = Ontology::load_embedded().unwrap();
        let displayed = templates::index(Context::new(&env, common::walk(repo.path())))
            .unwrap()
            .then(Display)
            .unwrap();
        let started = Instant::now();
        displayed
            .then(Export {
                ontology: &ontology,
                envelope: envelope(),
            })
            .unwrap();
        started.elapsed().as_secs_f64() * 1000.0
    }

    let pool = rayon::ThreadPoolBuilder::new()
        .stack_size(templates::WORKER_STACK_BYTES)
        .build()
        .unwrap();
    let best = |levels| (0..3).map(|_| export_ms(levels)).fold(f64::MAX, f64::min);
    let (small, large) = pool.install(|| (best(1_000), best(16_000)));
    assert!(
        large < 64.0 * small.max(1.0),
        "exporting 16x the nesting went from {small:.0} ms to {large:.0} ms"
    );
}
