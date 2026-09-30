use std::path::Path;

use arrow::record_batch::RecordBatch;
use code_graph_incremental::pipeline::{Display, Emit, Export, Exported};
use code_graph_incremental::treesitter::SupportLang;
use code_graph_incremental::{Context, Env, Envelope, Limits, Scalar, inventory, templates};
use ontology::Ontology;
use orbit_utils::arrow::ArrowUtils;

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

fn export_repo(root: &Path) -> Exported {
    let env = Env::with_limits(SupportLang::Python, Limits::UNLIMITED).unwrap();
    let ontology = Ontology::load_embedded().unwrap();
    let (repo, inventory) = inventory::walk(root).unwrap();
    templates::index(Context::new(&env), repo, inventory.into_inner())
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
        .map(|row| ArrowUtils::array_value_to_string(array.as_ref(), row).unwrap_or_default())
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
    assert_eq!(column(table(&exported, "gl_directory"), "path"), ["src"]);
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
    let (files, inventory) = inventory::walk(repo.path()).unwrap();
    let mut seen: Vec<(String, usize)> = Vec::new();

    let displayed = templates::index(Context::new(&env), files, inventory.into_inner())
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
    assert_eq!(
        empty,
        ["gl_directory"],
        "a flat repository has no directories"
    );
    assert_eq!(displayed.state.trees.len(), 2);
}
