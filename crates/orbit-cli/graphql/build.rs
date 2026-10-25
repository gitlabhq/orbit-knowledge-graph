use std::fs;
use std::path::{Path, PathBuf};

const SCHEMA: &str = "schema/gitlab.graphql";
const QUERIES: &str = "queries";

fn main() {
    cynic_codegen::register_schema("gitlab")
        .from_sdl_file(SCHEMA)
        .expect("GitLab GraphQL schema must load")
        .as_default()
        .expect("GitLab GraphQL schema must register");
    let out_dir = PathBuf::from(std::env::var("OUT_DIR").expect("cargo sets OUT_DIR"));
    format_schema_module(&out_dir.join("cynic-schemas/gitlab.rs"));
    generate_queries(&out_dir.join("queries.rs"));
}

fn format_schema_module(path: &Path) {
    let source = fs::read_to_string(path).expect("cynic writes the schema module");
    write_formatted(path, &source);
}

fn generate_queries(target: &Path) {
    println!("cargo:rerun-if-changed={QUERIES}");
    let schema = fs::read_to_string(SCHEMA).expect("GitLab GraphQL schema must load");
    let generator =
        cynic_querygen::Generator::new(schema).expect("GitLab GraphQL schema must parse");
    let modules: String = query_files()
        .iter()
        .map(|path| {
            let name = path
                .file_stem()
                .and_then(|stem| stem.to_str())
                .expect("query file name");
            let query = fs::read_to_string(path).expect("query file must be readable");
            let structs = generator
                .generate(&query)
                .unwrap_or_else(|error| panic!("{}: {error}", path.display()));
            format!("pub mod {name} {{ use crate::schema; {structs} }}")
        })
        .collect();
    write_formatted(target, &modules);
}

fn query_files() -> Vec<PathBuf> {
    let mut paths: Vec<PathBuf> = fs::read_dir(QUERIES)
        .expect("queries directory must exist")
        .map(|entry| entry.expect("queries directory must be readable").path())
        .filter(|path| {
            path.extension()
                .is_some_and(|extension| extension == "graphql")
        })
        .collect();
    paths.sort();
    paths
}

fn write_formatted(path: &Path, source: &str) {
    let module = syn::parse_file(source).expect("generated code must be valid Rust");
    fs::write(path, prettyplease::unparse(&module)).expect("OUT_DIR must be writable");
}
