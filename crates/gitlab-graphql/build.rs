use std::path::PathBuf;

fn main() {
    cynic_codegen::register_schema("gitlab")
        .from_sdl_file("schema/gitlab.graphql")
        .expect("GitLab GraphQL schema must load")
        .as_default()
        .expect("GitLab GraphQL schema must register");
    format_schema_module();
}

fn format_schema_module() {
    let path = PathBuf::from(std::env::var("OUT_DIR").expect("cargo sets OUT_DIR"))
        .join("cynic-schemas/gitlab.rs");
    let source = std::fs::read_to_string(&path).expect("cynic writes the schema module");
    let module = syn::parse_file(&source).expect("cynic writes valid Rust");
    std::fs::write(&path, prettyplease::unparse(&module)).expect("schema module must be writable");
}
