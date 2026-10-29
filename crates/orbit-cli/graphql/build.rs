use std::fs;
use std::path::{Path, PathBuf};

use syn::punctuated::Punctuated;
use syn::{Attribute, Field, Item, ItemStruct, Meta, Token, parse_quote};

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
    write_formatted(path, &parse_generated(&source));
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
                .unwrap_or_else(|| panic!("{}: query file name must be UTF-8", path.display()));
            let query = fs::read_to_string(path).expect("query file must be readable");
            let structs = generator
                .generate(&query)
                .unwrap_or_else(|error| panic!("{}: {error}", path.display()));
            format!("pub mod {name} {{ use crate::schema; {structs} }}")
        })
        .collect();
    let mut queries = parse_generated(&modules);
    queries
        .items
        .iter_mut()
        .filter_map(|item| match item {
            Item::Mod(module) => module.content.as_mut(),
            _ => None,
        })
        .flat_map(|(_, items)| items.iter_mut())
        .for_each(make_fragment_serializable);
    write_formatted(target, &queries);
}

fn make_fragment_serializable(item: &mut Item) {
    let Item::Struct(fragment) = item else {
        return;
    };
    if !derives_query_fragment(fragment) {
        return;
    }
    fragment
        .attrs
        .push(parse_quote!(#[derive(serde::Serialize)]));
    fragment
        .attrs
        .push(parse_quote!(#[serde(rename_all = "camelCase")]));
    fragment
        .fields
        .iter_mut()
        .filter(|field| is_conditional(field))
        .for_each(make_optional);
}

fn derives_query_fragment(fragment: &ItemStruct) -> bool {
    fragment
        .attrs
        .iter()
        .any(|attr| attr.path().is_ident("derive") && names_any_of(attr, &["QueryFragment"]))
}

fn is_conditional(field: &Field) -> bool {
    field
        .attrs
        .iter()
        .any(|attr| attr.path().is_ident("directives") && names_any_of(attr, &["include", "skip"]))
}

fn names_any_of(attr: &Attribute, names: &[&str]) -> bool {
    attr.parse_args_with(Punctuated::<Meta, Token![,]>::parse_terminated)
        .expect("generated attributes must parse")
        .iter()
        .filter_map(|meta| meta.path().segments.last())
        .any(|segment| names.iter().any(|name| segment.ident == name))
}

fn make_optional(field: &mut Field) {
    let ty = &field.ty;
    field.ty = parse_quote!(Option<#ty>);
    field
        .attrs
        .push(parse_quote!(#[serde(skip_serializing_if = "Option::is_none")]));
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

fn parse_generated(source: &str) -> syn::File {
    syn::parse_file(source).expect("generated code must be valid Rust")
}

fn write_formatted(path: &Path, file: &syn::File) {
    fs::write(path, prettyplease::unparse(file)).expect("OUT_DIR must be writable");
}
