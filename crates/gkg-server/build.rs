fn main() {
    println!("cargo:rerun-if-changed=build.rs");
    validate_named_queries();
    #[cfg(feature = "regenerate-protos")]
    regenerate_protos();
}

fn validate_named_queries() {
    let dir = std::path::PathBuf::from(
        std::env::var("NAMED_QUERIES_DIR")
            .expect("NAMED_QUERIES_DIR must be set via .cargo/config.toml [env]"),
    );
    println!("cargo:rerun-if-changed={}", dir.display());

    let ontology = ontology::Ontology::load_embedded()
        .unwrap_or_else(|e| panic!("embedded ontology failed to load: {e}"));

    let ctx = compiler::SecurityContext::new(1, vec!["1/".into()])
        .expect("static security context must be valid");

    let queries = named_queries::NamedQueries::load_from_dir(&dir)
        .unwrap_or_else(|e| panic!("named queries failed to load: {e}"));

    let values = named_queries::BindingValues { current_user_id: 1 };
    for query in queries.iter() {
        let rendered = query
            .render(&values)
            .unwrap_or_else(|e| panic!("named query failed to render: {e}"));
        if let Err(e) = compiler::compile(&rendered, &ontology, &ctx) {
            panic!("named query `{}` failed to compile: {e}", query.name);
        }
    }
}

#[cfg(feature = "regenerate-protos")]
fn regenerate_protos() {
    use std::path::PathBuf;
    use std::process::Command;

    println!("cargo:rerun-if-changed=proto/gkg.proto");

    let proto_path = PathBuf::from("proto/gkg.proto");
    if !proto_path.exists() {
        println!("cargo:warning=proto/gkg.proto not found, skipping proto regeneration");
        return;
    }

    if Command::new("protoc").arg("--version").output().is_err() {
        println!("cargo:warning=protoc not found, skipping proto regeneration");
        return;
    }

    let out_dir = PathBuf::from(std::env::var("CARGO_MANIFEST_DIR").unwrap()).join("src/proto");

    std::fs::create_dir_all(&out_dir).expect("Failed to create src/proto directory");

    tonic_prost_build::configure()
        .out_dir(&out_dir)
        .compile_protos(&["proto/gkg.proto"], &["proto"])
        .expect("Failed to compile gkg protos");

    println!("cargo:warning=Regenerated protos to {}", out_dir.display());
}
