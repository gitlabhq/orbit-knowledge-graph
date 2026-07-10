fn main() {
    println!("cargo:rerun-if-changed=build.rs");
    validate_named_queries();
    validate_migration_ledger();
    #[cfg(feature = "regenerate-protos")]
    regenerate_protos();
}

/// Fails the build on ontology/DDL drift from the fingerprint snapshot or a
/// malformed ledger. Mirrors `cargo xtask migration-ledger check`.
fn validate_migration_ledger() {
    let config_dir = std::path::PathBuf::from(env!("CONFIG_DIR"));
    let ledger_path = config_dir.join(ontology::migrations::LEDGER_FILE);
    let fingerprint_path = config_dir.join(ontology::migrations::FINGERPRINT_FILE);
    let version_path = config_dir.join("SCHEMA_VERSION");
    println!("cargo:rerun-if-changed={}", ledger_path.display());
    println!("cargo:rerun-if-changed={}", fingerprint_path.display());
    println!("cargo:rerun-if-changed={}", version_path.display());
    println!("cargo:rerun-if-changed={}/ontology", config_dir.display());

    let ontology = ontology::Ontology::load_embedded()
        .unwrap_or_else(|e| panic!("embedded ontology failed to load: {e}"));

    let current = ontology::migrations::Fingerprints {
        sources: ontology::migrations::source_fingerprints(),
        ddl: compiler::ddl_fingerprints(&ontology),
        active_objects: compiler::active_object_fingerprints(&ontology),
    };

    let committed_text = std::fs::read_to_string(&fingerprint_path).unwrap_or_else(|e| {
        panic!(
            "reading {}: {e}. Run `mise schema:bump` to create the fingerprint snapshot.",
            fingerprint_path.display()
        )
    });
    let committed = ontology::migrations::Fingerprints::parse(&committed_text)
        .unwrap_or_else(|e| panic!("{e}"));

    let version: u32 = std::fs::read_to_string(&version_path)
        .unwrap_or_else(|e| panic!("reading {}: {e}", version_path.display()))
        .trim()
        .parse()
        .unwrap_or_else(|e| panic!("{} must contain a u32: {e}", version_path.display()));

    let ledger_text = std::fs::read_to_string(&ledger_path)
        .unwrap_or_else(|e| panic!("reading {}: {e}", ledger_path.display()));
    let ledger = ontology::migrations::MigrationLedger::parse(&ledger_text)
        .unwrap_or_else(|e| panic!("{e}"));

    ontology::migrations::verify_snapshot(&ontology, &current, &committed, &ledger, version)
        .unwrap_or_else(|e| panic!("{e}"));
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
            .render(&values, &query.example_parameters())
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
