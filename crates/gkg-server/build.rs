fn main() {
    println!("cargo:rerun-if-changed=build.rs");
    validate_prompts();
    validate_named_queries();
    validate_migration_ledger();
    validate_authored_etl_sql();
    validate_channel_allowlists();
    #[cfg(feature = "regenerate-protos")]
    regenerate_protos();
}

/// ADR 013 §9: build-time presence/validity check for `channel_allowlist`.
///
/// Every node in the shipped ontology must have a non-empty, syntactically
/// valid `channel_allowlist` under its `redaction` block. An empty or missing
/// allowlist resolves to the empty channel set (fail-closed by design), which
/// would mean the entity is invisible to *every* channel including
/// `core_feature` — almost always a "forgot to populate" bug rather than an
/// intentional lockout. Failing the build catches this before the pass ships.
///
/// This complements the JSON schema check (`ontology-schema-validate`) but is
/// stricter: the JSON schema alone can't tell that a *loaded* node's
/// resolved allowlist is empty, only that the YAML is syntactically well-
/// formed. Running here in `build.rs` also means egress-less CI environments
/// and every local build catch the same drift, not just the CI job.
fn validate_channel_allowlists() {
    let ontology = ontology::Ontology::load_embedded()
        .unwrap_or_else(|e| panic!("embedded ontology failed to load: {e}"));
    let mut offenders = Vec::new();
    for node in ontology.nodes() {
        let Some(redaction) = node.redaction.as_ref() else {
            // Nodes with no redaction block can't be gated (no ontology row
            // → no channel_allowlist), so they're implicitly unrestricted.
            // Consistent with the compiler's `channel_allowlist_for_table`
            // returning None for these.
            continue;
        };
        if redaction.channel_allowlist.is_fail_closed_empty() {
            offenders.push(node.name.clone());
        }
    }
    if !offenders.is_empty() {
        panic!(
            "ADR 013: the following nodes are missing a `channel_allowlist` (or ship an empty one), \
             which resolves to no allowed channels — nobody would see them, not even `core_feature`. \
             Add a non-empty `channel_allowlist:` under each node's `redaction:` block. Offenders: {}",
            offenders.join(", ")
        );
    }
}

fn validate_prompts() {
    let dir = std::path::Path::new(env!("PROMPTS_DIR")).join("remote");
    println!("cargo:rerun-if-changed={}", dir.display());
    gkg_prompts::Prompts::load_dir(&dir).unwrap_or_else(|e| panic!("{e}"));
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
        auxiliary_schema: compiler::auxiliary_schema_fingerprints(&ontology),
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

/// Fails the build when authored ETL SQL hardcodes a watermark/deleted column
/// instead of using the `{{watermark_column}}`/`{{deleted_column}}` markers.
fn validate_authored_etl_sql() {
    let ontology = ontology::Ontology::load_embedded()
        .unwrap_or_else(|e| panic!("embedded ontology failed to load: {e}"));
    ontology::etl_sql::validate_authored_etl_sql(&ontology).unwrap_or_else(|e| panic!("{e}"));
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

    let values = named_queries::BindingValues {
        current_user_id: 1,
        current_channel: None,
    };
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
