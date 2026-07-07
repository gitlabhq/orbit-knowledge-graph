fn main() {
    println!("cargo:rerun-if-changed=build.rs");
    validate_named_queries();
    validate_migration_ledger();
    #[cfg(feature = "regenerate-protos")]
    regenerate_protos();
}

/// Fails the build when the ontology + generated DDL drift from the committed
/// fingerprint snapshot, or when the migration ledger is malformed. Mirrors
/// `cargo xtask migration-ledger check` so the same guarantee holds locally and
/// in CI even without network access.
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
    };

    let committed_text = std::fs::read_to_string(&fingerprint_path).unwrap_or_else(|e| {
        panic!(
            "reading {}: {e}. Run `mise schema:bump` to create the fingerprint snapshot.",
            fingerprint_path.display()
        )
    });
    let committed = ontology::migrations::Fingerprints::parse(&committed_text)
        .unwrap_or_else(|e| panic!("{e}"));

    if current != committed {
        let (sources, ddl) = current.diff(&committed);
        panic!(
            "ontology drift not reflected in {}.\n  changed sources: {sources:?}\n  \
             changed tables: {ddl:?}\nRun `mise schema:bump` to record the change.",
            fingerprint_path.display()
        );
    }

    let version: u32 = std::fs::read_to_string(&version_path)
        .unwrap_or_else(|e| panic!("reading {}: {e}", version_path.display()))
        .trim()
        .parse()
        .unwrap_or_else(|e| panic!("{} must contain a u32: {e}", version_path.display()));

    let ledger_text = std::fs::read_to_string(&ledger_path)
        .unwrap_or_else(|e| panic!("reading {}: {e}", ledger_path.display()));
    let ledger = ontology::migrations::MigrationLedger::parse(&ledger_text)
        .unwrap_or_else(|e| panic!("{e}"));
    ledger
        .validate(&ontology, version)
        .unwrap_or_else(|e| panic!("migration ledger invalid: {e}"));
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

    let mut paths: Vec<_> = std::fs::read_dir(&dir)
        .unwrap_or_else(|e| panic!("failed to read {}: {e}", dir.display()))
        .map(|entry| entry.expect("failed to read directory entry").path())
        .filter(|path| path.extension().is_some_and(|ext| ext == "yaml"))
        .collect();
    paths.sort();
    assert!(
        !paths.is_empty(),
        "no named queries found in {}",
        dir.display()
    );

    let mut names = std::collections::HashSet::new();
    for path in paths {
        let query = NamedQuery::parse(&path);
        assert!(
            names.insert(query.name.clone()),
            "duplicate named query `{}`",
            query.name
        );

        let rendered = query.rendered_for_validation();
        if let Err(e) = compiler::compile(&rendered.to_string(), &ontology, &ctx) {
            panic!("named query `{}` failed to compile: {e}", query.name);
        }
    }
}

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct NamedQuery {
    name: String,
    description: String,
    #[serde(default)]
    bindings: Vec<String>,
    query: serde_json::Value,
}

const BINDING_KEY: &str = "$binding";
const KNOWN_BINDINGS: &[&str] = &["current_user_id"];

impl NamedQuery {
    fn parse(path: &std::path::Path) -> Self {
        let content = std::fs::read_to_string(path)
            .unwrap_or_else(|e| panic!("failed to read {}: {e}", path.display()));
        let query: NamedQuery = serde_yaml::from_str(&content)
            .unwrap_or_else(|e| panic!("failed to parse {}: {e}", path.display()));

        let stem = path
            .file_stem()
            .map(|s| s.to_string_lossy().to_string())
            .unwrap_or_default();
        assert_eq!(
            query.name,
            stem,
            "{}: `name` must match the file stem",
            path.display()
        );
        assert!(
            !query.description.trim().is_empty(),
            "named query `{}` needs a description",
            query.name
        );
        query
    }

    fn rendered_for_validation(&self) -> serde_json::Value {
        let mut rendered = self.query.clone();
        let mut used = std::collections::HashSet::new();
        self.substitute(&mut rendered, &mut used);
        for binding in &self.bindings {
            assert!(
                used.contains(binding.as_str()),
                "named query `{}` declares binding `{binding}` but never uses it; \
                 remove it from `bindings:`",
                self.name
            );
        }
        rendered
    }

    fn substitute(
        &self,
        value: &mut serde_json::Value,
        used: &mut std::collections::HashSet<String>,
    ) {
        match value {
            serde_json::Value::Object(map) => {
                if let Some(binding) = map.get(BINDING_KEY) {
                    assert_eq!(
                        map.len(),
                        1,
                        "named query `{}`: a {BINDING_KEY} object must have no other keys",
                        self.name
                    );
                    let binding = binding.as_str().unwrap_or_else(|| {
                        panic!(
                            "named query `{}`: {BINDING_KEY} value must be a string",
                            self.name
                        )
                    });
                    assert!(
                        KNOWN_BINDINGS.contains(&binding),
                        "named query `{}` uses unknown binding `{binding}`",
                        self.name
                    );
                    assert!(
                        self.bindings.iter().any(|b| b == binding),
                        "named query `{}` uses undeclared binding `{binding}`; declare it under `bindings:`",
                        self.name
                    );
                    used.insert(binding.to_string());
                    *value = serde_json::Value::from(1);
                    return;
                }
                for nested in map.values_mut() {
                    self.substitute(nested, used);
                }
            }
            serde_json::Value::Array(items) => {
                for item in items {
                    self.substitute(item, used);
                }
            }
            _ => {}
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
