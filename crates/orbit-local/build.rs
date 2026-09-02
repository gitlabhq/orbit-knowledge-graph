fn main() {
    #[cfg(target_os = "macos")]
    {
        println!("cargo:rustc-link-arg-bin=orbit=-Wl,-rpath,@loader_path/deps");
    }

    validate_prompts();
    locate_reranker_bundle();

    println!(
        "cargo:rerun-if-changed={}",
        std::path::Path::new(env!("CONFIG_DIR"))
            .join("setup")
            .display()
    );

    // Release jobs run on `vX.Y.Z` tags (.gitlab/ci/release-local.yml), so
    // CI_COMMIT_TAG is the authoritative release version. `git describe` is a
    // best-effort convenience for local/dev builds; the static Cargo.toml
    // version is the last-resort fallback (source-tarball builds with no git).
    let version = std::env::var("CI_COMMIT_TAG")
        .ok()
        .filter(|tag| !tag.is_empty())
        .or_else(|| {
            std::process::Command::new("git")
                // --match pins us to release tags: this repo also has
                // `clients/orbitpb/vX.Y.Z` tags, and a bare --tags would pick the
                // nearest of either namespace.
                .args([
                    "describe", "--tags", "--always", "--dirty", "--match", "v[0-9]*",
                ])
                .output()
                .ok()
                .filter(|out| out.status.success())
                .map(|out| String::from_utf8_lossy(&out.stdout).trim().to_string())
                .filter(|described| !described.is_empty())
        })
        .unwrap_or_else(|| env!("CARGO_PKG_VERSION").to_string());

    // Publish strips the leading `v` (scripts/upload-local-cli-release.sh uses
    // `${CI_COMMIT_TAG#v}`), so the package-registry version glab downloads and
    // compares against in `--update` is a bare semver. Match that here.
    let version = version.strip_prefix('v').unwrap_or(&version);

    println!("cargo:rustc-env=ORBIT_VERSION={version}");
    println!("cargo:rerun-if-env-changed=CI_COMMIT_TAG");
    // Rebuild when HEAD or refs move so dev builds don't report a stale tag.
    println!("cargo:rerun-if-changed=../../.git/HEAD");
    println!("cargo:rerun-if-changed=../../.git/refs");
}

fn validate_prompts() {
    let dir = std::path::Path::new(env!("PROMPTS_DIR")).join("local");
    println!("cargo:rerun-if-changed={}", dir.display());
    orbit_prompts::Prompts::load_dir(&dir).unwrap_or_else(|e| panic!("{e}"));
}

fn locate_reranker_bundle() {
    println!("cargo::rustc-check-cfg=cfg(orbit_reranker_bundle)");
    println!("cargo:rerun-if-env-changed=ORBIT_RERANK_BUNDLE_DIR");
    let dir = std::env::var_os("ORBIT_RERANK_BUNDLE_DIR")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|| {
            std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../target/rerank-bundle")
        });
    let files = ["config.json", "tokenizer.json", "model.safetensors"];
    for file in files {
        println!("cargo:rerun-if-changed={}", dir.join(file).display());
    }
    let Ok(dir) = dir.canonicalize() else {
        println!(
            "cargo:warning=no reranker bundle at {}; `ask` will not rerank. Run `cargo xtask rerank-bundle`.",
            dir.display()
        );
        return;
    };
    if let Some(missing) = files.iter().find(|f| !dir.join(f).is_file()) {
        println!(
            "cargo:warning={} is missing; `ask` will not rerank. Run `cargo xtask rerank-bundle`.",
            dir.join(missing).display()
        );
        return;
    }
    println!("cargo:rustc-cfg=orbit_reranker_bundle");
    println!("cargo:rustc-env=ORBIT_RERANK_BUNDLE_DIR={}", dir.display());
}
