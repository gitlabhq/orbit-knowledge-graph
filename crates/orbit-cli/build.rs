fn main() {
    orbit_duckdb_link::link_downloaded_duckdb("orbit");

    validate_prompts();
    validate_skills();

    println!(
        "cargo:rerun-if-changed={}",
        std::path::Path::new(env!("ORBIT_CONFIG_DIR"))
            .join("setup")
            .display()
    );
    println!("cargo:rerun-if-changed=../../packages/agent-plugins");

    // Publish strips the leading `v` (scripts/release/upload-local-cli-release.sh uses
    // `${CI_COMMIT_TAG#v}`), so the package-registry version glab downloads and
    // compares against in `--update` is a bare semver. Match that here.
    if let Some(tag) = std::env::var("CI_COMMIT_TAG")
        .ok()
        .filter(|tag| !tag.is_empty())
    {
        let version = tag.strip_prefix('v').unwrap_or(&tag);
        println!("cargo:rustc-env=ORBIT_RELEASE_VERSION={version}");
    }
    println!("cargo:rerun-if-env-changed=CI_COMMIT_TAG");
}

fn validate_prompts() {
    let dir = std::path::Path::new(env!("PROMPTS_DIR")).join("local");
    println!("cargo:rerun-if-changed={}", dir.display());
    orbit_prompts::Prompts::load_dir(&dir).unwrap_or_else(|e| panic!("{e}"));
}

fn validate_skills() {
    let repository = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let remote = repository.join("skills/orbit");
    let local = repository.join("skills/orbit-cli");
    let commands = repository.join("crates/orbit-cli/src/main.rs");
    println!("cargo:rerun-if-changed={}", remote.display());
    println!("cargo:rerun-if-changed={}", local.display());
    println!("cargo:rerun-if-changed={}", commands.display());
    let validation = orbit_prompts::validate_skill_pair(remote, local, commands)
        .unwrap_or_else(|error| panic!("Orbit skill validation failed: {error}"));
    println!(
        "cargo:rustc-env=ORBIT_SKILL_REMOTE_COMMANDS={}",
        validation
            .remote_commands
            .into_iter()
            .collect::<Vec<_>>()
            .join(",")
    );
}
