fn main() {
    #[cfg(target_os = "macos")]
    {
        println!("cargo:rustc-link-arg-bin=orbit=-Wl,-rpath,@loader_path/deps");
    }

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
                // `clients/gkgpb/vX.Y.Z` tags, and a bare --tags would pick the
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
