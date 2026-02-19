fn main() {
    println!("cargo:rerun-if-changed=build.rs");
    #[cfg(feature = "regenerate-protos")]
    regenerate_protos();
}

#[cfg(feature = "regenerate-protos")]
fn expand_tilde(path: &str) -> String {
    if let Some(rest) = path.strip_prefix("~/")
        && let Ok(home) = std::env::var("HOME")
    {
        return format!("{home}/{rest}");
    }
    path.to_string()
}

#[cfg(feature = "regenerate-protos")]
fn regenerate_protos() {
    println!("cargo:rerun-if-env-changed=GITALY_PROTO_ROOT");

    let Ok(root) = std::env::var("GITALY_PROTO_ROOT").map(|p| expand_tilde(&p)) else {
        println!("cargo:warning=GITALY_PROTO_ROOT not set, skipping proto regeneration");
        return;
    };
    let proto_dir = format!("{root}/proto");

    if !std::path::Path::new(&proto_dir).exists() {
        println!(
            "cargo:warning=GITALY_PROTO_ROOT path does not exist ({proto_dir}), skipping proto regeneration"
        );
        return;
    }

    println!("cargo:rerun-if-changed={proto_dir}");

    let protos = [
        format!("{proto_dir}/repository.proto"),
        format!("{proto_dir}/commit.proto"),
        format!("{proto_dir}/ref.proto"),
        format!("{proto_dir}/diff.proto"),
        format!("{proto_dir}/blob.proto"),
    ];

    if let Ok(protoc) = protoc_bin_vendored::protoc_bin_path() {
        println!("cargo:warning=Using vendored protoc at {protoc:?}");
        // SAFETY: build script runs single-threaded before compilation
        unsafe {
            std::env::set_var("PROTOC", protoc);
        }
    }

    let out_dir =
        std::path::PathBuf::from(std::env::var("CARGO_MANIFEST_DIR").unwrap()).join("src/proto");

    tonic_prost_build::configure()
        .out_dir(&out_dir)
        .compile_protos(&protos, &[proto_dir])
        .expect("Failed to compile Gitaly protos");

    println!("cargo:warning=Regenerated protos to {}", out_dir.display());
}
