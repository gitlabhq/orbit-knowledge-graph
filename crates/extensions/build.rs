fn main() {
    println!("cargo:rerun-if-changed=build.rs");
    #[cfg(feature = "regenerate-protos")]
    regenerate_protos();
}

/// Regenerates the committed gRPC stubs from the contract proto. The generated
/// `src/proto/*.rs` is checked in, so a normal build needs neither protoc nor
/// this feature. Run `cargo build -p extensions --features regenerate-protos`
/// after editing the proto.
#[cfg(feature = "regenerate-protos")]
fn regenerate_protos() {
    use std::path::PathBuf;

    const PROTO: &str = "proto/orbit/extensions/v1alpha1/provider.proto";
    println!("cargo:rerun-if-changed={PROTO}");

    let out_dir = PathBuf::from(std::env::var("CARGO_MANIFEST_DIR").unwrap()).join("src/proto");
    std::fs::create_dir_all(&out_dir).expect("create src/proto directory");

    tonic_prost_build::configure()
        .out_dir(&out_dir)
        .build_server(true)
        .build_client(true)
        .compile_protos(&[PROTO], &["proto"])
        .expect("compile extensions proto");

    println!(
        "cargo:warning=regenerated extensions protos to {}",
        out_dir.display()
    );
}
