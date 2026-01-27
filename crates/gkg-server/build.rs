fn main() {
    #[cfg(feature = "regenerate-protos")]
    regenerate_protos();
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
