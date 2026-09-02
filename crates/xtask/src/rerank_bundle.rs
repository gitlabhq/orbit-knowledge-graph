//! f16 halves what the CLI embeds; it upcasts to f32 at load, so scores stay within ~1e-3.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::process::Command;

use anyhow::{Context, Result, bail};
use safetensors::tensor::TensorView;
use safetensors::{Dtype, SafeTensors};
use sha2::{Digest, Sha256};

pub const MODEL: &str = "cross-encoder/ms-marco-MiniLM-L2-v2";
/// Upstream commit the scores were validated against; `main` is mutable.
const REVISION: &str = "1b5cd67b15209f24824c50370e0397743aa9b787";
const HUB_BASE: &str = "https://huggingface.co";
const FILES: [(&str, &str); 3] = [
    (
        "config.json",
        "7868e36c3024c21f7a3ac64e058b36898a331035784cce7ec1496b434aa44c4f",
    ),
    (
        "tokenizer.json",
        "d241a60d5e8f04cc1b2b3e9ef7a4921b27bf526d9f6050ab90f9267a1f9e5c66",
    ),
    (
        "model.safetensors",
        "88f11fa671e11c53b5cfe88bb6594139ec4991eaf8cf6a10bd61c9abbc4f691a",
    ),
];

pub fn run(out: Option<&Path>) -> Result<()> {
    let out = out.map_or_else(default_out, Path::to_path_buf);
    let marker = out.join("MODEL");
    let stamp = format!("{MODEL}@{REVISION}\n");
    if std::fs::read_to_string(&marker).is_ok_and(|m| m == stamp)
        && FILES.iter().all(|(f, _)| out.join(f).is_file())
    {
        println!(
            "bundle for {MODEL}@{REVISION} already present in {}",
            out.display()
        );
        return Ok(());
    }
    std::fs::create_dir_all(&out)?;
    for (file, sha256) in FILES {
        let url = format!("{HUB_BASE}/{MODEL}/resolve/{REVISION}/{file}");
        let target = out.join(file);
        let status = Command::new("curl")
            .args(["-sSfL", "--retry", "3", "-o"])
            .arg(&target)
            .arg(&url)
            .status()
            .context("failed to run curl")?;
        if !status.success() {
            bail!("failed to download {url}");
        }
        let actual: String = Sha256::digest(std::fs::read(&target)?)
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect();
        if actual != sha256 {
            bail!("{file} from {url} has sha256 {actual}, expected {sha256}");
        }
    }
    let weights = out.join("model.safetensors");
    let f32_bytes = std::fs::read(&weights)?;
    let f16_bytes = to_f16(&f32_bytes)?;
    std::fs::write(&weights, &f16_bytes)?;
    std::fs::write(&marker, stamp)?;
    println!(
        "bundled {MODEL}@{REVISION} into {} ({} -> {} bytes of weights)",
        out.display(),
        f32_bytes.len(),
        f16_bytes.len()
    );
    Ok(())
}

fn default_out() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../../target/rerank-bundle")
}

/// Non-F32 tensors (`position_ids` is I64) pass through unchanged.
fn to_f16(bytes: &[u8]) -> Result<Vec<u8>> {
    let tensors = SafeTensors::deserialize(bytes).context("invalid safetensors file")?;
    let mut converted: Vec<(String, Vec<u8>, Vec<usize>, Dtype)> = Vec::new();
    for (name, view) in tensors.tensors() {
        let (data, dtype) = match view.dtype() {
            Dtype::F32 => {
                let halves: Vec<u8> = view
                    .data()
                    .chunks_exact(4)
                    .flat_map(|c| {
                        half::f16::from_f32(f32::from_le_bytes([c[0], c[1], c[2], c[3]]))
                            .to_le_bytes()
                    })
                    .collect();
                (halves, Dtype::F16)
            }
            other => (view.data().to_vec(), other),
        };
        converted.push((name, data, view.shape().to_vec(), dtype));
    }
    let views: Vec<(String, TensorView<'_>)> = converted
        .iter()
        .map(|(name, data, shape, dtype)| {
            TensorView::new(*dtype, shape.clone(), data)
                .map(|v| (name.clone(), v))
                .context("failed to build tensor view")
        })
        .collect::<Result<_>>()?;
    let metadata: Option<HashMap<String, String>> = None;
    safetensors::serialize(views, metadata).context("failed to serialize f16 safetensors")
}
