//! f16 halves what the CLI embeds; it upcasts to f32 at load, so scores stay within ~1e-3.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::process::Command;

use anyhow::{Context, Result, bail};
use safetensors::tensor::TensorView;
use safetensors::{Dtype, SafeTensors};

pub const DEFAULT_MODEL: &str = "cross-encoder/ms-marco-MiniLM-L2-v2";
const HUB_BASE: &str = "https://huggingface.co";
const FILES: [&str; 3] = ["config.json", "tokenizer.json", "model.safetensors"];

pub fn run(model: &str, out: Option<&Path>) -> Result<()> {
    let out = out.map_or_else(default_out, Path::to_path_buf);
    let marker = out.join("MODEL");
    if std::fs::read_to_string(&marker).is_ok_and(|m| m.trim() == model)
        && FILES.iter().all(|f| out.join(f).is_file())
    {
        println!("bundle for {model} already present in {}", out.display());
        return Ok(());
    }
    std::fs::create_dir_all(&out)?;
    for file in FILES {
        let url = format!("{HUB_BASE}/{model}/resolve/main/{file}");
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
    }
    let weights = out.join("model.safetensors");
    let f32_bytes = std::fs::read(&weights)?;
    let f16_bytes = to_f16(&f32_bytes)?;
    std::fs::write(&weights, &f16_bytes)?;
    std::fs::write(&marker, format!("{model}\n"))?;
    println!(
        "bundled {model} into {} ({} -> {} bytes of weights)",
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
