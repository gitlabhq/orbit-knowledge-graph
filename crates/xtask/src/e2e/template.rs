//! Minimal YAML template renderer.
//!
//! Loads `.yaml.tmpl` files from `e2e/templates/` and replaces `${VAR}`
//! tokens with supplied values. No external deps required.

use std::collections::HashMap;
use std::fs;
use std::path::Path;

use anyhow::{Context, Result};

/// Read a template file and replace all `${KEY}` tokens with the values
/// from `vars`. Unknown tokens are left as-is.
pub fn render(template_path: &Path, vars: &HashMap<&str, &str>) -> Result<String> {
    let raw = fs::read_to_string(template_path)
        .with_context(|| format!("reading template {}", template_path.display()))?;

    let mut output = raw;
    for (key, value) in vars {
        output = output.replace(&format!("${{{key}}}"), value);
    }

    Ok(output)
}
