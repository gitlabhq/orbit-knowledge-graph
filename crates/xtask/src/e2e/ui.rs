//! Styled terminal output helpers backed by [`cliclack`].

use anyhow::Result;

/// Print a phase / session header.
pub fn banner(title: &str) -> Result<()> {
    cliclack::intro(title)?;
    Ok(())
}

/// Print a key-value detail line (e.g. config summary).
pub fn detail(key: &str, value: &str) -> Result<()> {
    cliclack::log::remark(format!("{key}: {value}"))?;
    Ok(())
}

/// Print a numbered step header.
pub fn step(number: u8, text: &str) -> Result<()> {
    cliclack::log::step(format!("{number}. {text}"))?;
    Ok(())
}

/// Print an info / status line.
pub fn info(text: &str) -> Result<()> {
    cliclack::log::info(text)?;
    Ok(())
}

/// Print a warning.
pub fn warn(text: &str) -> Result<()> {
    cliclack::log::warning(text)?;
    Ok(())
}

/// Print a success / step-completion line.
pub fn done(text: &str) -> Result<()> {
    cliclack::log::success(text)?;
    Ok(())
}

/// Print a detail line (indented, for sub-steps like file copies).
pub fn detail_item(text: &str) -> Result<()> {
    cliclack::log::remark(text)?;
    Ok(())
}

/// Print a final summary / outro banner.
pub fn outro(text: &str) -> Result<()> {
    cliclack::outro(text)?;
    Ok(())
}
