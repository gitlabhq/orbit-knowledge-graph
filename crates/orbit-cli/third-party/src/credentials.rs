use anyhow::{Context, Result};

const SERVICE: &str = "gitlab-orbit-third-party";

#[cfg(target_os = "macos")]
pub const STORE_NAME: &str = "macOS Keychain";
#[cfg(target_os = "windows")]
pub const STORE_NAME: &str = "Windows Credential Manager";
#[cfg(not(any(target_os = "macos", target_os = "windows")))]
pub const STORE_NAME: &str = "kernel keyring until reboot";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TokenFrom {
    Env,
    Keychain,
}

fn entry(source: &str) -> Result<keyring::Entry> {
    keyring::Entry::new(SERVICE, source).context("opening the OS keychain")
}

pub fn store(source: &str, token: &str) -> Result<()> {
    entry(source)?
        .set_password(token)
        .with_context(|| format!("saving the {source} token to the OS keychain"))
}

pub fn load(source: &str) -> Result<Option<String>> {
    match entry(source)?.get_password() {
        Ok(token) => Ok(Some(token)),
        Err(keyring::Error::NoEntry) => Ok(None),
        Err(error) => Err(error).with_context(|| format!("reading the {source} token")),
    }
}

pub fn remove(source: &str) -> Result<bool> {
    match entry(source)?.delete_credential() {
        Ok(()) => Ok(true),
        Err(keyring::Error::NoEntry) => Ok(false),
        Err(error) => Err(error).with_context(|| format!("removing the {source} token")),
    }
}

pub fn resolve(source: &str, env: Option<&str>) -> Option<(String, TokenFrom)> {
    if let Some(token) = env.and_then(variable).filter(|token| !token.is_empty()) {
        return Some((token, TokenFrom::Env));
    }
    load(source)
        .ok()
        .flatten()
        .map(|token| (token, TokenFrom::Keychain))
}

#[cfg(not(test))]
fn variable(name: &str) -> Option<String> {
    std::env::var(name).ok()
}

#[cfg(test)]
fn variable(name: &str) -> Option<String> {
    match name {
        "ORBIT_TEST_TOKEN_GOOD" => Some("xoxb-good".into()),
        "ORBIT_TEST_TOKEN_GRID" => Some("xoxe-grid".into()),
        _ => std::env::var(name).ok(),
    }
}
