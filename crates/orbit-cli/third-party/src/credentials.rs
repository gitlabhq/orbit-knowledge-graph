use anyhow::{Context, Result};

const SERVICE: &str = "gitlab-orbit-third-party";

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
    if let Some(token) = env
        .and_then(|name| std::env::var(name).ok())
        .filter(|token| !token.is_empty())
    {
        return Some((token, TokenFrom::Env));
    }
    load(source)
        .ok()
        .flatten()
        .map(|token| (token, TokenFrom::Keychain))
}
