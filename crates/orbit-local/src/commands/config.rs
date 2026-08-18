use anyhow::Result;

use crate::settings;

pub fn get(key: &str) -> Result<()> {
    match settings::get(key)? {
        Some(value) => println!("{value}"),
        None => println!("(unset)"),
    }
    Ok(())
}

pub fn set(key: &str, value: &str) -> Result<()> {
    let (normalized, path) = settings::set(key, value)?;
    println!("{key} = {normalized} (saved to {})", path.display());
    Ok(())
}

pub fn list() -> Result<()> {
    for (key, value) in settings::list() {
        match value {
            Some(value) => println!("{key} = {value}"),
            None => println!("{key} = (unset)"),
        }
    }
    Ok(())
}
