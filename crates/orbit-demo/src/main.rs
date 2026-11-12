use std::path::PathBuf;

use orbit_demo::config::Config;
use orbit_demo::lockdown::system_resolve;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let path = std::env::args()
        .nth(1)
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from(concat!(env!("CARGO_MANIFEST_DIR"), "/demo.yaml")));
    let checked = Config::read(&path)?.check(&system_resolve)?;
    let demo = orbit_demo::start(checked).await?;
    println!(
        "Orbit demo: http://localhost:{}/",
        demo.addresses.page.port()
    );
    tokio::signal::ctrl_c().await?;
    Ok(())
}
