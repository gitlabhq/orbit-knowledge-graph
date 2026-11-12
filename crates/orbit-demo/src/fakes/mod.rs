//! The fakes of the remote providers, each a loopback HTTP server that speaks the request the
//! demo's executors render, the way `SeededColumnResolver` stands in for Gitaly.

pub mod orbit_next;
pub mod slack;

use std::net::SocketAddr;
use std::sync::{Arc, RwLock};

use axum::Router;
use axum::middleware::from_fn_with_state;
use axum::routing::post;
use integration_testkit::seeded_remote::SeededDataExtraction;

use crate::lockdown::{guard, listen};
use crate::seed::OrbitNext;

/// Seeded state a reload swaps while requests keep reading the previous one.
pub type Shared<T> = Arc<RwLock<Arc<T>>>;

pub fn shared<T>(value: T) -> Shared<T> {
    Arc::new(RwLock::new(Arc::new(value)))
}

pub fn current<T>(shared: &Shared<T>) -> Arc<T> {
    Arc::clone(
        &shared
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner()),
    )
}

pub fn replace<T>(shared: &Shared<T>, value: T) {
    *shared
        .write()
        .unwrap_or_else(|poisoned| poisoned.into_inner()) = Arc::new(value);
}

pub struct Fakes {
    pub orbit_next: Shared<OrbitNext>,
    pub slack: Shared<SeededDataExtraction>,
}

impl Fakes {
    /// Serves both fakes on the loopback ports of the checked endpoints, and returns the
    /// addresses they listen on.
    pub async fn serve(
        &self,
        orbit_next: SocketAddr,
        slack: SocketAddr,
    ) -> anyhow::Result<(SocketAddr, SocketAddr)> {
        let code = Router::new()
            .route(orbit_next::PATH, post(orbit_next::search))
            .with_state(Arc::clone(&self.orbit_next));
        let live = Router::new()
            .route(slack::PATH, post(slack::extract))
            .with_state(Arc::clone(&self.slack));
        Ok((
            serve(code, orbit_next.port()).await?,
            serve(live, slack.port()).await?,
        ))
    }
}

/// Serves a router on a loopback port behind the request guard.
pub async fn serve(router: Router, port: u16) -> anyhow::Result<SocketAddr> {
    let listener = listen(port).await?;
    let address = listener.local_addr()?;
    let router = router.layer(from_fn_with_state(address.port(), guard));
    tokio::spawn(async move { axum::serve(listener, router).await });
    Ok(address)
}
