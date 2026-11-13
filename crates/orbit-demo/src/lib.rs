//! An interactive demo of the Orbit query engine on seeded local data. It runs each query
//! through the real DAG runner and streams the runner's events to a page on loopback.

pub mod api;
pub mod callers;
pub mod client;
pub mod config;
pub mod executors;
pub mod fakes;
pub mod lockdown;
pub mod plan_text;
pub mod run;
pub mod seed;
pub mod store;

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use compiler::dag::{Primary, Remotes};
use query_data_model::{Catalog, ClickHouse, Model};

use crate::callers::Callers;
use crate::config::Checked;
use crate::fakes::{Fakes, Shared, shared};
use crate::seed::Seed;
use crate::store::Store;

/// Where the demo's own servers listen.
#[derive(Debug, Clone, Copy)]
pub struct Addresses {
    pub page: SocketAddr,
    pub orbit_next: SocketAddr,
    pub slack: SocketAddr,
}

pub struct Demo {
    pub model: Arc<Model>,
    pub catalog: Arc<Catalog<ClickHouse>>,
    pub primary: Primary,
    pub remotes: Remotes,
    pub store: Store,
    pub fakes: Fakes,
    pub callers: Shared<Callers>,
    pub examples: Shared<Vec<seed::Example>>,
    pub seed_dir: PathBuf,
    pub addresses: Addresses,
    pub runs: api::Runs,
}

impl Demo {
    /// Re-reads the seed directory, then reloads ClickHouse, the fakes and the callers from it.
    pub async fn reload(&self) -> anyhow::Result<usize> {
        let seed = Seed::read(&self.seed_dir)?;
        let rows = self.store.reload(&self.catalog, &seed.tables).await?;
        fakes::replace(&self.fakes.orbit_next, seed.orbit_next);
        fakes::replace(&self.fakes.slack, seed.slack);
        fakes::replace(&self.callers, seed.callers);
        fakes::replace(&self.examples, seed.examples);
        Ok(rows)
    }
}

/// Starts the fakes and the page on loopback against a checked config, after seeding.
pub async fn start(checked: Checked) -> anyhow::Result<Arc<Demo>> {
    orbit_server_config::query::init_embedded_defaults();
    let seed = Seed::read(&checked.config.seed)?;
    let model = integration_testkit::dag_fakes::dag_model();
    let catalog = Arc::new(Catalog::<ClickHouse>::from_model(&model)?);
    let store = Store::new(&checked);
    store
        .create(
            &catalog,
            checked.clickhouse,
            &checked.config.clickhouse.seeder,
        )
        .await?;
    store.reload(&catalog, &seed.tables).await?;
    let fakes = Fakes {
        orbit_next: shared(seed.orbit_next),
        slack: shared(seed.slack),
    };
    let (orbit_next, slack) = fakes.serve(checked.orbit_next, checked.slack).await?;
    let page = lockdown::listen(checked.config.page_port).await?;
    let addresses = Addresses {
        page: page.local_addr()?,
        orbit_next,
        slack,
    };
    let demo = Arc::new(Demo {
        primary: Primary::ClickHouse(Arc::clone(&catalog)),
        remotes: Remotes::all(&model),
        model,
        catalog,
        store,
        fakes,
        callers: shared(seed.callers),
        examples: shared(seed.examples),
        seed_dir: checked.config.seed.clone(),
        addresses,
        runs: api::Runs::default(),
    });
    let router = api::router(Arc::clone(&demo));
    let port = addresses.page.port();
    let router = router.layer(axum::middleware::from_fn_with_state(port, lockdown::guard));
    tokio::spawn(async move { axum::serve(page, router).await });
    Ok(demo)
}
