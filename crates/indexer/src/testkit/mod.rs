pub mod builders;
#[cfg(test)]
pub mod logs;
pub mod mocks;

pub use builders::*;
pub use mocks::*;

use opentelemetry::metrics::Meter;

pub fn test_meter() -> Meter {
    opentelemetry::global::meter_provider().meter("test")
}

#[expect(
    clippy::expect_used,
    reason = "the embedded model is validated at build time"
)]
pub fn siphon_source() -> query_data_model::catalog::sources::DataSource {
    query_data_model::Packages::embedded()
        .load()
        .expect("embedded model")
        .sources()
        .source_named(orbit_migrations::completion::SIPHON_SOURCE)
        .expect("embedded siphon source")
        .clone()
}
