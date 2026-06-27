pub mod builders;
pub mod mocks;

pub use builders::*;
pub use mocks::*;

use opentelemetry::metrics::Meter;

pub fn test_meter() -> Meter {
    opentelemetry::global::meter_provider().meter("test")
}
