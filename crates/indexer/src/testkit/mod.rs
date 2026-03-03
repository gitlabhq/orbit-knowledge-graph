//! Test utilities and mocks for etl-engine testing.
//!
//! This module provides reusable mocks and builders for testing
//! engine components in isolation.

pub mod builders;
pub mod mocks;

pub use builders::*;
pub use mocks::*;

use opentelemetry::metrics::Meter;

/// Returns a no-op OTel meter for use in tests.
pub fn test_meter() -> Meter {
    opentelemetry::global::meter_provider().meter("test")
}
