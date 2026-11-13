//! Generated from `proto/orbit/config/v1/config.proto`. Regenerate with
//! `mise proto:config`.

#[allow(clippy::all)]
mod orbit_config_v1 {
    include!("orbit.config.v1.rs");
}

pub use orbit_config_v1::*;

/// Registered with the loader so it can resolve these types, and the
/// `buf.validate` rules attached to them, by protobuf name.
pub const FILE_DESCRIPTOR_SET: &[u8] = include_bytes!("config_descriptor.bin");
