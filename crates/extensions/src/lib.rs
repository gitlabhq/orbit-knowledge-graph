//! The Orbit extensions contract, version `v1alpha1`.
//!
//! Orbit is a platform. It knows nothing about any integration. A provider
//! implements the gRPC [`proto::provider_client`] contract and serves declarative
//! package documents. Orbit validates each package ([`package`]), translates it
//! into ontology sources and merges it with the bundled core model ([`loader`]),
//! and creates one graph table per entity outside the core schema version
//! ([`storage`]). The provider client ([`provider`]) fetches manifests and
//! packages, signs a short-lived JWT per call, and refreshes on a digest change.

pub mod bootstrap;
pub mod live;
pub mod loader;
pub mod package;
pub mod proto;
pub mod provider;
pub mod search;
pub mod storage;
