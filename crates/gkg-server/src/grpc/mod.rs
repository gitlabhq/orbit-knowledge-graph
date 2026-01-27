//! gRPC module for the Knowledge Graph service.
//!
//! This module contains:
//! - Server: gRPC server setup using tonic
//! - Service: KnowledgeGraphService implementation with bidirectional streaming
//! - Auth: JWT authentication interceptor

mod auth;
mod server;
mod service;

pub use server::Server;
pub use service::KnowledgeGraphServiceImpl;
