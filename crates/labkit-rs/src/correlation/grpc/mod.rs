//! gRPC interceptors for correlation ID propagation.
//!
//! This module provides Tonic interceptors for extracting and injecting
//! correlation IDs in gRPC requests.
//!
//! # Server Side
//!
//! Use [`server_interceptor`] to extract correlation IDs from incoming requests:
//!
//! ```rust,ignore
//! use labkit_rs::correlation::grpc::server_interceptor;
//!
//! Server::builder()
//!     .add_service(MyServiceServer::with_interceptor(my_service, server_interceptor))
//!     .serve(addr)
//!     .await?;
//! ```
//!
//! # Client Side
//!
//! Use [`client_interceptor`] to inject correlation IDs into outgoing requests:
//!
//! ```rust,ignore
//! use labkit_rs::correlation::grpc::client_interceptor;
//!
//! let client = MyServiceClient::with_interceptor(channel, client_interceptor);
//! ```

mod client;
mod server;

#[cfg(test)]
mod tests;

pub use client::{
    ClientConfig, client_interceptor, create_client_interceptor, inject_correlation_id,
    inject_to_request,
};
pub use server::{
    ServerConfig, context_from_request, create_server_interceptor, extract_from_request,
    server_interceptor, with_correlation, with_correlation_id_stream, with_correlation_stream,
};
