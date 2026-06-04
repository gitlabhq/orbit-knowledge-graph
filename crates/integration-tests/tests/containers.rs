mod common;
mod indexer;

mod migration {
    pub mod generation;
}

mod canary {
    pub mod setup_test;
}

mod server {
    pub mod analytics;
    pub mod corpus_smoke;
    pub mod data_correctness;
    pub mod denormalization;
    pub mod goon_formatter;
    pub mod graph_formatter;
    pub mod graph_status;
    pub mod grpc_tls;
    pub mod health;
    pub mod hydration;
    pub mod redaction;
    pub mod schema_readiness;
    pub mod telemetry;
}
