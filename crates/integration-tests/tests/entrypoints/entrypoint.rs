#[path = "../common.rs"]
mod common;
#[path = "../indexer"]
mod indexer;

#[path = "../canary"]
mod canary {
    pub mod setup_test;
}

#[path = "../server"]
mod server {
    pub mod data_correctness;
    pub mod graph_formatter;
    pub mod graph_stats;
    pub mod health;
    pub mod hydration;
    pub mod redaction;
    pub mod telemetry;
}
