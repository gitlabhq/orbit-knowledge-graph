mod common;
mod indexer;

mod canary {
    pub mod setup_test;
}

mod server {
    pub mod data_correctness;
    pub mod graph_formatter;
    pub mod health;
    pub mod hydration;
    pub mod redaction;
}
