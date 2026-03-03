pub mod ir;

mod apply;
mod generate;
mod introspect;
mod runner;

pub use generate::{CreateReport, GenerateReport};
pub use runner::{
    ApplyReport, RollbackReport, apply, apply_dir, create, generate, rollback, rollback_dir,
};

#[derive(Debug, thiserror::Error)]
pub enum MigrationError {
    #[error("ClickHouse error: {0}")]
    ClickHouse(#[from] clickhouse_client::ClickHouseError),

    #[error("ontology error: {0}")]
    Ontology(#[from] ontology::OntologyError),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("introspection error: {0}")]
    Introspection(String),
}
