//! The DAG runner: runs a query's stages in dependency order through the executors a
//! deployment registers, reporting each step to an observer.

mod combine;
mod executor;
mod observer;
mod remote;
mod rows;
mod runner;

pub use executor::{Executed, Executor, ExecutorError, Executors};
pub use observer::{Event, KEY_SAMPLE, Observer, Recording, Silent, StageInfo};
pub use rows::from_rows;
pub use runner::{DagError, Outcome, Runner};
