//! One bulk insert per non-empty destination table, split into [`stage`]
//! (open the writers) and [`drain`] (await them) so the runner can overlap
//! the drain of page N with the extract of page N+1.

use std::time::{Duration, Instant};

use futures::stream::FuturesUnordered;
use futures::{FutureExt, StreamExt};

use crate::destination::Destination;
use crate::handler::HandlerError;

use super::super::page::{StagedWrites, TransformedPage};

/// Writers return un-awaited so the runner can overlap them with the next
/// page's extract; no insert is opened for a table with no rows this page.
pub(in crate::modules::sdlc) async fn stage(
    destination: &dyn Destination,
    outputs: &[String],
    transformed: TransformedPage,
) -> Result<StagedWrites, HandlerError> {
    let futures = FuturesUnordered::new();
    let mut per_table = Vec::new();

    for (index, batches) in transformed.batches_by_table.into_iter().enumerate() {
        if batches.is_empty() {
            continue;
        }
        let rows: u64 = batches.iter().map(|b| b.num_rows() as u64).sum();
        let bytes: u64 = batches
            .iter()
            .map(|b| b.get_array_memory_size() as u64)
            .sum();
        per_table.push((index, rows, bytes));

        let table = outputs[index].clone();
        let writer = destination.new_batch_writer(&table).await.map_err(|err| {
            HandlerError::Processing(format!("failed to create writer for {table}: {err}"))
        })?;
        futures.push(
            async move {
                writer.write_batch(&batches).await.map_err(|err| {
                    HandlerError::Processing(format!("failed to write to {table}: {err}"))
                })
            }
            .boxed(),
        );
    }

    Ok(StagedWrites { futures, per_table })
}

/// Returns the drain time, timed here because the runner overlaps the drain
/// with an extract.
pub(in crate::modules::sdlc) async fn drain(
    staged: StagedWrites,
) -> Result<Duration, HandlerError> {
    let start = Instant::now();
    let mut futures = staged.futures;
    while let Some(result) = futures.next().await {
        result?;
    }
    Ok(start.elapsed())
}
