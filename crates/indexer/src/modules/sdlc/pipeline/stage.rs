//! The processor-stage seam.
//!
//! A [`PageStage`] is one typed transition over a page: its `In`/`Out`
//! associated types make the phase explicit, so a stage that consumes a
//! [`TransformedPage`](super::page::TransformedPage) cannot be wired before the
//! transform produces one. New blocks (schema-conformance, redaction, …) plug
//! in here by implementing this trait; that is the expandable seam.
//!
//! Extraction (producer) and writing (consumer) are *not* `PageStage`s: the
//! runner prefetches the producer and overlaps the consumer's drain with the
//! next extract, so they are the loop's fixed endpoints, not interchangeable
//! middle blocks.
//!
//! A chaining combinator (`Then<A, B>`) is deliberately deferred until a second
//! processor stage exists — with one stage it would be dead code, and the
//! payload types already enforce ordering on their own.

use async_trait::async_trait;

use crate::handler::HandlerError;

#[async_trait]
pub(in crate::modules::sdlc) trait PageStage: Send + Sync {
    type In: Send + 'static;
    type Out: Send + 'static;

    async fn run(&self, input: Self::In) -> Result<Self::Out, HandlerError>;
}
