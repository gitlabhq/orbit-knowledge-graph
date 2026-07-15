use std::collections::VecDeque;

use async_trait::async_trait;

use crate::durability::WriteDurability;
use crate::handler::HandlerError;
use crate::observer::IndexingMode;

use super::{
    ExtractPage, ExtractRun, ExtractRunCompletion, ExtractRunContext, ExtractSession, Extractor,
};

pub(in crate::modules::sdlc) struct MemoryExtractor {
    pages: Vec<ExtractPage>,
}

struct MemoryExtractSession {
    pages: VecDeque<ExtractPage>,
}

struct MemoryExtractRunCompletion;

impl MemoryExtractor {
    pub fn new(pages: Vec<ExtractPage>) -> Self {
        Self { pages }
    }
}

#[async_trait]
impl Extractor for MemoryExtractor {
    async fn start_extraction(
        &self,
        _context: ExtractRunContext,
    ) -> Result<ExtractRun, HandlerError> {
        Ok(ExtractRun {
            indexing_mode: IndexingMode::Full,
            sessions: vec![Box::new(MemoryExtractSession {
                pages: self.pages.clone().into(),
            })],
            completion: Box::new(MemoryExtractRunCompletion),
        })
    }
}

#[async_trait]
impl ExtractSession for MemoryExtractSession {
    async fn get_next_page(&mut self) -> Result<Option<ExtractPage>, HandlerError> {
        Ok(self.pages.pop_front())
    }

    async fn save_page_resume(&self, _resume: &super::ExtractResume) -> Result<(), HandlerError> {
        Ok(())
    }

    async fn save_completed(&self, _durability: WriteDurability) -> Result<(), HandlerError> {
        Ok(())
    }
}

#[async_trait]
impl ExtractRunCompletion for MemoryExtractRunCompletion {
    async fn finish_extraction(self: Box<Self>) -> Result<(), HandlerError> {
        Ok(())
    }
}
