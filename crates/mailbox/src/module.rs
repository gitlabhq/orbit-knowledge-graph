//! MailboxModule implementing etl-engine Module trait.

use std::sync::Arc;

use etl_engine::entities::Entity;
use etl_engine::module::{Handler, Module};

use crate::handler::MailboxHandler;
use crate::storage::{PluginStore, TraversalPathResolver};

pub struct MailboxModule {
    plugin_store: Arc<PluginStore>,
    traversal_resolver: Arc<TraversalPathResolver>,
}

impl MailboxModule {
    pub fn new(
        plugin_store: Arc<PluginStore>,
        traversal_resolver: Arc<TraversalPathResolver>,
    ) -> Self {
        Self {
            plugin_store,
            traversal_resolver,
        }
    }
}

impl Module for MailboxModule {
    fn name(&self) -> &str {
        "mailbox"
    }

    fn handlers(&self) -> Vec<Box<dyn Handler>> {
        vec![Box::new(MailboxHandler::new(
            self.plugin_store.clone(),
            self.traversal_resolver.clone(),
        ))]
    }

    fn entities(&self) -> Vec<Entity> {
        Vec::new()
    }
}
