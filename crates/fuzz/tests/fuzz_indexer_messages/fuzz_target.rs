use bolero::check;
use indexer::topic::{
    CodeIndexingTaskRequest, EntityIndexingRequest, GlobalIndexingRequest,
    NamespaceDeletionRequest, NamespaceIndexingRequest,
};

fn main() {
    check!().for_each(|input: &[u8]| {
        let _ = serde_json::from_slice::<GlobalIndexingRequest>(input);
        let _ = serde_json::from_slice::<NamespaceIndexingRequest>(input);
        let _ = serde_json::from_slice::<EntityIndexingRequest>(input);
        let _ = serde_json::from_slice::<CodeIndexingTaskRequest>(input);
        let _ = serde_json::from_slice::<NamespaceDeletionRequest>(input);
    });
}
