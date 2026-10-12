mod stream;

use ontology::Ontology;

pub use query_engine::types::{
    NodeRef, QueryResult, QueryResultRow, ResourceAuthorization, ResourceCheck,
};
pub use stream::{
    RedactionExchangeError, RedactionExchangeResult, RedactionMessage, RedactionService,
};

#[derive(Clone, Copy, Debug)]
pub struct RedactionByAbility(pub bool);

pub fn use_client_abilities(query_result: &mut QueryResult, ontology: &Ontology, by_ability: bool) {
    if !by_ability {
        return;
    }
    for node in ontology.nodes() {
        if let Some(ability) = node
            .redaction
            .as_ref()
            .and_then(|r| r.preferred_ability.as_deref())
        {
            query_result.use_ability(&node.name, ability);
        }
    }
}
