use std::sync::Arc;

use ontology::Ontology;

const LOCAL_DDL: &str = include_str!(concat!(env!("CONFIG_DIR"), "/graph_local.sql"));

pub(crate) fn local_ddl() -> &'static str {
    LOCAL_DDL
}

pub(crate) fn test_ontology() -> Arc<Ontology> {
    Arc::new(Ontology::load_embedded().expect("embedded ontology"))
}

pub(crate) fn test_security_ctx() -> compiler::SecurityContext {
    compiler::SecurityContext::new(1, vec!["1/".into()]).unwrap()
}
