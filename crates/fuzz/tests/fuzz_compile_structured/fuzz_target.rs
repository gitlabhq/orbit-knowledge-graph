use bolero::check;
use compiler::{Ontology, SecurityContext, compile};
use gkg_fuzz::generators::FuzzQuery;
use std::sync::OnceLock;

fn ontology() -> &'static Ontology {
    static ONTOLOGY: OnceLock<Ontology> = OnceLock::new();
    ONTOLOGY.get_or_init(|| Ontology::load_embedded().expect("load embedded ontology"))
}

fn ctx() -> &'static SecurityContext {
    static CTX: OnceLock<SecurityContext> = OnceLock::new();
    CTX.get_or_init(|| SecurityContext::new(1, vec!["1/".into()]).expect("create security context"))
}

fn main() {
    check!()
        .with_type::<FuzzQuery>()
        .for_each(|query: &FuzzQuery| {
            let _ = compile(&query.json, ontology(), ctx());
        });
}
