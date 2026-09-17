use bolero::check;
use compiler::{Frontend, Ontology, SecurityContext, compile};
use orbit_fuzz::generators::FuzzQuery;
use std::sync::{Arc, OnceLock};

fn ontology() -> &'static Arc<Ontology> {
    static ONTOLOGY: OnceLock<Arc<Ontology>> = OnceLock::new();
    ONTOLOGY.get_or_init(|| Arc::new(Ontology::load_embedded().expect("load embedded ontology")))
}

fn ctx() -> &'static SecurityContext {
    static CTX: OnceLock<SecurityContext> = OnceLock::new();
    CTX.get_or_init(|| SecurityContext::new(1, vec!["1/".into()]).expect("create security context"))
}

fn main() {
    check!()
        .with_type::<FuzzQuery>()
        .for_each(|query: &FuzzQuery| {
            let _ = compile(&query.json, Frontend::JsonDsl, ontology(), ctx());
        });
}
