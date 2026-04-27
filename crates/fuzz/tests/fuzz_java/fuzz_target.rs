use bolero::check;
use code_graph::v2::config::Language;
use code_graph::v2::dsl::types::DslLanguage;
use code_graph::v2::langs::generic::java::JavaDsl;
use code_graph::v2::trace::Tracer;
use std::sync::OnceLock;

fn spec() -> &'static code_graph::v2::dsl::types::LanguageSpec {
    static SPEC: OnceLock<code_graph::v2::dsl::types::LanguageSpec> = OnceLock::new();
    SPEC.get_or_init(JavaDsl::spec)
}

fn main() {
    let tracer = Tracer::new(false);
    check!().for_each(|input: &[u8]| {
        let _ = spec().parse_full_collect(input, "fuzz.java", Language::Java, &tracer);
    });
}
