use orbit_fuzz::grammar::{Bytes, Grammar};

const LOCAL_DERIVATION_CAP: usize = 10_000;

fn assert_consumed(text: &str, context: &str) {
    if let Err(error) = compiler::passes::frontend::gql::parse(text) {
        assert!(error.is_client_safe(), "{context}: {text:?}\n{error}");
        assert!(
            !error.to_string().contains("Orbit query syntax"),
            "{context}: derived text is not grammar-valid: {text:?}\n{error}"
        );
    }
}

#[test]
fn every_local_derivation_of_every_rule_is_consumed() {
    let grammar = Grammar::orbit();
    for rule in grammar.rules_reachable_from("Query") {
        let texts = grammar.local_derivations("Query", rule, LOCAL_DERIVATION_CAP);
        assert!(!texts.is_empty(), "{rule} has no faithful derivation");
        for text in texts {
            assert_consumed(&text, rule);
        }
    }
}

#[test]
fn byte_driven_derivations_follow_the_input() {
    let grammar = Grammar::orbit();
    let texts: Vec<String> = (0u8..8)
        .filter_map(|seed| grammar.derive("Query", &mut Bytes(&[seed; 64])))
        .collect();
    assert!(texts.len() > 1, "{texts:?}");
    assert!(texts.iter().any(|t| t != &texts[0]), "{texts:?}");
    for text in &texts {
        assert_consumed(text, "bytes");
    }
}
