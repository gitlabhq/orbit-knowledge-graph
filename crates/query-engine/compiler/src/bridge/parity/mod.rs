mod canonical;
mod corpus;

use std::collections::BTreeMap;
use std::sync::Arc;

use query_data_model::{ClickHouseDataModel, Model};

use crate::passes::frontend::{gql, json_dsl};
use crate::passes::{relationships, validate};
use corpus::{Case, Surface};

#[derive(Debug, PartialEq)]
enum Outcome {
    Accepted(String),
    Rejected(&'static str, String),
}

fn old(case: &Case, model: &ClickHouseDataModel) -> Outcome {
    let checked = |input: crate::Input| {
        let validator = validate::Validator::new(model);
        validator.check_shape(&input)?;
        validator.check_references(&input)?;
        if case.surface == Surface::Gql {
            relationships::validate_relationships(&input, model)?;
        }
        Ok(input)
    };
    let input = match case.surface {
        Surface::Json => json_dsl::parse(&case.text, model),
        Surface::Gql => gql::parse(&case.text),
    };
    match input.and_then(checked) {
        Ok(input) => Outcome::Accepted(canonical::canonical(&input)),
        Err(error) => Outcome::Rejected(canonical::old_class(&error), error.to_string()),
    }
}

fn new(case: &Case, model: &Model) -> Outcome {
    let catalog = model.logical().as_admin();
    let statement = match case.surface {
        Surface::Json => query_data_model::surface::json::parse(&case.text, catalog),
        Surface::Gql => query_data_model::surface::gql::parse(&case.text, catalog),
    };
    match statement.map(|statement| super::input(&statement, model)) {
        Ok(Ok(input)) => Outcome::Accepted(canonical::canonical(&input)),
        Ok(Err(error)) => Outcome::Rejected("bridge", error.to_string()),
        Err(error) => Outcome::Rejected(canonical::new_class(&error), error.to_string()),
    }
}

fn sorted(canonical: &str) -> Vec<&str> {
    let mut lines: Vec<&str> = canonical.lines().collect();
    lines.sort_unstable();
    lines
}

fn first_difference(old: &str, new: &str) -> String {
    let (old, new): (Vec<_>, Vec<_>) = (old.lines().collect(), new.lines().collect());
    let index = (0..old.len().max(new.len()))
        .find(|index| old.get(*index) != new.get(*index))
        .unwrap_or_default();
    format!(
        "\n      old: {}\n      new: {}",
        old.get(index).unwrap_or(&"-"),
        new.get(index).unwrap_or(&"-")
    )
}

const MATCHED_AT_LEAST: usize = 947;

#[test]
fn surfaces_build_the_same_input_as_the_frontends() {
    let data_model = Arc::new(ClickHouseDataModel::embedded().unwrap());
    let model = data_model.catalog().model();
    let cases = corpus::load();
    let mut tally: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for case in &cases {
        let label = format!("{} ({:?})", case.name, case.surface);
        let (key, detail) = match (old(case, &data_model), new(case, model)) {
            (Outcome::Accepted(a), Outcome::Accepted(b)) if a == b => {
                ("match: accepted".into(), label)
            }
            (Outcome::Rejected(a, _), Outcome::Rejected(b, _)) if a == b => {
                ("match: rejected".into(), label)
            }
            (Outcome::Accepted(a), Outcome::Accepted(b)) if sorted(&a) == sorted(&b) => {
                ("node declaration order only".into(), label)
            }
            (Outcome::Accepted(a), Outcome::Accepted(b)) => (
                "input differs".into(),
                format!("{label}{}", first_difference(&a, &b)),
            ),
            (Outcome::Rejected(a, old), Outcome::Rejected(b, new)) => (
                format!("class differs: old {a}, new {b}"),
                format!("{label}\n      old: {old}\n      new: {new}"),
            ),
            (Outcome::Accepted(_), Outcome::Rejected(b, new)) => {
                (format!("new rejects ({b})"), format!("{label}: {new}"))
            }
            (Outcome::Rejected(a, old), Outcome::Accepted(_)) => {
                (format!("new accepts (old {a})"), format!("{label}: {old}"))
            }
        };
        tally.entry(key).or_default().push(detail);
    }
    let matched: usize = tally
        .iter()
        .filter(|(key, _)| key.starts_with("match"))
        .map(|(_, cases)| cases.len())
        .sum();
    println!("surface parity: {matched} of {} cases match", cases.len());
    for (key, cases) in &tally {
        println!("  {key}: {}", cases.len());
        if !key.starts_with("match") {
            for case in cases.iter().take(40) {
                println!("    {case}");
            }
        }
    }
    assert!(
        matched >= MATCHED_AT_LEAST,
        "parity regressed: {matched} matched"
    );
}
