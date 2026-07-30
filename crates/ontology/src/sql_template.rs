//! Single MiniJinja entry point for every ontology SQL template — `.sql.j2`
//! extracts, extract filters, and refreshable-view selects — so callers share
//! one strict-undefined environment instead of hand-rolled marker substitution.

use std::collections::HashSet;

use minijinja::{Environment, UndefinedBehavior};
use serde::Serialize;

pub use minijinja::{Error, context};

pub fn render(template: &str, ctx: impl Serialize) -> Result<String, Error> {
    environment().render_str(template, ctx)
}

pub fn undeclared_variables(template: &str) -> Result<HashSet<String>, Error> {
    Ok(environment()
        .template_from_str(template)?
        .undeclared_variables(false))
}

fn environment() -> Environment<'static> {
    let mut environment = Environment::new();
    environment.set_undefined_behavior(UndefinedBehavior::Strict);
    environment
}
