mod clauses;
mod expr;
mod query;

use std::collections::HashMap;

pub use orbit_utils::query_types::ParamValue;
use orbit_utils::query_types::SqlType;
use serde_json::Value;

use super::plan::{Expr, Function, Measure, ReadMode, RelationalPlan, TextMatch, TimeUnit};
use super::{RelationalBackend, RelationalFacts};
use crate::catalog::Catalog;
use crate::plan::tracking::{NodeId, Ref};

#[derive(Debug, Clone, PartialEq)]
pub struct ParameterizedSql {
    pub sql: String,
    pub parameters: HashMap<String, ParamValue>,
    pub settings: Vec<(String, String)>,
}

pub trait Dialect: Sized {
    type Backend: RelationalBackend;

    const UNION_TAIL_WRAPS: bool;

    fn parameter(&mut self, data_type: SqlType, value: &Value) -> String;
    fn scan(&self, table: &str, alias: &str, read: ReadMode) -> String;
    fn cross_join(&self, left: &str, right: &str) -> String;
    fn shared(&self, name: &str, materialized: bool, body: &str) -> String;
    fn call(renderer: &mut Renderer<'_, Self>, function: Function, args: &[Expr]) -> String;
    fn measure(renderer: &mut Renderer<'_, Self>, measure: &Measure) -> String;
    fn time_bucket(&self, unit: TimeUnit, value: &str) -> String;
    fn text_search(&self, mode: TextMatch, value: &str, query: &str) -> String;
    fn lambda(&self, param: &str, body: &str) -> String;
    fn empty_array(&self, types: &[SqlType]) -> String;
    fn into_parameters(self) -> HashMap<String, ParamValue>;
}

pub struct Renderer<'p, D: Dialect> {
    catalog: &'p Catalog<D::Backend>,
    plan: &'p RelationalPlan<D::Backend>,
    owners: HashMap<Ref, (NodeId, usize)>,
    pub dialect: D,
}

impl<'p, D: Dialect> Renderer<'p, D> {
    pub fn render(
        catalog: &'p Catalog<D::Backend>,
        plan: &'p RelationalPlan<D::Backend>,
        dialect: D,
    ) -> ParameterizedSql {
        let mut owners = HashMap::new();
        for node in plan.plan.nodes() {
            for (position, output) in plan.plan.outputs(node).enumerate() {
                owners.entry(output).or_insert((node, position));
            }
        }
        let mut renderer = Self {
            catalog,
            plan,
            owners,
            dialect,
        };
        let sql = renderer.statement(&plan.with, plan.plan.root());
        ParameterizedSql {
            sql,
            parameters: renderer.dialect.into_parameters(),
            settings: plan.settings.clone(),
        }
    }

    fn table(&self, store: usize, index: usize) -> &'p str {
        let (_, facts) = self
            .catalog
            .stores()
            .nth(store)
            .expect("plans scan tables of their catalog");
        &facts.relational().tables[index].name
    }
}
