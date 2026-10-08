use super::api::*;
use crate::input::Input;
use crate::passes::plan::HydrationCompileOptions;
use query_data_model::QueryDataModel;

mod aggregation;
mod hydration;
mod neighbors;
mod pathfinding;
mod predicates;
mod traversal;

impl<'a, M: QueryDataModel + ?Sized> QueryGraph<'a, M> {
    pub fn plan(&mut self, input: &Input) -> Result<QueryId> {
        self.plan_with_options(input, HydrationCompileOptions::default())
    }

    pub fn plan_with_options(
        &mut self,
        input: &Input,
        options: HydrationCompileOptions,
    ) -> Result<QueryId> {
        use crate::input::QueryType;
        match input.query_type {
            QueryType::Hydration => self.hydration(input, options),
            QueryType::Neighbors => self.neighbors(input),
            QueryType::PathFinding => self.pathfinding(input),
            QueryType::Traversal => self.query(|q| traversal::build(q, input)),
            QueryType::Aggregation => self.query(|q| aggregation::build(q, input)),
        }
    }

    pub fn pathfinding(&mut self, input: &Input) -> Result<QueryId> {
        self.query(|q| pathfinding::build(q, input))
    }

    pub fn neighbors(&mut self, input: &Input) -> Result<QueryId> {
        self.query(|q| neighbors::build(q, input))
    }

    pub fn hydration(
        &mut self,
        input: &Input,
        options: HydrationCompileOptions,
    ) -> Result<QueryId> {
        self.query(|q| hydration::build(q, input, options))
    }
}
