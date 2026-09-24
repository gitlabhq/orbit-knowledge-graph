use crate::passes::logical_v3::RelationId;
use std::fmt::Debug;

pub trait Backend: Debug + Clone + Copy + PartialEq + Eq + 'static {
    type Access: Debug + Clone + PartialEq + Eq;
    type Dedup: Debug + Clone + PartialEq + Eq;

    fn node_dedup() -> Option<Self::Dedup>;
    fn latest_by() -> Self::Dedup;
    fn optimize(
        plan: crate::passes::physical_v3::PhysicalPlan<Self>,
        catalog: &crate::passes::physical_v3::PhysicalCatalog<'_>,
    ) -> crate::passes::physical_v3::PhysicalPlan<Self>;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ClickHouse;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DuckDb;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClickHouseAccess {
    Table(String),
    EdgeTables(Vec<String>),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DuckDbAccess {
    Table(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClickHouseDedup {
    Final,
    LimitBy,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DuckDbDedup {
    CurrentSnapshot,
}

impl Backend for ClickHouse {
    type Access = ClickHouseAccess;
    type Dedup = ClickHouseDedup;

    fn node_dedup() -> Option<Self::Dedup> {
        Some(ClickHouseDedup::Final)
    }

    fn latest_by() -> Self::Dedup {
        ClickHouseDedup::LimitBy
    }

    fn optimize(
        plan: crate::passes::physical_v3::PhysicalPlan<Self>,
        catalog: &crate::passes::physical_v3::PhysicalCatalog<'_>,
    ) -> crate::passes::physical_v3::PhysicalPlan<Self> {
        crate::passes::physical_v3::optimize_clickhouse(plan, catalog)
    }
}

impl Backend for DuckDb {
    type Access = DuckDbAccess;
    type Dedup = DuckDbDedup;

    fn node_dedup() -> Option<Self::Dedup> {
        None
    }

    fn latest_by() -> Self::Dedup {
        DuckDbDedup::CurrentSnapshot
    }

    fn optimize(
        plan: crate::passes::physical_v3::PhysicalPlan<Self>,
        _catalog: &crate::passes::physical_v3::PhysicalCatalog<'_>,
    ) -> crate::passes::physical_v3::PhysicalPlan<Self> {
        plan
    }
}

pub fn relation_alias(relation: RelationId) -> String {
    format!("r{}", relation.0)
}
