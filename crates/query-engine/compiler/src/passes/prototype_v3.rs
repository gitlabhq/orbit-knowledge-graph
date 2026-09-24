use crate::error::Result;
use crate::input::Input;
use crate::passes::frontend::Frontend;
use crate::passes::{logical_v3, physical_v3};
use ontology::Ontology;

pub enum Plans {
    ClickHouse {
        logical: logical_v3::LogicalPlan,
        physical: physical_v3::Planned<physical_v3::ClickHouse>,
    },
    DuckDb {
        logical: logical_v3::LogicalPlan,
        physical: physical_v3::Planned<physical_v3::DuckDb>,
    },
}

impl Plans {
    pub fn logical(&self) -> String {
        match self {
            Self::ClickHouse { logical, .. } | Self::DuckDb { logical, .. } => logical.explain(),
        }
    }

    pub fn physical(&self) -> String {
        match self {
            Self::ClickHouse { physical, .. } => physical.plan.explain(),
            Self::DuckDb { physical, .. } => physical.plan.explain(),
        }
    }

    pub fn lower(self, input: &Input) -> Result<(crate::ast::Node, physical_v3::PhysicalMetadata)> {
        match self {
            Self::ClickHouse { physical, .. } => Ok((
                crate::passes::lower_v3::lower(physical.plan, input)?,
                physical.metadata,
            )),
            Self::DuckDb { physical, .. } => Ok((
                crate::passes::lower_v3::lower(physical.plan, input)?,
                physical.metadata,
            )),
        }
    }
}

pub fn plan(
    raw: &str,
    frontend: Frontend,
    backend: crate::Backend,
    ontology: &Ontology,
) -> Result<Plans> {
    let input = parse_validate_normalize(raw, frontend, ontology, backend)?;
    let logical = logical_v3::plan(&input);
    Ok(match backend {
        crate::Backend::ClickHouse => Plans::ClickHouse {
            physical: physical_v3::optimize(
                physical_v3::clickhouse(logical.clone(), &input),
                &input,
            ),
            logical,
        },
        crate::Backend::DuckDb => Plans::DuckDb {
            physical: physical_v3::optimize(physical_v3::duckdb(logical.clone(), &input), &input),
            logical,
        },
    })
}

pub fn plan_input(
    input: Input,
    backend: crate::Backend,
    ontology: &Ontology,
) -> Result<(Input, Plans)> {
    let input = validate_normalize(input, ontology, backend)?;
    let logical = logical_v3::plan(&input);
    let plans = match backend {
        crate::Backend::ClickHouse => Plans::ClickHouse {
            physical: physical_v3::optimize(
                physical_v3::clickhouse(logical.clone(), &input),
                &input,
            ),
            logical,
        },
        crate::Backend::DuckDb => Plans::DuckDb {
            physical: physical_v3::optimize(physical_v3::duckdb(logical.clone(), &input), &input),
            logical,
        },
    };
    Ok((input, plans))
}

pub fn compile_input(
    input: Input,
    backend: crate::Backend,
    ontology: &Ontology,
) -> Result<crate::passes::codegen::ParameterizedQuery> {
    let (mut input, plans) = plan_input(input, backend, ontology)?;
    let (mut node, metadata) = plans.lower(&input)?;
    metadata.prepare_result_input(&mut input);
    let result_context =
        crate::passes::enforce::enforce_return(&mut node, &input, &metadata.node_sources)?;
    match backend {
        crate::Backend::ClickHouse => crate::passes::codegen::clickhouse::codegen(
            &node,
            result_context,
            orbit_server_config::QueryConfig::default(),
        ),
        crate::Backend::DuckDb => crate::passes::codegen::duckdb::codegen(&node, result_context),
    }
}

fn parse_validate_normalize(
    raw: &str,
    frontend: Frontend,
    ontology: &Ontology,
    backend: crate::Backend,
) -> Result<Input> {
    let input = match frontend {
        Frontend::JsonDsl => crate::passes::frontend::json_dsl::parse(raw, ontology)?,
        Frontend::Gql => crate::passes::frontend::gql::parse(raw)?,
    };
    validate_normalize(input, ontology, backend)
}

fn validate_normalize(
    mut input: Input,
    ontology: &Ontology,
    backend: crate::Backend,
) -> Result<Input> {
    let validator = match backend {
        crate::Backend::ClickHouse => crate::passes::validate::Validator::new(ontology),
        crate::Backend::DuckDb => crate::passes::validate::Validator::new(ontology)
            .with_skip(crate::passes::validate::Skip { selectivity: true }),
    };
    validator.check_shape(&input)?;
    validator.check_references(&input)?;
    validator.annotate_filter_types(&mut input);
    crate::passes::normalize::normalize(input, ontology)
}
