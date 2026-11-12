use query_data_model::catalog::Check;
use query_data_model::implementations::relational::plan::RelationalPlan;
use query_data_model::{Catalog, DataType};
use std::collections::{HashMap, HashSet};

use query_data_model::Direction;
use query_data_model::implementations::relational::RelationalBackend;
use query_data_model::implementations::relational::plan::{
    Column, Expr, Operation, Relation, RelationalBuilder, RelationalError, Selection,
};
use query_data_model::plan::logical::{
    EdgeField, Expr as Logical, LogicalOperation, LogicalType, Measure, Projection,
};
use query_data_model::plan::tracking::{Declare, Ref, Step};

use crate::constants::{
    primary_key_column, redaction_id_column, redaction_type_column, traversal_path_column,
};
use crate::error::{QueryError, Result};
use crate::logical::columns::{free_text, redaction_column};
use crate::logical::{Binding, Kind, Query};
use crate::passes::codegen::{SqlBackend, rejected};
use crate::passes::plan::Term;
use crate::passes::plan::build::term;
use crate::passes::plan::predicates::{Column as Named, OutputValue};
use crate::passes::plan::{NodeBinding, PlanMetadata};
use crate::types::QueryType;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EntityAuthConfig {
    pub provider: String,
    pub resource_type: String,
    pub ability: String,
    /// The property the check keys rows by: the entity's id, or the id of the object it names.
    pub auth_id_column: String,
    pub key_source: KeySource,
    pub key_kind: KeyKind,
}

/// Where a returned node holds the key a row check derives.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KeySource {
    /// The node's own id.
    Identity,
    /// The redaction id column the base query returns.
    Redaction,
    /// A read of the key property by the node's id after the base query.
    Lookup,
}

/// The type of key a row check sends its provider, from the property it keys rows by.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KeyKind {
    Int,
    Text,
    Uuid,
}

impl KeyKind {
    /// The loader admits only integer, string and uuid check keys.
    pub(crate) fn of(data_type: DataType) -> Self {
        match data_type {
            DataType::Int => Self::Int,
            DataType::Uuid => Self::Uuid,
            _ => Self::Text,
        }
    }
}

/// The row checks each checked entity's returned rows go through, by entity name; a row comes
/// back only if every one allows it.
pub fn entity_auth(model: &Catalog<impl SqlBackend>) -> HashMap<String, Vec<EntityAuthConfig>> {
    let graph = model.graph();
    let access = model.model().access();
    graph
        .entities()
        .filter_map(|entity| {
            let redaction = redaction_column(model, entity.id);
            let checks: Vec<EntityAuthConfig> = model
                .checks(entity.id)
                .iter()
                .filter_map(|check| match check {
                    Check::Rows {
                        provider,
                        permission,
                        resource,
                        id,
                        ..
                    } => {
                        let key = graph.property(*id);
                        let key_source = match key.name.as_str() {
                            _ if entity.identity == [*id] => KeySource::Identity,
                            name if Some(name) == redaction => KeySource::Redaction,
                            _ => KeySource::Lookup,
                        };
                        Some(EntityAuthConfig {
                            provider: provider.clone(),
                            resource_type: resource.clone(),
                            ability: access.permission(*permission).name.clone(),
                            auth_id_column: key.name.clone(),
                            key_source,
                            key_kind: KeyKind::of(key.data_type),
                        })
                    }
                    Check::Delegated { .. } => None,
                })
                .collect();
            (!checks.is_empty()).then(|| (entity.name.clone(), checks))
        })
        .collect()
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RedactionNode {
    pub alias: String,
    pub entity_type: String,
    pub pk_column: String,
    pub id_column: String,
    pub type_column: String,
}

#[derive(Debug, Clone)]
pub struct EdgeMeta {
    pub column_prefix: String,
    pub path_column: Option<String>,
    pub rel_types: Vec<String>,
    pub from_alias: String,
    pub to_alias: String,
    pub type_column: String,
    pub src_column: String,
    pub src_type_column: String,
    pub dst_column: String,
    pub dst_type_column: String,
    pub relationship: usize,
}

#[derive(Debug, Clone, Default)]
pub struct ResultContext {
    pub query_type: Option<QueryType>,
    nodes: HashMap<String, RedactionNode>,
    entity_auth: HashMap<String, Vec<EntityAuthConfig>>,
    edges: Vec<EdgeMeta>,
    neighbors_center: Option<(String, orbit_authorization::Key)>,
    labels: HashMap<String, String>,
    /// Nodes whose rows a remote stage already put through every check of their entity.
    stage_checked: HashSet<String>,
}

impl ResultContext {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_query_type(mut self, query_type: QueryType) -> Self {
        self.query_type = Some(query_type);
        self
    }

    pub fn add_node(&mut self, alias: &str, entity_type: &str) {
        self.nodes.insert(
            alias.to_string(),
            RedactionNode {
                alias: alias.to_string(),
                entity_type: entity_type.to_string(),
                pk_column: primary_key_column(alias),
                id_column: redaction_id_column(alias),
                type_column: redaction_type_column(alias),
            },
        );
    }

    /// A node of a remote stage, whose rows that stage put through every check of the entity
    /// before the engine joined them.
    pub fn add_stage_checked_node(&mut self, alias: &str, entity_type: &str) {
        self.add_node(alias, entity_type);
        self.stage_checked.insert(alias.to_string());
    }

    pub fn stage_checked(&self, alias: &str) -> bool {
        self.stage_checked.contains(alias)
    }

    pub fn add_entity_auth(&mut self, entity_type: impl Into<String>, config: EntityAuthConfig) {
        self.entity_auth
            .entry(entity_type.into())
            .or_default()
            .push(config);
    }

    /// Every row check of the entity.
    pub fn entity_checks(&self, entity_type: &str) -> &[EntityAuthConfig] {
        self.entity_auth.get(entity_type).map_or(&[], Vec::as_slice)
    }

    pub fn entity_auth(&self) -> impl Iterator<Item = (&str, &EntityAuthConfig)> {
        self.entity_auth
            .iter()
            .flat_map(|(entity, checks)| checks.iter().map(move |check| (entity.as_str(), check)))
    }

    pub fn nodes(&self) -> impl Iterator<Item = &RedactionNode> {
        self.nodes.values()
    }

    pub fn get(&self, alias: &str) -> Option<&RedactionNode> {
        self.nodes.get(alias)
    }

    pub fn is_empty(&self) -> bool {
        self.nodes.is_empty()
    }

    pub fn len(&self) -> usize {
        self.nodes.len()
    }

    pub fn add_edge(&mut self, edge: EdgeMeta) {
        self.edges.push(edge);
    }

    pub fn edges(&self) -> &[EdgeMeta] {
        &self.edges
    }

    pub fn neighbors_center(&self) -> Option<(&str, &orbit_authorization::Key)> {
        self.neighbors_center
            .as_ref()
            .map(|(entity, id)| (entity.as_str(), id))
    }

    pub fn label(&self, entity: &str) -> Option<&str> {
        self.labels.get(entity).map(String::as_str)
    }
}

/// The columns a query returns, as the formatters lay them out.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct ResultShape {
    /// Each binding's alias and entity, in declaration order.
    pub nodes: Vec<(String, String)>,
    pub relationship_columns: Vec<RelationshipColumn>,
    pub column_order: Vec<String>,
    pub groups: Vec<GroupColumn>,
    pub measures: Vec<MeasureColumn>,
    pub neighbors_direction: Option<Direction>,
    pub limit: u32,
    pub skip: Option<u32>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RelationshipColumn {
    pub name: String,
    pub relationship: usize,
    pub kind: RelationshipColumnKind,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RelationshipColumnKind {
    Type,
    Relationship,
}

/// A group key: a node, or one of its properties.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GroupColumn {
    pub name: String,
    pub node: String,
    pub property: Option<String>,
    pub entity: Option<String>,
}

impl GroupColumn {
    pub fn kind(&self) -> &'static str {
        match self.property {
            None => "node",
            Some(_) => "property",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MeasureColumn {
    pub name: String,
    pub function: &'static str,
    pub node: String,
    pub property: Option<String>,
}

impl ResultShape {
    pub fn new(query: Query<'_>) -> Result<Self> {
        let graph = query.model.graph();
        let bindings = query.bindings();
        let nodes: Vec<(String, String)> = bindings
            .iter()
            .map(|binding| {
                let entity = graph.entity(binding.entity).name.clone();
                (binding.name.to_string(), entity)
            })
            .collect();
        let entity = |alias: &str| {
            nodes
                .iter()
                .find(|(node, _)| node == alias)
                .map(|(_, entity)| entity.clone())
        };
        let target = |expr: &Logical| -> (String, Option<String>) {
            match expr {
                Logical::Property { binding, property }
                | Logical::Truncate {
                    binding, property, ..
                } => (
                    query.name(*binding).to_string(),
                    Some(query.property_name(*property).to_string()),
                ),
                other => (
                    query.name(crate::logical::binding(*other)).to_string(),
                    None,
                ),
            }
        };
        let mut shape = Self {
            limit: query
                .row_limit()
                .ok_or_else(|| QueryError::PipelineInvariant("queries carry a row limit".into()))?,
            skip: query.skip(),
            ..Self::default()
        };
        let steps = query.steps();
        let mut grouped: HashMap<Ref, String> = HashMap::new();
        let mut edges: HashMap<Ref, usize> = steps
            .iter()
            .enumerate()
            .map(|(index, step)| (step.edge, index))
            .collect();
        for (node, operation) in query.operations() {
            let outputs: Vec<Ref> = query.plan.outputs(node).collect();
            match operation {
                LogicalOperation::Neighbors { direction, .. } => {
                    edges.insert(outputs[outputs.len() - 2], 0);
                    shape.neighbors_direction = Some(*direction);
                }
                LogicalOperation::Aggregate { groups, measures } => {
                    for (group, output) in groups.iter().zip(&outputs) {
                        let (node, property) = target(group);
                        if property.is_none() {
                            grouped.insert(*output, node.clone());
                        }
                        shape.groups.push(GroupColumn {
                            name: query.name(*output).to_string(),
                            entity: entity(&node),
                            node,
                            property,
                        });
                    }
                    for (measure, output) in measures.iter().zip(&outputs[groups.len()..]) {
                        let (function, input) = match measure {
                            Measure::Count(input) => ("count", input.as_ref()),
                            Measure::CountDistinct(input) => ("count", Some(input)),
                            Measure::Sum(input) => ("sum", Some(input)),
                            Measure::Avg(input) => ("avg", Some(input)),
                            Measure::Min(input) => ("min", Some(input)),
                            Measure::Max(input) => ("max", Some(input)),
                        };
                        let (node, property) = input.map(target).unwrap_or_default();
                        shape.measures.push(MeasureColumn {
                            name: query.name(*output).to_string(),
                            function,
                            node,
                            property,
                        });
                    }
                }
                LogicalOperation::Select(items) => {
                    let mut order = Vec::new();
                    for (item, output) in items.iter().zip(outputs) {
                        let name = query.name(output).to_string();
                        let (binding, kind) = match item {
                            Projection::Graph { binding, .. } => match query.plan.value(*binding) {
                                LogicalType::Entity(_) => {
                                    let alias = grouped.get(binding).cloned();
                                    order.push(
                                        alias.unwrap_or_else(|| query.name(*binding).to_string()),
                                    );
                                    continue;
                                }
                                LogicalType::Edge => {
                                    (binding, RelationshipColumnKind::Relationship)
                                }
                                _ => continue,
                            },
                            Projection::Value(Logical::Edge {
                                binding,
                                field: EdgeField::Kind,
                            }) => (binding, RelationshipColumnKind::Type),
                            Projection::Value(_) => continue,
                        };
                        let relationship = edges.get(binding).copied().ok_or_else(|| {
                            QueryError::PipelineInvariant("an unbound edge projection".into())
                        })?;
                        shape.relationship_columns.push(RelationshipColumn {
                            name: name.clone(),
                            relationship,
                            kind,
                        });
                        order.push(name);
                    }
                    if !shape.relationship_columns.is_empty() {
                        shape.column_order = order;
                    }
                }
                _ => {}
            }
        }
        shape.nodes = nodes;
        Ok(shape)
    }
}

pub fn apply<B: SqlBackend>(
    plan: RelationalPlan<B>,
    query: Query<'_>,
    metadata: &PlanMetadata,
    model: &Catalog<B>,
) -> Result<(RelationalPlan<B>, ResultContext)> {
    let kind = query.kind();
    let mut ctx = ResultContext::new().with_query_type(kind.into());
    ctx.entity_auth = entity_auth(model);
    let facts = query.leaves();
    let anchors = query.anchors(&facts);
    if kind == Kind::Neighbors
        && let Some(center) = anchors.first()
        && let Some([id]) = center.pinned.as_deref()
    {
        let entity = model.graph().entity(center.binding.entity);
        ctx.neighbors_center = Some((entity.name.clone(), id.clone()));
        ctx.labels = model
            .graph()
            .entities()
            .map(|entity| (entity.name.clone(), entity.label.clone()))
            .collect();
    }
    let bindings = query.bindings();
    let selectable_nodes: HashSet<&str> = match kind {
        Kind::Aggregation => aggregation_row_nodes(query, model),
        Kind::Traversal | Kind::Neighbors => bindings.iter().map(|binding| binding.name).collect(),
        Kind::PathFinding => HashSet::new(),
    };
    for binding in &bindings {
        if selectable_nodes.contains(binding.name) {
            ctx.add_node(binding.name, &model.graph().entity(binding.entity).name);
        }
    }
    let contract = return_columns(kind, &bindings, &selectable_nodes, &metadata.nodes, model)?;
    if matches!(kind, Kind::Traversal | Kind::Aggregation) {
        use crate::constants::{
            EDGE_DST_KIND_SUFFIX, EDGE_DST_SUFFIX, EDGE_KIND_SUFFIX, EDGE_SRC_KIND_SUFFIX,
            EDGE_SRC_SUFFIX,
        };
        for edge in &metadata.edges {
            let prefix = edge.column_prefix.clone();
            ctx.edges.push(EdgeMeta {
                type_column: format!("{prefix}{EDGE_KIND_SUFFIX}"),
                src_column: format!("{prefix}{EDGE_SRC_SUFFIX}"),
                src_type_column: format!("{prefix}{EDGE_SRC_KIND_SUFFIX}"),
                dst_column: format!("{prefix}{EDGE_DST_SUFFIX}"),
                dst_type_column: format!("{prefix}{EDGE_DST_KIND_SUFFIX}"),
                column_prefix: prefix,
                path_column: edge.path_column.clone(),
                rel_types: edge.rel_types.clone(),
                relationship: edge.relationship,
                from_alias: String::new(),
                to_alias: String::new(),
            });
        }
    }
    let plan = match contract.outputs.is_empty() {
        true => plan,
        false => enforce(plan, model, contract, kind)?,
    };
    Ok((plan, ctx))
}

/// The aliases an aggregation returns as rows: node groups and free-text property groups.
fn aggregation_row_nodes<'p>(
    query: Query<'p>,
    model: &Catalog<impl SqlBackend>,
) -> HashSet<&'p str> {
    let Some((groups, _)) = query.aggregate() else {
        return HashSet::new();
    };
    groups
        .iter()
        .filter_map(|group| match group {
            Logical::Node(binding) => Some(query.name(*binding)),
            Logical::Property { binding, property }
            | Logical::Truncate {
                binding, property, ..
            } if free_text(model, *property) => Some(query.name(*binding)),
            _ => None,
        })
        .collect()
}

struct Contract {
    outputs: Vec<(String, Term)>,
    groups: Vec<Term>,
}

fn return_columns(
    kind: Kind,
    nodes: &[Binding<'_>],
    selectable_nodes: &HashSet<&str>,
    bindings: &HashMap<String, NodeBinding>,
    model: &Catalog<impl SqlBackend>,
) -> Result<Contract> {
    let mut contract = Contract {
        outputs: Vec::new(),
        groups: Vec::new(),
    };
    for node in nodes {
        if !selectable_nodes.contains(node.name) {
            continue;
        }
        let entity_id = node.entity;
        let entity = &model.graph().entity(entity_id).name;
        let binding = bindings.get(node.name).ok_or_else(|| {
            QueryError::PipelineInvariant(format!("node '{}' has no lowered binding", node.name))
        })?;
        if matches!(binding, NodeBinding::Filtered) {
            return Err(QueryError::PipelineInvariant(format!(
                "node '{}' has no result identity",
                node.name
            )));
        }
        let NodeBinding::Values {
            identity,
            table_alias,
            traversal_path,
        } = binding
        else {
            continue;
        };
        let id_column = crate::logical::columns::identity_column(model, entity_id)?;
        let redaction_column = redaction_column(model, entity_id).unwrap_or(id_column);
        let needs_separate_pk = redaction_column != id_column;
        let authorization_id = if needs_separate_pk {
            let table_alias = table_alias.as_deref().ok_or_else(|| {
                QueryError::PipelineInvariant(format!(
                    "node '{}' is authorized by {redaction_column} but has no table read",
                    node.name
                ))
            })?;
            Term::Column(Named::new(table_alias, redaction_column))
        } else {
            identity.clone()
        };
        if needs_separate_pk {
            contract.set(primary_key_column(node.name), identity.clone());
            contract.group(identity.clone());
        }
        contract.set(redaction_id_column(node.name), authorization_id.clone());
        contract.group(authorization_id);
        let label = Term::Value(Box::new(OutputValue::Text(entity.clone())));
        contract.set(redaction_type_column(node.name), label);
        if kind != Kind::Aggregation
            && model.has_path_column(entity_id)
            && let Some(path) = traversal_path
        {
            let path = Term::Column(path.clone());
            contract.set(traversal_path_column(node.name), path);
        }
    }
    Ok(contract)
}

impl Contract {
    fn set(&mut self, name: String, value: Term) {
        match self
            .outputs
            .iter_mut()
            .find(|(current, _)| *current == name)
        {
            Some(output) => output.1 = value,
            None => self.outputs.push((name, value)),
        }
    }

    fn group(&mut self, value: Term) {
        if !self.groups.contains(&value) {
            self.groups.push(value);
        }
    }
}

fn enforce<B: SqlBackend>(
    plan: RelationalPlan<B>,
    model: &Catalog<B>,
    contract: Contract,
    kind: Kind,
) -> Result<RelationalPlan<B>> {
    let root = plan.plan.root();
    let block = plan.block(root);
    let Some(select) = block.select else {
        return Err(QueryError::PipelineInvariant(
            "result columns cannot extend a union".into(),
        ));
    };
    let consumer = block
        .order
        .or(block.limit_by)
        .or(block.limit)
        .unwrap_or(select);
    let computed: Vec<Ref> = match block.compute.map(|node| plan.plan.operation(node)) {
        Some(Operation::Compute(values)) => {
            let outputs: Vec<Ref> = plan.plan.outputs(block.compute.unwrap_or(root)).collect();
            outputs[outputs.len() - values.len()..].to_vec()
        }
        _ => Vec::new(),
    };
    let mut names: Vec<Option<String>> = computed
        .iter()
        .map(|output| plan.plan.name(*output).map(str::to_string))
        .collect();
    let mut bound: Vec<(Expr, Ref)> = Vec::new();
    let mut aggregate = None;
    if let Some(node) = block.aggregate
        && let Operation::Aggregate { groups, .. } = plan.plan.operation(node)
    {
        let outputs: Vec<Ref> = plan.plan.outputs(node).collect();
        let regrouped = kind == Kind::Aggregation && !groups.is_empty();
        aggregate = Some((node, groups.clone(), outputs, regrouped));
    }
    let mut added: Vec<Ref> = Vec::new();
    model
        .rebuild(plan, |q, step| {
            let node = Some(step.node());
            if let Some((aggregate, groups, outputs, regrouped)) = &aggregate
                && node == Some(*aggregate)
            {
                if *regrouped {
                    let (groups, rows) = regroup(q, step, &contract, groups.len(), outputs)?;
                    bound = groups;
                    return Ok(rows);
                }
                let rows = step.keep(q)?;
                bound = groups
                    .iter()
                    .zip(outputs)
                    .map(|(group, output)| Ok((step.expr(group.clone())?, step.output(*output)?)))
                    .collect::<std::result::Result<_, RelationalError>>()?;
                return Ok(rows);
            }
            let scope = Scope {
                contract: &contract,
                bound: &bound,
            };
            if block.compute.is_some() && node == block.compute {
                let Operation::Compute(values) = step.operation.clone() else {
                    return Err(RelationalError::Incomplete("a compute"));
                };
                let source = first(std::mem::take(&mut step.inputs))?;
                let (rows, columns) = scope.extend(q, &mut names, values, source)?;
                for (old, new) in computed.iter().zip(&columns) {
                    step.reconnect(*old, new.output())?;
                }
                for column in &columns[computed.len()..] {
                    step.expose(column.output());
                    added.push(column.output());
                }
                return Ok(rows);
            }
            if node != Some(consumer) && node != Some(select) {
                return step.keep(q);
            }
            if block.compute.is_none() && node == Some(consumer) {
                let source = step.inputs.remove(0);
                let (rows, columns) = scope.extend(q, &mut names, Vec::new(), source)?;
                step.inputs.insert(0, rows);
                added = columns.iter().map(|column| column.output()).collect();
                if node != Some(select) {
                    for column in columns {
                        step.expose(column.output());
                    }
                    return step.keep(q);
                }
            }
            if let Operation::Select(Selection::Columns(columns)) = &mut step.operation {
                columns.extend(added.iter().copied());
            }
            step.keep(q)
        })
        .map_err(rejected)
}

fn first(inputs: Vec<Relation>) -> std::result::Result<Relation, RelationalError> {
    inputs
        .into_iter()
        .next()
        .ok_or(RelationalError::Incomplete("an input"))
}

fn regroup<'c, B: RelationalBackend>(
    q: &mut RelationalBuilder<'c, B>,
    step: &mut Step<'_, '_, RelationalBuilder<'c, B>>,
    contract: &Contract,
    width: usize,
    outputs: &[Ref],
) -> std::result::Result<(Vec<(Expr, Ref)>, Relation), RelationalError> {
    let Operation::Aggregate {
        mut groups,
        measures,
    } = step.operation.clone()
    else {
        return Err(RelationalError::Incomplete("an aggregate"));
    };
    let source = first(std::mem::take(&mut step.inputs))?;
    for group in &contract.groups {
        let group = term::<B>(&[&source], group, &[])?;
        if !groups.contains(&group) {
            groups.push(group);
        }
    }
    let aggregated = q.aggregate(source, groups.clone(), measures)?;
    let fresh = aggregated
        .groups
        .iter()
        .take(width)
        .chain(&aggregated.measures);
    for (old, new) in outputs.iter().zip(fresh) {
        step.reconnect(*old, new.output())?;
    }
    for group in &aggregated.groups[width..] {
        step.expose(group.output());
    }
    let outputs = aggregated.groups.iter().map(|group| group.output());
    Ok((groups.into_iter().zip(outputs).collect(), aggregated.rows))
}

struct Scope<'a> {
    contract: &'a Contract,
    bound: &'a [(Expr, Ref)],
}

impl Scope<'_> {
    fn extend<B: RelationalBackend>(
        &self,
        q: &mut RelationalBuilder<'_, B>,
        names: &mut Vec<Option<String>>,
        mut values: Vec<Expr>,
        source: Relation,
    ) -> std::result::Result<(Relation, Vec<Column>), RelationalError> {
        for (name, value) in &self.contract.outputs {
            let value = term::<B>(&[&source], value, &[])?;
            let value = match self.bound.iter().find(|(group, _)| *group == value) {
                Some((_, output)) => Expr::Column(*output),
                None => value,
            };
            match names
                .iter()
                .position(|current| current.as_deref() == Some(name))
            {
                Some(index) => values[index] = value,
                None => {
                    names.push(Some(name.clone()));
                    values.push(value);
                }
            }
        }
        let items = names
            .iter()
            .cloned()
            .zip(values)
            .map(|(name, value)| Declare { name, value })
            .collect();
        q.compute(source, items)
    }
}
