use std::collections::{HashMap, HashSet};

use ontology::constants::DEFAULT_PRIMARY_KEY;
use query_data_model::EntityAuthConfig;
use query_data_model::implementations::relational::RelationalBackend;
use query_data_model::implementations::relational::plan::{
    Column, Expr, Operation, Relation, RelationalBuilder, RelationalError, Selection,
};
use query_data_model::plan::tracking::{Declare, Ref};

use crate::constants::{
    primary_key_column, redaction_id_column, redaction_type_column, traversal_path_column,
};
use crate::error::{QueryError, Result};
use crate::input::{Input, QueryType};
use crate::passes::codegen::{PlanOf, RelationalModel, rejected};
use crate::passes::lower::Term;
use crate::passes::lower::build::term;
use crate::passes::lower::{LoweredMetadata, NodeBinding};
use crate::passes::plan::predicates::{Column as Named, OutputValue};

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
    entity_auth: HashMap<String, EntityAuthConfig>,
    edges: Vec<EdgeMeta>,
    neighbors_center: Option<(String, i64)>,
    labels: HashMap<String, String>,
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

    pub fn add_entity_auth(&mut self, entity_type: impl Into<String>, config: EntityAuthConfig) {
        self.entity_auth.insert(entity_type.into(), config);
    }

    pub fn get_entity_auth(&self, entity_type: &str) -> Option<&EntityAuthConfig> {
        self.entity_auth.get(entity_type)
    }

    pub fn entity_auth(&self) -> impl Iterator<Item = (&str, &EntityAuthConfig)> {
        self.entity_auth.iter().map(|(k, v)| (k.as_str(), v))
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

    pub fn neighbors_center(&self) -> Option<(&str, i64)> {
        self.neighbors_center
            .as_ref()
            .map(|(entity, id)| (entity.as_str(), *id))
    }

    pub fn label(&self, entity: &str) -> Option<&str> {
        self.labels.get(entity).map(String::as_str)
    }
}

pub fn apply<M: RelationalModel>(
    plan: PlanOf<M>,
    input: &Input,
    metadata: &LoweredMetadata,
    model: &M,
) -> Result<(PlanOf<M>, ResultContext)> {
    let mut ctx = ResultContext::new().with_query_type(input.query_type);
    ctx.entity_auth.clone_from(model.entity_auth());
    if input.query_type == QueryType::Neighbors
        && let Some(node) = input.nodes.first()
        && let (Some(entity), [id]) = (&node.entity, node.node_ids.as_slice())
    {
        ctx.neighbors_center = Some((entity.clone(), *id));
        ctx.labels = model
            .graph()
            .entities()
            .map(|entity| (entity.name.clone(), entity.label.clone()))
            .collect();
    }
    let selectable_nodes: HashSet<&str> = match input.query_type {
        QueryType::Aggregation => input.aggregation_row_nodes(model),
        QueryType::Traversal | QueryType::Neighbors => {
            input.nodes.iter().map(|n| n.id.as_str()).collect()
        }
        QueryType::PathFinding | QueryType::Hydration => HashSet::new(),
    };
    for node in &input.nodes {
        if selectable_nodes.contains(node.id.as_str())
            && let Some(entity) = &node.entity
        {
            ctx.add_node(&node.id, entity);
        }
    }
    let contract = return_columns(input, &selectable_nodes, &metadata.nodes, model)?;
    if matches!(
        input.query_type,
        QueryType::Traversal | QueryType::Aggregation
    ) {
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
        false => enforce(plan, model, contract, input.query_type)?,
    };
    Ok((plan, ctx))
}

struct Contract {
    outputs: Vec<(String, Term)>,
    groups: Vec<Term>,
}

fn return_columns(
    input: &Input,
    selectable_nodes: &HashSet<&str>,
    bindings: &HashMap<String, NodeBinding>,
    model: &impl query_data_model::OrbitQueryModel,
) -> Result<Contract> {
    let mut contract = Contract {
        outputs: Vec::new(),
        groups: Vec::new(),
    };
    for node in &input.nodes {
        let Some(entity) = &node.entity else {
            continue;
        };
        if !selectable_nodes.contains(node.id.as_str()) {
            continue;
        }
        let entity_id = model
            .entity(entity)
            .map(|entity| entity.id)
            .ok_or_else(|| QueryError::PipelineInvariant(format!("unknown entity '{entity}'")))?;
        let binding = bindings.get(&node.id).ok_or_else(|| {
            QueryError::PipelineInvariant(format!("node '{}' has no lowered binding", node.id))
        })?;
        if matches!(binding, NodeBinding::Filtered) {
            return Err(QueryError::PipelineInvariant(format!(
                "node '{}' has no result identity",
                node.id
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
        let redaction_column = model
            .redaction_id_column(entity_id)
            .unwrap_or(DEFAULT_PRIMARY_KEY);
        let needs_separate_pk = redaction_column != DEFAULT_PRIMARY_KEY;
        let authorization_id = if needs_separate_pk {
            let table_alias = table_alias.as_deref().ok_or_else(|| {
                QueryError::PipelineInvariant(format!(
                    "node '{}' is authorized by {redaction_column} but has no table read",
                    node.id
                ))
            })?;
            Term::Value(OutputValue::Column(Named::new(
                table_alias,
                redaction_column,
            )))
        } else {
            identity.clone()
        };
        if needs_separate_pk {
            contract.set(primary_key_column(&node.id), identity.clone());
            contract.group(identity.clone());
        }
        contract.set(redaction_id_column(&node.id), authorization_id.clone());
        contract.group(authorization_id);
        let kind = Term::Value(OutputValue::Text(entity.clone()));
        contract.set(redaction_type_column(&node.id), kind);
        if input.query_type != QueryType::Aggregation
            && model.entity_has_traversal_path(entity)
            && let Some(path) = traversal_path
        {
            let path = Term::Value(OutputValue::Column(path.clone()));
            contract.set(traversal_path_column(&node.id), path);
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

fn enforce<M: RelationalModel>(
    plan: PlanOf<M>,
    model: &M,
    contract: Contract,
    query_type: QueryType,
) -> Result<PlanOf<M>> {
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
        bound = groups
            .iter()
            .cloned()
            .zip(outputs.iter().copied())
            .collect();
        if query_type == QueryType::Aggregation && !groups.is_empty() {
            aggregate = Some((node, groups.len(), outputs));
        }
    }
    let mut added: Vec<Ref> = Vec::new();
    model
        .catalog()
        .rewrite(plan, |q, rows| {
            let node = q.original();
            if let Some((aggregate, width, outputs)) = &aggregate
                && node == Some(*aggregate)
            {
                let (groups, rows) = regroup(q, &contract, *width, outputs, rows)?;
                bound = groups;
                return Ok(rows);
            }
            if block.compute.is_some() && node == block.compute {
                let detached = q.detach(rows)?;
                let Operation::Compute(values) = detached.operation else {
                    return Err(RelationalError::Incomplete("a compute"));
                };
                let source = first(detached.inputs)?;
                let scope = Scope {
                    contract: &contract,
                    bound: &bound,
                };
                let (rows, columns) = scope.extend(q, &mut names, values, source)?;
                for (old, new) in computed.iter().zip(&columns) {
                    if *old != new.output() {
                        q.reconnect(*old, new.output())?;
                    }
                }
                for column in &columns[computed.len()..] {
                    q.expose(*column)?;
                    added.push(column.output());
                }
                return Ok(rows);
            }
            if node != Some(consumer) && node != Some(select) {
                return Ok(rows);
            }
            let mut detached = q.detach(rows)?;
            if block.compute.is_none() && node == Some(consumer) {
                let source = detached.inputs.remove(0);
                let scope = Scope {
                    contract: &contract,
                    bound: &bound,
                };
                let (rows, columns) = scope.extend(q, &mut names, Vec::new(), source)?;
                detached.inputs.insert(0, rows);
                added = columns.iter().map(|column| column.output()).collect();
                if node != Some(select) {
                    let rows = q.reattach(detached)?;
                    for column in columns {
                        q.expose(column)?;
                    }
                    return Ok(rows);
                }
            }
            if let Operation::Select(Selection::Columns(columns)) = &mut detached.operation {
                columns.extend(added.iter().copied());
            }
            q.reattach(detached)
        })
        .map_err(rejected)
}

fn first(inputs: Vec<Relation>) -> std::result::Result<Relation, RelationalError> {
    inputs
        .into_iter()
        .next()
        .ok_or(RelationalError::Incomplete("an input"))
}

fn regroup<B: RelationalBackend>(
    q: &mut RelationalBuilder<'_, B>,
    contract: &Contract,
    width: usize,
    outputs: &[Ref],
    rows: Relation,
) -> std::result::Result<(Vec<(Expr, Ref)>, Relation), RelationalError> {
    let detached = q.detach(rows)?;
    let Operation::Aggregate {
        mut groups,
        measures,
    } = detached.operation
    else {
        return Err(RelationalError::Incomplete("an aggregate"));
    };
    let source = first(detached.inputs)?;
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
        if *old != new.output() {
            q.reconnect(*old, new.output())?;
        }
    }
    for group in &aggregated.groups[width..] {
        q.expose(*group)?;
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
