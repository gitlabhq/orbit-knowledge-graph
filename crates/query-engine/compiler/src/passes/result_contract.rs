use crate::ast::{Expr, Function, Node, Query, SelectExpr, TableRef};
use crate::constants::{
    owner_key_column, partial_count_column, primary_key_column, redaction_id_column,
    redaction_type_column, split_partials_column, traversal_path_column,
};
use crate::error::{QueryError, Result};
use crate::input::{AggFunction, ENTITY_ROWS, Input, OrderDirection, QueryType};
use crate::passes::lower::{LoweredMetadata, NodeBinding};
use ontology::constants::DEFAULT_PRIMARY_KEY;
use query_data_model::{EntityAuthConfig, ReadGrain};
use std::collections::HashMap;

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

pub const MAX_SPLIT_GROUPS: u32 = 1_000;
pub const MAX_SPLIT_OWNERS: usize = 500;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Fold {
    Add,
    Min,
    Max,
    Mean { count: String },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SplitMeasure {
    pub name: String,
    pub fold: Fold,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SplitOwner {
    pub id_column: String,
    pub key: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OwnerSplit {
    pub partials_column: String,
    pub partials: Vec<String>,
    pub owners: Vec<SplitOwner>,
    pub measures: Vec<SplitMeasure>,
    pub order: Vec<(String, bool)>,
    pub skip: usize,
    pub limit: usize,
}

#[derive(Debug, Clone, Default)]
pub struct ResultContext {
    pub query_type: Option<QueryType>,
    nodes: HashMap<String, RedactionNode>,
    entity_auth: HashMap<String, EntityAuthConfig>,
    edges: Vec<EdgeMeta>,
    split: Option<OwnerSplit>,
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

    pub fn split(&self) -> Option<&OwnerSplit> {
        self.split.as_ref()
    }

    pub fn with_split(mut self, split: OwnerSplit) -> Self {
        self.split = Some(split);
        self
    }
}

pub fn apply(
    node: &mut Node,
    input: &Input,
    metadata: &LoweredMetadata,
    model: &(impl query_data_model::OrbitQueryModel + ?Sized),
) -> Result<ResultContext> {
    let mut ctx = ResultContext::new().with_query_type(input.query_type);
    ctx.entity_auth.clone_from(model.entity_auth());
    apply_with(node, input, metadata, model, &mut ctx, |entity| {
        model
            .redaction_id_column(entity)
            .unwrap_or(DEFAULT_PRIMARY_KEY)
            .to_string()
    })?;
    Ok(ctx)
}

fn apply_with(
    node: &mut Node,
    input: &Input,
    metadata: &LoweredMetadata,
    model: &(impl query_data_model::OrbitQueryModel + ?Sized),
    ctx: &mut ResultContext,
    redaction_column: impl Fn(query_data_model::EntityId) -> String,
) -> Result<()> {
    let selectable_nodes: HashMap<&str, &ReadGrain> = match input.query_type {
        QueryType::Aggregation => input.aggregation_row_nodes(model),
        QueryType::Traversal | QueryType::Neighbors => input
            .nodes
            .iter()
            .map(|n| (n.id.as_str(), &ENTITY_ROWS))
            .collect(),
        QueryType::PathFinding | QueryType::Hydration => HashMap::new(),
    };
    match node {
        Node::Query(q) => {
            for node in &input.nodes {
                if selectable_nodes.contains_key(node.id.as_str())
                    && let Some(entity) = &node.entity
                {
                    ctx.add_node(&node.id, entity);
                }
            }
            enforce_return_columns(
                q,
                input,
                &selectable_nodes,
                &metadata.nodes,
                model,
                redaction_column,
            )?;
            let split_nodes = input.owner_split_nodes(model);
            if input.query_type == QueryType::Aggregation && !split_nodes.is_empty() {
                ctx.split = Some(roll_up(q, input, &split_nodes)?);
            }
        }
        Node::Insert(_) => return Ok(()),
    }

    if matches!(
        input.query_type,
        QueryType::Traversal | QueryType::Aggregation
    ) {
        use crate::constants::{
            EDGE_DST_SUFFIX, EDGE_DST_TYPE_SUFFIX, EDGE_SRC_SUFFIX, EDGE_SRC_TYPE_SUFFIX,
            EDGE_TYPE_SUFFIX,
        };
        for edge in &metadata.edges {
            let prefix = edge.column_prefix.clone();
            ctx.edges.push(EdgeMeta {
                type_column: format!("{prefix}{EDGE_TYPE_SUFFIX}"),
                src_column: format!("{prefix}{EDGE_SRC_SUFFIX}"),
                src_type_column: format!("{prefix}{EDGE_SRC_TYPE_SUFFIX}"),
                dst_column: format!("{prefix}{EDGE_DST_SUFFIX}"),
                dst_type_column: format!("{prefix}{EDGE_DST_TYPE_SUFFIX}"),
                column_prefix: prefix,
                path_column: edge.path_column.clone(),
                rel_types: edge.rel_types.clone(),
                relationship: edge.relationship,
                from_alias: String::new(),
                to_alias: String::new(),
            });
        }
    }
    Ok(())
}

fn roll_up(q: &mut Query, input: &Input, nodes: &[(&str, &[String])]) -> Result<OwnerSplit> {
    let measures = input
        .aggregation
        .metrics
        .iter()
        .map(|metric| {
            let name = metric.output_name();
            let fold = match metric.expr.function() {
                AggFunction::Count | AggFunction::Sum => Fold::Add,
                AggFunction::Min => Fold::Min,
                AggFunction::Max => Fold::Max,
                AggFunction::Avg => Fold::Mean {
                    count: partial_count_column(&name),
                },
                AggFunction::Collect => {
                    return Err(QueryError::PipelineInvariant(format!(
                        "aggregation \"{name}\" cannot be folded across owners"
                    )));
                }
            };
            Ok(SplitMeasure { name, fold })
        })
        .collect::<Result<Vec<_>>>()?;
    let owners: Vec<SplitOwner> = nodes
        .iter()
        .map(|(node, _)| SplitOwner {
            id_column: redaction_id_column(node),
            key: (0..)
                .map(|index| owner_key_column(node, index))
                .take_while(|column| q.selects_alias(column))
                .collect(),
        })
        .collect();
    let is_partial = |alias: &String| {
        measures.iter().any(|measure| {
            measure.name == *alias
                || matches!(&measure.fold, Fold::Mean { count } if count == alias)
        }) || owners
            .iter()
            .any(|owner| owner.id_column == *alias || owner.key.contains(alias))
    };

    let mut inner = std::mem::take(q);
    inner.order_by.clear();
    inner.limit = None;
    inner.offset = None;
    let (partials, groups): (Vec<String>, Vec<String>) = inner
        .select
        .iter()
        .filter_map(|select| select.alias.clone())
        .partition(is_partial);
    let partials_column = split_partials_column().to_string();
    let collected = Expr::Aggregate {
        function: AggFunction::Collect,
        argument: Some(Box::new(Expr::func(
            Function::Tuple,
            partials.iter().map(Expr::ident).collect(),
        ))),
        distinct: false,
        condition: None,
    };
    *q = Query {
        ctes: std::mem::take(&mut inner.ctes),
        select: groups
            .iter()
            .map(|group| SelectExpr::new(Expr::ident(group), group))
            .chain([SelectExpr::new(collected, &partials_column)])
            .collect(),
        from: TableRef::Subquery {
            query: Box::new(inner),
            alias: "owners".into(),
        },
        group_by: groups.iter().map(Expr::ident).collect(),
        limit: Some(MAX_SPLIT_GROUPS + 1),
        ..Query::default()
    };

    let order = input
        .aggregation
        .sort
        .iter()
        .map(|sort| (sort.column.clone(), sort.direction == OrderDirection::Desc))
        .chain(groups.into_iter().map(|group| (group, false)))
        .collect();
    Ok(OwnerSplit {
        partials_column,
        partials,
        owners,
        measures,
        order,
        skip: input.skip.unwrap_or(0) as usize,
        limit: input.fetch_limit() as usize,
    })
}

fn ensure_in_group_by(q: &mut Query, query_type: QueryType, expr: Expr) {
    if query_type == QueryType::Aggregation && !q.group_by.is_empty() && !q.group_by.contains(&expr)
    {
        q.group_by.push(expr);
    }
}

fn set_output(q: &mut Query, output: SelectExpr) {
    for arm in &mut q.union_all {
        set_output(arm, output.clone());
    }
    match q
        .select
        .iter_mut()
        .find(|select| select.alias == output.alias)
    {
        Some(existing) => *existing = output,
        None => q.select.push(output),
    }
}

fn enforce_return_columns(
    q: &mut Query,
    input: &Input,
    selectable_nodes: &HashMap<&str, &ReadGrain>,
    bindings: &HashMap<String, NodeBinding>,
    model: &(impl query_data_model::OrbitQueryModel + ?Sized),
    redaction_column: impl Fn(query_data_model::EntityId) -> String,
) -> Result<()> {
    for node in &input.nodes {
        let Some(entity) = &node.entity else {
            continue;
        };
        let Some(grain) = selectable_nodes.get(node.id.as_str()) else {
            continue;
        };
        let entity_id = model
            .entity(entity)
            .map(|entity| entity.id)
            .ok_or_else(|| QueryError::PipelineInvariant(format!("unknown entity '{entity}'")))?;
        let id_col = redaction_id_column(&node.id);
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
        let redaction_column = redaction_column(entity_id);
        let needs_separate_pk = redaction_column != DEFAULT_PRIMARY_KEY;
        let authorization_id = if needs_separate_pk {
            let table_alias = table_alias.as_deref().ok_or_else(|| {
                QueryError::PipelineInvariant(format!(
                    "node '{}' is authorized by {redaction_column} but has no table read",
                    node.id
                ))
            })?;
            Expr::col(table_alias, redaction_column)
        } else {
            identity.clone()
        };

        match grain {
            ReadGrain::Owner { .. } if !needs_separate_pk => {
                let representative = Expr::Aggregate {
                    function: AggFunction::Min,
                    argument: Some(Box::new(authorization_id)),
                    distinct: false,
                    condition: None,
                };
                set_output(q, SelectExpr::new(representative, id_col.clone()));
            }
            ReadGrain::Owner { .. } => {
                set_output(q, SelectExpr::new(authorization_id.clone(), id_col.clone()));
                ensure_in_group_by(q, input.query_type, authorization_id);
            }
            ReadGrain::Entity => {
                if needs_separate_pk {
                    let name = primary_key_column(&node.id);
                    set_output(q, SelectExpr::new(identity.clone(), name));
                    ensure_in_group_by(q, input.query_type, identity.clone());
                }
                set_output(q, SelectExpr::new(authorization_id.clone(), id_col.clone()));
                ensure_in_group_by(q, input.query_type, authorization_id);
            }
        }
        let type_col = redaction_type_column(&node.id);
        set_output(q, SelectExpr::new(Expr::string(entity), type_col));
        if input.query_type != QueryType::Aggregation
            && model.entity_has_traversal_path(entity)
            && let Some(path) = traversal_path
        {
            let name = traversal_path_column(&node.id);
            set_output(q, SelectExpr::new(path.clone(), name));
        }
    }
    Ok(())
}
