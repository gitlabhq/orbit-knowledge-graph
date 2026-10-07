use super::*;
use std::convert::Infallible;

#[path = "render_expression.rs"]
mod expressions;

struct RenderedInput<'a> {
    sql: String,
    tail: String,
    groups: Vec<Expression<'a>>,
}

impl<'a> RenderedInput<'a> {
    fn rows(sql: String) -> Self {
        Self {
            sql,
            tail: String::new(),
            groups: vec![],
        }
    }
}

impl<'a, M: QueryDataModel + ?Sized> QueryGraph<'a, M, Infallible> {
    pub fn render(&self, root: BlockId) -> Result<String> {
        if !self.block(root)?.required.is_empty() {
            return Err(GraphError::DefinitionVisibility);
        }
        self.render_block(root, true)
    }

    pub fn render_parameterized(
        mut self,
        root: BlockId,
    ) -> Result<(
        String,
        std::collections::HashMap<String, orbit_utils::query_types::ParamValue>,
    )> {
        let mut bindings = orbit_utils::query_types::ParamBindings::default();
        for id in self.reachable_blocks(root)? {
            let operation = self
                .block_mut(id)?
                .operation
                .as_mut()
                .ok_or(GraphError::EmptyProjection)?;
            for output in &mut operation.outputs {
                if let Some(value) = &mut output.value {
                    value.bind_parameters(&mut bindings);
                }
            }
            if let QueryKind::Project(input) = &mut operation.kind {
                input.bind_parameters(&mut bindings);
            }
        }
        Ok((self.render(root)?, bindings.into_map()))
    }

    fn render_block(&self, id: BlockId, public: bool) -> Result<String> {
        let block = self.block(id)?;
        let operation = self.query_operation(id)?;
        let mut sql = String::new();
        if !block.definitions.is_empty() {
            let definitions = block
                .definitions
                .iter()
                .enumerate()
                .map(|(slot, definition)| {
                    Ok(format!(
                        "{} AS ({})",
                        definition_name(DefinitionId { block: id, slot }),
                        self.render_block(definition.body, false)?
                    ))
                })
                .collect::<Result<Vec<_>>>()?;
            sql.push_str(&format!("WITH {} ", definitions.join(", ")));
        }
        let label = |slot, output: &Output<'a>| {
            if public {
                quoted(&output.label)
            } else {
                output_name(OutputId { block: id, slot })
            }
        };
        match &operation.kind {
            QueryKind::Project(input) => {
                let mut needed = Vec::new();
                let projection = operation
                    .outputs
                    .iter()
                    .enumerate()
                    .map(|(slot, output)| {
                        let value = output.value.as_ref().ok_or(GraphError::MissingOutput)?;
                        collect_columns(value, &mut needed)?;
                        Ok(format!(
                            "{} AS {}",
                            self.render_expression_at(value, true)?,
                            label(slot, output)
                        ))
                    })
                    .collect::<Result<Vec<_>>>()?;
                let rendered = self.render_operation(input, &needed)?;
                let groups = self.render_groups(&rendered.groups)?;
                sql.push_str(&format!(
                    "SELECT {} FROM ({}) AS q{groups}{}",
                    projection.join(", "),
                    rendered.sql,
                    rendered.tail
                ));
            }
            QueryKind::UnionAll(arms) => {
                let arms = arms
                    .iter()
                    .map(|arm| {
                        let projection = operation
                            .outputs
                            .iter()
                            .enumerate()
                            .map(|(slot, output)| {
                                format!(
                                    "u.{} AS {}",
                                    output_name(OutputId { block: *arm, slot }),
                                    label(slot, output)
                                )
                            })
                            .collect::<Vec<_>>()
                            .join(", ");
                        Ok(format!(
                            "(SELECT {projection} FROM ({}) AS u)",
                            self.render_block(*arm, false)?
                        ))
                    })
                    .collect::<Result<Vec<_>>>()?;
                sql.push_str(&arms.join(" UNION ALL "));
            }
        }
        Ok(sql)
    }

    fn render_column(&self, column: ColumnRef<'a>) -> Result<String> {
        let port = match column.port {
            Port::Stored(column) => quoted(column.name()),
            Port::Output(output) => output_name(output),
        };
        Ok(format!("{}.{port}", relation_name(column.relation)))
    }

    fn render_source(
        &self,
        relation: RelationId,
        read: ReadMode,
        needed: &[ColumnRef<'a>],
        predicate: Option<&Expression<'a>>,
    ) -> Result<String> {
        let source = match self.relation(relation)?.source {
            Source::Stored(table) => quoted(table.name()),
            Source::Derived(body) => format!("({})", self.render_block(body, false)?),
            Source::Definition(definition) => definition_name(definition),
        };
        let projection = needed
            .iter()
            .map(|column| {
                Ok(format!(
                    "{} AS {}",
                    self.render_column(*column)?,
                    value_name(*column)
                ))
            })
            .collect::<Result<Vec<_>>>()?;
        let projection = nonempty_projection(projection);
        let current = if matches!(read, ReadMode::Current) {
            " FINAL"
        } else {
            ""
        };
        let filter = predicate
            .map(|predicate| {
                self.render_expression_at(predicate, false)
                    .map(|sql| format!(" WHERE {sql}"))
            })
            .transpose()?
            .unwrap_or_default();
        Ok(format!(
            "SELECT {projection} FROM {source} AS {}{current}{filter}",
            relation_name(relation)
        ))
    }

    fn render_operation(
        &self,
        operation: &LoweredOperation<'a>,
        needed: &[ColumnRef<'a>],
    ) -> Result<RenderedInput<'a>> {
        use OperationKind::*;
        Ok(match operation.kind() {
            One => RenderedInput::rows("SELECT 1 AS _unit".into()),
            Source { relation, read } => {
                RenderedInput::rows(self.render_source(*relation, *read, needed, None)?)
            }
            Filter { input, predicate } => {
                if let Source { relation, read } = input.kind()
                    && matches!(self.relation(*relation)?.source, super::Source::Stored(_))
                {
                    return Ok(RenderedInput::rows(self.render_source(
                        *relation,
                        *read,
                        needed,
                        Some(predicate),
                    )?));
                }
                let mut required = needed.to_vec();
                collect_columns(predicate, &mut required)?;
                let source = self.materialize_operation(input, &required)?;
                RenderedInput::rows(format!(
                    "SELECT {} FROM ({source}) AS q WHERE {}",
                    selection(needed),
                    self.render_expression_at(predicate, true)?
                ))
            }
            Join {
                left,
                right,
                kind,
                condition,
            } => self.render_join(left, right, *kind, condition, needed)?,
            Aggregate { input, groups } => {
                let mut required = needed.to_vec();
                for group in groups {
                    collect_columns(group, &mut required)?;
                }
                RenderedInput {
                    sql: self.materialize_operation(input, &required)?,
                    tail: String::new(),
                    groups: groups.clone(),
                }
            }
            Sort { input, keys } => {
                let mut required = needed.to_vec();
                extend_columns(&mut required, keys.iter().map(|(column, _)| *column));
                let mut rendered = self.render_operation(input, &required)?;
                if !rendered.tail.is_empty() {
                    rendered = RenderedInput::rows(self.materialize_operation(input, &required)?);
                }
                let order = keys
                    .iter()
                    .map(|(column, descending)| {
                        format!(
                            "q.{} {}",
                            value_name(*column),
                            if *descending { "DESC" } else { "ASC" }
                        )
                    })
                    .collect::<Vec<_>>()
                    .join(", ");
                rendered.tail = format!(" ORDER BY {order}");
                rendered
            }
            FirstBy { input, keys } => {
                let mut required = needed.to_vec();
                extend_columns(&mut required, keys.iter().copied());
                let mut rendered = self.render_operation(input, &required)?;
                let keys = keys
                    .iter()
                    .map(|column| format!("q.{}", value_name(*column)))
                    .collect::<Vec<_>>()
                    .join(", ");
                rendered.tail.push_str(&format!(" LIMIT 1 BY {keys}"));
                rendered
            }
            Limit { input, count } => {
                let mut rendered = if matches!(input.kind(), Limit { .. }) {
                    RenderedInput::rows(self.materialize_operation(input, needed)?)
                } else {
                    self.render_operation(input, needed)?
                };
                rendered.tail.push_str(&format!(" LIMIT {count}"));
                rendered
            }
            Expand { input, column } => {
                let mut required = needed.to_vec();
                extend_columns(&mut required, [*column]);
                let sql = self.materialize_operation(input, &required)?;
                let projection = required
                    .iter()
                    .map(|value| {
                        if value == column {
                            format!("arrayJoin(q.{0}) AS {0}", value_name(*value))
                        } else {
                            format!("q.{}", value_name(*value))
                        }
                    })
                    .collect::<Vec<_>>()
                    .join(", ");
                RenderedInput::rows(format!("SELECT {projection} FROM ({sql}) AS q"))
            }
            Materialize { input, .. } => {
                RenderedInput::rows(self.materialize_operation(input, needed)?)
            }
            Latest { requirement, .. } => match *requirement {},
        })
    }

    fn render_join(
        &self,
        left: &LoweredOperation<'a>,
        right: &LoweredOperation<'a>,
        kind: JoinKind,
        condition: &Expression<'a>,
        needed: &[ColumnRef<'a>],
    ) -> Result<RenderedInput<'a>> {
        let mut required = needed.to_vec();
        collect_columns(condition, &mut required)?;
        let left_needed = required
            .iter()
            .filter(|column| left.columns().contains(column))
            .copied()
            .collect::<Vec<_>>();
        let right_needed = required
            .iter()
            .filter(|column| right.columns().contains(column))
            .copied()
            .collect::<Vec<_>>();
        let left_sql = self.materialize_operation(left, &left_needed)?;
        let right_sql = self.materialize_operation(right, &right_needed)?;
        if matches!(kind, JoinKind::Membership) {
            let Expression::Equal(value, key) = condition else {
                return Err(GraphError::JoinShape);
            };
            let (Expression::Column(value), Expression::Column(key)) =
                (value.as_ref(), key.as_ref())
            else {
                return Err(GraphError::JoinShape);
            };
            return Ok(RenderedInput::rows(format!(
                "SELECT {} FROM ({left_sql}) AS q WHERE q.{} IN (SELECT k.{} FROM ({right_sql}) AS k)",
                selection(needed),
                value_name(*value),
                value_name(*key)
            )));
        }
        let column = |column: ColumnRef<'a>| {
            let side = if left.columns().contains(&column) {
                "l"
            } else {
                "r"
            };
            Ok(format!("{side}.{}", value_name(column)))
        };
        let projection = nonempty_projection(
            needed
                .iter()
                .map(|value| column(*value))
                .collect::<Result<Vec<_>>>()?,
        );
        let keyword = match kind {
            JoinKind::Inner => "INNER JOIN",
            JoinKind::Cross => "CROSS JOIN",
            JoinKind::Semi => "LEFT SEMI JOIN",
            JoinKind::Membership => unreachable!(),
        };
        let condition = if matches!(kind, JoinKind::Cross) {
            String::new()
        } else {
            format!(" ON {}", self.render_expression_with(condition, &column)?)
        };
        Ok(RenderedInput::rows(format!(
            "SELECT {projection} FROM ({left_sql}) AS l {keyword} ({right_sql}) AS r{condition}"
        )))
    }

    fn render_groups(&self, groups: &[Expression<'a>]) -> Result<String> {
        if groups.is_empty() {
            return Ok(String::new());
        }
        let values = groups
            .iter()
            .map(|group| self.render_expression_at(group, true))
            .collect::<Result<Vec<_>>>()?;
        Ok(format!(" GROUP BY {}", values.join(", ")))
    }

    fn materialize_operation(
        &self,
        operation: &LoweredOperation<'a>,
        needed: &[ColumnRef<'a>],
    ) -> Result<String> {
        let rendered = self.render_operation(operation, needed)?;
        if rendered.tail.is_empty() && rendered.groups.is_empty() {
            return Ok(rendered.sql);
        }
        Ok(format!(
            "SELECT {} FROM ({}) AS q{}{}",
            selection(needed),
            rendered.sql,
            self.render_groups(&rendered.groups)?,
            rendered.tail
        ))
    }

    fn render_expression_at(
        &self,
        expression: &Expression<'a>,
        materialized: bool,
    ) -> Result<String> {
        self.render_expression_with(expression, &|column| {
            if materialized {
                Ok(format!("q.{}", value_name(column)))
            } else {
                self.render_column(column)
            }
        })
    }
}

fn quoted(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}
fn nonempty_projection(values: Vec<String>) -> String {
    if values.is_empty() {
        "1 AS _unit".into()
    } else {
        values.join(", ")
    }
}
fn selection(columns: &[ColumnRef<'_>]) -> String {
    nonempty_projection(
        columns
            .iter()
            .map(|column| format!("q.{}", value_name(*column)))
            .collect(),
    )
}

fn extend_columns<'a>(
    needed: &mut Vec<ColumnRef<'a>>,
    columns: impl IntoIterator<Item = ColumnRef<'a>>,
) {
    for column in columns {
        if !needed.contains(&column) {
            needed.push(column);
        }
    }
}

fn collect_columns<'a>(expression: &Expression<'a>, needed: &mut Vec<ColumnRef<'a>>) -> Result<()> {
    expression.columns(&mut |column| {
        extend_columns(needed, [column]);
        Ok(())
    })
}

fn value_name(column: ColumnRef<'_>) -> String {
    let port = match column.port {
        Port::Stored(column) => format!("stored_{}", column.ordinal()),
        Port::Output(output) => output_name(output),
    };
    quoted(&format!("{}_{}", relation_name(column.relation), port))
}

pub(super) fn relation_name(id: RelationId) -> String {
    format!("r{}_{}", id.block.slot, id.slot)
}
pub(super) fn output_name(id: OutputId) -> String {
    format!("o{}_{}", id.block.slot, id.slot)
}
pub(super) fn definition_name(id: DefinitionId) -> String {
    format!("d{}_{}", id.block.slot, id.slot)
}
