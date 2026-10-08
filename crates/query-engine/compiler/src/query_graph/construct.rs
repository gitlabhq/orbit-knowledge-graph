use super::*;
use orbit_utils::query_types::SqlType;

pub struct QueryScope<'graph, 'catalog, M: QueryDataModel + ?Sized> {
    pub(super) graph: &'graph mut QueryGraph<'catalog, M>,
    pub(super) id: QueryId,
}

impl<'a, M: QueryDataModel + ?Sized> QueryScope<'_, 'a, M> {
    pub fn catalog(&self) -> &'a M {
        self.graph.catalog
    }

    pub fn scan(&mut self, table: &str, read: Read) -> Result<Rows<'a>> {
        let table = self
            .graph
            .catalog
            .stored_table(table)
            .ok_or_else(|| Error::Unknown(table.into()))?;
        let values = table
            .columns()
            .map(|column| {
                let scalar = match column.data_type().ok_or(Error::Type)? {
                    ontology::DataType::Int => SqlType::Int64,
                    ontology::DataType::Float => SqlType::Float64,
                    ontology::DataType::Bool => SqlType::Bool,
                    ontology::DataType::Date => SqlType::Date,
                    ontology::DataType::DateTime => SqlType::Timestamp {
                        precision: 6,
                        timezone: None,
                    },
                    _ => SqlType::String,
                };
                let value = ValueType::Scalar(scalar);
                Ok((
                    column.name().into(),
                    if column.is_array() {
                        ValueType::Array(Box::new(value))
                    } else {
                        value
                    },
                ))
            })
            .collect::<Result<Vec<_>>>()?;
        let (source, columns) = self.graph.columns(self.id, values);
        Ok(Rows {
            scope: self.id,
            lowered: self.graph.lowered,
            kind: OperationKind::Scan {
                table,
                label: None,
                read,
                source,
            },
            columns,
            sources: HashSet::from([source]),
        })
    }

    pub fn cte(
        &mut self,
        name: &str,
        build: impl FnOnce(&mut QueryScope<'_, 'a, M>) -> Result<Rows<'a>>,
    ) -> Result<Cte> {
        let query = self.subquery(build)?;
        let definition = Cte {
            query,
            scope: self.id,
        };
        self.graph.queries[query.slot].attached = true;
        self.graph.queries[self.id.slot]
            .definitions
            .push((name.into(), definition));
        Ok(definition)
    }

    pub fn subquery(
        &mut self,
        build: impl FnOnce(&mut QueryScope<'_, 'a, M>) -> Result<Rows<'a>>,
    ) -> Result<QueryId> {
        let parent = self.graph.get(self.id)?;
        let mut visible = parent.visible.clone();
        visible.extend(parent.definitions.iter().map(|(_, definition)| *definition));
        self.graph.build_query(Some(self.id), visible, build)
    }

    pub fn read(&mut self, definition: Cte) -> Result<Rows<'a>> {
        let query = self.graph.get(self.id)?;
        if !query.visible.contains(&definition)
            && !query
                .definitions
                .iter()
                .any(|(_, owned)| *owned == definition)
        {
            return Err(Error::CteScope);
        }
        self.read_query(definition.query)
    }

    pub fn from(&mut self, query: QueryId) -> Result<Rows<'a>> {
        self.require_query(query)?;
        self.graph.queries[query.slot].parent = Some(self.id);
        self.graph.queries[query.slot].attached = true;
        self.read_query(query)
    }

    pub fn scalar(&mut self, query: QueryId, column: &str) -> Result<Expr> {
        self.require_query(query)?;
        let rows = self.graph.rows(query)?;
        if !rows.scalar() {
            return Err(Error::Aggregate);
        }
        let column = rows.column(column)?;
        self.graph.queries[query.slot].parent = Some(self.id);
        self.graph.queries[query.slot].attached = true;
        Ok(Expr(Box::new(ExprKind::Scalar { query, column })))
    }

    fn read_query(&mut self, query: QueryId) -> Result<Rows<'a>> {
        let values = self
            .graph
            .rows(query)?
            .columns
            .iter()
            .map(|column| (column.name().to_owned(), column.data_type().clone()))
            .collect::<Vec<_>>();
        let (source, columns) = self.graph.columns(self.id, values);
        Ok(Rows {
            scope: self.id,
            lowered: self.graph.lowered,
            kind: OperationKind::Read { query, source },
            columns,
            sources: HashSet::from([source]),
        })
    }

    pub fn filter(&self, input: Rows<'a>, predicate: Expr) -> Result<Rows<'a>> {
        self.require_rows(&input)?;
        if predicate.infer(self, &input.columns, false)? != ValueType::Scalar(SqlType::Bool) {
            return Err(Error::Type);
        }
        Ok(input.wrap(|input| OperationKind::Filter { input, predicate }))
    }

    pub fn join(&self, left: Rows<'a>, right: Rows<'a>, condition: Expr) -> Result<Rows<'a>> {
        self.join_rows(left, right, Join::Inner, condition)
    }
    pub fn semi_join(&self, left: Rows<'a>, right: Rows<'a>, condition: Expr) -> Result<Rows<'a>> {
        self.join_rows(left, right, Join::Semi, condition)
    }
    pub fn cross_join(&self, left: Rows<'a>, right: Rows<'a>) -> Result<Rows<'a>> {
        self.join_rows(left, right, Join::Cross, lit(true))
    }

    pub fn filter_in(
        &self,
        input: Rows<'a>,
        value: Column,
        keys: Rows<'a>,
        key: Column,
    ) -> Result<Rows<'a>> {
        if !input.columns.contains(&value) || !keys.columns.contains(&key) {
            return Err(Error::Column);
        }
        self.join_rows(input, keys, Join::Membership, value.eq(key))
    }

    fn join_rows(
        &self,
        left: Rows<'a>,
        right: Rows<'a>,
        kind: Join,
        condition: Expr,
    ) -> Result<Rows<'a>> {
        self.require_rows(&left)?;
        self.require_rows(&right)?;
        if !left.sources.is_disjoint(&right.sources) {
            return Err(Error::ReusedSource);
        }
        let mut columns = left.columns.clone();
        columns.extend_from_slice(&right.columns);
        if condition.infer(self, &columns, false)? != ValueType::Scalar(SqlType::Bool) {
            return Err(Error::Type);
        }
        if matches!(kind, Join::Semi | Join::Membership) {
            columns.clone_from(&left.columns);
        }
        let sources = left.sources.union(&right.sources).copied().collect();
        Ok(Rows {
            scope: self.id,
            lowered: self.graph.lowered,
            columns,
            sources,
            kind: OperationKind::Join {
                left: Box::new(left),
                right: Box::new(right),
                kind,
                condition,
            },
        })
    }

    pub fn select(
        &mut self,
        input: Rows<'a>,
        values: impl IntoIterator<Item = Named>,
    ) -> Result<Rows<'a>> {
        self.require_rows(&input)?;
        let values = values.into_iter().collect::<Vec<_>>();
        if values.is_empty() {
            return Err(Error::Outputs);
        }
        let types = values
            .iter()
            .map(|value| {
                Ok((
                    value.name.clone(),
                    value.value.infer(self, &input.columns, false)?,
                ))
            })
            .collect::<Result<Vec<_>>>()?;
        let (_, columns) = self.graph.columns(self.id, types);
        Ok(Rows {
            scope: self.id,
            lowered: self.graph.lowered,
            sources: input.sources.clone(),
            columns,
            kind: OperationKind::Select {
                input: Box::new(input),
                values,
            },
        })
    }

    pub fn select_all(&self, input: Rows<'a>) -> Result<Rows<'a>> {
        self.require_rows(&input)?;
        if input.columns.is_empty() {
            return Err(Error::Outputs);
        }
        Ok(input)
    }

    pub fn values(&mut self, values: impl IntoIterator<Item = Named>) -> Result<Rows<'a>> {
        self.select(
            Rows {
                scope: self.id,
                lowered: self.graph.lowered,
                kind: OperationKind::Unit,
                columns: vec![],
                sources: HashSet::new(),
            },
            values,
        )
    }

    pub fn aggregate(
        &mut self,
        input: Rows<'a>,
        groups: impl IntoIterator<Item = Named>,
        measures: impl IntoIterator<Item = Named>,
    ) -> Result<Rows<'a>> {
        self.require_rows(&input)?;
        let groups = groups.into_iter().collect::<Vec<_>>();
        let measures = measures.into_iter().collect::<Vec<_>>();
        let mut values = Vec::new();
        for (outputs, aggregate) in [(&groups, false), (&measures, true)] {
            for output in outputs {
                if aggregate && !matches!(output.value.kind(), ExprKind::Aggregate { .. }) {
                    return Err(Error::Aggregate);
                }
                values.push((
                    output.name.clone(),
                    output.value.infer(self, &input.columns, aggregate)?,
                ));
            }
        }
        if values.is_empty() {
            return Err(Error::Outputs);
        }
        let (_, columns) = self.graph.columns(self.id, values);
        Ok(Rows {
            scope: self.id,
            lowered: self.graph.lowered,
            sources: input.sources.clone(),
            columns,
            kind: OperationKind::Aggregate {
                input: Box::new(input),
                groups,
                measures,
            },
        })
    }

    pub fn expand(&mut self, input: Rows<'a>, value: Named) -> Result<Rows<'a>> {
        self.require_rows(&input)?;
        let ValueType::Array(element) = value.value.infer(self, &input.columns, false)? else {
            return Err(Error::Type);
        };
        let (_, output) = self
            .graph
            .columns(self.id, [(value.name.clone(), *element)]);
        let mut columns = input.columns.clone();
        columns.extend(output);
        Ok(Rows {
            scope: self.id,
            lowered: self.graph.lowered,
            sources: input.sources.clone(),
            columns,
            kind: OperationKind::Expand {
                input: Box::new(input),
                value,
            },
        })
    }

    pub fn sort(&self, input: Rows<'a>, keys: impl IntoIterator<Item = Order>) -> Result<Rows<'a>> {
        self.require_rows(&input)?;
        let keys = keys.into_iter().collect::<Vec<_>>();
        for key in &keys {
            key.value.infer(self, &input.columns, false)?;
        }
        if keys.is_empty() {
            return Ok(input);
        }
        Ok(input.wrap(|input| OperationKind::Sort { input, keys }))
    }

    pub fn limit(&self, input: Rows<'a>, count: u32) -> Result<Rows<'a>> {
        self.require_rows(&input)?;
        Ok(input.wrap(|input| OperationKind::Limit { input, count }))
    }

    pub fn latest(
        &self,
        input: Rows<'a>,
        keys: impl IntoIterator<Item = Column>,
        version: Column,
    ) -> Result<Rows<'a>> {
        self.require_rows(&input)?;
        if self.graph.lowered {
            return Err(Error::Latest);
        }
        let mut scan = &input;
        loop {
            match scan.kind() {
                OperationKind::Filter { input, .. } => scan = input,
                OperationKind::Join {
                    left,
                    kind: Join::Semi | Join::Membership,
                    ..
                } => scan = left,
                _ => break,
            }
        }
        let OperationKind::Scan {
            table,
            read: Read::Raw,
            source,
            ..
        } = scan.kind()
        else {
            return Err(Error::Latest);
        };
        let keys = keys.into_iter().collect::<Vec<_>>();
        let expected = self
            .graph
            .catalog
            .table_sort_key(table.name())
            .ok_or(Error::Latest)?;
        if expected.is_empty()
            || keys.len() != expected.len()
            || !input.columns.contains(&version)
            || version.0.source != *source
            || version.name() != ontology::VERSION_COLUMN
            || keys.iter().zip(expected).any(|(column, name)| {
                column.0.source != *source
                    || column.name() != name
                    || !input.columns.contains(column)
            })
        {
            return Err(Error::Latest);
        }
        Ok(input.wrap(|input| OperationKind::Latest {
            input,
            keys,
            version,
        }))
    }

    pub fn union_all(&mut self, arms: impl IntoIterator<Item = QueryId>) -> Result<Rows<'a>> {
        let arms = arms.into_iter().collect::<Vec<_>>();
        let mut seen = HashSet::new();
        let first = *arms.first().ok_or(Error::Outputs)?;
        let values = self
            .graph
            .rows(first)?
            .columns
            .iter()
            .map(|column| (column.name().to_owned(), column.data_type().clone()))
            .collect::<Vec<_>>();
        for arm in &arms {
            self.require_query(*arm)?;
            if !seen.insert(*arm) {
                return Err(Error::Ownership);
            }
            let columns = &self.graph.rows(*arm)?.columns;
            if columns.len() != values.len()
                || columns
                    .iter()
                    .zip(&values)
                    .any(|(column, (_, data_type))| column.data_type() != data_type)
            {
                return Err(Error::Outputs);
            }
        }
        for arm in &arms {
            self.graph.queries[arm.slot].parent = Some(self.id);
            self.graph.queries[arm.slot].attached = true;
        }
        let (_, columns) = self.graph.columns(self.id, values);
        Ok(Rows {
            scope: self.id,
            lowered: self.graph.lowered,
            columns,
            sources: HashSet::new(),
            kind: OperationKind::Union { arms },
        })
    }

    pub(super) fn require_rows(&self, rows: &Rows<'a>) -> Result<()> {
        if rows.scope != self.id {
            return Err(Error::Scope);
        }
        if rows.lowered != self.graph.lowered {
            return Err(Error::Replacement);
        }
        Ok(())
    }

    pub(super) fn require_query(&self, query: QueryId) -> Result<()> {
        let mut owner = Some(self.id);
        while let Some(scope) = owner {
            if query == scope {
                return Err(Error::Ownership);
            }
            owner = self.graph.get(scope)?.parent;
        }
        let query = self.graph.get(query)?;
        if query.attached
            || query.parent.is_some_and(|parent| parent != self.id)
            || query.rows.is_none()
        {
            return Err(Error::Ownership);
        }
        Ok(())
    }
}
