use super::*;

impl<'a> Rows<'a> {
    pub fn inputs(&self) -> impl Iterator<Item = &Rows<'a>> {
        use OperationKind::*;
        let inputs = match &self.kind {
            Join { left, right, .. } => [Some(left.as_ref()), Some(right.as_ref())],
            Filter { input, .. }
            | Select { input, .. }
            | Aggregate { input, .. }
            | Expand { input, .. }
            | Sort { input, .. }
            | Limit { input, .. }
            | Latest { input, .. }
            | FirstBy { input, .. } => [Some(input.as_ref()), None],
            _ => [None, None],
        };
        inputs.into_iter().flatten()
    }

    fn map_inputs<E>(
        self,
        mut map: impl FnMut(Rows<'a>) -> std::result::Result<Rows<'a>, E>,
    ) -> std::result::Result<Self, E> {
        use OperationKind::*;
        let mut child = |input: Box<Rows<'a>>| map(*input).map(Box::new);
        let kind = match self.kind {
            Filter { input, predicate } => Filter {
                input: child(input)?,
                predicate,
            },
            Join {
                left,
                right,
                kind,
                condition,
            } => Join {
                left: child(left)?,
                right: child(right)?,
                kind,
                condition,
            },
            Select { input, values } => Select {
                input: child(input)?,
                values,
            },
            Aggregate {
                input,
                groups,
                measures,
            } => Aggregate {
                input: child(input)?,
                groups,
                measures,
            },
            Expand { input, value } => Expand {
                input: child(input)?,
                value,
            },
            Sort { input, keys } => Sort {
                input: child(input)?,
                keys,
            },
            Limit { input, count } => Limit {
                input: child(input)?,
                count,
            },
            Latest {
                input,
                keys,
                version,
            } => Latest {
                input: child(input)?,
                keys,
                version,
            },
            FirstBy { input, keys } => FirstBy {
                input: child(input)?,
                keys,
            },
            kind => kind,
        };
        Ok(Self { kind, ..self })
    }

    pub fn into_select(self) -> Result<(Rows<'a>, Vec<Named>)> {
        match self.kind {
            OperationKind::Select { input, values } => Ok((*input, values)),
            _ => Err(Error::Replacement),
        }
    }

    pub fn into_aggregate(self) -> Result<(Rows<'a>, Vec<Named>, Vec<Named>)> {
        match self.kind {
            OperationKind::Aggregate {
                input,
                groups,
                measures,
            } => Ok((*input, groups, measures)),
            _ => Err(Error::Aggregate),
        }
    }

    pub fn remove_limit(self) -> Result<Self> {
        match self.kind {
            OperationKind::Limit { input, .. } => Ok(*input),
            _ => Err(Error::Replacement),
        }
    }

    pub fn remove_sort(self) -> Result<Self> {
        match self.kind {
            OperationKind::Sort { input, .. } => Ok(*input),
            _ => Err(Error::Replacement),
        }
    }

    pub fn walk<E>(
        &self,
        visit: &mut impl FnMut(&Rows<'a>) -> std::result::Result<(), E>,
    ) -> std::result::Result<(), E> {
        for child in self.inputs() {
            child.walk(visit)?;
        }
        visit(self)
    }

    pub fn expressions(&self) -> impl Iterator<Item = &Expr> {
        let mut single = None;
        let mut outputs: &[Named] = &[];
        let mut measures: &[Named] = &[];
        let mut keys: &[Order] = &[];
        match self.kind() {
            OperationKind::Filter { predicate, .. } => single = Some(predicate),
            OperationKind::Join {
                condition, kind, ..
            } if *kind != Join::Cross => {
                single = Some(condition);
            }
            OperationKind::Select { values, .. } => outputs = values,
            OperationKind::Aggregate {
                groups,
                measures: values,
                ..
            } => {
                outputs = groups;
                measures = values;
            }
            OperationKind::Expand { value, .. } => single = Some(&value.value),
            OperationKind::Sort { keys: values, .. } => keys = values,
            _ => {}
        }
        single
            .into_iter()
            .chain(outputs.iter().chain(measures).map(|value| &value.value))
            .chain(keys.iter().map(|key| &key.value))
    }
}

impl<'a, M: QueryDataModel + ?Sized> QueryScope<'_, 'a, M> {
    pub fn map_expressions(
        &self,
        mut rows: Rows<'a>,
        mut map: impl FnMut(&Expr) -> Result<Expr>,
    ) -> Result<Rows<'a>> {
        self.require_rows(&rows)?;
        let mut replace =
            |expression: &mut Expr, columns: &[Column], measure: bool| -> Result<()> {
                let replacement = map(expression)?;
                if measure && !matches!(replacement.kind(), ExprKind::Aggregate { .. }) {
                    return Err(Error::Aggregate);
                }
                if replacement.infer(self, columns, measure)?
                    != expression.infer(self, columns, measure)?
                {
                    return Err(Error::Replacement);
                }
                *expression = replacement;
                Ok(())
            };
        match &mut rows.kind {
            OperationKind::Filter { input, predicate } => {
                replace(predicate, &input.columns, false)?;
            }
            OperationKind::Join {
                left,
                right,
                condition,
                kind,
            } if *kind != Join::Cross => {
                let mut columns = left.columns.clone();
                columns.extend_from_slice(&right.columns);
                replace(condition, &columns, false)?;
                if *kind == Join::Membership {
                    let ExprKind::Binary {
                        operator: Operator::Equal,
                        left: value,
                        right: key,
                    } = condition.kind()
                    else {
                        return Err(Error::Replacement);
                    };
                    value.infer(self, &left.columns, false)?;
                    key.infer(self, &right.columns, false)?;
                }
            }
            OperationKind::Select { input, values } => {
                for value in values {
                    replace(&mut value.value, &input.columns, false)?;
                }
            }
            OperationKind::Aggregate {
                input,
                groups,
                measures,
            } => {
                for group in groups {
                    replace(&mut group.value, &input.columns, false)?;
                }
                for measure in measures {
                    replace(&mut measure.value, &input.columns, true)?;
                }
            }
            OperationKind::Expand { input, value } => {
                replace(&mut value.value, &input.columns, false)?;
            }
            OperationKind::Sort { input, keys } => {
                for key in keys {
                    replace(&mut key.value, &input.columns, false)?;
                }
            }
            _ => {}
        }
        Ok(rows)
    }
}

impl<'a, M: QueryDataModel + ?Sized> QueryGraph<'a, M> {
    pub fn map_result<E: From<Error>>(
        mut self,
        root: QueryId,
        edit: impl FnOnce(&mut QueryScope<'_, 'a, M>, Rows<'a>) -> std::result::Result<Rows<'a>, E>,
    ) -> std::result::Result<Self, E> {
        let query = self.get(root)?;
        if query.parent.is_some() || query.attached {
            return Err(Error::Ownership.into());
        }
        let rows = self.queries[root.slot]
            .rows
            .take()
            .ok_or(Error::Ownership)?;
        let scope = &mut QueryScope {
            graph: &mut self,
            id: root,
        };
        let rows = edit(scope, rows)?;
        scope.require_rows(&rows)?;
        if rows.columns.is_empty() {
            return Err(Error::Outputs.into());
        }
        self.queries[root.slot].rows = Some(rows);
        Ok(self)
    }

    pub fn rewrite<E: From<Error>>(
        mut self,
        root: QueryId,
        mut rewrite: impl FnMut(
            &mut QueryScope<'_, 'a, M>,
            Rows<'a>,
        ) -> std::result::Result<Rows<'a>, E>,
    ) -> std::result::Result<Self, E> {
        for id in self.reachable(root)? {
            let rows = self.queries[id.slot].rows.take().ok_or(Error::Ownership)?;
            let rows = rewrite_rows(
                &mut QueryScope {
                    graph: &mut self,
                    id,
                },
                rows,
                &mut rewrite,
            )?;
            self.queries[id.slot].rows = Some(rows);
        }
        Ok(self)
    }

    pub fn lower(mut self) -> LoweredGraph<'a, M> {
        for query in &mut self.queries {
            query.rows = query.rows.take().map(lower);
        }
        self.lowered = true;
        LoweredGraph(self)
    }

    pub fn reachable(&self, root: QueryId) -> Result<Vec<QueryId>> {
        let mut pending = vec![root];
        let mut seen = HashSet::new();
        let mut queries = Vec::new();
        while let Some(id) = pending.pop() {
            if !seen.insert(id) {
                continue;
            }
            let query = self.get(id)?;
            for (_, definition) in &query.definitions {
                pending.push(definition.query);
            }
            self.rows(id)?.walk(&mut |rows| {
                match rows.kind() {
                    OperationKind::Read { query, .. } => pending.push(*query),
                    OperationKind::Union { arms } => pending.extend(arms),
                    _ => {}
                }
                for expression in rows.expressions() {
                    expression.walk(&mut |value| {
                        if let ExprKind::Scalar { query, .. } = value.kind() {
                            pending.push(*query);
                        }
                        Ok::<_, Error>(())
                    })?;
                }
                Ok::<_, Error>(())
            })?;
            queries.push(id);
        }
        Ok(queries)
    }
}

impl<'a, M: QueryDataModel + ?Sized> LoweredGraph<'a, M> {
    pub fn query(
        &mut self,
        build: impl FnOnce(&mut QueryScope<'_, 'a, M>) -> Result<Rows<'a>>,
    ) -> Result<QueryId> {
        self.0.query(build)
    }

    pub fn map_result<E: From<Error>>(
        self,
        root: QueryId,
        edit: impl FnOnce(&mut QueryScope<'_, 'a, M>, Rows<'a>) -> std::result::Result<Rows<'a>, E>,
    ) -> std::result::Result<Self, E> {
        self.0.map_result(root, edit).map(Self)
    }

    pub fn rewrite<E: From<Error>>(
        self,
        root: QueryId,
        rewrite: impl FnMut(&mut QueryScope<'_, 'a, M>, Rows<'a>) -> std::result::Result<Rows<'a>, E>,
    ) -> std::result::Result<Self, E> {
        self.0.rewrite(root, rewrite).map(Self)
    }

    pub fn graph(&self) -> &QueryGraph<'a, M> {
        &self.0
    }
}

fn rewrite_rows<'a, M: QueryDataModel + ?Sized, E: From<Error>>(
    scope: &mut QueryScope<'_, 'a, M>,
    rows: Rows<'a>,
    rewrite: &mut impl FnMut(&mut QueryScope<'_, 'a, M>, Rows<'a>) -> std::result::Result<Rows<'a>, E>,
) -> std::result::Result<Rows<'a>, E> {
    let rows = rows.map_inputs(|child| rewrite_rows(scope, child, rewrite))?;
    if let OperationKind::FirstBy { input, .. } = rows.kind()
        && !matches!(input.kind(), OperationKind::Sort { .. })
    {
        return Err(Error::Replacement.into());
    }
    let columns = rows.columns.clone();
    let sources = rows.sources.clone();
    let scalar = rows.scalar();
    let replacement = rewrite(scope, rows)?;
    scope.require_rows(&replacement)?;
    if replacement.columns != columns
        || replacement.sources != sources
        || replacement.scalar() != scalar
    {
        return Err(Error::Replacement.into());
    }
    Ok(replacement)
}

fn lower(rows: Rows<'_>) -> Rows<'_> {
    let mut rows = rows
        .map_inputs(|input| Ok::<_, std::convert::Infallible>(lower(input)))
        .unwrap();
    rows.lowered = true;
    let OperationKind::Latest {
        input,
        keys,
        version,
    } = rows.kind
    else {
        return rows;
    };
    let order = keys
        .iter()
        .map(Column::asc)
        .chain([version.desc()])
        .collect();
    let sorted = input.wrap(|input| OperationKind::Sort { input, keys: order });
    Rows {
        kind: OperationKind::FirstBy {
            input: Box::new(sorted),
            keys,
        },
        ..rows
    }
}
