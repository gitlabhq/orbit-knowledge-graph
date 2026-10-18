use super::*;

#[derive(Debug, Clone, PartialEq)]
pub enum BooleanExpression<T> {
    Leaf(T),
    And(Vec<Self>),
    Not(Box<Self>),
}

impl<T> BooleanExpression<T> {
    pub fn leaves(&self) -> impl Iterator<Item = &T> {
        self.polarized_leaves().into_iter().map(|(leaf, _)| leaf)
    }

    pub fn negated_leaves(&self) -> impl Iterator<Item = &T> {
        self.polarized_leaves()
            .into_iter()
            .filter_map(|(leaf, negated)| negated.then_some(leaf))
    }

    fn polarized_leaves(&self) -> Vec<(&T, bool)> {
        self.fold(
            &|leaf| vec![(leaf, false)],
            &|children| children.concat(),
            &|child| child.into_iter().map(|(leaf, _)| (leaf, true)).collect(),
        )
    }

    pub fn try_map<U, E>(
        self,
        map: &mut impl FnMut(T) -> Result<U, E>,
    ) -> Result<BooleanExpression<U>, E> {
        Ok(match self {
            Self::Leaf(leaf) => BooleanExpression::Leaf(map(leaf)?),
            Self::And(children) => BooleanExpression::And(
                children
                    .into_iter()
                    .map(|child| child.try_map(map))
                    .collect::<Result<_, _>>()?,
            ),
            Self::Not(child) => BooleanExpression::Not(Box::new(child.try_map(map)?)),
        })
    }

    pub fn visit_mut(&mut self, visit: &mut impl FnMut(&mut T)) {
        match self {
            Self::Leaf(leaf) => visit(leaf),
            Self::And(children) => children.iter_mut().for_each(|child| child.visit_mut(visit)),
            Self::Not(child) => child.visit_mut(visit),
        }
    }

    pub fn fold<'s, R>(
        &'s self,
        leaf: &impl Fn(&'s T) -> R,
        and: &impl Fn(Vec<R>) -> R,
        not: &impl Fn(R) -> R,
    ) -> R {
        match self {
            Self::Leaf(value) => leaf(value),
            Self::And(children) => and(children
                .iter()
                .map(|child| child.fold(leaf, and, not))
                .collect()),
            Self::Not(child) => not(child.fold(leaf, and, not)),
        }
    }

    pub fn depth(&self) -> usize {
        self.fold(
            &|_| 0,
            &|children| children.into_iter().max().unwrap_or(0) + 1,
            &|depth| depth + 1,
        )
    }

    pub fn conjuncts(self) -> Vec<Self> {
        match self {
            Self::And(children) => children,
            expression => vec![expression],
        }
    }
}

impl BooleanExpression<Condition> {
    pub fn normalize(self) -> Self {
        self.with_polarity(false)
    }

    fn with_polarity(self, negated: bool) -> Self {
        match self {
            Self::Leaf(mut leaf) if negated => {
                match leaf.filter.op.unwrap_or(FilterOp::Eq).complement() {
                    Some(op) => {
                        leaf.filter.op = Some(op);
                        Self::Leaf(leaf)
                    }
                    None => Self::Not(Box::new(Self::Leaf(leaf))),
                }
            }
            Self::Leaf(leaf) => Self::Leaf(leaf),
            Self::Not(child) => child.with_polarity(!negated),
            Self::And(mut children) if children.len() == 1 => {
                children.pop().expect("one child").with_polarity(negated)
            }
            Self::And(children) => {
                let conjunction = Self::And(
                    children
                        .into_iter()
                        .flat_map(|child| child.with_polarity(false).conjuncts())
                        .collect(),
                );
                if negated {
                    Self::Not(Box::new(conjunction))
                } else {
                    conjunction
                }
            }
        }
    }
}

impl FilterOp {
    pub fn is_like(self) -> bool {
        matches!(self, Self::Contains | Self::StartsWith | Self::EndsWith)
    }

    pub fn is_token(self) -> bool {
        matches!(self, Self::TokenMatch | Self::AllTokens | Self::AnyTokens)
    }

    pub fn is_ordering(self) -> bool {
        matches!(self, Self::Gt | Self::Gte | Self::Lt | Self::Lte)
    }

    pub fn complement(self) -> Option<Self> {
        match self {
            Self::Eq => Some(Self::Ne),
            Self::Ne => Some(Self::Eq),
            Self::IsNull => Some(Self::IsNotNull),
            Self::IsNotNull => Some(Self::IsNull),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PredicateTarget {
    Node(String),
    Relationship(usize),
}

#[derive(Debug, Clone, PartialEq)]
pub struct Condition {
    pub target: PredicateTarget,
    pub property: String,
    pub filter: InputFilter,
}

impl Condition {
    pub const RELATIONSHIP_TYPE: &str = "type()";

    pub fn is_relationship_type(&self) -> bool {
        matches!(self.target, PredicateTarget::Relationship(_))
            && self.property == Self::RELATIONSHIP_TYPE
    }

    pub fn relationship_type_names(&self) -> Option<Vec<&str>> {
        match (self.filter.op, self.filter.value.as_ref()?) {
            (Some(FilterOp::Eq | FilterOp::Ne), Value::String(name)) => Some(vec![name]),
            (Some(FilterOp::In), Value::Array(names)) if !names.is_empty() => {
                names.iter().map(Value::as_str).collect()
            }
            _ => None,
        }
    }

    pub fn compares_aliases(&self) -> bool {
        match (&self.target, &self.filter.rhs_column) {
            (PredicateTarget::Node(alias), Some((rhs, _))) => alias != rhs,
            (PredicateTarget::Relationship(_), rhs) => rhs.is_some(),
            (PredicateTarget::Node(_), None) => false,
        }
    }

    pub fn node_properties(&self) -> impl Iterator<Item = (&str, &str)> {
        let lhs = match &self.target {
            PredicateTarget::Node(alias) => Some((alias.as_str(), self.property.as_str())),
            PredicateTarget::Relationship(_) => None,
        };
        lhs.into_iter().chain(
            self.filter
                .rhs_column
                .as_ref()
                .map(|(alias, property)| (alias.as_str(), property.as_str())),
        )
    }

    pub fn references(&self, target: &PredicateTarget) -> bool {
        !self.is_relationship_type() && self.target == *target
            || matches!(target, PredicateTarget::Node(alias)
                if self.node_properties().any(|(node, _)| node == alias))
    }
}

impl BooleanExpression<Condition> {
    pub fn relationship_type(&self) -> Option<(&Condition, bool)> {
        let (expression, negated) = match self {
            Self::Not(child) => (child.as_ref(), true),
            expression => (expression, false),
        };
        match expression {
            Self::Leaf(condition) if condition.is_relationship_type() => Some((
                condition,
                negated != (condition.filter.op == Some(FilterOp::Ne)),
            )),
            _ => None,
        }
    }

    pub fn local_node(&self) -> Option<&str> {
        let mut aliases = self.leaves().map(|condition| match &condition.target {
            PredicateTarget::Node(alias) if !condition.compares_aliases() => Some(alias.as_str()),
            _ => None,
        });
        let first = aliases.next()??;
        aliases.all(|alias| alias == Some(first)).then_some(first)
    }
}

impl Input {
    pub fn conditions(&self) -> impl Iterator<Item = &Condition> {
        self.predicates.iter().flat_map(BooleanExpression::leaves)
    }

    pub fn references(&self, target: &PredicateTarget) -> bool {
        self.conditions()
            .any(|condition| condition.references(target))
    }

    pub fn node(&self, alias: &str) -> Option<&InputNode> {
        self.nodes.iter().find(|node| node.id == alias)
    }

    pub fn node_mut(&mut self, alias: &str) -> Option<&mut InputNode> {
        self.nodes.iter_mut().find(|node| node.id == alias)
    }

    pub fn entity_of(&self, alias: &str) -> Option<&str> {
        self.node(alias)?.entity.as_deref()
    }

    pub fn has_negation(&self) -> bool {
        self.predicates
            .iter()
            .any(|root| matches!(root, BooleanExpression::Not(_)))
    }

    pub fn absorb(&mut self, expression: BooleanExpression<Condition>) -> crate::error::Result<()> {
        if expression.negated_leaves().any(|condition| {
            std::iter::once(condition.property.as_str())
                .chain(condition.node_properties().map(|(_, property)| property))
                .any(|property| property == TRAVERSAL_PATH_COLUMN)
        }) {
            return Err(crate::error::QueryError::Validation(
                "traversal_path cannot be used under NOT; Orbit scopes every query to your authorized paths".into(),
            ));
        }
        for conjunct in expression.normalize().conjuncts() {
            match conjunct {
                BooleanExpression::Leaf(condition)
                    if !condition.compares_aliases() && !condition.is_relationship_type() =>
                {
                    let filters = match &condition.target {
                        PredicateTarget::Node(alias) => {
                            self.node_mut(alias).map(|node| &mut node.filters)
                        }
                        PredicateTarget::Relationship(index) => self
                            .relationships
                            .get_mut(*index)
                            .map(|relationship| &mut relationship.filters),
                    }
                    .ok_or_else(|| {
                        crate::error::QueryError::PipelineInvariant(format!(
                            "condition target {:?} is not declared",
                            condition.target
                        ))
                    })?;
                    filters
                        .entry(condition.property)
                        .or_default()
                        .push(condition.filter);
                }
                root => self.predicates.push(root),
            }
        }
        Ok(())
    }

    pub(crate) fn lift_negated_filters(&mut self) -> crate::error::Result<()> {
        for index in 0..self.nodes.len() {
            let alias = self.nodes[index].id.clone();
            for (property, filters) in std::mem::take(&mut self.nodes[index].negated_filters) {
                let leaves = filters
                    .into_iter()
                    .map(|filter| {
                        BooleanExpression::Leaf(Condition {
                            target: PredicateTarget::Node(alias.clone()),
                            property: property.clone(),
                            filter,
                        })
                    })
                    .collect();
                self.absorb(BooleanExpression::Not(Box::new(BooleanExpression::And(
                    leaves,
                ))))?;
            }
        }
        Ok(())
    }

    pub(crate) fn promote_id_filters(&mut self) -> crate::error::Result<()> {
        for node in &mut self.nodes {
            let Some(filters) = node.filters.get_mut("id") else {
                continue;
            };
            if node.node_ids.is_empty()
                && let Some((index, ids)) = filters
                    .iter()
                    .enumerate()
                    .find_map(|(index, filter)| Some((index, pinned_ids(filter)?)))
            {
                node.node_ids = ids?;
                filters.remove(index);
            }
            if node.id_range.is_none()
                && let [left, right] = filters.as_slice()
            {
                let lower = [left, right]
                    .into_iter()
                    .find(|f| matches!(f.op, Some(FilterOp::Gte | FilterOp::Gt)));
                let upper = [left, right]
                    .into_iter()
                    .find(|f| matches!(f.op, Some(FilterOp::Lte | FilterOp::Lt)));
                if let (Some(lower), Some(upper)) = (lower, upper)
                    && let (Some(start), Some(end)) = (
                        lower.value.as_ref().and_then(serde_json::Value::as_i64),
                        upper.value.as_ref().and_then(serde_json::Value::as_i64),
                    )
                {
                    let start = start.checked_add(i64::from(lower.op == Some(FilterOp::Gt)));
                    let end = end.checked_sub(i64::from(upper.op == Some(FilterOp::Lt)));
                    if let (Some(start), Some(end)) = (start, end) {
                        node.id_range = Some(InputIdRange { start, end });
                        filters.clear();
                    }
                }
            }
            if filters.is_empty() {
                node.filters.remove("id");
            }
        }
        Ok(())
    }

    pub fn remove_relationship(&mut self, index: usize) -> InputRelationship {
        for root in &mut self.predicates {
            root.visit_mut(&mut |condition| {
                if let PredicateTarget::Relationship(position) = &mut condition.target
                    && *position > index
                {
                    *position -= 1;
                }
            });
        }
        self.relationships.remove(index)
    }
}

fn pinned_ids(filter: &InputFilter) -> Option<crate::error::Result<Vec<i64>>> {
    match (&filter.op, filter.value.as_ref()?) {
        (Some(FilterOp::In), serde_json::Value::Array(values)) if !values.is_empty() => Some(
            values
                .iter()
                .map(|value| {
                    node_id(value).ok_or_else(|| {
                        crate::error::QueryError::Validation(
                            "node IDs must be signed 64-bit integers".into(),
                        )
                    })
                })
                .collect(),
        ),
        (None | Some(FilterOp::Eq), value) => node_id(value).map(|id| Ok(vec![id])),
        _ => None,
    }
}

fn node_id(value: &serde_json::Value) -> Option<i64> {
    value
        .as_i64()
        .or_else(|| value.as_str().and_then(|text| text.parse().ok()))
}
