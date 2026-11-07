use std::collections::HashMap;

use query_data_model::Catalog;
use query_data_model::implementations::relational::plan::{
    AggregateFunction, BinaryOp, Expr, JoinKind, Measure, Operation, ReadMode, RelationalPlan,
    Selection, Sharing, TextMatch, UnaryOp,
};
use query_data_model::implementations::relational::{CurrentRows, RelationalBackend};
use query_data_model::plan::tracking::{BodyId, NodeId, Ref};

use super::explain::{filter, leaf, literal, membership_keys, scan, sort};
use super::operator::Operator;
use super::pattern::Expression as Tree;

pub fn tree<B: RelationalBackend>(plan: &RelationalPlan<B>, catalog: &Catalog<B>) -> Tree {
    let mut owners = HashMap::new();
    for node in plan.plan.nodes() {
        for (position, output) in plan.plan.outputs(node).enumerate() {
            owners.entry(output).or_insert((node, position));
        }
    }
    let mut readers = vec![0; plan.bodies.len()];
    for node in plan.plan.nodes() {
        if let Operation::Read { body, .. } = plan.plan.operation(node) {
            readers[body.index()] += 1;
        }
    }
    let shares = plan
        .plan
        .bodies()
        .filter(|body| plan.body(*body).sharing == Sharing::Pending)
        .enumerate()
        .map(|(position, body)| (body, format!("shared_{position}")))
        .collect();
    let explain = Emitted {
        plan,
        catalog,
        owners,
        current_is_final: B::CAPABILITIES.current_rows == CurrentRows::Final,
        readers,
        shares,
    };
    let mut with: Vec<BodyId> = plan.with.clone();
    with.extend(plan.plan.bodies().filter(|body| explain.reused(*body)));
    with.sort();
    let tree = explain.statement(&with, plan.plan.root());
    let inline = explain.inline_memberships();
    match (inline.is_empty(), tree.label) {
        (true, _) => tree,
        (false, Operator::With) => {
            let mut children = tree.children;
            let main = children.pop();
            let children = children.into_iter().chain(inline).chain(main);
            Tree::node(Operator::With, "", children.collect())
        }
        (false, _) => Tree::node(
            Operator::With,
            "",
            inline.into_iter().chain([tree]).collect(),
        ),
    }
}

struct Emitted<'p, B: RelationalBackend> {
    plan: &'p RelationalPlan<B>,
    catalog: &'p Catalog<B>,
    owners: HashMap<Ref, (NodeId, usize)>,
    current_is_final: bool,
    readers: Vec<usize>,
    shares: HashMap<BodyId, String>,
}

impl<B: RelationalBackend> Emitted<'_, B> {
    fn operation(&self, node: NodeId) -> &Operation {
        self.plan.plan.operation(node)
    }

    /// A planned share that several reads use, shown as the CTE materialization makes of it.
    fn reused(&self, body: BodyId) -> bool {
        self.shares.contains_key(&body) && self.readers[body.index()] > 1
    }

    fn name(&self, body: BodyId) -> &str {
        self.shares
            .get(&body)
            .map_or_else(|| self.plan.plan.body(body).0, String::as_str)
    }

    fn statement(&self, with: &[BodyId], root: NodeId) -> Tree {
        let tree = self.query(root);
        if with.is_empty() {
            return tree;
        }
        let shared = with.iter().map(|body| {
            let (_, root) = self.plan.plan.body(*body);
            let inner = self.statement(&self.plan.body(*body).with, root);
            Tree::node(Operator::Cte, self.name(*body), vec![inner])
        });
        Tree::node(Operator::With, "", shared.chain([tree]).collect())
    }

    fn body(&self, body: BodyId) -> Tree {
        let (_, root) = self.plan.plan.body(body);
        self.statement(&self.plan.body(body).with, root)
    }

    fn query(&self, top: NodeId) -> Tree {
        let block = self.plan.block(top);
        let mut tree = match block.select {
            Some(select) => {
                let mut tree = self.relation(block.base);
                for node in &block.wheres {
                    tree = self.filtered(node, tree);
                }
                let projection = self.projection(select, block.compute);
                tree = match block.aggregate.map(|node| self.operation(node)) {
                    Some(Operation::Aggregate { groups, .. }) => {
                        let items = groups
                            .iter()
                            .map(|group| format!("group {}", self.expression(group)))
                            .chain([projection])
                            .collect::<Vec<_>>();
                        Tree::node(Operator::Aggregate, items.join(", "), vec![tree])
                    }
                    _ => Tree::node(Operator::Project, projection, vec![tree]),
                };
                for node in &block.havings {
                    tree = self.filtered(node, tree);
                }
                tree
            }
            None => {
                let arms = self.plan.plan.inputs(block.base).iter();
                let arms = arms.map(|arm| self.query(*arm)).collect();
                Tree::node(Operator::Union, "ALL", arms)
            }
        };
        if let Some(Operation::Order(orders)) = block.order.map(|node| self.operation(node)) {
            let keys = orders
                .iter()
                .map(|order| (self.expression(&order.value), order.descending));
            tree = sort(tree, keys);
        }
        match block.limit_by.map(|node| self.operation(node)) {
            Some(Operation::Latest { key, version }) => {
                let keys: Vec<_> = key.iter().map(|key| self.column(*key)).collect();
                let order = keys.iter().map(|key| (key.clone(), false));
                tree = sort(tree, order.chain([(self.column(*version), true)]));
                let text = format!("LimitBy 1 BY {}", keys.join(", "));
                tree = Tree::node(Operator::Deduplicate, text, vec![tree]);
            }
            Some(Operation::LimitBy { count, by }) => {
                let by: Vec<_> = by.iter().map(|value| self.expression(value)).collect();
                let text = format!("LimitBy {count} BY {}", by.join(", "));
                tree = Tree::node(Operator::Deduplicate, text, vec![tree]);
            }
            _ => {}
        }
        if let Some(Operation::Limit { count, offset }) =
            block.limit.map(|node| self.operation(node))
        {
            if let Some(offset) = offset {
                tree = Tree::node(Operator::Offset, offset.to_string(), vec![tree]);
            }
            if let Some(count) = count {
                tree = Tree::node(Operator::Limit, count.to_string(), vec![tree]);
            }
        }
        tree
    }

    fn relation(&self, node: NodeId) -> Tree {
        match self.operation(node) {
            Operation::Scan { table, read, alias } => scan(
                self.catalog.table_name(*table),
                alias,
                *read == ReadMode::Current && self.current_is_final,
            ),
            Operation::Read { body, alias } => {
                let (_, root) = self.plan.plan.body(*body);
                let name = self.name(*body);
                let range = alias.as_deref().unwrap_or(name);
                match (self.plan.body(*body).sharing, self.operation(root)) {
                    (Sharing::Shared { .. }, _) => scan(name, range, false),
                    (Sharing::Pending, _) if self.reused(*body) => {
                        leaf(Operator::Scan, format!("Share({name}) AS {range}"))
                    }
                    (Sharing::Pending, Operation::Select(Selection::All))
                        if self.filters_in_place(root, alias.as_deref()) =>
                    {
                        let mut tree = self.query(root);
                        tree.children.pop().unwrap_or(tree)
                    }
                    (Sharing::Inline | Sharing::Pending, Operation::Union) => {
                        let arms = self.plan.plan.inputs(root).iter();
                        let arms = arms.map(|arm| self.query(*arm)).collect();
                        Tree::node(Operator::Union, format!("ALL AS {range}"), arms)
                    }
                    (Sharing::Inline | Sharing::Pending, _) => {
                        Tree::node(Operator::Bind, range, vec![self.body(*body)])
                    }
                }
            }
            Operation::Join(kind) => {
                let head = match kind {
                    JoinKind::Inner(on) => format!("ON {}", self.expression(on)),
                    JoinKind::Left(on) => format!("LEFT ON {}", self.expression(on)),
                    JoinKind::Cross => "CROSS".into(),
                };
                let inputs = self.plan.plan.inputs(node);
                let sides = vec![self.relation(inputs[0]), self.relation(inputs[1])];
                Tree::node(Operator::Join, head, sides)
            }
            _ => self.query(node),
        }
    }

    fn filtered(&self, node: &NodeId, input: Tree) -> Tree {
        match self.operation(*node) {
            Operation::Filter(predicate) => self.predicate(predicate, input),
            _ => input,
        }
    }

    fn predicate(&self, predicate: &Expr, input: Tree) -> Tree {
        match predicate {
            Expr::Binary {
                op: BinaryOp::And,
                left,
                right,
            } => self.predicate(right, self.predicate(left, input)),
            Expr::In { value, query } if self.plan.plan.body(*query).0 == "subquery" => Tree::node(
                Operator::SemiJoin,
                format!("{} IN subquery", self.expression(value)),
                vec![input, self.body(*query)],
            ),
            _ => filter(vec![self.expression(predicate)], input),
        }
    }

    /// A share that only filters a source under its reader's alias, which materialization reads in place.
    fn filters_in_place(&self, root: NodeId, alias: Option<&str>) -> bool {
        let mut base = self.plan.plan.inputs(root)[0];
        while let Operation::Filter(_) = self.operation(base) {
            base = self.plan.plan.inputs(base)[0];
        }
        let named = match self.operation(base) {
            Operation::Scan { alias, .. } => Some(alias.as_str()),
            Operation::Read { alias, .. } => alias.as_deref(),
            _ => None,
        };
        alias.is_some() && named == alias
    }

    /// The relation a membership body reads its keys from, and whether it is a CTE.
    fn membership_source(&self, body: BodyId) -> Option<(&str, BodyId, bool)> {
        let (_, root) = self.plan.plan.body(body);
        match self.operation(self.plan.block(root).base) {
            Operation::Read { body, alias } => {
                let shared = matches!(self.plan.body(*body).sharing, Sharing::Shared { .. })
                    || self.reused(*body);
                let name = alias.as_deref().unwrap_or(self.name(*body));
                Some((name, *body, shared))
            }
            _ => None,
        }
    }

    /// Membership keys read from rows that are not a CTE, each listed beside the CTEs under its name.
    fn inline_memberships(&self) -> Vec<Tree> {
        let mut seen: Vec<(&str, BodyId)> = Vec::new();
        for node in self.plan.plan.nodes() {
            for body in self.operation(node).memberships() {
                if let Some((name, source, false)) = self.membership_source(body)
                    && !seen.iter().any(|(_, known)| *known == source)
                {
                    seen.push((name, source));
                }
            }
        }
        seen.into_iter()
            .map(|(name, source)| Tree::node(Operator::Bind, name, vec![self.body(source)]))
            .collect()
    }

    fn projection(&self, select: NodeId, compute: Option<NodeId>) -> String {
        let Operation::Select(Selection::Columns(columns)) = self.operation(select) else {
            return "*".into();
        };
        columns
            .iter()
            .map(|column| match self.computed(*column, compute) {
                Some(value) => match self.plan.plan.name(*column) {
                    Some(name) => format!("{} AS {name}", self.expression(value)),
                    None => self.expression(value),
                },
                None => self.column(*column),
            })
            .collect::<Vec<_>>()
            .join(", ")
    }

    fn computed(&self, column: Ref, compute: Option<NodeId>) -> Option<&Expr> {
        let compute = compute?;
        let (owner, position) = self.owners.get(&column)?;
        let Operation::Compute(values) = self.operation(compute) else {
            return None;
        };
        let width = self.plan.plan.outputs(compute).count();
        (*owner == compute).then(|| &values[position + values.len() - width])
    }
}

impl<B: RelationalBackend> Emitted<'_, B> {
    fn column(&self, column: Ref) -> String {
        let name = self.plan.plan.name(column).unwrap_or_default();
        let Some((owner, position)) = self.owners.get(&column) else {
            return name.to_string();
        };
        match self.operation(*owner) {
            Operation::Scan { alias, .. } => format!("{alias}.{name}"),
            Operation::Read { body, alias } => {
                let range = alias.as_deref().unwrap_or(self.plan.plan.body(*body).0);
                format!("{range}.{name}")
            }
            Operation::Aggregate { groups, measures } => match groups.get(*position) {
                Some(group) => self.expression(group),
                None => self.measure(&measures[position - groups.len()]),
            },
            _ => name.to_string(),
        }
    }

    fn measure(&self, measure: &Measure) -> String {
        let args: Vec<_> = measure
            .args
            .iter()
            .map(|arg| self.expression(arg))
            .collect();
        let function = match measure.function {
            AggregateFunction::ArgMax | AggregateFunction::ArgMaxOrNull => {
                return format!("{:?}({})", measure.function, args.join(", "));
            }
            function => format!("{function:?}").to_uppercase(),
        };
        let distinct = if measure.distinct { "DISTINCT " } else { "" };
        let mut value = format!("{function}({distinct}{})", args.join(", "));
        if let Some(condition) = &measure.condition {
            value.push_str(&format!(" FILTER [{}]", self.expression(condition)));
        }
        value
    }

    fn expression(&self, value: &Expr) -> String {
        match value {
            Expr::Column(column) => self.column(*column),
            Expr::Variable(name) => name.clone(),
            Expr::Parameter { value, .. } => literal(value),
            Expr::List(items) => format!("[{}]", self.list(items)),
            Expr::Null => "null".into(),
            Expr::Bool(value) => value.to_string(),
            Expr::Call { function, args } => format!("{function:?}({})", self.list(args)),
            Expr::EmptyArray(fields) => format!("empty_tuple_array({fields:?})"),
            Expr::TimeBucket { unit, value } => {
                format!("bucket({}, {})", unit.name(), self.expression(value))
            }
            Expr::TextSearch { mode, value, query } => {
                let mode = match mode {
                    TextMatch::Contains => "Contains",
                    TextMatch::Token => "TokenMatch",
                    TextMatch::AllTokens => "AllTokens",
                    TextMatch::AnyTokens => "AnyTokens",
                };
                let (value, query) = (self.expression(value), self.expression(query));
                format!("{mode}({value}, {query})")
            }
            Expr::Lambda { param, body } => format!("{param} -> {}", self.expression(body)),
            Expr::Binary { op, left, right } => {
                let operand = |value: &Expr| {
                    let text = self.expression(value);
                    let grouped = matches!(value, Expr::Binary { op: child, .. }
                        if child != op || !matches!(op, BinaryOp::And | BinaryOp::Or));
                    if grouped { format!("({text})") } else { text }
                };
                format!("{} {} {}", operand(left), binary(*op), operand(right))
            }
            Expr::Unary { op, value } => match op {
                UnaryOp::IsNull => format!("{} IS NULL", self.expression(value)),
                UnaryOp::IsNotNull => format!("{} IS NOT NULL", self.expression(value)),
                UnaryOp::Not => format!("NOT({})", self.expression(value)),
            },
            Expr::In { value, query } => {
                let (name, root) = self.plan.plan.body(*query);
                let value = self.expression(value);
                if name == "subquery" {
                    return format!("{value} IN subquery");
                }
                let name = self
                    .membership_source(*query)
                    .map_or(name, |(source, ..)| source);
                let keys: Vec<_> = self
                    .plan
                    .plan
                    .outputs(root)
                    .map(|key| self.plan.plan.name(key).unwrap_or_default().to_string())
                    .collect();
                format!("{value} IN {name}.{}", membership_keys(&keys))
            }
            Expr::Scalar(body) => {
                let (_, root) = self.plan.plan.body(*body);
                let block = self.plan.block(root);
                let source = match self.operation(block.base) {
                    Operation::Scan { table, alias, .. } => {
                        format!("{} AS {alias}", self.catalog.table_name(*table))
                    }
                    _ => "subquery".into(),
                };
                let predicates =
                    block
                        .wheres
                        .iter()
                        .filter_map(|node| match self.operation(*node) {
                            Operation::Filter(predicate) => Some(self.expression(predicate)),
                            _ => None,
                        });
                match predicates.collect::<Vec<_>>().as_slice() {
                    [] => format!("scalar({source})"),
                    predicates => format!("scalar({source} WHERE {})", predicates.join(" AND ")),
                }
            }
        }
    }

    fn list(&self, values: &[Expr]) -> String {
        values
            .iter()
            .map(|value| self.expression(value))
            .collect::<Vec<_>>()
            .join(", ")
    }
}

fn binary(op: BinaryOp) -> &'static str {
    match op {
        BinaryOp::Eq => "=",
        BinaryOp::Ne => "!=",
        BinaryOp::Lt => "<",
        BinaryOp::Le => "<=",
        BinaryOp::Gt => ">",
        BinaryOp::Ge => ">=",
        BinaryOp::In => "IN",
        BinaryOp::And => "AND",
        BinaryOp::Or => "OR",
        BinaryOp::Add => "+",
    }
}
