mod conditions;
mod returns;

use super::ast::{
    BooleanExpression, Direction, MapEntry, NodePattern, Pattern, PatternElement,
    Predicate as Leaf, Query, Range, Relationship,
};
use super::errors::invalid;
use crate::RelationshipId;
use crate::plan::logical::{
    Hops, LogicalBuilder, LogicalCatalog, PlanError, Predicate, Rows, Step,
};
use crate::plan::tracking::Declared;
use crate::surface::{Statement, SurfaceError};

type Built<'m> = Result<Rows<'m>, SurfaceError>;
type Conditions<'i> = Vec<BooleanExpression<Leaf<'i>>>;

pub(super) fn lower(
    query: Query<'_>,
    catalog: LogicalCatalog<'_>,
) -> Result<Statement, SurfaceError> {
    let Query {
        pattern,
        predicates,
        projections,
        order,
        skip,
        limit,
        debug,
    } = query;
    let (graph, path) = Graph::collect(pattern)?;
    let plan = catalog.build(|q| -> Built<'_> {
        let rows = match &path {
            Some(name) => graph.shortest(q, name)?,
            None => graph.walk(q)?,
        };
        let rows = conditions::filter(q, rows, predicates)?;
        let nodes = graph.nodes.iter().map(|node| rows.binding(&node.alias));
        q.declare_order(nodes.collect::<Result<Vec<_>, _>>()?)?;
        let dynamic = path.or_else(|| graph.unlabeled().map(str::to_string));
        let nodes: Vec<&str> = graph.nodes.iter().map(|node| node.alias.as_str()).collect();
        let rows = returns::project(q, rows, &nodes, dynamic.as_deref(), projections, order)?;
        Ok(q.slice(rows, limit.map(u64::from), skip.map(u64::from))?)
    })?;
    Ok(Statement { plan, debug })
}

/// The pattern as written: nodes once each, and relationships between node positions.
struct Graph<'i> {
    nodes: Vec<Node<'i>>,
    edges: Vec<Edge<'i>>,
}

struct Node<'i> {
    alias: String,
    label: Option<String>,
    properties: Vec<MapEntry<'i>>,
}

struct Edge<'i> {
    variable: Option<String>,
    types: Vec<String>,
    from: usize,
    to: usize,
    direction: Direction,
    hops: Hops,
    properties: Vec<MapEntry<'i>>,
}

/// A relationship by position, with its near node, far node, and direction from the near node.
type Walked = (usize, usize, usize, Direction);

const DISCONNECTED: &str = "each relationship must connect one new node to the preceding pattern; disconnected hops and cycles are unsupported";

impl<'i> Graph<'i> {
    fn collect(pattern: Pattern<'i>) -> Result<(Self, Option<String>), SurfaceError> {
        let (elements, path) = match pattern {
            Pattern::Elements(elements) => (elements, None),
            Pattern::Shortest { variable, element } => (vec![*element], Some(variable.value)),
        };
        let mut graph = Self {
            nodes: Vec::new(),
            edges: Vec::new(),
        };
        for PatternElement { head, chain } in elements {
            let mut from = graph.declare(head)?;
            for (relationship, node) in chain {
                let to = graph.declare(node)?;
                graph.link(relationship, from, to)?;
                from = to;
            }
        }
        if path.is_none() {
            graph.steps()?;
        }
        Ok((graph, path))
    }

    /// The first labeled node, then each relationship from the end the pattern already reached.
    fn steps(&self) -> Result<(usize, Vec<Walked>), SurfaceError> {
        let anchor = self
            .nodes
            .iter()
            .position(|node| node.label.is_some())
            .unwrap_or_default();
        let mut bound = vec![anchor];
        let mut steps = Vec::new();
        for (index, edge) in self.edges.iter().enumerate() {
            let step = match (bound.contains(&edge.from), bound.contains(&edge.to)) {
                (true, false) => (index, edge.from, edge.to, edge.direction),
                (false, true) => (index, edge.to, edge.from, reverse(edge.direction)),
                _ => return Err(SurfaceError::schema(DISCONNECTED)),
            };
            bound.push(step.2);
            steps.push(step);
        }
        if bound.len() != self.nodes.len() {
            return Err(SurfaceError::schema(
                "all declared nodes must belong to one connected pattern",
            ));
        }
        Ok((anchor, steps))
    }

    fn unlabeled(&self) -> Option<&str> {
        self.nodes
            .iter()
            .find(|node| node.label.is_none())
            .map(|node| node.alias.as_str())
    }

    fn declare(&mut self, pattern: NodePattern<'i>) -> Result<usize, SurfaceError> {
        let alias = pattern.variable.value;
        let Some(index) = self.nodes.iter().position(|node| node.alias == alias) else {
            self.nodes.push(Node {
                alias,
                label: pattern.label.map(|label| label.value),
                properties: pattern.properties,
            });
            return Ok(self.nodes.len() - 1);
        };
        if pattern
            .label
            .is_some_and(|label| self.nodes[index].label.as_deref() != Some(&label.value))
        {
            return Err(invalid(
                pattern.span,
                "a repeated node must keep its original label",
            ));
        }
        if !pattern.properties.is_empty() {
            return Err(invalid(
                pattern.span,
                "declare a node's properties once or use WHERE predicates",
            ));
        }
        Ok(index)
    }

    fn link(
        &mut self,
        relationship: Relationship<'i>,
        from: usize,
        to: usize,
    ) -> Result<(), SurfaceError> {
        if relationship.types.is_empty()
            && let Some(alias) = &relationship.variable
            && type_shaped(&alias.value)
        {
            return Err(invalid(
                alias.span,
                &format!(
                    "[{0}] declares a variable, not a relationship type; write [:{0}] for the type, or use a lowercase variable",
                    alias.value
                ),
            ));
        }
        let hops = match relationship.range {
            Some(range) => hops(range)?,
            None => Hops::ONE,
        };
        self.edges.push(Edge {
            variable: relationship.variable.map(|alias| alias.value),
            types: relationship
                .types
                .into_iter()
                .map(|name| name.value)
                .collect(),
            from,
            to,
            direction: relationship.direction,
            hops,
            properties: relationship.properties,
        });
        Ok(())
    }

    fn walk<'m>(&self, q: &mut LogicalBuilder<'m>) -> Built<'m> {
        let (anchor, steps) = self.steps()?;
        let mut rows = bind(q, &self.nodes[anchor])?;
        for (index, near, far, direction) in steps {
            let (edge, target) = (&self.edges[index], &self.nodes[far]);
            let from = rows.binding(&self.nodes[near].alias)?;
            let step = edge.step(q, direction)?.named(&target.alias);
            rows = match &target.label {
                Some(label) => q.traverse(rows, from, step.to(q.catalog().entity_named(label)?))?,
                None => q.neighbors(rows, from, step)?,
            };
            rows = edge.inline(q, rows)?;
            rows = inline(q, rows, target)?;
        }
        Ok(rows)
    }

    fn shortest<'m>(&self, q: &mut LogicalBuilder<'m>, name: &str) -> Built<'m> {
        let [edge] = self.edges.as_slice() else {
            return Err(SurfaceError::schema(
                "a shortest path requires one bounded relationship between two labeled nodes",
            ));
        };
        let start = bind(q, &self.nodes[edge.from])?;
        let end = bind(q, &self.nodes[edge.to])?;
        let step = edge.step(q, edge.direction)?;
        let rows = q.shortest(start, end, step, name)?;
        edge.inline(q, rows)
    }
}

impl Edge<'_> {
    fn step(&self, q: &LogicalBuilder<'_>, direction: Direction) -> Result<Step, SurfaceError> {
        let step = Step::new(direction)
            .via(relationships(q, &self.types)?)
            .hops(self.hops);
        Ok(match &self.variable {
            Some(variable) => step.edge(variable),
            None => step,
        })
    }

    fn inline<'m>(&self, q: &mut LogicalBuilder<'m>, rows: Rows<'m>) -> Built<'m> {
        let link = rows.edge().ok_or(PlanError::NoEntity)?;
        let predicates = conditions::entries(&self.properties, q.catalog().model(), |name| {
            Ok(rows.edge_column(link, name)?)
        })?;
        conjoin(q, rows, predicates)
    }
}

fn bind<'m>(q: &mut LogicalBuilder<'m>, node: &Node<'_>) -> Built<'m> {
    let label = node.label.as_deref().ok_or_else(|| {
        SurfaceError::schema(format!(
            "({0}) needs a label, such as ({0}:Entity); only the far endpoint of a neighbors query may omit it",
            node.alias
        ))
    })?;
    let rows = q.entities_as(q.catalog().entity_named(label)?, &node.alias)?;
    inline(q, rows, node)
}

fn inline<'m>(q: &mut LogicalBuilder<'m>, rows: Rows<'m>, node: &Node<'_>) -> Built<'m> {
    let binding = rows.binding(&node.alias)?;
    let predicates = conditions::entries(&node.properties, q.catalog().model(), |name| {
        Ok(binding.property_named(name)?)
    })?;
    conjoin(q, rows, predicates)
}

fn conjoin<'m>(
    q: &mut LogicalBuilder<'m>,
    rows: Rows<'m>,
    mut predicates: Vec<Predicate>,
) -> Built<'m> {
    Ok(match predicates.len() {
        0 => rows,
        1 => q.filter(rows, predicates.remove(0))?,
        _ => q.filter(rows, Predicate::And(predicates))?,
    })
}

fn relationships(
    q: &LogicalBuilder<'_>,
    types: &[String],
) -> Result<Vec<Declared<RelationshipId>>, SurfaceError> {
    Ok(types
        .iter()
        .map(|name| q.catalog().relationship_named(name))
        .collect::<Result<_, _>>()?)
}

fn reverse(direction: Direction) -> Direction {
    match direction {
        Direction::Outgoing => Direction::Incoming,
        Direction::Incoming => Direction::Outgoing,
        Direction::Both => Direction::Both,
    }
}

fn hops(range: Range<'_>) -> Result<Hops, SurfaceError> {
    let max = if range.dots { range.end } else { range.start }.ok_or_else(|| {
        invalid(
            range.span,
            "unbounded paths are unsupported; use *1..3 or an exact *2",
        )
    })?;
    Ok(Hops {
        min: range.start.unwrap_or(1),
        max,
    })
}

fn type_shaped(name: &str) -> bool {
    name.len() > 1
        && name.starts_with(|c: char| c.is_ascii_uppercase())
        && name
            .chars()
            .all(|c| c.is_ascii_uppercase() || c.is_ascii_digit() || c == '_')
}
