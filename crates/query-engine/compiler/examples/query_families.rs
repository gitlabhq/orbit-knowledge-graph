use std::sync::Arc;

use compiler::query_graph::{
    BlockId, Expression as Expr, GraphError, OutputId, PhysicalOperation as Operation, QueryGraph,
    Relational,
};
use query_data_model::ClickHouseDataModel;

type Graph<'a> = QueryGraph<'a, ClickHouseDataModel, Expr<'a>, Operation<'a>>;
type Result<T> = std::result::Result<T, GraphError>;

fn current_projects(graph: &mut Graph<'_>) -> Result<(BlockId, OutputId)> {
    let block = graph.select(Operation::One);
    let scan = graph.scan(block, "gl_project", "project")?;
    let id = graph.stored_column(scan, "id")?;
    let version = graph.stored_column(scan, "_version")?;
    let deletion = graph.stored_column(scan, "_deleted")?;
    let output = graph.project(block, "id", Expr::Column(id))?;
    *graph.operation_mut(block)? = Operation::source(scan).latest(version, Some(deletion));
    Ok((block, output))
}

fn traversal(graph: &mut Graph<'_>) -> Result<BlockId> {
    let (projects, id) = current_projects(graph)?;
    let root = graph.select(Operation::One);
    let project = graph.derive(root, projects, "project")?;
    let edge = graph.scan(root, "gl_edge", "edge")?;
    let project_id = graph.output_column(project, id)?;
    let source = graph.stored_column(edge, "source_id")?;
    let target = graph.stored_column(edge, "target_id")?;
    *graph.operation_mut(root)? = Operation::source(project).join(
        Operation::source(edge),
        Expr::equal(Expr::Column(project_id), Expr::Column(source)),
    );
    graph.project(root, "project_id", Expr::Column(project_id))?;
    graph.project(root, "target_id", Expr::Column(target))?;
    Ok(root)
}

fn aggregation(graph: &mut Graph<'_>) -> Result<BlockId> {
    let traversal = traversal(graph)?;
    let project_id = graph.outputs(traversal)?.next().unwrap();
    let target_id = graph.outputs(traversal)?.nth(1).unwrap();
    let root = graph.select(Operation::One);
    let rows = graph.derive(root, traversal, "traversal")?;
    let group = graph.output_column(rows, project_id)?;
    let target = graph.output_column(rows, target_id)?;
    graph.project(root, "project_id", Expr::Column(group))?;
    graph.project(root, "count", Expr::Count)?;
    let condition = Expr::Greater(Box::new(Expr::Column(target)), Box::new(Expr::Integer(0)));
    graph.project(
        root,
        "positive_count",
        Expr::CountIf(Box::new(condition.clone())),
    )?;
    graph.project(
        root,
        "positive_sum",
        Expr::Sum {
            value: Box::new(Expr::Column(target)),
            condition: Some(Box::new(condition)),
        },
    )?;
    *graph.operation_mut(root)? = Operation::source(rows).aggregate(vec![group]);
    Ok(root)
}

fn fused_neighbors(graph: &mut Graph<'_>) -> Result<BlockId> {
    let body = graph.select(Operation::One);
    let edge = graph.scan(body, "gl_edge", "edge")?;
    let source = graph.stored_column(edge, "source_id")?;
    let target = graph.stored_column(edge, "target_id")?;
    let deleted = graph.stored_column(edge, "_deleted")?;
    let arms = Expr::Concat(vec![
        Expr::Keep {
            condition: Box::new(Expr::equal(Expr::Column(source), Expr::Integer(1))),
            value: Box::new(Expr::Tuple(vec![Expr::Column(target), Expr::Boolean(true)])),
        },
        Expr::Keep {
            condition: Box::new(Expr::equal(Expr::Column(target), Expr::Integer(1))),
            value: Box::new(Expr::Tuple(vec![
                Expr::Column(source),
                Expr::Boolean(false),
            ])),
        },
    ]);
    let output = graph.project(body, "arms", arms)?;
    *graph.operation_mut(body)? =
        Operation::source(edge).filter(Expr::equal(Expr::Column(deleted), Expr::Boolean(false)));
    let root = graph.select(Operation::One);
    let rows = graph.derive(root, body, "directions")?;
    let row = graph.output_column(rows, output)?;
    *graph.operation_mut(root)? = Operation::source(rows).expand(row);
    graph.project(
        root,
        "neighbor_id",
        Expr::Field {
            tuple: Box::new(Expr::Column(row)),
            index: 0,
        },
    )?;
    graph.project(
        root,
        "outgoing",
        Expr::Field {
            tuple: Box::new(Expr::Column(row)),
            index: 1,
        },
    )?;
    Ok(root)
}

fn neighbors(graph: &mut Graph<'_>) -> Result<BlockId> {
    let mut arms = Vec::new();
    for (center_name, neighbor_name, outgoing) in [
        ("source_id", "target_id", true),
        ("target_id", "source_id", false),
    ] {
        let arm = graph.select(Operation::One);
        let edge = graph.scan(arm, "gl_edge", "edge")?;
        let center = graph.stored_column(edge, center_name)?;
        let neighbor = graph.stored_column(edge, neighbor_name)?;
        let deleted = graph.stored_column(edge, "_deleted")?;
        *graph.operation_mut(arm)? = Operation::source(edge).filter(Expr::And(
            Box::new(Expr::equal(Expr::Column(center), Expr::Integer(1))),
            Box::new(Expr::equal(Expr::Column(deleted), Expr::Boolean(false))),
        ));
        graph.project(arm, "neighbor_id", Expr::Column(neighbor))?;
        graph.project(arm, "outgoing", Expr::Boolean(outgoing))?;
        arms.push(arm);
    }
    graph.union_all(arms, vec!["neighbor_id".into(), "outgoing".into()])
}

fn pathfinding(graph: &mut Graph<'_>) -> Result<BlockId> {
    let root = graph.select(Operation::One);
    let mut arms = Vec::new();
    for depth in 1..=2 {
        let arm = graph.select(Operation::One);
        let first = graph.scan(arm, "gl_edge", "first")?;
        let start = graph.stored_column(first, "source_id")?;
        let mut end = graph.stored_column(first, "target_id")?;
        *graph.operation_mut(arm)? = Operation::source(first);
        if depth == 2 {
            let second = graph.scan(arm, "gl_edge", "second")?;
            let next = graph.stored_column(second, "source_id")?;
            *graph.operation_mut(arm)? = Operation::source(first).join(
                Operation::source(second),
                Expr::equal(Expr::Column(end), Expr::Column(next)),
            );
            end = graph.stored_column(second, "target_id")?;
        }
        graph.project(arm, "start", Expr::Column(start))?;
        graph.project(arm, "end", Expr::Column(end))?;
        graph.project(arm, "depth", Expr::Integer(depth))?;
        arms.push(arm);
    }
    let frontier = graph.union_all(arms, vec!["start".into(), "end".into(), "depth".into()])?;
    let outputs = graph.outputs(frontier)?.collect::<Vec<_>>();
    let definition = graph.define(root, frontier, "frontier", false)?;
    let reference = graph.reference(root, definition, "paths")?;
    let start = graph.output_column(reference, outputs[0])?;
    *graph.operation_mut(root)? =
        Operation::source(reference).filter(Expr::equal(Expr::Column(start), Expr::Integer(1)));
    for (output, name) in outputs.into_iter().zip(["start", "end", "depth"]) {
        let column = graph.output_column(reference, output)?;
        graph.project(root, name, Expr::Column(column))?;
    }
    Ok(root)
}

fn hydration(graph: &mut Graph<'_>) -> Result<BlockId> {
    let (rows, id) = current_projects(graph)?;
    let keys = graph.select(Operation::One);
    let key = graph.project(keys, "id", Expr::Integer(1))?;
    let root = graph.select(Operation::One);
    let projects = graph.derive(root, rows, "projects")?;
    let selected = graph.derive(root, keys, "keys")?;
    let id = graph.output_column(projects, id)?;
    let key = graph.output_column(selected, key)?;
    *graph.operation_mut(root)? = Operation::source(projects).semi_join(
        Operation::source(selected),
        Expr::equal(Expr::Column(id), Expr::Column(key)),
    );
    graph.project(root, "id", Expr::Column(id))?;
    graph.project(root, "entity", Expr::Text("Project".into()))?;
    Ok(root)
}

fn authorize_and_page<'a, L>(
    graph: &mut QueryGraph<'a, ClickHouseDataModel, Expr<'a>, Relational<'a, L>>,
    body: BlockId,
) -> Result<BlockId> {
    let outputs = graph.outputs(body)?.collect::<Vec<_>>();
    let authorized = graph.select(Relational::One);
    let rows = graph.derive(authorized, body, "rows")?;
    let projects = graph.scan(authorized, "gl_project", "authorization")?;
    let identity = graph.output_column(rows, outputs[0])?;
    let project_id = graph.stored_column(projects, "id")?;
    let path = graph.stored_column(projects, "traversal_path")?;
    let condition = Expr::And(
        Box::new(Expr::equal(
            Expr::Column(identity),
            Expr::Column(project_id),
        )),
        Box::new(Expr::StartsWith(
            Box::new(Expr::Column(path)),
            Box::new(Expr::Text("1/".into())),
        )),
    );
    *graph.operation_mut(authorized)? =
        Relational::source(rows).semi_join(Relational::current(projects), condition);
    for output in outputs {
        let label = graph.output_label(output)?.to_owned();
        let column = graph.output_column(rows, output)?;
        graph.project(authorized, label, Expr::Column(column))?;
    }
    let outputs = graph.outputs(authorized)?.collect::<Vec<_>>();
    let page = graph.select(Relational::One);
    let rows = graph.derive(page, authorized, "page")?;
    let id = graph.output_column(rows, outputs[0])?;
    *graph.operation_mut(page)? = Relational::source(rows)
        .filter(Expr::Greater(
            Box::new(Expr::Column(id)),
            Box::new(Expr::Integer(0)),
        ))
        .sort(vec![(id, false)])
        .limit(10);
    for output in outputs {
        let label = graph.output_label(output)?.to_owned();
        let column = graph.output_column(rows, output)?;
        graph.project(page, label, Expr::Column(column))?;
    }
    Ok(page)
}

fn main() -> std::result::Result<(), Box<dyn std::error::Error>> {
    let catalog = ClickHouseDataModel::derive(Arc::new(ontology::Ontology::load_embedded()?))?;
    for family in [
        "traversal",
        "aggregation",
        "neighbors",
        "fused_neighbors",
        "pathfinding",
        "hydration",
    ] {
        let mut graph = Graph::new(&catalog);
        let body = match family {
            "traversal" => traversal(&mut graph)?,
            "aggregation" => aggregation(&mut graph)?,
            "neighbors" => neighbors(&mut graph)?,
            "fused_neighbors" => fused_neighbors(&mut graph)?,
            "pathfinding" => pathfinding(&mut graph)?,
            _ => hydration(&mut graph)?,
        };
        let planned = graph.explain(body)?;
        let mut graph = graph.lower_operations()?;
        let outputs = graph.outputs(body)?.collect::<Vec<_>>();
        let root = graph.select(compiler::query_graph::LoweredOperation::One);
        let rows = graph.derive(root, body, "post_lowering_page")?;
        let key = graph.output_column(rows, outputs[0])?;
        *graph.operation_mut(root)? = compiler::query_graph::LoweredOperation::source(rows)
            .filter(Expr::Greater(
                Box::new(Expr::Column(key)),
                Box::new(Expr::Integer(0)),
            ))
            .sort(vec![(key, false)])
            .limit(10);
        for output in outputs {
            let label = graph.output_label(output)?.to_owned();
            let value = graph.output_column(rows, output)?;
            graph.project(root, label, Expr::Column(value))?;
        }
        graph.fuse_filters();
        let emitted = graph.explain(root)?;
        graph.validate_lowered(root)?;
        let sql = graph.render(root)?;
        assert!(planned.contains("Operation"));
        assert!(emitted.contains("Sort"));
        assert!(sql.ends_with("LIMIT 10"));
        println!("{family}\nPLANNED\n{planned}\nEMITTED\n{emitted}\nSQL\n{sql}\n");
    }
    let mut graph = Graph::new(&catalog);
    let body = traversal(&mut graph)?;
    let mut graph = graph.lower_operations()?;
    let root = authorize_and_page(&mut graph, body)?;
    graph.validate_lowered(root)?;
    println!("post-lowering-authorization\n{}", graph.render(root)?);
    let mut incompatible = Graph::new(&catalog);
    let left = incompatible.select(Operation::One);
    let right = incompatible.select(Operation::One);
    incompatible.project(left, "id", Expr::Integer(1))?;
    incompatible.project(right, "id", Expr::Text("one".into()))?;
    let root = incompatible.union_all(vec![left, right], vec!["id".into()])?;
    assert_eq!(
        incompatible.lower_operations()?.render(root),
        Err(GraphError::UnionType)
    );

    let mut graph = Graph::new(&catalog);
    let root = aggregation(&mut graph)?;
    let output = graph.outputs(root)?.nth(2).unwrap();
    let mut graph = graph.lower_operations()?;
    let original = graph.replace_output(output, Expr::CountIf(Box::new(Expr::Count)))?;
    assert_eq!(
        graph.validate_lowered(root),
        Err(GraphError::AggregatePlacement)
    );
    graph.replace_output(output, original)?;
    graph.validate_lowered(root)?;

    let mut graph = Graph::new(&catalog);
    let block = graph.select(Operation::One);
    let scan = graph.scan(block, "gl_project", "project")?;
    let id = graph.stored_column(scan, "id")?;
    let version = graph.stored_column(scan, "_version")?;
    let deletion = graph.stored_column(scan, "_deleted")?;
    let name = graph.stored_column(scan, "name")?;
    let projected_id = graph.project(block, "id", Expr::Column(id))?;
    *graph.operation_mut(block)? = Operation::source(scan)
        .filter(Expr::equal(Expr::Column(id), Expr::Integer(1)))
        .latest(version, Some(deletion))
        .filter(Expr::equal(
            Expr::Column(name),
            Expr::Text("current name".into()),
        ))
        .sort(vec![(id, false)])
        .limit(5);
    let planned = graph.explain(block)?;
    let mut graph = graph.lower_operations()?;
    graph.fuse_filters();
    assert_eq!(graph.output_label(projected_id)?, "id");
    let sql = graph.render(block)?;
    assert!(!sql.contains("description"));
    assert!(sql.contains("stored__version"));
    assert!(sql.contains("stored_traversal_path"));
    assert!(sql.contains("LIMIT 1 BY"));
    println!(
        "predicate-placement\n{planned}\n{}\n{sql}",
        graph.explain(block)?
    );

    let mut graph = Graph::new(&catalog);
    let values = graph.select(Operation::One);
    let values_output = graph.project(values, "values", Expr::Integers(vec![1, 1, 2]))?;
    let root = graph.select(Operation::One);
    let rows = graph.derive(root, values, "values")?;
    let value = graph.output_column(rows, values_output)?;
    graph.project(root, "value", Expr::Column(value))?;
    *graph.operation_mut(root)? = Operation::source(rows).expand(value).filter(Expr::Greater(
        Box::new(Expr::Column(value)),
        Box::new(Expr::Integer(0)),
    ));
    println!(
        "multiplicity-preserving-expand\n{}",
        graph.lower_operations()?.render(root)?
    );

    let mut graph = Graph::new(&catalog);
    let values = graph.select(Operation::One);
    let output = graph.project(values, "values", Expr::Integers(vec![1, 1, 2]))?;
    let count = graph.select(Operation::One);
    let rows = graph.derive(count, values, "rows")?;
    let value = graph.output_column(rows, output)?;
    *graph.operation_mut(count)? = Operation::source(rows).expand(value).aggregate(vec![]);
    graph.project(count, "count", Expr::Count)?;
    let sql = graph.lower_operations()?.render(count)?;
    assert!(sql.contains("arrayJoin"));
    assert!(sql.contains("COUNT(*)"));

    let mut graph = Graph::new(&catalog);
    let block = graph.select(Operation::One);
    let scan = graph.scan(block, "gl_project", "project")?;
    let id = graph.stored_column(scan, "id")?;
    graph.project(block, "id", Expr::Column(id))?;
    *graph.operation_mut(block)? = Operation::source(scan)
        .filter(Expr::Greater(
            Box::new(Expr::Column(id)),
            Box::new(Expr::Integer(0)),
        ))
        .filter(Expr::equal(Expr::Column(id), Expr::Integer(1)));
    let mut graph = graph.lower_operations()?;
    let before = graph.explain(block)?;
    graph.fuse_filters();
    let after = graph.explain(block)?;
    assert_eq!(before.matches("Filter {").count(), 2);
    assert_eq!(after.matches("Filter {").count(), 1);
    assert_eq!(graph.render(block)?.matches(" WHERE ").count(), 1);
    Ok(())
}
