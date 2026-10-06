use std::sync::Arc;

use compiler::query_graph::{ColumnRef, Expression, GraphError, PhysicalOperation, QueryGraph};
use query_data_model::ClickHouseDataModel;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let catalog = ClickHouseDataModel::derive(Arc::new(ontology::Ontology::load_embedded()?))?;
    let mut graph = QueryGraph::<_, ColumnRef<'_>, Vec<ColumnRef<'_>>>::new(&catalog);

    let root = graph.select(vec![]);
    let body = graph.select(vec![]);
    let parent = graph.scan(body, "gl_project", "project")?;
    let child = graph.scan(body, "gl_project", "project")?;
    let parent_id = graph.stored_column(parent, "id")?;
    let child_id = graph.stored_column(child, "id")?;
    assert_ne!(parent_id, child_id);
    graph.operation_mut(body)?.extend([parent_id, child_id]);
    let id = graph.project(body, "id", parent_id)?;
    let duplicate = graph.project(body, "also_id", parent_id)?;
    assert_ne!(id, duplicate);
    assert_eq!(
        graph.projection(id)?.value,
        graph.projection(duplicate)?.value
    );
    let selected = graph.define(root, body, "selected", false)?;

    let first_arm = graph.select(vec![]);
    let first = graph.reference(first_arm, selected, "left")?;
    let first_id = graph.output_column(first, id)?;
    let first_output = graph.project(first_arm, "left_id", first_id)?;
    let second_arm = graph.select(vec![]);
    let second = graph.reference(second_arm, selected, "right")?;
    let second_id = graph.output_column(second, id)?;
    graph.project(second_arm, "right_id", second_id)?;
    assert_ne!(first_id, second_id);
    assert_eq!(
        graph.check_column(second_arm, first_id),
        Err(GraphError::OutsideBlock)
    );

    let union = graph.union_all(vec![first_arm, second_arm], vec!["id".into()])?;
    let union_id = graph.outputs(union)?.next().unwrap();
    assert_ne!(union_id, first_output);
    assert_eq!(graph.union_inputs(union_id)?[0], first_output);
    let rows = graph.derive(root, union, "rows")?;
    assert_eq!(
        graph.output_column(rows, first_output),
        Err(GraphError::MissingOutput)
    );
    let result_column = graph.output_column(rows, union_id)?;
    let result = graph.project(root, "id", result_column)?;

    let page = graph.select(vec![]);
    let inner = graph.derive(page, root, "page")?;
    let page_id = graph.output_column(inner, result)?;
    graph.operation_mut(page)?.push(page_id);
    let page_output = graph.project(page, "id", page_id)?;
    assert_eq!(
        graph.check_column(page, result_column),
        Err(GraphError::OutsideBlock)
    );
    assert_eq!(graph.projection(id)?.value, parent_id);

    graph.validate(page, |block, outputs, operation| {
        for column in outputs
            .iter()
            .map(|output| output.value)
            .chain(operation.iter().copied())
        {
            graph.check_column(block, column)?;
        }
        Ok(())
    })?;

    let lowered = graph.lower(|column| Ok(Box::new(column)), Ok)?;
    assert_eq!(*lowered.projection(page_output)?.value, page_id);
    assert_eq!(lowered.output_label(duplicate)?, "also_id");
    lowered.validate(page, |block, outputs, operation| {
        for column in outputs
            .iter()
            .map(|output| *output.value)
            .chain(operation.iter().copied())
        {
            lowered.check_column(block, column)?;
        }
        Ok(())
    })?;

    let mut latest = QueryGraph::<_, Expression<'_>, PhysicalOperation<'_>>::new(&catalog);
    let result = latest.select(PhysicalOperation::One);
    let projects = latest.scan(result, "gl_project", "projects")?;
    let id = latest.stored_column(projects, "id")?;
    let name = latest.stored_column(projects, "name")?;
    let version = latest.stored_column(projects, "_version")?;
    let deletion = latest.stored_column(projects, "_deleted")?;
    let public_id = latest.project(result, "id", Expression::Column(id))?;
    let public_name = latest.project(result, "name", Expression::Column(name))?;
    *latest.operation_mut(result)? =
        PhysicalOperation::source(projects).latest(version, Some(deletion));
    let outer = latest.select(PhysicalOperation::One);
    let rows = latest.derive(outer, result, "results")?;
    let reference = latest.output_column(rows, public_id)?;
    *latest.operation_mut(outer)? = PhysicalOperation::source(rows);
    latest.project(outer, "id", Expression::Column(reference))?;
    let latest = latest.lower_operations()?;
    assert_eq!(latest.output_label(public_id)?, "id");
    assert_eq!(latest.output_label(public_name)?, "name");
    assert_eq!(latest.output_column(rows, public_id)?, reference);
    assert_eq!(latest.check_column(result, id), Ok(()));
    let sql = latest.render(outer)?;
    assert!(sql.contains("LIMIT 1 BY q.\"r0_0_stored_traversal_path\", q.\"r0_0_stored_id\""));
    assert!(sql.contains("q.\"r0_0_stored__version\" DESC"));
    assert!(sql.contains("WHERE (q.\"r0_0_stored__deleted\" = false)"));
    println!("{sql}");

    let mut foreign = QueryGraph::<_, (), ()>::new(&catalog);
    let foreign_root = foreign.select(());
    assert_eq!(
        foreign.derive(foreign_root, root, "foreign"),
        Err(GraphError::ForeignGraph)
    );
    let mut visibility = QueryGraph::<_, (), ()>::new(&catalog);
    let root = visibility.select(());
    let hidden = visibility.select(());
    let body = visibility.select(());
    let definition = visibility.define(hidden, body, "private", false)?;
    visibility.derive(root, hidden, "nested")?;
    visibility.reference(root, definition, "capture")?;
    assert_eq!(
        visibility.validate(root, |_, _, _| Ok(())),
        Err(GraphError::DefinitionVisibility)
    );

    for recursive in [false, true] {
        let mut graph = QueryGraph::<_, (), ()>::new(&catalog);
        let root = graph.select(());
        let body = graph.select(());
        let definition = graph.define(root, body, "walk", recursive)?;
        graph.reference(body, definition, "previous")?;
        let result = graph.validate(root, |_, _, _| Ok(()));
        assert_eq!(
            result,
            if recursive {
                Ok(())
            } else {
                Err(GraphError::DefinitionVisibility)
            }
        );
    }

    let mut ownership = QueryGraph::<_, (), ()>::new(&catalog);
    let root = ownership.select(());
    ownership.derive(root, root, "cycle")?;
    assert_eq!(
        ownership.validate(root, |_, _, _| Ok(())),
        Err(GraphError::BlockOwnership)
    );
    println!(
        "Self-join, CTE reuse, duplicate projections, UNION, wrapping, and consuming lowering preserve declaration identity."
    );
    Ok(())
}
