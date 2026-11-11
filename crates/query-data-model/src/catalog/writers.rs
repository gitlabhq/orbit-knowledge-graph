use std::collections::HashMap;

use super::Diagnostics;
use super::sources::{RowWriter, Sources, Writer};
use super::store::Store;
use crate::{Endpoint, EntityId, GraphCatalog, RelationshipVariantId};

/// Gives every stored entity and every edge-table variant its one writer. A foreign-key
/// variant's writer is the writer of the node whose row holds the key.
pub(super) fn assign(
    graph: &GraphCatalog,
    stores: &[Store],
    sources: &mut Sources,
    packages: &[String],
    diagnostics: &mut Diagnostics,
) {
    let (mut entities, mut variants) = candidates(graph, sources);
    for entity in graph.entities() {
        let writers = entities.remove(&entity.id).unwrap_or_default();
        let holder = stores
            .iter()
            .find_map(|store| store.source(entity.id).filter(|held| held.table.is_some()));
        let package = holder.map_or_else(
            || declared(sources, packages, &writers),
            |held| held.package.clone(),
        );
        let bound = stores.iter().any(|store| store.source(entity.id).is_some());
        match (holder, writers.as_slice()) {
            (Some(_), [writer]) => {
                sources.writers.entities.insert(entity.id, *writer);
            }
            (Some(_), []) => diagnostics.error(
                &package,
                format!("stored entity {} has no writer", entity.name),
            ),
            (None, [writer]) if !bound => diagnostics.error(
                &package,
                format!(
                    "{} writes entity {}, which no store holds",
                    sources.row_writer_name(*writer),
                    entity.name
                ),
            ),
            (None, [] | [_]) => {}
            (_, writers) => diagnostics.error(
                &package,
                format!(
                    "entity {} has two writers: {}",
                    entity.name,
                    names(sources, writers)
                ),
            ),
        }
    }
    for variant in graph.variants() {
        let writers = variants.remove(&variant.id).unwrap_or_default();
        let label = format!(
            "{}({} -> {})",
            graph.relationship(variant.relationship).name,
            graph.entity(variant.source).name,
            graph.entity(variant.target).name
        );
        let bound = stores.iter().find_map(|store| {
            let table = store.edge_table(variant.id)?;
            let (_, package, binding) =
                store.bindings().find(|(bound, ..)| *bound == variant.id)?;
            Some((table, package, binding.fk.as_ref()))
        });
        let Some((table, package, fk)) = bound else {
            let fk_only = stores
                .iter()
                .any(|store| store.binding(variant.id).is_some());
            if let ([writer, ..], true) = (writers.as_slice(), fk_only) {
                diagnostics.error(
                    &declared(sources, packages, &writers),
                    format!(
                        "{} writes {label}, which no edge table holds",
                        sources.row_writer_name(*writer)
                    ),
                );
            }
            continue;
        };
        let writer = match writers.as_slice() {
            [writer] => *writer,
            [] => {
                diagnostics.error(
                    package,
                    format!("{label} in edge table {table} has no writer"),
                );
                continue;
            }
            writers => {
                diagnostics.error(
                    package,
                    format!("{label} has two writers: {}", names(sources, writers)),
                );
                continue;
            }
        };
        if let Some(key) = fk {
            let node = match key.foreign_key.holder {
                Endpoint::Source => variant.source,
                Endpoint::Target => variant.target,
            };
            let node_writer = sources.entity_writer(node);
            if node_writer.is_some_and(|node_writer| node_writer != writer) {
                diagnostics.error(
                    package,
                    format!(
                        "foreign-key variant {label} is written by {}, not by the writer of {}",
                        sources.row_writer_name(writer),
                        graph.entity(node).name
                    ),
                );
                continue;
            }
        }
        sources.writers.variants.insert(variant.id, writer);
    }
}

type Candidates = (
    HashMap<EntityId, Vec<RowWriter>>,
    HashMap<RelationshipVariantId, Vec<RowWriter>>,
);

fn candidates(graph: &GraphCatalog, sources: &Sources) -> Candidates {
    let mut entities = HashMap::<_, Vec<_>>::new();
    let mut variants = HashMap::<_, Vec<_>>::new();
    for (index, pipeline) in sources.pipelines().iter().enumerate() {
        let writer = RowWriter::Pipeline(index);
        let emitted: Vec<_> = match pipeline.writer {
            Writer::Entity(entity) => {
                entities.entry(entity).or_default().push(writer);
                edge_variants(pipeline)
            }
            Writer::Relationship(_) => edge_variants(pipeline),
            Writer::Derived(derived) => sources.derived()[derived]
                .emits
                .iter()
                .flat_map(|relationship| graph.relationship(*relationship).variants.clone())
                .collect(),
        };
        for variant in emitted {
            let writers = variants.entry(variant).or_default();
            if !writers.contains(&writer) {
                writers.push(writer);
            }
        }
    }
    for (index, direct) in sources.direct_sources().iter().enumerate() {
        let writer = RowWriter::Direct(index);
        for entity in &direct.entities {
            entities.entry(*entity).or_default().push(writer);
        }
        for variant in &direct.variants {
            variants.entry(*variant).or_default().push(writer);
        }
    }
    (entities, variants)
}

fn edge_variants(pipeline: &super::sources::Pipeline) -> Vec<RelationshipVariantId> {
    pipeline
        .transform
        .edges()
        .iter()
        .flat_map(|edge| edge.variants.iter().copied())
        .collect()
}

fn names(sources: &Sources, writers: &[RowWriter]) -> String {
    writers
        .iter()
        .map(|writer| sources.row_writer_name(*writer))
        .collect::<Vec<_>>()
        .join(" and ")
}

/// The package that declares the first writer, to blame when no store holds what it writes.
fn declared(sources: &Sources, packages: &[String], writers: &[RowWriter]) -> String {
    match writers.first() {
        Some(RowWriter::Pipeline(index)) => packages[*index].clone(),
        Some(RowWriter::Direct(index)) => sources.direct_sources()[*index].package.clone(),
        None => String::new(),
    }
}
