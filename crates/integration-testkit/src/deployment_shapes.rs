//! Every deployment shape, shipped and test, loads into a model in which each stored entity has
//! exactly one complete store and a connection to run on.

use compiler::dag::Connections;
use query_data_model::catalog::{Needs, Unplaced};
use query_data_model::{Deployments, Packages};

fn check(deployments: &Deployments, packages: &Packages) {
    for shape in deployments.shapes() {
        let model = packages
            .deployed(deployments, shape)
            .unwrap_or_else(|error| panic!("shape {shape} does not load: {error}"));
        let connections = Connections::of(&model).unwrap();
        for entity in model.graph().entities() {
            let store = match model.get_store(entity.id, &Needs::default()) {
                Ok(placement) => model.store(placement.parts()[0].0),
                Err(Unplaced::Unstored { .. }) => continue,
                Err(error) => panic!("shape {shape}: {error}"),
            };
            assert!(
                store.complete(entity.id),
                "shape {shape}: {} reads from a partial store",
                entity.name
            );
            assert!(
                connections.get(store.connection()).is_some(),
                "shape {shape}: {} has no connection to run on",
                entity.name
            );
        }
    }
}

#[test]
fn every_shipped_deployment_shape_places_each_stored_entity_once() {
    check(
        &Deployments::embedded(),
        &query_data_model::testkit::with_demo(),
    );
}

#[test]
fn every_test_deployment_shape_places_each_stored_entity_once() {
    check(
        &query_data_model::testkit::deployments(),
        &crate::dag_fakes::peer_packages(),
    );
}
