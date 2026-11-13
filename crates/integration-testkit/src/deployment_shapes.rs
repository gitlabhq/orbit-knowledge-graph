//! Every deployment shape, shipped and test, loads into a model in which each stored entity has
//! exactly one complete store and a connection to run on.

use compiler::dag::Connections;
use query_data_model::{Deployments, Packages};

fn check(deployments: &Deployments, packages: &Packages) {
    for shape in deployments.shapes() {
        let model = packages
            .deployed(deployments, shape)
            .unwrap_or_else(|error| panic!("shape {shape} does not load: {error}"));
        let connections = Connections::of(&model).unwrap();
        for entity in model.graph().entities() {
            let stores = model.stores().iter();
            if !stores
                .clone()
                .any(|store| store.source(entity.id).is_some())
            {
                continue;
            }
            let complete = stores.filter(|store| store.complete(entity.id)).count();
            assert_eq!(
                complete, 1,
                "shape {shape}: {} complete stores",
                entity.name
            );
            assert!(
                connections.placed(entity.id).is_ok(),
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
