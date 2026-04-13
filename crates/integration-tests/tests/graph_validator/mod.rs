use integration_testkit::graph_validator::run_yaml_suite;

const STRUCTURAL: &str =
    include_str!("../../integration-testkit/src/graph_validator/fixtures/structural.yaml");
const PYTHON_RESOLUTION: &str =
    include_str!("../../integration-testkit/src/graph_validator/fixtures/python_resolution.yaml");
const JAVA_RESOLUTION: &str =
    include_str!("../../integration-testkit/src/graph_validator/fixtures/java_resolution.yaml");
const KOTLIN_RESOLUTION: &str =
    include_str!("../../integration-testkit/src/graph_validator/fixtures/kotlin_resolution.yaml");
const CONTAINMENT: &str =
    include_str!("../../integration-testkit/src/graph_validator/fixtures/containment.yaml");

#[tokio::test]
async fn structural_invariants() {
    run_yaml_suite(STRUCTURAL).await;
}

#[tokio::test]
async fn python_call_resolution() {
    run_yaml_suite(PYTHON_RESOLUTION).await;
}

#[tokio::test]
async fn java_call_resolution() {
    run_yaml_suite(JAVA_RESOLUTION).await;
}

#[tokio::test]
async fn kotlin_call_resolution() {
    run_yaml_suite(KOTLIN_RESOLUTION).await;
}

#[tokio::test]
async fn containment_hierarchy() {
    run_yaml_suite(CONTAINMENT).await;
}
