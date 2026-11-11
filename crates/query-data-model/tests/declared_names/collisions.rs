//! Literals that contain a declared name as an ordinary word, each checked by hand to read no
//! package fact. An entry covers only `name` in `file`: in a literal that is exactly `text` when
//! `text` is the bare name, otherwise in a literal containing `text`.

pub struct Collision {
    pub file: &'static str,
    pub name: &'static str,
    pub text: &'static str,
    pub reason: &'static str,
}

const PROSE: &str = "an English word in a message";
const INSTANCE: &str = "the deployment's ClickHouse instance label, not a package name";
const INDEXING: &str = "the code indexing module or migration scope of the deployment config";

const fn at(
    file: &'static str,
    name: &'static str,
    text: &'static str,
    reason: &'static str,
) -> Collision {
    Collision {
        file,
        name,
        text,
        reason,
    }
}

#[rustfmt::skip]
pub const COLLISIONS: &[Collision] = &[
    at("crates/query-data-model/src/plan/code/mod.rs", "code", "unknown code index", PROSE),
    at("crates/query-data-model/src/plan/code/mod.rs", "code", "a code search", PROSE),
    at("crates/query-data-model/src/plan/code/mod.rs", "code", "a code request", PROSE),
    at("crates/query-data-model/src/plan/logical/results.rs", "graph", "dynamic graph result", PROSE),
    at("crates/query-data-model/src/plan/logical/results.rs", "graph", "a graph value", PROSE),
    at("crates/query-data-model/src/surface/gql/lower/returns.rs", "graph", "dynamic graph results", PROSE),
    at("crates/query-data-model/src/surface/gql/mod.rs", "graph", "not graph queries", PROSE),
    at("crates/query-data-model/src/surface/gql/errors.rs", "CONTAINS", "ENDS WITH, or CONTAINS", "the GQL string operator"),
    at("crates/query-engine/profiler/src/main.rs", "graph", "graph", INSTANCE),
    at("crates/orbit-server/src/clickhouse_setup.rs", "graph", "graph", INSTANCE),
    at("crates/orbit-server/src/clickhouse_setup.rs", "graph", "a replicated graph needs", PROSE),
    at("crates/orbit-server/src/health_check.rs", "graph", "graph", INSTANCE),
    at("crates/orbit-server/src/health_check.rs", "graph", "clickhouse-graph", INSTANCE),
    at("crates/orbit-server/src/health_check.rs", "datalake", "datalake", INSTANCE),
    at("crates/orbit-server/src/health_check.rs", "datalake", "clickhouse-datalake", INSTANCE),
    at("crates/orbit-server/src/main.rs", "graph", "graph", INSTANCE),
    at("crates/orbit-server/src/auth/validator.rs", "graph", "gitlab-knowledge-graph", "the JWT audience"),
    at("crates/orbit-server/src/cluster_health.rs", "Deployment", "Deployment", "a Kubernetes resource kind"),
    at("crates/orbit-server/src/graph_status/toon.rs", "graph", "graph status", PROSE),
    at("crates/orbit-server/src/grpc/service.rs", "graph", "Fetching graph", PROSE),
    at("crates/orbit-server/src/pipeline/helpers.rs", "Pipeline", "Pipeline error", PROSE),
    at("crates/orbit-server/src/pipeline/helpers.rs", "graph", "of the graph", PROSE),
    at("crates/orbit-server/src/tools/registry.rs", "graph", "a graph pattern table", PROSE),
    at("crates/orbit-server-config/src/engine.rs", "code", "code-indexing pipeline", INDEXING),
    at("crates/orbit-server-config/src/engine.rs", "code", "code", INDEXING),
    at("crates/orbit-migrations/src/execute.rs", "checkpoint", "seeding checkpoint", PROSE),
    at("crates/orbit-migrations/src/scope.rs", "code", "code", INDEXING),
    at("crates/orbit-migrations/src/scope.rs", "code", "scope code", INDEXING),
];

impl Collision {
    pub fn covers(&self, file: &str, name: &str, literal: &str) -> bool {
        let text = match self.text == self.name {
            true => literal == self.text,
            false => literal.contains(self.text),
        };
        self.file == file && self.name == name && text
    }
}
