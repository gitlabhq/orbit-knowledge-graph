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
const METRIC: &str = "a metric name is an observability contract";
const SUBJECT: &str = "a NATS stream or subject, a wire contract";

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
    at("crates/indexer/src/checkpoint.rs", "checkpoint", "checkpoint store", PROSE),
    at("crates/indexer/src/clickhouse/clients.rs", "graph", "graph", INSTANCE),
    at("crates/indexer/src/clickhouse/clients.rs", "datalake", "datalake", INSTANCE),
    at("crates/indexer/src/clickhouse/writer.rs", "graph", "graph store unavailable", PROSE),
    at("crates/indexer/src/config.rs", "graph", "Invalid graph ClickHouse", PROSE),
    at("crates/indexer/src/lib.rs", "graph", "to graph ClickHouse", PROSE),
    at("crates/indexer/src/modules/code/arrow_converter.rs", "graph", "is not a graph table", PROSE),
    at("crates/indexer/src/modules/code/arrow_converter.rs", "graph", "has no graph table", PROSE),
    at("crates/indexer/src/modules/code/arrow_converter.rs", "graph", "this same graph", PROSE),
    at("crates/indexer/src/modules/code/arrow_converter.rs", "graph", "ClickHouse graph conversion", PROSE),
    at("crates/indexer/src/modules/code/config.rs", "graph", "has no graph table", PROSE),
    at("crates/indexer/src/modules/code/config.rs", "code", "by the code indexer", PROSE),
    at("crates/indexer/src/modules/code/config.rs", "code", "code indexer emitted", PROSE),
    at("crates/indexer/src/modules/code/handler.rs", "code", "deserialize code indexing", PROSE),
    at("crates/indexer/src/modules/code/handler.rs", "code", "received code indexing", PROSE),
    at("crates/indexer/src/modules/code/handler.rs", "checkpoint", "deleted-project checkpoint", PROSE),
    at("crates/indexer/src/modules/code/handler.rs", "checkpoint", "load checkpoint", PROSE),
    at("crates/indexer/src/modules/code/handler.rs", "code", "starting code indexing", PROSE),
    at("crates/indexer/src/modules/code/handler.rs", "code", "code indexing deferred", PROSE),
    at("crates/indexer/src/modules/code/handler.rs", "code", "code indexing job exceeded", PROSE),
    at("crates/indexer/src/modules/code/handler.rs", "code", "index code", PROSE),
    at("crates/indexer/src/modules/code/incremental/output.rs", "local", "not a local entity", PROSE),
    at("crates/indexer/src/modules/code/incremental/run.rs", "code", "a code graph entity", PROSE),
    at("crates/indexer/src/modules/code/incremental/run.rs", "graph", "code graph entity", PROSE),
    at("crates/indexer/src/modules/code/pipeline.rs", "checkpoint", "before the checkpoint", PROSE),
    at("crates/indexer/src/modules/code/pipeline.rs", "checkpoint", "set checkpoint", PROSE),
    at("crates/indexer/src/modules/code/pipeline.rs", "checkpoint", "checkpoint", METRIC),
    at("crates/indexer/src/modules/code/pipeline.rs", "code", "completed code indexing", PROSE),
    at("crates/indexer/src/modules/code/pipeline.rs", "code", "code indexing cancelled", PROSE),
    at("crates/indexer/src/modules/code/pipeline.rs", "graph", "partial graph", PROSE),
    at("crates/indexer/src/modules/code/pipeline.rs", "code", "fatal code indexing", PROSE),
    at("crates/indexer/src/modules/code/pipeline.rs", "code", "code-graph", PROSE),
    at("crates/indexer/src/modules/code/pipeline.rs", "graph", "code-graph", PROSE),
    at("crates/indexer/src/modules/code/pipeline.rs", "code", "during code indexing", PROSE),
    at("crates/indexer/src/modules/sdlc/datalake.rs", "datalake", "datalake query failed", PROSE),
    at("crates/indexer/src/modules/sdlc/datalake.rs", "datalake", "datalake unavailable", PROSE),
    at("crates/indexer/src/modules/sdlc/datalake.rs", "datalake", "datalake summary", PROSE),
    at("crates/indexer/src/modules/sdlc/pipeline.rs", "datalake", "datalake query failed", PROSE),
    at("crates/indexer/src/modules/sdlc/plan/build.rs", "checkpoint", "and checkpoint keys", PROSE),
    at("crates/indexer/src/modules/sdlc/plan/transform.rs", "graph", "has no graph table", PROSE),
    at("crates/indexer/src/observer.rs", "code", "code", INDEXING),
    at("crates/indexer/src/orchestrator/dispatch/code_backfill.rs", "code", "dispatched code backfill", PROSE),
    at("crates/indexer/src/orchestrator/dispatch/code_backfill.rs", "code", "for code backfill", PROSE),
    at("crates/indexer/src/orchestrator/scheduled/backfill_sweep.rs", "code", "active code backfill", PROSE),
    at("crates/indexer/src/orchestrator/scheduled/migration_completion.rs", "code", "code coverage", PROSE),
    at("crates/indexer/src/orchestrator/scheduled/migration_completion.rs", "code", "code", METRIC),
    at("crates/indexer/src/orchestrator/scheduled/migration_completion.rs", "code", "count code-eligible", PROSE),
    at("crates/indexer/src/orchestrator/scheduled/migration_completion.rs", "code", "count code-indexed", PROSE),
    at("crates/indexer/src/orchestrator/siphon/code_indexing_task.rs", "code", "dispatch.code.task", METRIC),
    at("crates/indexer/src/orchestrator/siphon/code_indexing_task.rs", "code", "dispatched code indexing", PROSE),
    at("crates/indexer/src/orchestrator/siphon/code_indexing_task.rs", "code", "skipped code indexing", PROSE),
    at("crates/indexer/src/orchestrator/siphon/router.rs", "siphon", "dispatch.siphon", METRIC),
    at("crates/indexer/src/orchestrator/siphon/router.rs", "siphon", "siphon drain", PROSE),
    at("crates/indexer/src/topic.rs", "code", "code-indexing-task", SUBJECT),
    at("crates/indexer/src/topic.rs", "code", "code.task.indexing.requested", SUBJECT),
    at("crates/query-data-model/src/catalog/sources.rs", "graph", "a graph entity", PROSE),
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
