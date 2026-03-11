//! Tests for the INSERT...SELECT emit pass using the indexer pipeline.

#[cfg(test)]
mod tests {
    use llqm::backend::clickhouse::{ClickHouseBackend, InsertSelectPass};
    use llqm::ir::expr::{self, DataType};
    use llqm::ir::plan::Rel;
    use llqm::pipeline::{Frontend, Pipeline};

    struct SelectFrontend;

    impl Frontend for SelectFrontend {
        type Input = ();
        type Error = std::convert::Infallible;

        fn lower(&self, _: ()) -> Result<llqm::ir::plan::Plan, Self::Error> {
            Ok(Rel::read("gl_project", "p", &[("id", DataType::Int64)])
                .filter(
                    expr::func(
                        "startsWith",
                        vec![
                            expr::col("p", "traversal_path"),
                            expr::param("traversal_path", DataType::String),
                        ],
                    )
                    .and(expr::col("p", "_deleted").eq(expr::raw("false"))),
                )
                .project(&[
                    (expr::col("p", "id"), "id"),
                    (expr::col("p", "name"), "name"),
                    (expr::raw("true"), "_deleted"),
                    (expr::raw("now64(6)"), "_version"),
                ])
                .into_plan())
        }
    }

    #[test]
    fn insert_select_with_columns() {
        let pass = InsertSelectPass::new("gl_project", &["id", "name", "_deleted", "_version"]);

        let pq = Pipeline::new()
            .input(SelectFrontend, ())
            .lower()
            .unwrap()
            .emit(&ClickHouseBackend)
            .unwrap()
            .pass(&pass)
            .unwrap()
            .finish();

        let sql = &pq.sql;

        assert!(
            sql.starts_with("INSERT INTO gl_project (id, name, _deleted, _version) SELECT"),
            "sql: {sql}"
        );
        assert!(sql.contains("true AS _deleted"), "sql: {sql}");
        assert!(sql.contains("now64(6) AS _version"), "sql: {sql}");
        assert!(
            sql.contains("startsWith(p.traversal_path, {traversal_path:String})"),
            "sql: {sql}"
        );
    }

    #[test]
    fn insert_select_without_columns() {
        let pass = InsertSelectPass::new("gl_project", &[]);

        let pq = Pipeline::new()
            .input(SelectFrontend, ())
            .lower()
            .unwrap()
            .emit(&ClickHouseBackend)
            .unwrap()
            .pass(&pass)
            .unwrap()
            .finish();

        assert!(
            pq.sql.starts_with("INSERT INTO gl_project SELECT"),
            "sql: {}",
            pq.sql
        );
    }

    #[test]
    fn insert_select_preserves_params() {
        let pass = InsertSelectPass::new("t", &["id"]);

        let pq = Pipeline::new()
            .input(SelectFrontend, ())
            .lower()
            .unwrap()
            .emit(&ClickHouseBackend)
            .unwrap()
            .pass(&pass)
            .unwrap()
            .finish();

        assert!(
            pq.sql.contains("{traversal_path:String}"),
            "params should be preserved: {}",
            pq.sql
        );
    }
}
