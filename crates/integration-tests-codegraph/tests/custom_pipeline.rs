//! Integration test proving the custom pipeline framework works e2e.
//!
//! RubyPipeline: Prism parse → Arrow RecordBatches directly, no CodeGraph.

use arrow::array::AsArray;
use code_graph::v2::custom::ruby::RubyPipeline;
use code_graph::v2::{LanguagePipeline, PipelineOutput};

fn write_fixture(dir: &std::path::Path, path: &str, content: &str) {
    let full = dir.join(path);
    if let Some(parent) = full.parent() {
        std::fs::create_dir_all(parent).unwrap();
    }
    std::fs::write(full, content).unwrap();
}

#[test]
fn ruby_custom_pipeline_extracts_class_and_methods() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path().to_string_lossy().to_string();

    write_fixture(
        tmp.path(),
        "app/services/user_service.rb",
        r#"
class UserService
  def greet(name)
    "Hello, #{name}"
  end

  def farewell
    "Goodbye"
  end
end
"#,
    );

    let files = vec![format!("{root}/app/services/user_service.rb")];
    let result = RubyPipeline::process_files(&files, &root).expect("pipeline should succeed");

    let batches = match result {
        PipelineOutput::Batches(b) => b,
        PipelineOutput::Graph(_) => panic!("expected Batches output"),
    };

    // Should have File, Definition, and edge batches
    let tables: Vec<&str> = batches.iter().map(|(name, _batch)| name.as_str()).collect();
    assert!(tables.contains(&"File"), "missing File batch");
    assert!(tables.contains(&"Definition"), "missing Definition batch");
    assert!(
        tables.contains(&"DefinitionToDefinition"),
        "missing edge batch"
    );

    // Check definitions
    let def_batch = &batches.iter().find(|(n, _)| n == "Definition").unwrap().1;
    assert_eq!(
        def_batch.num_rows(),
        3,
        "expected 3 defs (UserService, greet, farewell)"
    );

    let names: Vec<&str> = def_batch
        .column_by_name("name")
        .unwrap()
        .as_string::<i32>()
        .iter()
        .flatten()
        .collect();
    assert!(names.contains(&"UserService"));
    assert!(names.contains(&"greet"));
    assert!(names.contains(&"farewell"));

    let fqns: Vec<&str> = def_batch
        .column_by_name("fqn")
        .unwrap()
        .as_string::<i32>()
        .iter()
        .flatten()
        .collect();
    assert!(fqns.contains(&"UserService"));
    assert!(fqns.contains(&"UserService::greet"));
    assert!(fqns.contains(&"UserService::farewell"));

    let def_types: Vec<&str> = def_batch
        .column_by_name("definition_type")
        .unwrap()
        .as_string::<i32>()
        .iter()
        .flatten()
        .collect();
    assert!(def_types.contains(&"Class"));
    assert!(def_types.contains(&"Method"));

    // Check edges: File→UserService, UserService→greet, UserService→farewell
    let edge_batch = &batches
        .iter()
        .find(|(n, _)| n == "DefinitionToDefinition")
        .unwrap()
        .1;
    assert_eq!(edge_batch.num_rows(), 3, "expected 3 containment edges");
}

#[test]
fn ruby_custom_pipeline_handles_nested_modules() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path().to_string_lossy().to_string();

    write_fixture(
        tmp.path(),
        "lib/api/v1/handler.rb",
        r#"
module Api
  module V1
    class Handler
      def process
      end
    end
  end
end
"#,
    );

    let files = vec![format!("{root}/lib/api/v1/handler.rb")];
    let result = RubyPipeline::process_files(&files, &root).expect("pipeline should succeed");

    let batches = match result {
        PipelineOutput::Batches(b) => b,
        PipelineOutput::Graph(_) => panic!("expected Batches output"),
    };

    let def_batch = &batches.iter().find(|(n, _)| n == "Definition").unwrap().1;
    assert_eq!(
        def_batch.num_rows(),
        4,
        "expected 4 defs (Api, V1, Handler, process)"
    );

    let fqns: Vec<&str> = def_batch
        .column_by_name("fqn")
        .unwrap()
        .as_string::<i32>()
        .iter()
        .flatten()
        .collect();
    assert!(fqns.contains(&"Api"));
    assert!(fqns.contains(&"Api::V1"));
    assert!(fqns.contains(&"Api::V1::Handler"));
    assert!(fqns.contains(&"Api::V1::Handler::process"));
}

#[test]
fn ruby_custom_pipeline_multiple_files() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path().to_string_lossy().to_string();

    write_fixture(tmp.path(), "a.rb", "class A; end\n");
    write_fixture(tmp.path(), "b.rb", "class B; end\n");

    let files = vec![format!("{root}/a.rb"), format!("{root}/b.rb")];
    let result = RubyPipeline::process_files(&files, &root).expect("pipeline should succeed");

    let batches = match result {
        PipelineOutput::Batches(b) => b,
        PipelineOutput::Graph(_) => panic!("expected Batches output"),
    };

    let file_batch = &batches.iter().find(|(n, _)| n == "File").unwrap().1;
    assert_eq!(file_batch.num_rows(), 2, "expected 2 files");

    let def_batch = &batches.iter().find(|(n, _)| n == "Definition").unwrap().1;
    assert_eq!(def_batch.num_rows(), 2, "expected 2 defs (A, B)");
}
