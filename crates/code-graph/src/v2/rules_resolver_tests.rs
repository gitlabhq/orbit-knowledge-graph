//! End-to-end tests for the `RulesResolver` path.
//!
//! Each test: parse files → build ResolutionContext → RulesResolver::resolve → assert edges.

#[cfg(test)]
mod tests {
    use code_graph_types::CanonicalParser;
    use code_graph_types::{CanonicalResult, EdgeKind, NodeKind};
    use parser_core::dsl::types::DslParser;
    use parser_core::v2::langs::{java::JavaDsl, kotlin::KotlinDsl, python::PythonDsl};
    use rustc_hash::FxHashMap;

    use crate::linker::v2::{ReferenceResolver, ResolutionContext, ResolvedEdge, RulesResolver};

    // ── Helpers ─────────────────────────────────────────────────

    fn parse_python(files: &[(&str, &str)]) -> Vec<CanonicalResult> {
        let parser = DslParser::<PythonDsl>::default();
        files
            .iter()
            .map(|(path, source)| parser.parse_file(source.as_bytes(), path).unwrap().0)
            .collect()
    }

    fn parse_java(files: &[(&str, &str)]) -> Vec<CanonicalResult> {
        let parser = DslParser::<JavaDsl>::default();
        files
            .iter()
            .map(|(path, source)| parser.parse_file(source.as_bytes(), path).unwrap().0)
            .collect()
    }

    fn parse_kotlin(files: &[(&str, &str)]) -> Vec<CanonicalResult> {
        let parser = DslParser::<KotlinDsl>::default();
        files
            .iter()
            .map(|(path, source)| parser.parse_file(source.as_bytes(), path).unwrap().0)
            .collect()
    }

    fn resolve_python(files: &[(&str, &str)]) -> Vec<ResolvedEdge> {
        use crate::v2::lang_rules::python::PythonRules;
        let results = parse_python(files);
        let ctx: ResolutionContext<()> =
            ResolutionContext::build(results, FxHashMap::default(), "/".to_string());
        RulesResolver::<PythonRules>::resolve(&ctx)
    }

    fn resolve_java(files: &[(&str, &str)]) -> Vec<ResolvedEdge> {
        use crate::v2::lang_rules::java::JavaRules;
        let results = parse_java(files);
        let ctx: ResolutionContext<()> =
            ResolutionContext::build(results, FxHashMap::default(), "/".to_string());
        RulesResolver::<JavaRules>::resolve(&ctx)
    }

    fn resolve_kotlin(files: &[(&str, &str)]) -> Vec<ResolvedEdge> {
        use crate::v2::lang_rules::kotlin::KotlinRules;
        let results = parse_kotlin(files);
        let ctx: ResolutionContext<()> =
            ResolutionContext::build(results, FxHashMap::default(), "/".to_string());
        RulesResolver::<KotlinRules>::resolve(&ctx)
    }

    fn call_edges(edges: &[ResolvedEdge]) -> Vec<&ResolvedEdge> {
        edges
            .iter()
            .filter(|e| e.relationship.edge_kind == EdgeKind::Calls)
            .collect()
    }

    fn cross_file_calls(edges: &[ResolvedEdge]) -> Vec<&ResolvedEdge> {
        edges
            .iter()
            .filter(|e| {
                e.relationship.edge_kind == EdgeKind::Calls
                    && e.source.file_idx() != e.target.file_idx
            })
            .collect()
    }

    // ── Python tests ────────────────────────────────────────────

    #[test]
    fn python_direct_function_call() {
        let edges = resolve_python(&[(
            "main.py",
            r#"
def greet():
    pass

greet()
"#,
        )]);

        let calls = call_edges(&edges);
        assert!(
            !calls.is_empty(),
            "Should resolve greet() call. Edges: {:?}",
            edges
                .iter()
                .map(|e| format!(
                    "{},{}→{},{}",
                    e.source.file_idx(), 0, e.target.file_idx, e.target.def_idx
                ))
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn python_cross_file_import() {
        let edges = resolve_python(&[
            (
                "utils.py",
                r#"
def helper():
    pass
"#,
            ),
            (
                "main.py",
                r#"
from utils import helper

helper()
"#,
            ),
        ]);

        let cross = cross_file_calls(&edges);
        assert!(
            !cross.is_empty(),
            "Should resolve helper() across files via import. All edges: {:?}",
            edges
                .iter()
                .map(|e| format!(
                    "{},{}→{},{}",
                    e.source.file_idx(), 0, e.target.file_idx, e.target.def_idx
                ))
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn python_same_class_method_call() {
        let edges = resolve_python(&[(
            "service.py",
            r#"
class Service:
    def process(self):
        pass

    def run(self):
        self.process()
"#,
        )]);

        let calls = call_edges(&edges);
        assert!(
            !calls.is_empty(),
            "Should resolve self.process() within class"
        );
    }

    #[test]
    fn python_edge_metadata() {
        let edges = resolve_python(&[(
            "main.py",
            r#"
def foo():
    pass

def bar():
    foo()
"#,
        )]);

        let calls = call_edges(&edges);
        if let Some(edge) = calls.first() {
            assert_eq!(edge.relationship.source_node, NodeKind::Definition);
            assert_eq!(edge.relationship.target_node, NodeKind::Definition);
            // Source and target are in the same file
            assert_eq!(edge.source.file_idx()(), edge.target.file_idx);
        }
    }

    // ── Java tests ──────────────────────────────────────────────

    #[test]
    fn java_same_class_method_call() {
        let edges = resolve_java(&[(
            "App.java",
            r#"
public class App {
    public void run() {
        helper();
    }
    private void helper() {}
}
"#,
        )]);

        let calls = call_edges(&edges);
        assert!(
            !calls.is_empty(),
            "Should resolve helper() within same class"
        );
    }

    #[test]
    fn java_constructor_call() {
        let edges = resolve_java(&[(
            "App.java",
            r#"
public class App {
    public void run() {
        new App();
    }
}
"#,
        )]);

        let calls = call_edges(&edges);
        assert!(!calls.is_empty(), "Should resolve new App() to App class");
    }

    #[test]
    fn java_cross_file_import() {
        let edges = resolve_java(&[
            (
                "com/example/Service.java",
                r#"
package com.example;

public class Service {
    public void serve() {}
}
"#,
            ),
            (
                "com/example/App.java",
                r#"
package com.example;

import com.example.Service;

public class App {
    public void run() {
        new Service();
    }
}
"#,
            ),
        ]);

        let cross = cross_file_calls(&edges);
        assert!(
            !cross.is_empty(),
            "Should resolve new Service() across files. All edges: {:?}",
            edges
                .iter()
                .map(|e| format!(
                    "{},{}→{},{}",
                    e.source.file_idx(), 0, e.target.file_idx, e.target.def_idx
                ))
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn java_same_package_no_import() {
        let edges = resolve_java(&[
            (
                "com/example/Helper.java",
                r#"
package com.example;

public class Helper {
    public static void help() {}
}
"#,
            ),
            (
                "com/example/App.java",
                r#"
package com.example;

public class App {
    public void run() {
        new Helper();
    }
}
"#,
            ),
        ]);

        let cross = cross_file_calls(&edges);
        assert!(
            !cross.is_empty(),
            "Should resolve new Helper() via same-package lookup"
        );
    }

    #[test]
    fn java_wildcard_import() {
        let edges = resolve_java(&[
            (
                "com/util/Tool.java",
                r#"
package com.util;

public class Tool {
    public void doWork() {}
}
"#,
            ),
            (
                "com/app/Main.java",
                r#"
package com.app;

import com.util.*;

public class Main {
    public void run() {
        new Tool();
    }
}
"#,
            ),
        ]);

        let cross = cross_file_calls(&edges);
        assert!(
            !cross.is_empty(),
            "Should resolve new Tool() via wildcard import"
        );
    }

    // ── Kotlin tests ────────────────────────────────────────────

    #[test]
    fn kotlin_same_class_method_call() {
        let edges = resolve_kotlin(&[(
            "App.kt",
            r#"
class App {
    fun run() {
        helper()
    }
    fun helper() {}
}
"#,
        )]);

        let calls = call_edges(&edges);
        assert!(
            !calls.is_empty(),
            "Should resolve helper() within same class"
        );
    }

    #[test]
    fn kotlin_cross_file_import() {
        let edges = resolve_kotlin(&[
            (
                "com/example/Service.kt",
                r#"
package com.example

class Service {
    fun serve() {}
}
"#,
            ),
            (
                "com/example/App.kt",
                r#"
package com.example

import com.example.Service

class App {
    fun run() {
        Service()
    }
}
"#,
            ),
        ]);

        let cross = cross_file_calls(&edges);
        assert!(
            !cross.is_empty(),
            "Should resolve Service() across files. All edges: {:?}",
            edges
                .iter()
                .map(|e| format!(
                    "{},{}→{},{}",
                    e.source.file_idx(), 0, e.target.file_idx, e.target.def_idx
                ))
                .collect::<Vec<_>>()
        );
    }

    // ── Full pipeline tests ─────────────────────────────────────

    #[test]
    fn python_full_pipeline() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();

        std::fs::write(
            root.join("service.py"),
            r#"
class UserService:
    def get_user(self, id):
        return id

    def create_user(self, name):
        user = self.get_user(name)
        return user
"#,
        )
        .unwrap();

        std::fs::write(
            root.join("main.py"),
            r#"
from service import UserService

svc = UserService()
svc.create_user("alice")
"#,
        )
        .unwrap();

        let pipeline = crate::v2::Pipeline::new(crate::v2::PipelineConfig::default());
        let result = pipeline.run(root);

        assert_eq!(result.errors.len(), 0);
        assert_eq!(result.stats.files_parsed, 2);

        let call_count = result
            .graph
            .edges()
            .filter(|(_, _, e)| e.relationship.edge_kind == EdgeKind::Calls)
            .count();
        assert!(
            call_count > 0,
            "Pipeline should produce call edges. Total graph edges: {}",
            result.graph.edge_count()
        );
    }

    #[test]
    fn java_full_pipeline() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();
        std::fs::create_dir_all(root.join("com/example")).unwrap();

        std::fs::write(
            root.join("com/example/Service.java"),
            r#"
package com.example;

public class Service {
    public void process() {}
    public void run() {
        process();
    }
}
"#,
        )
        .unwrap();

        std::fs::write(
            root.join("com/example/App.java"),
            r#"
package com.example;

public class App {
    public void main() {
        new Service();
    }
}
"#,
        )
        .unwrap();

        let pipeline = crate::v2::Pipeline::new(crate::v2::PipelineConfig::default());
        let result = pipeline.run(root);

        assert_eq!(result.errors.len(), 0);
        assert_eq!(result.stats.files_parsed, 2);

        let call_count = result
            .graph
            .edges()
            .filter(|(_, _, e)| e.relationship.edge_kind == EdgeKind::Calls)
            .count();
        assert!(
            call_count > 0,
            "Pipeline should produce call edges. Total graph edges: {}",
            result.graph.edge_count()
        );
    }

    #[test]
    fn kotlin_full_pipeline() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();

        std::fs::write(
            root.join("Service.kt"),
            r#"
class Service {
    fun process() {}
    fun run() {
        process()
    }
}
"#,
        )
        .unwrap();

        let pipeline = crate::v2::Pipeline::new(crate::v2::PipelineConfig::default());
        let result = pipeline.run(root);

        assert_eq!(result.errors.len(), 0);
        assert_eq!(result.stats.files_parsed, 1);

        let call_count = result
            .graph
            .edges()
            .filter(|(_, _, e)| e.relationship.edge_kind == EdgeKind::Calls)
            .count();
        assert!(
            call_count > 0,
            "Pipeline should produce call edges. Total graph edges: {}",
            result.graph.edge_count()
        );
    }
}
