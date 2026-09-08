use jsonschema::validator_for;
use orbit_versions::VERSIONS;

const SCHEMA: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../config/schemas/context_response.schema.json"
));

#[test]
fn context_response_example_matches_versioned_schema() {
    let schema: serde_json::Value = serde_json::from_str(SCHEMA).unwrap();
    let validator = validator_for(&schema).unwrap();
    let example = serde_json::json!({
        "version": "1.0.0",
        "entities": [
            {
                "ref": "MergeRequest[123]",
                "type": "MergeRequest",
                "id": 123,
                "found": true,
                "summary": {
                    "title": "Add context endpoint",
                    "state": "opened",
                    "iid": 42,
                    "project_path": "gitlab-org/gitlab",
                    "web_url": "https://gitlab.com/gitlab-org/gitlab/-/merge_requests/42",
                    "author": {"id": 1, "username": "author", "name": "Author"},
                    "source_branch": "context-endpoint",
                    "target_branch": "master",
                    "head_pipeline_status": "success",
                    "reviewers": [{"id": 2, "username": "reviewer", "name": "Reviewer"}],
                    "linked_issues": [{
                        "ref": "Issue[456]",
                        "iid": 7,
                        "project_path": "gitlab-org/gitlab",
                        "title": "Add entity context"
                    }],
                    "future_additive_field": true
                }
            },
            {
                "ref": "Issue[456]",
                "type": "Issue",
                "id": 456,
                "found": true,
                "summary": {
                    "title": "Add entity context",
                    "state": "opened",
                    "iid": 7,
                    "project_path": "gitlab-org/gitlab",
                    "web_url": "https://gitlab.com/gitlab-org/gitlab/-/issues/7",
                    "labels": ["type::feature"],
                    "assignees": [{"id": 3, "username": "assignee", "name": "Assignee"}],
                    "milestone": {"title": "19.2"},
                    "linked_merge_requests": [{
                        "ref": "MergeRequest[123]",
                        "iid": 42,
                        "project_path": "gitlab-org/gitlab",
                        "title": "Add context endpoint"
                    }]
                }
            },
            {"ref": "Issue[999]", "type": "Issue", "id": 999, "found": false, "error": "not_found"},
            {"ref": "Foo[1]", "type": null, "id": null, "found": false, "error": "unsupported_type"},
            {"ref": "garbage", "type": null, "id": null, "found": false, "error": "invalid_ref"}
        ]
    });

    assert!(validator.is_valid(&example));
    assert_eq!(example["version"], VERSIONS.context_response_format);
    assert!(schema["$id"].as_str().unwrap().ends_with(&format!(
        "/v{}",
        VERSIONS.context_response_format.split('.').next().unwrap()
    )));
}
