use super::*;

async fn indexing_status_error(paths: &[&str]) -> tonic::Code {
    let request = GetIndexingStatusRequest {
        traversal_paths: paths.iter().map(|path| path.to_string()).collect(),
    };
    test_service()
        .get_indexing_status(authed_request(request))
        .await
        .unwrap_err()
        .code()
}

fn item_counts_request(path: &str, paths: &[&str]) -> GetItemCountsRequest {
    GetItemCountsRequest {
        traversal_path: path.to_string(),
        traversal_paths: paths.iter().map(|path| path.to_string()).collect(),
    }
}

fn member_of(group: &str) -> Claims {
    Claims {
        group_traversal_ids: vec![crate::auth::claims::TraversalPathClaim {
            path: TraversalPath::new_unchecked(group),
            access_levels: vec![20],
        }],
        ..test_claims()
    }
}

async fn item_counts(request: GetItemCountsRequest) -> Result<GetItemCountsResponse, tonic::Code> {
    test_service()
        .get_item_counts(signed_request(request, member_of("1/22/")))
        .await
        .map(Response::into_inner)
        .map_err(|status| status.code())
}

async fn item_counts_error(request: GetItemCountsRequest) -> tonic::Code {
    item_counts(request).await.unwrap_err()
}

async fn graph_status_error(path: &str, claims: Claims) -> tonic::Code {
    let request = GetGraphStatusRequest {
        traversal_path: path.to_string(),
        ..Default::default()
    };
    test_service()
        .get_graph_status(signed_request(request, claims))
        .await
        .unwrap_err()
        .code()
}

#[tokio::test]
async fn status_rpcs_reject_an_empty_or_oversized_path_list() {
    let paths: Vec<String> = (0..=MAX_STATUS_PATHS)
        .map(|id| format!("1/{id}/"))
        .collect();
    let oversized: Vec<&str> = paths.iter().map(String::as_str).collect();

    for paths in [&[][..], &oversized[..]] {
        assert_eq!(
            indexing_status_error(paths).await,
            tonic::Code::InvalidArgument
        );
    }
    assert_eq!(
        item_counts_error(item_counts_request("", &oversized)).await,
        tonic::Code::InvalidArgument
    );
}

#[tokio::test]
async fn status_rpcs_reject_a_malformed_path() {
    for path in ["", "1/abc/", "1/22"] {
        assert_eq!(
            indexing_status_error(&[path]).await,
            tonic::Code::InvalidArgument,
            "indexing status accepted {path:?}"
        );
        assert_eq!(
            item_counts_error(item_counts_request(path, &[])).await,
            tonic::Code::InvalidArgument,
            "item counts accepted {path:?}"
        );
        assert_eq!(
            item_counts_error(item_counts_request("", &["1/22/", path])).await,
            tonic::Code::InvalidArgument,
            "batched item counts accepted {path:?}"
        );
        assert_eq!(
            graph_status_error(path, test_claims()).await,
            tonic::Code::InvalidArgument,
            "graph status accepted {path:?}"
        );
    }
}

#[tokio::test]
async fn graph_status_rejects_a_malformed_path_from_an_admin() {
    let admin = Claims {
        admin: true,
        organization_id: Some(1),
        ..test_claims()
    };

    assert_eq!(
        graph_status_error("1/22/<script>", admin).await,
        tonic::Code::InvalidArgument
    );
}

#[tokio::test]
async fn status_rpcs_deny_a_path_outside_the_callers_groups() {
    assert_eq!(
        indexing_status_error(&["1/22/"]).await,
        tonic::Code::PermissionDenied
    );
    assert_eq!(
        item_counts_error(item_counts_request("1/33/", &[])).await,
        tonic::Code::PermissionDenied
    );
    assert_eq!(
        item_counts_error(item_counts_request("", &["1/22/", "1/33/"])).await,
        tonic::Code::PermissionDenied
    );
}

#[tokio::test]
async fn item_counts_answers_in_the_field_the_request_used() {
    let single = item_counts(item_counts_request("1/22/", &[]))
        .await
        .unwrap();
    let batched = item_counts(item_counts_request("", &["1/22/", "1/22/44/"]))
        .await
        .unwrap();

    assert!(!single.domains.is_empty() && single.namespaces.is_empty());
    let paths: Vec<&str> = batched
        .namespaces
        .iter()
        .map(|namespace| namespace.traversal_path.as_str())
        .collect();
    assert_eq!(
        (batched.domains.len(), paths),
        (0, vec!["1/22/", "1/22/44/"])
    );
    assert_eq!(
        item_counts_error(item_counts_request("1/22/", &["1/22/"])).await,
        tonic::Code::InvalidArgument
    );
}
