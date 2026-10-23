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

#[allow(deprecated)]
fn item_counts_request(path: &str, paths: &[&str]) -> GetItemCountsRequest {
    GetItemCountsRequest {
        traversal_path: path.to_string(),
        traversal_paths: paths.iter().map(|path| path.to_string()).collect(),
    }
}

async fn item_counts_error(request: GetItemCountsRequest) -> tonic::Code {
    test_service()
        .get_item_counts(authed_request(request))
        .await
        .unwrap_err()
        .code()
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
        item_counts_error(item_counts_request("1/22/", &[])).await,
        tonic::Code::PermissionDenied
    );
}

#[tokio::test]
async fn item_counts_rejects_a_request_with_both_path_fields() {
    assert_eq!(
        item_counts_error(item_counts_request("1/22/", &["1/22/"])).await,
        tonic::Code::InvalidArgument
    );
}
