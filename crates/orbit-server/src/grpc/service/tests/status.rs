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

async fn item_counts_error(path: &str) -> tonic::Code {
    let request = GetItemCountsRequest {
        traversal_path: path.to_string(),
    };
    test_service()
        .get_item_counts(authed_request(request))
        .await
        .unwrap_err()
        .code()
}

#[tokio::test]
async fn indexing_status_rejects_an_empty_or_oversized_path_list() {
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
            item_counts_error(path).await,
            tonic::Code::InvalidArgument,
            "item counts accepted {path:?}"
        );
    }
}

#[tokio::test]
async fn status_rpcs_deny_a_path_outside_the_callers_groups() {
    assert_eq!(
        indexing_status_error(&["1/22/"]).await,
        tonic::Code::PermissionDenied
    );
    assert_eq!(
        item_counts_error("1/22/").await,
        tonic::Code::PermissionDenied
    );
}
