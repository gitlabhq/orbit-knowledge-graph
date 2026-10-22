resource "google_iam_workload_identity_pool" "ci" {
  workload_identity_pool_id = "knowledge-graph-fuzz"
  display_name              = "knowledge-graph fuzz CI"
}

resource "google_iam_workload_identity_pool_provider" "gitlab" {
  workload_identity_pool_id          = google_iam_workload_identity_pool.ci.workload_identity_pool_id
  workload_identity_pool_provider_id = "gitlab"
  display_name                       = "gitlab.com"

  attribute_mapping = {
    "google.subject"        = "assertion.project_id + ':' + assertion.job_id"
    "attribute.fuzz_writer" = "assertion.ref_type == 'branch' && assertion.ref == 'main' && assertion.ref_protected == 'true' && assertion.pipeline_source == 'schedule' ? 'true' : 'false'"
  }

  attribute_condition = "assertion.project_id == '${local.gitlab_project_id}' && assertion.runner_environment == 'gitlab-hosted'"

  oidc {
    issuer_uri        = "https://gitlab.com"
    allowed_audiences = [local.audience]
  }
}
