resource "google_service_account" "node" {
  account_id   = "${var.cluster_name}-node"
  display_name = "RA bench GKE nodes (minimal)"
}

resource "google_project_iam_member" "node_log_writer" {
  project = local.project
  role    = "roles/logging.logWriter"
  member  = google_service_account.node.member
}

resource "google_project_iam_member" "node_metric_writer" {
  project = local.project
  role    = "roles/monitoring.metricWriter"
  member  = google_service_account.node.member
}

resource "google_project_iam_member" "node_monitoring_viewer" {
  project = local.project
  role    = "roles/monitoring.viewer"
  member  = google_service_account.node.member
}

resource "google_project_iam_member" "node_artifact_reader" {
  project = local.project
  role    = "roles/artifactregistry.reader"
  member  = google_service_account.node.member
}


