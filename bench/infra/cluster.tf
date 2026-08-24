resource "google_container_cluster" "bench" {
  name     = var.cluster_name
  location = local.zone

  remove_default_node_pool = true
  initial_node_count       = 1
  deletion_protection      = false

  enable_shielded_nodes = true
  datapath_provider     = "ADVANCED_DATAPATH"

  release_channel {
    channel = "REGULAR"
  }

  network    = google_compute_network.bench.id
  subnetwork = google_compute_subnetwork.bench.id

  private_cluster_config {
    enable_private_nodes = true
  }

  control_plane_endpoints_config {
    dns_endpoint_config {
      allow_external_traffic = true
    }
    ip_endpoints_config {
      enabled = false
    }
  }

  ip_allocation_policy {
    cluster_secondary_range_name  = "pods"
    services_secondary_range_name = "services"
  }

  workload_identity_config {
    workload_pool = "${local.project}.svc.id.goog"
  }

  monitoring_config {
    enable_components = ["SYSTEM_COMPONENTS"]
    managed_prometheus {
      enabled = true
    }
  }
}

# Workload pool: GKG components, NATS, and the e2e stack run here.
# Sized per tier from tiers.yaml.
resource "google_container_node_pool" "workload" {
  name       = "workload"
  cluster    = google_container_cluster.bench.id
  node_count = local.tier.nodes.count

  node_config {
    machine_type = local.tier.nodes.machine

    workload_metadata_config {
      mode = "GKE_METADATA"
    }

    service_account = google_service_account.node.email
    oauth_scopes    = ["https://www.googleapis.com/auth/cloud-platform"]

    shielded_instance_config {
      enable_secure_boot          = true
      enable_integrity_monitoring = true
    }
  }
}

# Dedicated ClickHouse pool: tainted so only the CH StatefulSet schedules here.
# Created only when var.dedicated_ch_pool is true.
resource "google_container_node_pool" "clickhouse" {
  count = var.dedicated_ch_pool ? 1 : 0

  name       = "clickhouse"
  cluster    = google_container_cluster.bench.id
  node_count = 1

  node_config {
    machine_type = local.tier.nodes.machine

    taint {
      key    = "dedicated"
      value  = "clickhouse"
      effect = "NO_SCHEDULE"
    }

    labels = {
      dedicated = "clickhouse"
    }

    workload_metadata_config {
      mode = "GKE_METADATA"
    }

    service_account = google_service_account.node.email
    oauth_scopes    = ["https://www.googleapis.com/auth/cloud-platform"]

    shielded_instance_config {
      enable_secure_boot          = true
      enable_integrity_monitoring = true
    }
  }
}
