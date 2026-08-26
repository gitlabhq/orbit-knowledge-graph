resource "google_compute_network" "bench" {
  name                    = local.cluster_name
  auto_create_subnetworks = false
}

resource "google_compute_subnetwork" "bench" {
  name                     = local.cluster_name
  region                   = local.region
  network                  = google_compute_network.bench.id
  ip_cidr_range            = "10.0.0.0/20"
  private_ip_google_access = true

  secondary_ip_range {
    range_name    = "pods"
    ip_cidr_range = "10.16.0.0/14"
  }

  secondary_ip_range {
    range_name    = "services"
    ip_cidr_range = "10.4.0.0/20"
  }
}

resource "google_compute_router" "bench" {
  name    = local.cluster_name
  region  = local.region
  network = google_compute_network.bench.id
}

resource "google_compute_router_nat" "bench" {
  name                               = local.cluster_name
  router                             = google_compute_router.bench.name
  region                             = local.region
  nat_ip_allocate_option             = "AUTO_ONLY"
  source_subnetwork_ip_ranges_to_nat = "ALL_SUBNETWORKS_ALL_IP_RANGES"
}
