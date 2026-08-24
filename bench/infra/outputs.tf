output "cluster_name" {
  value = google_container_cluster.bench.name
}

output "cluster_location" {
  value = google_container_cluster.bench.location
}

output "kctx" {
  description = "The kubectl context string for KCTX=."
  value       = "gke_${local.project}_${local.zone}_${google_container_cluster.bench.name}"
}

output "tier" {
  value = var.tier
}

output "node_pool_machine_type" {
  value = local.tier.nodes.machine
}

output "node_pool_count" {
  value = local.tier.nodes.count
}

output "datalake_dumps_bucket" {
  value = google_storage_bucket.datalake_dumps.name
}

output "code_corpus_bucket" {
  value = google_storage_bucket.code_corpus.name
}

output "dedicated_ch_pool" {
  value = var.dedicated_ch_pool
}

output "node_sa_email" {
  value = google_service_account.node.email
}

output "project" {
  value = local.project
}
