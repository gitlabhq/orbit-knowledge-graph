resource "google_storage_bucket" "datalake_dumps" {
  name                        = local.bench.buckets.datalake_dumps
  location                    = local.region
  uniform_bucket_level_access = true
  public_access_prevention    = "enforced"
  force_destroy               = false
}

resource "google_storage_bucket" "code_corpus" {
  name                        = local.bench.buckets.code_corpus
  location                    = local.region
  uniform_bucket_level_access = true
  public_access_prevention    = "enforced"
  force_destroy               = false
}

resource "google_storage_bucket_iam_member" "node_reads_dumps" {
  bucket = google_storage_bucket.datalake_dumps.name
  role   = "roles/storage.objectViewer"
  member = google_service_account.node.member
}

resource "google_storage_bucket_iam_member" "node_reads_corpus" {
  bucket = google_storage_bucket.code_corpus.name
  role   = "roles/storage.objectViewer"
  member = google_service_account.node.member
}
