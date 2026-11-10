resource "google_storage_bucket" "fuzz_corpus" {
  name                        = var.fuzz_corpus_bucket
  location                    = local.region
  uniform_bucket_level_access = true
  public_access_prevention    = "enforced"
  force_destroy               = false

  versioning {
    enabled = true
  }

  lifecycle_rule {
    condition {
      days_since_noncurrent_time = 30
    }
    action {
      type = "Delete"
    }
  }
}

data "google_iam_policy" "fuzz_corpus" {
  binding {
    role    = "roles/storage.legacyBucketOwner"
    members = [local.owners]
  }
  binding {
    role    = "roles/storage.legacyObjectOwner"
    members = [local.owners]
  }
  binding {
    role    = "roles/storage.objectViewer"
    members = ["${local.pool_principals}/*"]
  }
  binding {
    role    = "roles/storage.objectUser"
    members = ["${local.pool_principals}/attribute.fuzz_writer/true"]
  }
}

resource "google_storage_bucket_iam_policy" "fuzz_corpus" {
  bucket      = google_storage_bucket.fuzz_corpus.name
  policy_data = data.google_iam_policy.fuzz_corpus.policy_data
}

resource "google_storage_bucket" "fuzz_crashes" {
  name                        = var.fuzz_crashes_bucket
  location                    = local.region
  uniform_bucket_level_access = true
  public_access_prevention    = "enforced"
  force_destroy               = false

  lifecycle_rule {
    condition {
      age = 180
    }
    action {
      type = "Delete"
    }
  }
}

data "google_iam_policy" "fuzz_crashes" {
  binding {
    role    = "roles/storage.legacyBucketOwner"
    members = [local.owners]
  }
  binding {
    role    = "roles/storage.legacyObjectOwner"
    members = [local.owners]
  }
  binding {
    role    = "roles/storage.objectCreator"
    members = ["${local.pool_principals}/*"]
  }
}

resource "google_storage_bucket_iam_policy" "fuzz_crashes" {
  bucket      = google_storage_bucket.fuzz_crashes.name
  policy_data = data.google_iam_policy.fuzz_crashes.policy_data
}
