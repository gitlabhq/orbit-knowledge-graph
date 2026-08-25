# One-time bootstrap: creates the GCS bucket that stores Terraform state
# for the main bench stack. Run once, local state only.
#
# Usage:
#   cd bench/infra/bootstrap
#   terraform init
#   terraform apply

terraform {
  required_version = ">= 1.5"

  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 6.36"
    }
  }
}

locals {
  bench = yamldecode(file("${path.module}/../../config/bench.yaml"))
}

provider "google" {
  project = local.bench.project
}

resource "google_storage_bucket" "tf_state" {
  name                        = local.bench.buckets.tf_state
  location                    = local.bench.region
  uniform_bucket_level_access = true
  public_access_prevention    = "enforced"
  force_destroy               = false

  versioning {
    enabled = true
  }
}
