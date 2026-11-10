terraform {
  required_version = ">= 1.5"

  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 6.36"
    }
  }

  backend "gcs" {
    bucket = "gkg-tf-state-f2eec59d"
    prefix = "fuzz"
  }
}

variable "fuzz_corpus_bucket" {
  type = string
}

variable "fuzz_crashes_bucket" {
  type = string
}

provider "google" {
  project = "gl-knowledgegraph-prj-f2eec59d"
}

data "google_project" "this" {}

locals {
  region            = "us-east1"
  gitlab_project_id = "77960826"
  audience          = "gkg-fuzz"
  pool_principals   = "principalSet://iam.googleapis.com/${google_iam_workload_identity_pool.ci.name}"
  owners            = "projectOwner:${data.google_project.this.project_id}"
}

output "fuzz_corpus_wif_provider" {
  value     = "//iam.googleapis.com/${google_iam_workload_identity_pool_provider.gitlab.name}"
  sensitive = true
}
