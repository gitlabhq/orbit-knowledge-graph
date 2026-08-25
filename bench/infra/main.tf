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
    prefix = "bench"
  }
}

locals {
  bench   = yamldecode(file("${path.module}/../config/bench.yaml"))
  tiers   = yamldecode(file("${path.module}/../config/tiers.yaml"))
  tier    = local.tiers.tiers[var.tier]
  project = local.bench.project
  region  = local.bench.region
  zone    = local.bench.zone
}

provider "google" {
  project = local.project
}
