terraform {
  required_version = ">= 1.5"

  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 6.36"
    }
    helm = {
      source  = "hashicorp/helm"
      version = "~> 2.17"
    }

  }

  backend "gcs" {}
}

locals {
  bench        = yamldecode(file("${path.module}/../config/bench.yaml"))
  tiers        = yamldecode(file("${path.module}/../config/tiers.yaml"))
  tier         = local.tiers.tiers[var.tier]
  project      = local.bench.project
  region       = local.bench.region
  zone         = local.bench.zone
  cluster_name = var.cluster_name != "" ? var.cluster_name : "ra-bench-${var.tier}"
}

provider "google" {
  project = local.project
}

data "google_client_config" "default" {}

provider "helm" {
  kubernetes {
    host  = "https://${google_container_cluster.bench.endpoint}"
    token = data.google_client_config.default.access_token
  }
}


