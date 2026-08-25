# Cluster-level dependencies installed after the GKE cluster is created.
# These must exist before the e2e workload stack (provision.sh) can deploy.

resource "helm_release" "cert_manager" {
  name             = "cert-manager"
  repository       = "https://charts.jetstack.io"
  chart            = "cert-manager"
  namespace        = "cert-manager"
  create_namespace = true
  wait             = true

  set {
    name  = "crds.enabled"
    value = "true"
  }

  depends_on = [google_container_node_pool.workload]
}

resource "kubernetes_manifest" "selfsigned_issuer" {
  manifest = {
    apiVersion = "cert-manager.io/v1"
    kind       = "ClusterIssuer"
    metadata = {
      name = "selfsigned-issuer"
    }
    spec = {
      selfSigned = {}
    }
  }

  depends_on = [helm_release.cert_manager]
}

resource "kubernetes_manifest" "root_ca" {
  manifest = {
    apiVersion = "cert-manager.io/v1"
    kind       = "Certificate"
    metadata = {
      name      = "root-ca"
      namespace = "cert-manager"
    }
    spec = {
      isCA       = true
      commonName = "e2e-root-ca"
      secretName = "root-ca-secret"
      duration   = "87600h"
      issuerRef = {
        name = "selfsigned-issuer"
        kind = "ClusterIssuer"
      }
    }
  }

  depends_on = [kubernetes_manifest.selfsigned_issuer]
}

resource "kubernetes_manifest" "ca_issuer" {
  manifest = {
    apiVersion = "cert-manager.io/v1"
    kind       = "ClusterIssuer"
    metadata = {
      name = "ca-issuer"
    }
    spec = {
      ca = {
        secretName = "root-ca-secret"
      }
    }
  }

  depends_on = [kubernetes_manifest.root_ca]
}

resource "helm_release" "prometheus_operator_crds" {
  name             = "prometheus-operator-crds"
  repository       = "https://prometheus-community.github.io/helm-charts"
  chart            = "prometheus-operator-crds"
  namespace        = "monitoring"
  create_namespace = true

  depends_on = [google_container_node_pool.workload]
}
