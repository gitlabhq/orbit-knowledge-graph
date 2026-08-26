# Cluster-level dependencies installed after the GKE cluster is created.
# These must exist before the e2e workload stack (provision.sh) can deploy.
#
# cert-manager CRs (ClusterIssuers, root CA Certificate) are applied by
# infra.sh after this stack, because kubernetes_manifest does plan-time
# API discovery which fails when the CRDs do not yet exist.

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

resource "helm_release" "prometheus_operator_crds" {
  name             = "prometheus-operator-crds"
  repository       = "https://prometheus-community.github.io/helm-charts"
  chart            = "prometheus-operator-crds"
  namespace        = "monitoring"
  create_namespace = true

  depends_on = [google_container_node_pool.workload]
}
