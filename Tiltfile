# Tiltfile for local Knowledge Graph development
# Requires: colima with kubernetes enabled

update_settings(k8s_upsert_timeout_secs=600)
ci_settings(readiness_timeout='10m')

# Only allow local contexts to prevent accidental GCP deployments
# Change context with: kubectl config use-context <context>
allow_k8s_contexts(['colima', 'docker-desktop', 'minikube', 'kind-kind', 'rancher-desktop'])

# Read secrets from gitignored file
def load_secrets():
    secrets_file = '.tilt-secrets'
    if not os.path.exists(secrets_file):
        fail('''
Missing .tilt-secrets file. Create it with:

  cp .tilt-secrets.example .tilt-secrets

Then fill in passwords from GDK config.
''')

    secrets = {}
    content = str(read_file(secrets_file))
    for line in content.strip().split('\n'):
        line = line.strip()
        if not line or line.startswith('#'):
            continue
        if '=' in line:
            key, value = line.split('=', 1)
            secrets[key.strip()] = value.strip()
    return secrets

secrets = load_secrets()

# Generate secrets YAML from loaded values
secrets_yaml = '''
apiVersion: v1
kind: Secret
metadata:
  name: postgres-credentials
type: Opaque
stringData:
  password: "{postgres_password}"
---
apiVersion: v1
kind: Secret
metadata:
  name: clickhouse-credentials
type: Opaque
stringData:
  password: "{clickhouse_password}"
---
apiVersion: v1
kind: Secret
metadata:
  name: gkg-server-credentials
type: Opaque
stringData:
  jwt-secret: "{jwt_secret}"
'''.format(
    postgres_password=secrets.get('POSTGRES_PASSWORD', ''),
    clickhouse_password=secrets.get('CLICKHOUSE_PASSWORD', ''),
    jwt_secret=secrets.get('GKG_JWT_SECRET', ''),
)

k8s_yaml(blob(secrets_yaml))

# Build gkg-server: use Docker with cached volumes for fast incremental builds
custom_build(
    'gkg-server',
    './scripts/build-dev.sh $EXPECTED_REF',
    deps=['crates/', 'Cargo.toml', 'Cargo.lock'],
)

# Build helm dependencies
local('helm repo add gitlab https://charts.gitlab.io 2>/dev/null || true', quiet=True)
local('helm repo add nats https://nats-io.github.io/k8s/helm/charts/ 2>/dev/null || true', quiet=True)
local('helm repo add prometheus-community https://prometheus-community.github.io/helm-charts 2>/dev/null || true', quiet=True)
local('helm repo add grafana https://grafana.github.io/helm-charts 2>/dev/null || true', quiet=True)
local('helm dependency build ./helm-dev/gkg', quiet=True)
local('helm dependency build ./helm-dev/observability', quiet=True)

# Install Prometheus Operator CRDs (required for kube-prometheus-stack)
PROMETHEUS_OPERATOR_VERSION = 'v0.88.1'
PROMETHEUS_CRDS = [
    'alertmanagerconfigs', 'alertmanagers', 'podmonitors', 'probes',
    'prometheusagents', 'prometheuses', 'prometheusrules', 'scrapeconfigs',
    'servicemonitors', 'thanosrulers'
]
for crd in PROMETHEUS_CRDS:
    local(
        'kubectl apply --server-side -f https://raw.githubusercontent.com/prometheus-operator/prometheus-operator/{}/example/prometheus-operator-crd/monitoring.coreos.com_{}.yaml 2>/dev/null || true'.format(PROMETHEUS_OPERATOR_VERSION, crd),
        quiet=True
    )

# Deploy observability chart first (so OTEL endpoint is available)
k8s_yaml(helm(
    './helm-dev/observability',
    name='gkg-obs',
    namespace='default',
    values=['./helm-dev/observability/values-local.yaml'],
))

# Deploy gkg chart
k8s_yaml(helm(
    './helm-dev/gkg',
    name='gkg',
    namespace='default',
    values=['./helm-dev/gkg/values-local.yaml'],
))

# Skip readiness checks for components that may take time to connect
k8s_resource('gkg-indexer', pod_readiness='ignore')
k8s_resource('gkg-webserver', pod_readiness='ignore', port_forwards=['8080:8080'])
k8s_resource('gkg-health-check', pod_readiness='ignore', port_forwards=['4201:4201'])
