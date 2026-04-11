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

Then set GDK_ROOT to your GDK installation path.
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

gdk_root = secrets.get('GDK_ROOT', '')
if not gdk_root:
    fail('GDK_ROOT must be set in .tilt-secrets (path to your GDK installation)')

jwt_key_path = os.path.join(gdk_root, 'gitlab', '.gitlab_knowledge_graph_secret')
if not os.path.exists(jwt_key_path):
    fail('JWT key not found at %s — is GDK configured for Knowledge Graph?' % jwt_key_path)

jwt_secret = str(read_file(jwt_key_path)).strip()
clickhouse_password = secrets.get('CLICKHOUSE_PASSWORD', '')

# Single secret matching the official chart's secrets.keys structure
secrets_yaml = '''apiVersion: v1
kind: Secret
metadata:
  name: gkg-secrets
type: Opaque
stringData:
  gitlab-jwt-verifying-key: "{jwt}"
  gitlab-jwt-signing-key: "{jwt}"
  datalake-password: "{ch}"
  graph-password: "{ch}"
  graph-read-password: "{ch}"
'''.format(jwt=jwt_secret, ch=clickhouse_password)

k8s_yaml(blob(secrets_yaml))

# Build gkg-server: use Docker with cached volumes for fast incremental builds
custom_build(
    'gkg-server',
    './scripts/build-dev.sh $EXPECTED_REF',
    deps=['crates/', 'Cargo.toml', 'Cargo.lock'],
)

# Install PodMonitor CRD (the GKG chart creates PodMonitor resources)
PROMETHEUS_OPERATOR_VERSION = 'v0.90.0'
local(
    'kubectl apply --server-side -f https://raw.githubusercontent.com/prometheus-operator/prometheus-operator/{}/example/prometheus-operator-crd/monitoring.coreos.com_podmonitors.yaml 2>/dev/null || true'.format(PROMETHEUS_OPERATOR_VERSION),
    quiet=True
)

# Deploy local observability (standalone Prometheus + Grafana)
k8s_yaml('helm/local/prometheus.yaml')
# Build dashboards ConfigMap from JSON files and deploy Grafana
dashboards_cm = '''apiVersion: v1
kind: ConfigMap
metadata:
  name: grafana-dashboards
data:
'''
for f in listdir('helm/local/dashboards'):
    name = os.path.basename(f)
    if name.endswith('.json'):
        content = str(read_file(f))
        dashboards_cm += '  {}: |\n'.format(name)
        for line in content.split('\n'):
            dashboards_cm += '    {}\n'.format(line)
k8s_yaml(blob(dashboards_cm))
k8s_yaml('helm/local/grafana.yaml')

# Vendor helm chart on fresh clone only; re-sync manually with `helm/sync.sh`
if not os.path.exists('helm/gkg/Chart.yaml'):
    local('helm/sync.sh')

# Deploy gkg chart (vendored official chart + patches)
k8s_yaml(helm(
    './helm/gkg',
    name='gkg',
    namespace='default',
    values=['./helm/values/gkg-local.yaml'],
))

# Skip readiness checks for components that may take time to connect
k8s_resource('gkg-dispatcher', pod_readiness='ignore')
k8s_resource('gkg-indexer', pod_readiness='ignore')
k8s_resource('gkg-webserver', pod_readiness='ignore', port_forwards=['8080:8080', '50054:50054'])
k8s_resource('gkg-health-check', pod_readiness='ignore', port_forwards=['4201:4201'])
k8s_resource('grafana', port_forwards=['3030:3000'])
k8s_resource('prometheus', port_forwards=['9090:9090'])
