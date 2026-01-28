# Tiltfile for local Knowledge Graph development
# Requires: colima with kubernetes enabled

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
'''.format(
    postgres_password=secrets.get('POSTGRES_PASSWORD', ''),
    clickhouse_password=secrets.get('CLICKHOUSE_PASSWORD', ''),
)

k8s_yaml(blob(secrets_yaml))

# Build helm dependencies (gitlab-runner chart is gitignored)
local('helm repo add gitlab https://charts.gitlab.io 2>/dev/null || true', quiet=True)
local('helm dependency build ./helm-dev', quiet=True)

# Deploy helm chart with local values
k8s_yaml(helm(
    './helm-dev',
    name='gkg',
    namespace='default',
    values=['./helm-dev/values-local.yaml'],
))
