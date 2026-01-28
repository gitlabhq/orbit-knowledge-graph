# GitLab Knowledge Graph

Coming soon.

## Development Environment Setup

Prerequisites: [mise](https://mise.jdx.dev/), [Tilt](https://tilt.dev/), and a local Kubernetes cluster (Colima, Docker Desktop, or similar).

### 1. Install dependencies

```bash
mise install
```

### 2. Configure secrets

```bash
cp .tilt-secrets.example .tilt-secrets
```

Edit `.tilt-secrets` and fill in passwords from your GDK config.

### 3. Start local environment

```bash
tilt up
```

### 4. Access services

- **Grafana**: http://localhost:30300 (admin/admin)
- **Tilt UI**: http://localhost:10350
