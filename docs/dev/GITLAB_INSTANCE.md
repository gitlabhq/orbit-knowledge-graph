# Linux package Instance

VM: `vm-gitlab-omnibus` (10.128.0.4)

## Secrets

### Gitaly Token

Used for authenticated gRPC calls to Gitaly.

| Location | Path |
|----------|------|
| GCP Secret Manager | `gitaly-token` |
| VM | `/var/opt/gitlab/gitaly/config.toml` (`[auth].token`) |

### Shell Secret (JWT)

Used to sign JWT tokens for GitLab internal API authentication (e.g., `/api/v4/internal/*` endpoints).

| Location | Path |
|----------|------|
| GCP Secret Manager | `gkg-jwt-secret` |
| VM | `/var/opt/gitlab/gitlab-rails/etc/gitlab_shell_secret` |
| VM (also in) | `/etc/gitlab/gitlab-secrets.json` (`gitlab_shell.secret_token`) |

## Knowledge Graph Integration

Configure GitLab to connect to the gkg-webserver running in GKE.

Add to `/etc/gitlab/gitlab.rb`:

```ruby
gitlab_rails['knowledge_graph_base_url'] = 'http://<GKG_WEBSERVER_INTERNAL_IP>:8080'
```

Static internal IP: `10.128.0.51` (reserved as `gkg-webserver-ip` in GCP, persists across redeployments).

**Ports:**

- HTTP API: `8080`
- gRPC: `50051`

Add to `gitlab_rails['env']` in `/etc/gitlab/gitlab.rb`:

```ruby
gitlab_rails['env'] = {
  # ... existing env vars ...
  'KNOWLEDGE_GRAPH_BASE_URL' => 'http://10.128.0.51:8080',
  'KNOWLEDGE_GRAPH_GRPC_ENDPOINT' => '10.128.0.51:50051'
}
```

Then reconfigure:

```shell
sudo gitlab-ctl reconfigure
```

Verify connectivity:

```shell
curl "http://10.128.0.51:8080/health"
```

## AI Gateway / Duo (Staging)

When using a license from the staging CustomersDot, GitLab must be configured to use staging AI endpoints.

Add to `/etc/gitlab/gitlab.rb`:

```ruby
gitlab_rails['env'] = {
  'GITLAB_LICENSE_MODE' => 'test',
  'AI_GATEWAY_URL' => 'https://cloud.staging.gitlab.com/ai',
  'CUSTOMER_PORTAL_URL' => 'https://customers.staging.gitlab.com',
  'CLOUD_CONNECTOR_BASE_URL' => 'https://cloud.staging.gitlab.com'
}
```

Then reconfigure and restart:

```shell
sudo gitlab-ctl reconfigure
sudo gitlab-ctl restart
```

Verify in **Admin > GitLab Duo** health check.

## PostgreSQL Password

**Configuration:**

- Do NOT set `postgresql['sql_user_password']` in gitlab.rb
- PostgreSQL uses `password_encryption = md5` (set via `ALTER SYSTEM`, persists in postgresql.auto.conf)
- Password is set once manually and persists across reconfigures

**Initial setup (already done):**

```shell
# Set MD5 password encryption
sudo gitlab-psql -c "ALTER SYSTEM SET password_encryption = 'md5'"
sudo gitlab-ctl restart postgresql

# Set password matching GCP Secret Manager
sudo gitlab-psql -c "ALTER USER gitlab WITH PASSWORD '<password>'"
```

**If password breaks after upgrade:**

```shell
# Get password from GCP Secret Manager
gcloud secrets versions access latest --secret=postgres-password --project=gl-knowledgegraph-prj-f2eec59d

# Reset password
sudo gitlab-psql -c "ALTER USER gitlab WITH PASSWORD '<password>'"

# Restart siphon-producer
kubectl rollout restart deployment/siphon-producer -n gkg
```

## Related Documentation

- [GitLab Internal API](https://docs.gitlab.com/ee/development/internal_api/) - JWT authentication details
- [Configure Gitaly](https://docs.gitlab.com/administration/gitaly/configure_gitaly/) - Gitaly token configuration
