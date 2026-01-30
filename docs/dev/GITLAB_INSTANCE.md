# GitLab Omnibus Instance

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

```bash
sudo gitlab-ctl reconfigure
sudo gitlab-ctl restart
```

Verify in **Admin > GitLab Duo** health check.

## Related Documentation

- [GitLab Internal API](https://docs.gitlab.com/ee/development/internal_api/) - JWT authentication details
- [Configure Gitaly](https://docs.gitlab.com/administration/gitaly/configure_gitaly/) - Gitaly token configuration
