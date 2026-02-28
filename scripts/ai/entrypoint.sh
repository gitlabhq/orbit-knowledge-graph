#!/bin/sh
# Generate a self-signed TLS cert for the GitLab API proxy (port 8083).
# The cert is ephemeral — created fresh on each container start.
openssl req -x509 -nodes -newkey rsa:2048 \
  -keyout /tmp/k.pem -out /tmp/c.pem \
  -days 1 -subj '/CN=api-proxy' \
  -addext 'subjectAltName=DNS:api-proxy' 2>/dev/null

exec bun proxy.ts
