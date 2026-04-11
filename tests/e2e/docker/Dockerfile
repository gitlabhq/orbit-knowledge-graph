# gkg-e2e-base: GDK + ClickHouse + NATS + Siphon for e2e testing
#
# Build:  docker build -t gkg-e2e-base tests/e2e/docker/
# Run:    docker run -d --name gkg-e2e \
#           -p 13443:3443 -p 28123:8123 -p 24222:4222 \
#           --add-host gdk.test:127.0.0.1 gkg-e2e-base
#
# Two-phase setup:
# 1. Build time: Configure GDK with HTTPS/nginx (phase 1)
# 2. Runtime: Configure ClickHouse/NATS/Siphon, seed data (phase 2)
#
# Phase 2 runs at container startup because ClickHouse and NATS
# need a running init system which isn't available during docker build.

FROM registry.gitlab.com/gitlab-org/gitlab-development-kit/gitlab-gdk-in-a-box:latest

USER root

# Install mkcert for self-signed HTTPS certs
RUN ARCH=$(dpkg --print-architecture) \
    && curl -fsSL "https://dl.filippo.io/mkcert/latest?for=linux/${ARCH}" -o /usr/local/bin/mkcert \
    && chmod +x /usr/local/bin/mkcert

# Copy config and scripts
COPY gdk-phase1.yml /tmp/gdk-phase1-overlay.yml
COPY gdk.yml /tmp/gdk-e2e-overlay.yml
COPY setup.sh /home/gdk/setup.sh
COPY entrypoint.sh /home/gdk/entrypoint.sh
COPY seed-data.rb /home/gdk/seed-data.rb
RUN chmod +x /home/gdk/setup.sh /home/gdk/entrypoint.sh

EXPOSE 3443 8123 4222

USER gdk
WORKDIR /gitlab-gdk/gitlab-development-kit

# Generate mkcert CA and certs for gdk.test
RUN mkcert -install \
    && mkcert -cert-file gdk.test.pem -key-file gdk.test-key.pem gdk.test localhost 127.0.0.1

# Phase 1: Configure GDK with HTTPS/nginx (no ClickHouse/NATS/Siphon)
RUN /home/gdk/setup.sh

# Phase 2 runs at container startup via the entrypoint
ENTRYPOINT ["/home/gdk/entrypoint.sh"]
