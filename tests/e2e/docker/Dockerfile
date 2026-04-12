# gkg-e2e-base: GDK + ClickHouse + NATS + Siphon for e2e testing
#
# Build:  docker build -t gkg-e2e-base tests/e2e/docker/
# Run:    docker run -d --name gkg-e2e -p 3000:3000 -p 8123:8123 -p 4222:4222 gkg-e2e-base
#
# Uses HTTP (no nginx/HTTPS) matching GitLab's own e2e:test-on-gdk CI pattern.
# All services bind to 0.0.0.0 for CI service networking.

FROM registry.gitlab.com/gitlab-org/gitlab-development-kit/gitlab-gdk-in-a-box:latest

USER root

# Install ClickHouse (system package, same as VM setup)
RUN apt-get update -qq \
    && apt-get install -y -qq --no-install-recommends \
        apt-transport-https ca-certificates curl gnupg \
    && curl -fsSL 'https://packages.clickhouse.com/rpm/lts/repodata/repomd.xml.key' \
        | gpg --dearmor -o /usr/share/keyrings/clickhouse-keyring.gpg \
    && echo "deb [signed-by=/usr/share/keyrings/clickhouse-keyring.gpg] https://packages.clickhouse.com/deb stable main" \
        > /etc/apt/sources.list.d/clickhouse.list \
    && apt-get update -qq \
    && DEBIAN_FRONTEND=noninteractive apt-get install -y -qq --no-install-recommends \
        clickhouse-server clickhouse-client \
    && rm -rf /var/lib/apt/lists/*

# Install NATS server
RUN ARCH=$(dpkg --print-architecture) \
    && curl -fsSL "https://github.com/nats-io/nats-server/releases/download/v2.11.4/nats-server-v2.11.4-linux-${ARCH}.tar.gz" \
        | tar xz --strip-components=1 -C /usr/local/bin/ "nats-server-v2.11.4-linux-${ARCH}/nats-server"

# Copy config and scripts
COPY gdk.yml /tmp/gdk-e2e-overlay.yml
COPY setup.sh /home/gdk/setup.sh
COPY entrypoint.sh /home/gdk/entrypoint.sh
COPY seed-data.rb /home/gdk/seed-data.rb
RUN chmod +x /home/gdk/setup.sh /home/gdk/entrypoint.sh

EXPOSE 3000 8123 4222

USER gdk
WORKDIR /gitlab-gdk/gitlab-development-kit

# Configure GDK with ClickHouse, NATS, Siphon (HTTP, no nginx)
RUN /home/gdk/setup.sh

ENTRYPOINT ["/home/gdk/entrypoint.sh"]
