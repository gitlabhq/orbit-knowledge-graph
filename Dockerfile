# syntax=docker/dockerfile:1.7
FROM registry.gitlab.com/gitlab-org/rust/build-images/orbit-knowledge-graph:latest AS builder

WORKDIR /build
COPY . .

ARG ORBIT_BILLING_ENFORCED=false
ENV CARGO_INCREMENTAL=0

RUN --mount=type=secret,id=sccache_gcs_key \
    --mount=type=cache,target=/root/.cache/sccache \
    --mount=type=cache,target=/build/target \
    SCCACHE_BIN="$(mise which sccache)" && \
    if [ -s /run/secrets/sccache_gcs_key ]; then \
      export SCCACHE_GCS_KEY_PATH=/run/secrets/sccache_gcs_key \
             SCCACHE_GCS_BUCKET=gl-knowledgegraph-sccache \
             SCCACHE_GCS_RW_MODE=READ_WRITE; \
    fi && \
    export RUSTC_WRAPPER="$SCCACHE_BIN" ORBIT_BILLING_ENFORCED="${ORBIT_BILLING_ENFORCED}" && \
    "$SCCACHE_BIN" --start-server || true && \
    cargo build --release -p orbit-server --locked && \
    "$SCCACHE_BIN" --show-stats || true && \
    ./scripts/checks/fips/check-binary.sh target/release/gkg-server && \
    cp target/release/gkg-server /gkg-server

FROM registry.access.redhat.com/ubi10/ubi-minimal:10.1

ARG GKG_VERSION=dev
ENV GKG_VERSION=$GKG_VERSION

LABEL com.gitlab.image.fips="true" \
      com.gitlab.fips.module="AWS-LC"

WORKDIR /app

COPY --from=builder /gkg-server /usr/local/bin/gkg-server

# ClickHouse setup contract, shipped in the image so deployments read it at the exact version they run.
COPY config/clickhouse-setup.sql /usr/share/gkg/clickhouse-setup.sql

# Grafana dashboards, shipped in the image so deployments apply the flavor matching the running code.
COPY dashboards/dedicated/*.dashboard.json /usr/share/gkg/dashboards/dedicated/
COPY dashboards/orbit/*.dashboard.json /usr/share/gkg/dashboards/com/

# UID matches the chart's runAsUser; group 0 lets OpenShift's arbitrary UIDs keep the same access.
RUN echo 'gkg:x:65532:0::/nonexistent:/usr/sbin/nologin' >> /etc/passwd && \
    find / -xdev -perm /6000 -type f -exec chmod a-s {} +

USER 65532:0

ENTRYPOINT ["gkg-server"]
