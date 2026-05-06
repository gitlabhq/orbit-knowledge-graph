# syntax=docker/dockerfile:1.7
FROM registry.gitlab.com/gitlab-org/rust/build-images/orbit-knowledge-graph:latest AS builder

WORKDIR /build
COPY . .

ARG SCCACHE_GCS_BUCKET=gl-knowledgegraph-sccache
ENV CARGO_INCREMENTAL=0

RUN --mount=type=secret,id=sccache_gcs_key \
    --mount=type=cache,target=/root/.cache/sccache \
    --mount=type=cache,target=/build/target \
    if [ -s /run/secrets/sccache_gcs_key ]; then \
      export SCCACHE_GCS_KEY_PATH=/run/secrets/sccache_gcs_key \
             SCCACHE_GCS_BUCKET="${SCCACHE_GCS_BUCKET}" \
             SCCACHE_GCS_RW_MODE=READ_WRITE; \
    fi && \
    export RUSTC_WRAPPER=sccache && \
    sccache --start-server || true && \
    cargo build --release -p gkg-server --locked && \
    sccache --show-stats || true && \
    cp target/release/gkg-server /gkg-server

FROM registry.access.redhat.com/ubi10/ubi-minimal:10.1

ARG GKG_VERSION=dev
ENV GKG_VERSION=$GKG_VERSION

WORKDIR /app

COPY --from=builder /gkg-server /usr/local/bin/gkg-server

ENTRYPOINT ["gkg-server"]
