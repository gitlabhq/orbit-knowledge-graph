FROM registry.gitlab.com/gitlab-org/orbit/build-images/rust-builder:latest AS builder

ARG GKG_VERSION=dev

WORKDIR /build
COPY . .

ENV GKG_VERSION=$GKG_VERSION

RUN cargo build --release --workspace --locked \
    --exclude integration-tests --exclude integration-testkit \
    --features duckdb-client/bundled && \
    cp target/release/gkg-server /gkg-server

FROM registry.access.redhat.com/ubi10/ubi-minimal:10.1

WORKDIR /app

COPY --from=builder /gkg-server /usr/local/bin/gkg-server

ENTRYPOINT ["gkg-server"]
