FROM registry.gitlab.com/gitlab-org/orbit/build-images/rust-builder:latest AS builder

ARG GKG_VERSION=dev

WORKDIR /build
COPY . .

ENV GKG_VERSION=$GKG_VERSION

RUN cargo build --release --package gkg-server && \
    cp target/release/gkg-server /gkg-server

FROM registry.access.redhat.com/ubi10/ubi-minimal:10.1

WORKDIR /app

COPY --from=builder /gkg-server /usr/local/bin/gkg-server

ENTRYPOINT ["gkg-server"]
