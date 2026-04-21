FROM registry.gitlab.com/gitlab-org/rust/build-images/orbit-knowledge-graph:latest AS builder

WORKDIR /build
COPY . .

RUN cargo build --release -p gkg-server --locked && \
    cp target/release/gkg-server /gkg-server

FROM registry.access.redhat.com/ubi10/ubi-minimal:10.1

ARG GKG_VERSION=dev
ENV GKG_VERSION=$GKG_VERSION

WORKDIR /app

COPY --from=builder /gkg-server /usr/local/bin/gkg-server

ENTRYPOINT ["gkg-server"]
