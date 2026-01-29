FROM registry.gitlab.com/gitlab-org/orbit/build-images/rust-builder:latest AS builder

WORKDIR /build
COPY . .

RUN cargo build --release --package gkg-server

FROM gcr.io/distroless/cc-debian12

COPY --from=builder /build/target/release/gkg-server /usr/local/bin/gkg-server

ENTRYPOINT ["gkg-server"]
