FROM --platform=$BUILDPLATFORM registry.gitlab.com/gitlab-org/orbit/build-images/rust-builder:latest AS builder

ARG TARGETARCH
ARG GKG_VERSION=dev

RUN <<EOF
  set -e
  case "$TARGETARCH" in
    arm64) RUST_TARGET=aarch64-unknown-linux-gnu ;;
    amd64) RUST_TARGET=x86_64-unknown-linux-gnu ;;
    *)     echo "unsupported arch: $TARGETARCH" && exit 1 ;;
  esac
  rustup target add "$RUST_TARGET"
  if [ "$TARGETARCH" = "arm64" ]; then
    apt-get update && apt-get install -y --no-install-recommends gcc-aarch64-linux-gnu && rm -rf /var/lib/apt/lists/*
  fi
EOF

WORKDIR /build
COPY . .

ENV GKG_VERSION=$GKG_VERSION
ENV CARGO_TARGET_AARCH64_UNKNOWN_LINUX_GNU_LINKER=aarch64-linux-gnu-gcc

RUN <<EOF
  set -e
  case "$TARGETARCH" in
    arm64) RUST_TARGET=aarch64-unknown-linux-gnu ;;
    *)     RUST_TARGET=x86_64-unknown-linux-gnu ;;
  esac
  cargo build --release --target "$RUST_TARGET" --package gkg-server
  cp "target/$RUST_TARGET/release/gkg-server" /gkg-server
EOF

FROM registry.access.redhat.com/ubi9/ubi-micro:latest

COPY --from=builder /gkg-server /usr/local/bin/gkg-server

ENTRYPOINT ["gkg-server"]
