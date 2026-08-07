FROM registry.gitlab.com/gitlab-org/rust/build-images/orbit-knowledge-graph:latest AS builder

WORKDIR /build
COPY . .
RUN cargo build --release -p xtask && \
    cp target/release/xtask /xtask

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates curl && \
    rm -rf /var/lib/apt/lists/*
COPY --from=builder /xtask /usr/local/bin/xtask
COPY bench/query-mixes/ /bench/query-mixes/
ENTRYPOINT ["xtask"]
