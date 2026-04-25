FROM ubuntu:24.04

ENV DEBIAN_FRONTEND=noninteractive
ENV MISE_YES=1

RUN apt-get update && apt-get install -y --no-install-recommends \
    curl ca-certificates git build-essential && \
    rm -rf /var/lib/apt/lists/*

RUN curl https://mise.run | sh
ENV PATH="/root/.local/bin:/root/.local/share/mise/shims:${PATH}"

COPY mise.toml /opt/bootstrap/mise.toml
WORKDIR /opt/bootstrap
RUN mise trust && mise install

COPY entrypoint.sh /opt/entrypoint.sh
RUN chmod +x /opt/entrypoint.sh

EXPOSE 4096
ENTRYPOINT ["/opt/entrypoint.sh"]
