FROM alpine:3.21

RUN apk add --no-cache bash curl git python3 py3-pip nodejs npm

RUN curl https://mise.run | MISE_YES=1 sh
ENV PATH="/root/.local/bin:/root/.local/share/mise/shims:${PATH}"

COPY mise.toml /opt/bootstrap/mise.toml
WORKDIR /opt/bootstrap
RUN mise trust && mise install

COPY entrypoint.sh /opt/entrypoint.sh
RUN chmod +x /opt/entrypoint.sh

EXPOSE 4096
ENTRYPOINT ["/opt/entrypoint.sh"]
