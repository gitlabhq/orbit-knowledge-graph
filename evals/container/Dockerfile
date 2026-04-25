FROM alpine:3.21

RUN apk add --no-cache bash curl git python3 py3-pip gcompat

# mise (manages glab)
RUN curl https://mise.run | MISE_YES=1 sh
ENV PATH="/root/.local/bin:/root/.local/share/mise/shims:${PATH}"

COPY mise.toml /root/.config/mise/config.toml
RUN mise trust /root/.config/mise/config.toml && mise install

# opencode (standalone installer — aqua registry ships wrong binary)
RUN curl -fsSL https://opencode.ai/install | bash
ENV PATH="/root/.opencode/bin:${PATH}"

COPY entrypoint.sh /opt/entrypoint.sh
RUN chmod +x /opt/entrypoint.sh

EXPOSE 4096
ENTRYPOINT ["/opt/entrypoint.sh"]
