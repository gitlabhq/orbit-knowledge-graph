FROM alpine:3.21

RUN apk add --no-cache bash curl git python3 py3-pip gcompat libstdc++

# mise (manages glab)
RUN curl https://mise.run | MISE_YES=1 sh
ENV PATH="/root/.local/bin:/root/.local/share/mise/shims:${PATH}"

COPY mise.toml /root/.config/mise/config.toml
RUN mise trust /root/.config/mise/config.toml && mise install

# opencode — version passed from mise.toml via eval:build task.
# Uses curl installer because aqua registry ships wrong binary on linux.
ARG OPENCODE_VERSION
RUN test -n "${OPENCODE_VERSION}" || { echo "OPENCODE_VERSION build arg required"; exit 1; }
RUN curl -fsSL https://opencode.ai/install | bash -s -- --version ${OPENCODE_VERSION}
ENV PATH="/root/.opencode/bin:${PATH}"

COPY entrypoint.sh /opt/entrypoint.sh
RUN chmod +x /opt/entrypoint.sh

EXPOSE 4096
ENTRYPOINT ["/opt/entrypoint.sh"]
