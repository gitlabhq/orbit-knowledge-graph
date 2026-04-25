FROM ubuntu:24.04

ENV DEBIAN_FRONTEND=noninteractive
ENV MISE_YES=1

# System deps
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl ca-certificates git build-essential && \
    rm -rf /var/lib/apt/lists/*

# mise
RUN curl https://mise.run | sh && \
    echo 'eval "$(~/.local/bin/mise activate bash)"' >> /etc/bash.bashrc
ENV PATH="/root/.local/bin:/root/.local/share/mise/shims:${PATH}"

# Bootstrap from SSOT
COPY mise.toml /opt/eval/mise.toml
WORKDIR /opt/eval
RUN mise trust && mise install

# Eval runtime: tools, agents, skills
COPY requirements.txt /opt/eval/requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY tools/ /opt/eval/tools/
COPY agents/ /opt/eval/agents/
COPY skills/ /opt/eval/skills/

# OpenCode config: point to skills
COPY opencode.json /opt/eval/opencode.json

EXPOSE 4096
CMD ["opencode", "serve", "--port", "4096", "--print-logs"]
