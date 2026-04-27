#!/usr/bin/env bash
# orbit-setup - POC for `glab orbit setup`
# Installs the Orbit skill + MCP config for your AI coding agent.
# Fetches skill files directly from gitlab-org/orbit/knowledge-graph via glab api.
# No git clone required.
set -euo pipefail

REPO="gitlab-org%2Forbit%2Fknowledge-graph"
SKILL_REF="main"
SKILL_FILES=(
  "skills/orbit/SKILL.md"
  "skills/orbit/references/query_language.md"
  "skills/orbit/references/recipes.md"
  "skills/orbit/references/troubleshooting.md"
  "skills/orbit/scripts/orbit-query"
)

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

ok()   { echo -e "${GREEN}✓${NC} $*"; }
warn() { echo -e "${YELLOW}!${NC} $*"; }
fail() { echo -e "${RED}✗${NC} $*"; exit 1; }

# --- agent detection ---

detect_agent() {
  if [[ -n "${ORBIT_AGENT:-}" ]]; then
    echo "$ORBIT_AGENT"
  elif [[ -d "$HOME/.claude" ]]; then
    echo "claude-code"
  elif [[ -d "$HOME/.cursor" ]]; then
    echo "cursor"
  elif [[ -d "$HOME/.config/opencode" ]]; then
    echo "opencode"
  elif [[ -d "$HOME/.codex" ]]; then
    echo "codex"
  elif [[ -d "$HOME/.gemini" ]]; then
    echo "gemini"
  elif [[ -d "$HOME/.gitlab/duo" ]]; then
    echo "duo-cli"
  else
    echo "unknown"
  fi
}

skill_dir_for() {
  case "$1" in
    claude-code) echo "$HOME/.claude/skills/orbit" ;;
    opencode)    echo "$HOME/.config/opencode/skills/orbit" ;;
    duo-cli)     echo "$HOME/.gitlab/duo/skills/orbit" ;;
    *)           echo "" ;;  # cursor, codex, gemini: no skill convention yet
  esac
}

mcp_config_for() {
  case "$1" in
    claude-code) echo "$HOME/.claude/settings.json" ;;
    opencode)    echo "$HOME/.config/opencode/opencode.json" ;;
    cursor)      echo "$HOME/.cursor/mcp.json" ;;
    codex)       echo "$HOME/.codex/config.json" ;;
    gemini)      echo "$HOME/.gemini/settings.json" ;;
    duo-cli)     echo "" ;;  # tools already native via DAP, no MCP config needed
    *)           echo "" ;;
  esac
}

# --- fetch skill files ---

fetch_skill() {
  local skill_dir="$1"
  mkdir -p "$skill_dir/references" "$skill_dir/scripts"

  echo "  Fetching skill files from gitlab-org/orbit/knowledge-graph..."

  for file in "${SKILL_FILES[@]}"; do
    local encoded_path
    encoded_path=$(python3 -c "import urllib.parse; print(urllib.parse.quote('$file', safe=''))")
    local dest="$skill_dir/${file#skills/orbit/}"
    local dest_dir
    dest_dir=$(dirname "$dest")
    mkdir -p "$dest_dir"

    glab api "projects/${REPO}/repository/files/${encoded_path}/raw?ref=${SKILL_REF}" > "$dest" 2>/dev/null \
      || fail "Could not fetch $file - check glab auth and network"

    # make orbit-query executable
    [[ "$file" == *"orbit-query" ]] && chmod +x "$dest"
  done
}

# --- write MCP config ---

write_mcp_config() {
  local agent="$1"
  local config_file="$2"
  local instance_url
  instance_url=$(glab api "meta" 2>/dev/null | python3 -c "import sys,json; print(json.load(sys.stdin).get('gitlab_url','https://gitlab.com'))" 2>/dev/null || echo "https://gitlab.com")
  local mcp_url="${instance_url}/api/v4/orbit/mcp"

  if [[ "$agent" == "claude-code" ]]; then
    # use claude mcp add if available, otherwise write directly
    if command -v claude &>/dev/null; then
      claude mcp add gitlab-orbit -- npx mcp-remote "$mcp_url" 2>/dev/null \
        && return
    fi
    # fallback: write to settings.json directly
    python3 - "$config_file" "$mcp_url" <<'PYEOF'
import sys, json, os
config_file, mcp_url = sys.argv[1], sys.argv[2]
config = {}
if os.path.exists(config_file):
    try:
        config = json.load(open(config_file))
    except Exception:
        pass
config.setdefault("mcpServers", {})
config["mcpServers"]["gitlab-orbit"] = {
    "command": "npx",
    "args": ["mcp-remote", mcp_url]
}
json.dump(config, open(config_file, "w"), indent=2)
PYEOF

  elif [[ "$agent" == "opencode" || "$agent" == "cursor" || "$agent" == "codex" || "$agent" == "gemini" ]]; then
    python3 - "$config_file" "$mcp_url" <<'PYEOF'
import sys, json, os
config_file, mcp_url = sys.argv[1], sys.argv[2]
config = {}
if os.path.exists(config_file):
    try:
        config = json.load(open(config_file))
    except Exception:
        pass
config.setdefault("mcp", {})
config["mcp"]["gitlab-orbit"] = {
    "type": "local",
    "command": ["npx", "mcp-remote", mcp_url],
    "timeout": 120000,
    "enabled": True
}
json.dump(config, open(config_file, "w"), indent=2)
PYEOF
  fi
}

# --- verify connection ---

verify() {
  local status
  status=$(glab api orbit/status 2>/dev/null | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('status','unknown'))" 2>/dev/null || echo "error")
  if [[ "$status" == "healthy" ]]; then
    ok "Orbit API is healthy"
  elif [[ "$status" == "unknown" ]]; then
    warn "Orbit API reachable but service unhealthy - knowledge_graph feature flag may be off"
    echo "    Enable it: /chatops gitlab run feature set --user=<your-username> knowledge_graph true"
  else
    warn "Could not reach Orbit API (got: $status) - check glab auth"
  fi
}

# --- flags ---

SKIP_SKILL=false
SKIP_MCP=false
DRY_RUN=false
AGENT_OVERRIDE=""

for arg in "$@"; do
  case "$arg" in
    --skill-only) SKIP_MCP=true ;;
    --mcp-only)   SKIP_SKILL=true ;;
    --dry-run)    DRY_RUN=true ;;
    --agent=*)    AGENT_OVERRIDE="${arg#--agent=}" ;;
  esac
done

[[ -n "$AGENT_OVERRIDE" ]] && export ORBIT_AGENT="$AGENT_OVERRIDE"

# --- main ---

echo ""
echo "orbit-setup - connecting your AI agent to GitLab Orbit"
echo ""

# check glab auth
glab auth status &>/dev/null || fail "glab is not authenticated. Run: glab auth login"

AGENT=$(detect_agent)
SKILL_DIR=$(skill_dir_for "$AGENT")
MCP_CONFIG=$(mcp_config_for "$AGENT")

if [[ "$AGENT" == "unknown" ]]; then
  warn "No supported agent detected. Printing MCP config for manual setup:"
  echo ""
  INSTANCE_URL=$(glab api "meta" 2>/dev/null | python3 -c "import sys,json; print(json.load(sys.stdin).get('gitlab_url','https://gitlab.com'))" 2>/dev/null || echo "https://gitlab.com")
  echo '{'
  echo '  "mcpServers": {'
  echo '    "gitlab-orbit": {'
  echo '      "command": "npx",'
  echo "      \"args\": [\"mcp-remote\", \"${INSTANCE_URL}/api/v4/orbit/mcp\"]"
  echo '    }'
  echo '  }'
  echo '}'
  exit 0
fi

ok "Detected agent: $AGENT"

if [[ "$DRY_RUN" == true ]]; then
  echo ""
  warn "Dry run - no changes written"
  [[ -n "$SKILL_DIR" ]]  && echo "  Would install skill to: $SKILL_DIR"
  [[ -n "$MCP_CONFIG" ]] && echo "  Would write MCP config to: $MCP_CONFIG"
  exit 0
fi

# install skill
if [[ "$SKIP_SKILL" == false && -n "$SKILL_DIR" ]]; then
  fetch_skill "$SKILL_DIR"
  ok "Skill installed: $SKILL_DIR"
elif [[ -z "$SKILL_DIR" ]]; then
  warn "No skill convention for $AGENT - skipping skill install"
fi

# write MCP config
if [[ "$SKIP_MCP" == false && -n "$MCP_CONFIG" ]]; then
  write_mcp_config "$AGENT" "$MCP_CONFIG"
  ok "MCP config written: $MCP_CONFIG"
elif [[ -z "$MCP_CONFIG" ]]; then
  warn "No MCP config path for $AGENT - skipping"
fi

# verify
echo ""
verify

echo ""
echo "Orbit is ready. Ask your agent: \"Check the Orbit API status\""
echo ""
