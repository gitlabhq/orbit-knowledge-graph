#!/usr/bin/env bun
//
// Generates OPENCODE_CONFIG_CONTENT from agent prompts + shared instructions.
// Single source of truth for model, permissions, provider, and agent settings.
//
// Agent prompts:     .opencode/agent/*.md
// Shared instructions: .opencode/agent/shared/*.md (appended to every agent)
// Config (this file): model, temperature, steps, permissions
//
// Usage: OPENCODE_CONFIG_CONTENT=$(bun scripts/ai/config.ts)

import { ALLOWED_HOSTS } from "./sanitize";
import { readdir } from "fs/promises";
import { join, basename } from "path";

const MODEL = "anthropic/claude-opus-4-6";

const AGENTS: Record<string, Omit<AgentConfig, "prompt">> = {
  review: {
    mode: "primary",
    model: MODEL,
    temperature: 0.2,
    steps: 15,
    description: "Code review agent",
    permission: {
      edit: "deny",
    },
  },
  security: {
    mode: "primary",
    model: MODEL,
    temperature: 0.1,
    steps: 12,
    description: "Security review agent",
    permission: {
      edit: "deny",
    },
  },
};

type PermissionValue = string | Record<string, string>;
interface AgentConfig {
  mode: string;
  model: string;
  temperature: number;
  steps: number;
  description: string;
  prompt?: string;
  permission: Record<string, PermissionValue>;
}

const proxyDeny = {
  "http://api-proxy*": "deny",
  "https://api-proxy*": "deny",
  "http://localhost*": "deny",
  "http://127.0.0.1*": "deny",
};

const AGENT_DIR = join(import.meta.dir, "../../.opencode/agent");

function stripFrontmatter(content: string): string {
  if (!content.startsWith("---")) return content;
  const end = content.indexOf("---", 3);
  return end === -1 ? content : content.slice(end + 3);
}

async function loadShared(): Promise<string> {
  const dir = join(AGENT_DIR, "shared");
  const parts: string[] = [];
  try {
    for (const file of (await readdir(dir)).sort()) {
      if (!file.endsWith(".md")) continue;
      parts.push((await Bun.file(join(dir, file)).text()).trim());
    }
  } catch {}
  return parts.join("\n\n");
}

async function loadPrompts(shared: string): Promise<Record<string, string>> {
  const prompts: Record<string, string> = {};
  try {
    for (const file of await readdir(AGENT_DIR)) {
      if (!file.endsWith(".md")) continue;
      const name = basename(file, ".md");
      const content = stripFrontmatter(await Bun.file(join(AGENT_DIR, file)).text()).trim();
      prompts[name] = shared ? `${content}\n\n${shared}` : content;
    }
  } catch {}
  return prompts;
}

async function generate() {
  const shared = await loadShared();
  const prompts = await loadPrompts(shared);

  const agents: Record<string, AgentConfig> = {};
  for (const [name, config] of Object.entries(AGENTS)) {
    agents[name] = { ...config, prompt: prompts[name] };
  }

  const config = {
    model: MODEL,
    share: "disabled",
    autoupdate: false,
    provider: {
      anthropic: {
        options: {
          baseURL: "http://api-proxy:8080",
        },
      },
    },
    permission: {
      read: { "*": "allow", "/proc/*": "deny", "/sys/*": "deny" },
      bash: { "*": "allow", "*api-proxy*": "deny", "*/proc/*/environ*": "deny" },
      webfetch: { "*": "allow", ...proxyDeny },
    },
    agent: agents,
  };

  return JSON.stringify(config);
}

console.log(await generate());
