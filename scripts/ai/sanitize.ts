#!/usr/bin/env bun
//
// Sanitizes AI-generated content before it reaches GitLab.
// Used both as a CLI tool and imported by the proxy.
//
// Layers:
//   1. linkify-it     — bare URLs, bare domains, IP addresses, double protocols
//   2. Secret patterns — API keys, PATs, JWTs, PEM blocks, connection strings
//   3. marked + DOMPurify — markdown links [text](url), images ![alt](url)
//   4. DOMPurify      — strip all raw HTML tags (<script>, <img>, <iframe>)
//
// CLI usage: bun scripts/ai/sanitize.ts <file>

import LinkifyIt from "linkify-it";
import DOMPurify from "isomorphic-dompurify";
import { marked } from "marked";

const linkify = new LinkifyIt();

export const ALLOWED_HOSTS = [
  "gitlab.com",
  "gitlab.io",
  "gitlab.net",
  "docs.rs",
  "crates.io",
  "doc.rust-lang.org",
  "rust-lang.org",
  "github.com",
  "clickhouse.com",
  "nats.io",
  "anthropic.com",
];

const ALLOWED_URI_REGEXP = new RegExp(
  `^https?://(([\\w-]+\\.)*(${ALLOWED_HOSTS.map((h) => h.replace(/\./g, "\\.")).join("|")}))(\/|$|\\?|#)`,
);

const SECRET_PATTERNS: RegExp[] = [
  // GitLab tokens
  /glpat-[\w-]{20,}/g,
  /glptt-[\w-]{20,}/g,
  /gldt-[\w-]{20,}/g,
  /glrt-[\w-]{20,}/g,
  /glsoat-[\w-]{20,}/g,
  /GR1348941[\w-]{20,}/g,

  // Anthropic
  /sk-ant-[\w-]{20,}/g,

  // OpenAI
  /sk-[\w-]{40,}/g,

  // GitHub
  /ghp_[\w]{36,}/g,
  /gho_[\w]{36,}/g,
  /ghs_[\w]{36,}/g,
  /ghr_[\w]{36,}/g,
  /github_pat_[\w]{22,}/g,

  // AWS
  /AKIA[\w]{16}/g,
  /ASIA[\w]{16}/g,
  /aws_[\w]*key[\w]*\s*[:=]\s*['"]?[\w\/+]{20,}/gi,

  // GCP
  /AIza[\w-]{30,}/g,
  /"type"\s*:\s*"service_account"/g,

  // Azure
  /AccountKey=[\w\/+=]{44,}/g,

  // JWT tokens
  /eyJ[A-Za-z0-9_-]{10,}\.eyJ[A-Za-z0-9_-]{10,}\.[A-Za-z0-9_-]{10,}/g,

  // PEM blocks
  /-----BEGIN [A-Z ]+(PRIVATE KEY|CERTIFICATE|RSA|DSA|EC)-----[\s\S]*?-----END [A-Z ]+-----/g,

  // Bearer/auth headers
  /Bearer\s+[\w.\/-]{20,}/gi,
  /Authorization\s*[:=]\s*['"]?[\w.\/ -]{20,}/gi,

  // npm / PyPI
  /npm_[\w]{36,}/g,
  /pypi-[\w]{50,}/g,

  // HashiCorp Vault
  /hvs\.[\w]{24,}/g,
  /hvb\.[\w]{24,}/g,

  // Slack / Discord webhooks
  /hooks\.slack\.com\/services\/T[\w]+\/B[\w]+\/[\w]+/g,
  /discord(?:app)?\.com\/api\/webhooks\/\d+\/[\w-]+/g,

  // Sentry DSN
  /https?:\/\/[\w]+@[\w.]+\.ingest\.sentry\.io\/\d+/g,

  // Connection strings
  /(?:postgres|postgresql|mysql|mongodb|mongodb\+srv|redis|rediss|nats|clickhouse):\/\/[^\s"'`]+/gi,

  // Generic password/secret assignments
  /(?:password|passwd|secret|token|apikey|api_key)\s*[:=]\s*['"][^'"]{8,}['"]/gi,

  // IP:port patterns (excludes loopback and RFC1918)
  /\b(?!127\.)(?!10\.)(?!172\.(?:1[6-9]|2\d|3[01])\.)(?!192\.168\.)\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}:\d{2,5}\b/g,
];

function isAllowedUrl(raw: string): boolean {
  let parsed: URL;
  try {
    parsed = new URL(raw.startsWith("//") ? `https:${raw}` : raw);
  } catch {
    return false;
  }

  if (parsed.protocol !== "https:" && parsed.protocol !== "http:")
    return false;
  if (parsed.username || parsed.password) return false;

  const host = parsed.hostname.toLowerCase();
  if (/^\d{1,3}(\.\d{1,3}){3}$/.test(host)) return false;
  if (/^\[?[0-9a-f:]+\]?$/i.test(host)) return false;

  return ALLOWED_HOSTS.some((d) => host === d || host.endsWith(`.${d}`));
}

export function sanitize(text: string): string {
  let n = 0;

  const matches = linkify.match(text);
  if (matches) {
    for (let i = matches.length - 1; i >= 0; i--) {
      const m = matches[i];
      if (!isAllowedUrl(m.url)) {
        text =
          text.slice(0, m.index) + "[link redacted]" + text.slice(m.lastIndex);
        n++;
      }
    }
  }

  for (const p of SECRET_PATTERNS) {
    p.lastIndex = 0;
    text = text.replace(p, () => {
      n++;
      return "[redacted]";
    });
  }

  const html = marked.parse(text, { async: false }) as string;
  const clean = DOMPurify.sanitize(html, {
    ALLOWED_URI_REGEXP,
    ALLOWED_TAGS: ["a", "img"],
    ALLOWED_ATTR: ["href", "src"],
  });

  const kept = new Set<string>();
  for (const [, url] of clean.matchAll(/(?:href|src)="([^"]+)"/g))
    kept.add(url);

  text = text.replace(
    /(!?\[)([^\]]*)\]\(([^)]+)\)/g,
    (match, bracket, label, url) => {
      if (kept.has(url) || isAllowedUrl(url)) return match;
      n++;
      return `${bracket}${label}] [link redacted]`;
    },
  );

  text = text.replace(
    /^\[([^\]]+)\]:\s+(\S+)(.*)$/gm,
    (match, label, url, rest) => {
      if (isAllowedUrl(url)) return match;
      n++;
      return `[${label}]: [link redacted]${rest}`;
    },
  );

  text = text.replace(/<(https?:\/\/[^>]+)>/g, (match, url) => {
    if (isAllowedUrl(url)) return match;
    n++;
    return "[link redacted]";
  });

  if (n) console.log(`sanitized ${n} item(s)`);
  return text;
}

// CLI mode
if (import.meta.main) {
  const file = process.argv[2];
  if (!file || !(await Bun.file(file).exists())) process.exit(0);

  const content = await Bun.file(file).text();
  const result = sanitize(content);

  if (result !== content) await Bun.write(file, result);
}
