#!/usr/bin/env bun
//
// Reverse proxy for CI agent isolation.
// Runs as a GitLab CI service — holds real API tokens so the job container never sees them.
// Sanitizes outbound review content (note/body/description fields) before it reaches GitLab.
//
// Port 8080 (HTTP):  Anthropic API — injects x-api-key
// Port 8083 (HTTPS): GitLab API   — injects PRIVATE-TOKEN, sanitizes note bodies
//
// Tokens are injected at runtime via POST /_init from the job container.
// This avoids reliance on CI variable inheritance in service containers.
// The /_init endpoint accepts one call, then locks permanently.
//
// Security:
//   - Tokens only exist in proxy memory (never in env or on disk)
//   - Only allows exact upstream paths (Anthropic: /v1/*, GitLab: /api/*)
//   - Strips all auth-related headers from responses (prevents reflection)
//   - Sanitizes note/body/description/title fields via linkify-it + DOMPurify
//   - /_init endpoint locks after first call (one-time use)
//
// Requires: /tmp/k.pem, /tmp/c.pem (self-signed TLS cert for port 8083)

import { sanitize } from "./sanitize";

let anthropicKey: string | null = null;
let gitlabToken: string | null = null;
let initialized = false;

const SENSITIVE_RESPONSE_HEADERS = [
  "x-api-key",
  "authorization",
  "private-token",
  "cookie",
  "set-cookie",
  "x-request-id",
  "x-real-ip",
  "x-forwarded-for",
];

const CONTENT_FIELDS = new Set(["note", "body", "description", "title"]);

function sanitizeRequestBody(raw: string): string {
  try {
    const json = JSON.parse(raw);
    for (const key of Object.keys(json)) {
      if (typeof json[key] === "string" && CONTENT_FIELDS.has(key))
        json[key] = sanitize(json[key]);
    }
    return JSON.stringify(json);
  } catch {}
  try {
    const params = new URLSearchParams(raw);
    for (const key of params.keys()) {
      if (CONTENT_FIELDS.has(key))
        params.set(key, sanitize(params.get(key)!));
    }
    return params.toString();
  } catch {}
  return raw;
}

function stripResponseHeaders(res: Response): Response {
  const headers = new Headers(res.headers);
  for (const h of SENSITIVE_RESPONSE_HEADERS) headers.delete(h);
  headers.delete("content-encoding");
  headers.delete("content-length");
  return new Response(res.body, {
    status: res.status,
    statusText: res.statusText,
    headers,
  });
}

function forward(
  target: string,
  authHeader: Record<string, string>,
  req: Request,
): Request {
  const url = new URL(req.url);
  url.protocol = "https:";
  url.hostname = target;
  url.port = "443";

  const headers = new Headers(req.headers);
  headers.delete("host");
  headers.delete("private-token");
  headers.delete("x-api-key");
  headers.delete("authorization");
  headers.delete("accept-encoding");
  headers.set("host", target);
  for (const [k, v] of Object.entries(authHeader)) headers.set(k, v);

  return new Request(url, { method: req.method, headers, body: req.body });
}

async function handle(
  req: Request,
  target: string,
  authHeader: Record<string, string>,
  allowedPrefix: string,
  transform?: (body: string) => string,
): Promise<Response> {
  const path = new URL(req.url).pathname;
  if (!path.startsWith(allowedPrefix))
    return new Response("not found", { status: 404 });

  if (!initialized)
    return new Response("proxy not initialized", { status: 503 });

  try {
    if (transform) {
      const body = await req.text();
      const fwd = forward(target, authHeader, req);
      const res = await fetch(
        new Request(fwd.url, {
          method: fwd.method,
          headers: fwd.headers,
          body: transform(body),
        }),
      );
      return stripResponseHeaders(res);
    }

    const res = await fetch(forward(target, authHeader, req));
    return stripResponseHeaders(res);
  } catch {
    return new Response("proxy error", { status: 502 });
  }
}

async function handleInit(req: Request): Promise<Response> {
  if (initialized)
    return new Response("already initialized", { status: 403 });
  initialized = true;

  try {
    const { anthropic_key, gitlab_token } = await req.json();
    if (!anthropic_key || !gitlab_token) {
      initialized = false;
      return new Response("missing keys", { status: 400 });
    }

    anthropicKey = anthropic_key;
    gitlabToken = gitlab_token;
    console.log("proxy initialized with tokens");
    return new Response("ok");
  } catch {
    initialized = false;
    return new Response("invalid body", { status: 400 });
  }
}

Bun.serve({
  port: 8080,
  fetch: (req) => {
    const path = new URL(req.url).pathname;
    if (path === "/_init" && req.method === "POST") return handleInit(req);
    return handle(req, "api.anthropic.com", { "x-api-key": anthropicKey! }, "/v1/");
  },
  error: () => new Response("proxy error", { status: 500 }),
});

Bun.serve({
  port: 8083,
  tls: { key: Bun.file("/tmp/k.pem"), cert: Bun.file("/tmp/c.pem") },
  fetch: (req) => {
    const hasBody = req.method === "POST" || req.method === "PUT" || req.method === "PATCH";
    return handle(
      req,
      "gitlab.com",
      { "PRIVATE-TOKEN": gitlabToken! },
      "/api/",
      hasBody ? sanitizeRequestBody : undefined,
    );
  },
  error: () => new Response("proxy error", { status: 500 }),
});

console.log("proxy: anthropic=:8080 gitlab=:8083 (waiting for /_init)");
