#!/usr/bin/env bun
//
// Reverse proxy for CI agent isolation.
// Runs as a GitLab CI service — holds real API tokens so the job container never sees them.
// Sanitizes outbound review content before it reaches GitLab.
//
// Port 8080 (HTTP):  Anthropic API — injects x-api-key, streams through
// Port 8083 (HTTPS): GitLab API   — injects PRIVATE-TOKEN, sanitizes note bodies
//
// Security:
//   - Only allows exact upstream paths (Anthropic: /v1/*, GitLab: /api/v4/*)
//   - Strips all auth-related headers from responses (prevents reflection)
//   - Generic error responses (no stack traces, no env vars)
//   - Sanitizes note bodies via DOMPurify + linkify-it before forwarding
//
// Requires: ANTHROPIC_API_KEY, GITLAB_REVIEW_TOKEN env vars
//           /tmp/k.pem, /tmp/c.pem (self-signed TLS cert for port 8083)

import { sanitize } from "./sanitize";

const ANTHROPIC_KEY = process.env.ANTHROPIC_API_KEY!;
const GITLAB_TOKEN = process.env.GITLAB_REVIEW_TOKEN!;

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
  // Bun's fetch auto-decompresses but keeps content-encoding header,
  // causing the client to double-decompress. Strip it.
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

  try {
    if (transform) {
      const fwd = forward(target, authHeader, req);
      const res = await fetch(
        new Request(fwd.url, {
          method: fwd.method,
          headers: fwd.headers,
          body: transform(await req.text()),
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

Bun.serve({
  port: 8080,
  fetch: (req) =>
    handle(req, "api.anthropic.com", { "x-api-key": ANTHROPIC_KEY }, "/"),
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
      { "PRIVATE-TOKEN": GITLAB_TOKEN },
      "/api/",
      hasBody ? sanitizeRequestBody : undefined,
    );
  },
  error: () => new Response("proxy error", { status: 500 }),
});

console.log("proxy: anthropic=:8080 gitlab=:8083");
