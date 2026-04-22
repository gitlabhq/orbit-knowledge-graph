#!/usr/bin/env python3
"""
Thin wrapper for the GitLab Orbit REST API.

The agent calls this script via bash. It handles auth and endpoint routing,
outputting structured JSON to stdout.

Usage:
    # Execute a graph query (JSON DSL on stdin)
    echo '{"query_type": "search", "node": {...}}' | python tools/orbit_query.py query

    # Execute a graph query from a file
    python tools/orbit_query.py query --file query.json

    # Get the graph schema
    python tools/orbit_query.py schema
    python tools/orbit_query.py schema --expand User,Project

    # Get cluster status
    python tools/orbit_query.py status

    # Get available tools
    python tools/orbit_query.py tools

Environment variables:
    GITLAB_TOKEN    (required) GitLab personal access token
    GITLAB_HOST     (default: staging.gitlab.com)

Exit codes:
    0  success
    1  query/request error (bad query, 4xx)
    2  infrastructure error (connection, auth, 5xx)
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
from urllib.request import Request, urlopen
from urllib.error import HTTPError, URLError


def _resolve_token() -> str | None:
    """Resolve GitLab token: env var first, then glab auth fallback."""
    token = os.environ.get("GITLAB_TOKEN")
    if token:
        return token
    host = os.environ.get("GITLAB_HOST", "staging.gitlab.com")
    try:
        result = subprocess.run(
            ["glab", "auth", "status", "--hostname", host, "--show-token"],
            capture_output=True, text=True, timeout=5,
        )
        for line in (result.stdout + result.stderr).splitlines():
            if "Token found:" in line:
                return line.split("Token found:")[-1].strip()
    except (FileNotFoundError, subprocess.TimeoutExpired):
        pass
    return None


def _get_config() -> tuple[str, str]:
    token = _resolve_token()
    if not token:
        print(json.dumps({"error": "auth_error", "message": "GITLAB_TOKEN not set and glab auth not available"}))
        sys.exit(2)
    host = os.environ.get("GITLAB_HOST", "staging.gitlab.com")
    return host, token


def _request(method: str, url: str, token: str, data: bytes | None = None) -> dict:
    headers = {
        "PRIVATE-TOKEN": token,
        "Content-Type": "application/json",
        "Accept": "application/json",
    }
    req = Request(url, data=data, headers=headers, method=method)
    start = time.monotonic()
    try:
        with urlopen(req, timeout=120) as resp:
            body = resp.read().decode()
            elapsed = int((time.monotonic() - start) * 1000)
            print(f"[{elapsed}ms] {method} {url} -> {resp.status}", file=sys.stderr)
            return json.loads(body) if body else {}
    except HTTPError as e:
        elapsed = int((time.monotonic() - start) * 1000)
        print(f"[{elapsed}ms] {method} {url} -> {e.code}", file=sys.stderr)
        body = e.read().decode() if e.fp else ""
        error_data = {"error": "http_error", "status": e.code, "message": e.reason}
        try:
            error_data["detail"] = json.loads(body)
        except (json.JSONDecodeError, ValueError):
            error_data["detail"] = body[:500]
        print(json.dumps(error_data))
        sys.exit(1 if 400 <= e.code < 500 else 2)
    except URLError as e:
        print(json.dumps({"error": "connection_error", "message": str(e.reason)}))
        sys.exit(2)


def cmd_query(args: argparse.Namespace) -> None:
    host, token = _get_config()

    if args.file:
        with open(args.file) as f:
            query_dsl = json.load(f)
    else:
        raw = sys.stdin.read().strip()
        if not raw:
            print(json.dumps({"error": "input_error", "message": "no query on stdin"}))
            sys.exit(1)
        query_dsl = json.loads(raw)

    body = {
        "query": query_dsl,
        "query_type": "json",
        "format": args.format,
    }

    url = f"https://{host}/api/v4/orbit/query"
    result = _request("POST", url, token, json.dumps(body).encode())

    if args.format == "raw":
        nodes = result.get("nodes", [])
        result["row_count"] = len(nodes)

    print(json.dumps(result, indent=2))


def cmd_schema(args: argparse.Namespace) -> None:
    host, token = _get_config()
    params = []
    if args.expand:
        params.append(f"expand={args.expand}")
    if args.format != "raw":
        params.append(f"format={args.format}")
    qs = f"?{'&'.join(params)}" if params else ""
    url = f"https://{host}/api/v4/orbit/schema{qs}"
    result = _request("GET", url, token)
    print(json.dumps(result, indent=2))


def cmd_status(args: argparse.Namespace) -> None:
    host, token = _get_config()
    url = f"https://{host}/api/v4/orbit/status"
    result = _request("GET", url, token)
    print(json.dumps(result, indent=2))


def cmd_tools(args: argparse.Namespace) -> None:
    host, token = _get_config()
    url = f"https://{host}/api/v4/orbit/tools"
    result = _request("GET", url, token)
    print(json.dumps(result, indent=2))


def main() -> None:
    parser = argparse.ArgumentParser(description="Orbit Knowledge Graph query tool")
    sub = parser.add_subparsers(dest="command", required=True)

    p_query = sub.add_parser("query", help="Execute a graph query")
    p_query.add_argument("--file", "-f", help="Read query from file instead of stdin")
    p_query.add_argument("--format", default="raw", choices=["raw", "llm"],
                         help="Response format (default: raw)")
    p_query.set_defaults(func=cmd_query)

    p_schema = sub.add_parser("schema", help="Get graph schema")
    p_schema.add_argument("--expand", "-e", help="Comma-separated node names to expand")
    p_schema.add_argument("--format", default="raw", choices=["raw", "llm"])
    p_schema.set_defaults(func=cmd_schema)

    p_status = sub.add_parser("status", help="Get cluster status")
    p_status.set_defaults(func=cmd_status)

    p_tools = sub.add_parser("tools", help="List available tools")
    p_tools.set_defaults(func=cmd_tools)

    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
