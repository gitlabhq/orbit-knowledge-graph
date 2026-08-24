#!/usr/bin/env python3
"""
Concurrent load test directly against GKG gRPC, bypassing Rails.

Generates a JWT, connects to the GKG gRPC endpoint, fires N concurrent
requests per query type across R rounds, and handles the bidirectional
redaction exchange by approving every resource.

Usage:
  # Port-forward first:
  kubectl port-forward svc/gkg-webserver 50054:50054 -n <namespace>
  # Or use the VM tunnel:
  cd ~/gitlab/gdk-gkg-vm-workspace && bun vm tunnel gdk-vm-1

  # Run:
  GKG_JWT_SECRET=<base64-secret> python3 scripts/direct_grpc_test.py [--rounds 3] [--concurrency 20]

  # Single-shot debug mode (no concurrency, verbose output):
  GKG_JWT_SECRET=<secret> python3 scripts/direct_grpc_test.py --debug --query search_projects

Environment variables:
  GKG_JWT_SECRET    Base64-encoded JWT signing secret (required)
  STAGING_PAT       GitLab staging PAT to auto-fetch user info + traversal IDs

Presets (--preset staging):
  Uses michaelangeloio's staging identity with pre-computed traversal paths
  so you don't need STAGING_PAT or --user-id/--username flags.

Requires: pip install grpcio grpcio-tools protobuf
"""

import argparse
import base64
import hashlib
import hmac
import json
import os
import queue
import statistics
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

from constants import API
from utils import get_orbit_req_headers

import grpc
from grpc_tools import protoc

# ---------------------------------------------------------------------------
# Staging presets
# ---------------------------------------------------------------------------

STAGING_PRESETS = {
    "staging": {
        "user_id": 4040422,
        "username": "michaelangeloio",
        "org_id": 1,
        # Trie-compacted traversal paths for michaelangeloio on staging.
        # All 44 groups are under gitlab-org (9970), so "1/9970/" subsumes them.
        # Full (uncompacted) paths kept in comment for reference.
        "traversal_ids": [
            "1/9970/",
        ],
        # Full 44 paths before trie compaction:
        # "1/9970/", "1/9970/11570646/", "1/9970/12955768/",
        # "1/9970/1540914/", "1/9970/1602322/", "1/9970/16841395/",
        # "1/9970/16841395/2214172/", "1/9970/16841395/2214172/27137016/",
        # "1/9970/16841395/22772484/", "1/9970/17454999/", "1/9970/1755573/",
        # "1/9970/1761696/", "1/9970/1793858/", "1/9970/1819570/",
        # "1/9970/1920469/", "1/9970/1920469/1920510/", "1/9970/2068794/",
        # "1/9970/2072647/", "1/9970/2112424/", "1/9970/2112424/19421577/",
        # "1/9970/2112424/19421577/19421578/", "1/9970/2112424/19443832/",
        # "1/9970/22003264/", "1/9970/2255492/", "1/9970/2255492/2255494/",
        # "1/9970/2255492/2255494/2255495/", "1/9970/2255492/2255499/",
        # "1/9970/2255492/2256490/", "1/9970/2255492/2256490/2256491/",
        # "1/9970/2255492/2256492/", "1/9970/2255492/2259127/",
        # "1/9970/2255492/2259127/2259128/", "1/9970/2255492/2259127/2259129/",
        # "1/9970/2255492/2259127/2259129/2259130/", "1/9970/2255492/2259137/",
        # "1/9970/2255492/2259137/2259138/", "1/9970/2255492/2259137/2259139/",
        # "1/9970/2255492/2259137/2259139/2259140/", "1/9970/23057292/",
        # "1/9970/23081759/", "1/9970/23246561/", "1/9970/2462563/",
        # "1/9970/25647603/", "1/9970/25647603/25647604/"
    },
}

# ---------------------------------------------------------------------------
# JWT (HS256)
# ---------------------------------------------------------------------------

def _b64url(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode()


def _jwt_encode(payload: dict, secret: bytes) -> str:
    header = _b64url(json.dumps({"alg": "HS256", "typ": "JWT"}).encode())
    body = _b64url(json.dumps(payload).encode())
    sig_input = f"{header}.{body}".encode()
    sig = hmac.new(secret, sig_input, hashlib.sha256).digest()
    return f"{header}.{body}.{_b64url(sig)}"


def build_jwt(secret_b64: str, user_id: int, username: str, org_id: int,
              traversal_ids: list[str], admin: bool = False) -> str:
    now = int(time.time())
    payload = {
        "sub": f"user:{user_id}",
        "iss": "gitlab",
        "aud": "gitlab-knowledge-graph",
        "iat": now,
        "exp": now + 300,
        "user_id": user_id,
        "username": username,
        "admin": admin,
        "organization_id": org_id,
        "min_access_level": 20,
        "source_type": "core",
    }
    if not admin:
        payload["group_traversal_ids"] = traversal_ids
    return _jwt_encode(payload, base64.b64decode(secret_b64.strip()))

# ---------------------------------------------------------------------------
# Staging GitLab API helpers
# ---------------------------------------------------------------------------

import urllib.request

def _api_get(path: str):
    req = urllib.request.Request(f"{API}{path}", headers=get_orbit_req_headers())
    with urllib.request.urlopen(req) as resp:
        return json.load(resp)


def fetch_user_info() -> dict:
    return _api_get("/user")


def fetch_groups() -> list[dict]:
    groups, page = [], 1
    while True:
        batch = _api_get(f"/groups?min_access_level=20&per_page=100&page={page}")
        if not batch:
            break
        groups.extend(batch)
        page += 1
    return groups

# ---------------------------------------------------------------------------
# Proto compilation
# ---------------------------------------------------------------------------

def compile_proto(proto_path: str):
    import importlib
    import tempfile

    out_dir = tempfile.mkdtemp(prefix="gkg_proto_")
    proto_dir = os.path.dirname(proto_path)

    result = protoc.main([
        "grpc_tools.protoc",
        f"-I{proto_dir}",
        f"--python_out={out_dir}",
        f"--grpc_python_out={out_dir}",
        os.path.basename(proto_path),
    ])
    if result != 0:
        print(f"ERROR: protoc failed with code {result}")
        sys.exit(1)

    sys.path.insert(0, out_dir)
    return importlib.import_module("gkg_pb2"), importlib.import_module("gkg_pb2_grpc")

# ---------------------------------------------------------------------------
# gRPC call wrappers
# ---------------------------------------------------------------------------

def _approve_all(pb2, req):
    """Build a RedactionResponse that approves every resource."""
    return pb2.RedactionExchange(
        response=pb2.RedactionResponse(
            result_id=req.result_id,
            authorizations=[
                pb2.ResourceAuthorization(
                    resource_type=res.resource_type,
                    authorized={rid: True for rid in res.resource_ids},
                )
                for res in req.resources
            ],
        )
    )


def fire_query(stub, pb2, metadata, query: dict, verbose: bool = False):
    """Single bidi-streaming ExecuteQuery call with mock redaction. Returns (status, ms, err)."""
    start = time.perf_counter()
    request_queue = queue.Queue()

    request_queue.put(pb2.ExecuteQueryMessage(
        request=pb2.ExecuteQueryRequest(
            query=json.dumps(query),
            format=pb2.RESPONSE_FORMAT_RAW,
            query_type=pb2.QUERY_TYPE_JSON,
        )
    ))

    def request_iter():
        while True:
            msg = request_queue.get()
            if msg is None:
                return
            yield msg

    try:
        responses = stub.ExecuteQuery(request_iter(), metadata=metadata)
        for msg in responses:
            field = msg.WhichOneof("content")
            if field == "redaction":
                inner = msg.redaction.WhichOneof("content")
                if inner == "required":
                    if verbose:
                        for res in msg.redaction.required.resources:
                            print(f"    redaction: {res.resource_type} x{len(res.resource_ids)} -> APPROVED")
                    request_queue.put(pb2.ExecuteQueryMessage(
                        redaction=_approve_all(pb2, msg.redaction.required)
                    ))
                    continue
            elif field == "result":
                ms = (time.perf_counter() - start) * 1000
                meta = msg.result.metadata
                if verbose:
                    print(f"    rows={meta.row_count} type={meta.query_type} {ms:.0f}ms")
                return 200, ms, None
            elif field == "error":
                ms = (time.perf_counter() - start) * 1000
                err = f"{msg.error.code}: {msg.error.message}"
                if verbose:
                    print(f"    ERROR: {err}")
                return err, ms, err

        ms = (time.perf_counter() - start) * 1000
        return "no_result", ms, "stream ended without result"
    except grpc.RpcError as e:
        ms = (time.perf_counter() - start) * 1000
        return str(e.code()), ms, e.details()
    finally:
        request_queue.put(None)


def fire_unary(stub, pb2, metadata, rpc_name: str, verbose: bool = False):
    """Single unary RPC call. Returns (status, ms, err)."""
    start = time.perf_counter()
    try:
        if rpc_name == "get_cluster_health":
            stub.GetClusterHealth(pb2.GetClusterHealthRequest(format=pb2.RESPONSE_FORMAT_RAW), metadata=metadata)
        elif rpc_name == "list_tools":
            stub.ListTools(pb2.ListToolsRequest(), metadata=metadata)
        elif rpc_name == "get_graph_schema":
            stub.GetGraphSchema(pb2.GetGraphSchemaRequest(format=pb2.RESPONSE_FORMAT_RAW), metadata=metadata)
        ms = (time.perf_counter() - start) * 1000
        if verbose:
            print(f"    OK {ms:.0f}ms")
        return 200, ms, None
    except grpc.RpcError as e:
        ms = (time.perf_counter() - start) * 1000
        return str(e.code()), ms, e.details()

# ---------------------------------------------------------------------------
# Benchmark runner
# ---------------------------------------------------------------------------

def pct(sorted_vals, p):
    idx = min(int(len(sorted_vals) * p), len(sorted_vals) - 1)
    return sorted_vals[idx]


def bench(label, fire_fn, concurrency, rounds):
    all_times = []
    errors = 0
    error_bodies = {}

    for _ in range(rounds):
        with ThreadPoolExecutor(max_workers=concurrency) as pool:
            futures = [pool.submit(fire_fn) for _ in range(concurrency)]
            for f in as_completed(futures):
                status, ms, err_body = f.result()
                all_times.append(ms)
                if status != 200:
                    errors += 1
                    key = f"{status}: {(err_body or '')[:200]}"
                    error_bodies[key] = error_bodies.get(key, 0) + 1

    s = sorted(all_times)
    return {
        "label": label,
        "n": len(all_times),
        "errors": errors,
        "error_bodies": error_bodies,
        "min": min(s),
        "mean": statistics.mean(s),
        "median": statistics.median(s),
        "p90": pct(s, 0.9),
        "p99": pct(s, 0.99),
        "max": max(s),
        "stdev": statistics.stdev(s) if len(s) > 1 else 0,
    }

# ---------------------------------------------------------------------------
# Queries — same set as the Rails HTTP load test (snippet 5970846)
# ---------------------------------------------------------------------------

QUERIES = {
    "get_cluster_health": {
        "type": "unary",
        "rpc": "get_cluster_health",
    },
    "list_tools": {
        "type": "unary",
        "rpc": "list_tools",
    },
    "get_graph_schema": {
        "type": "unary",
        "rpc": "get_graph_schema",
    },
    # 0.83.1: no "search" query_type; use single-node traversal with node_ids/filters.
    "search (user)": {
        "type": "query",
        "body": {
            "query_type": "traversal",
            "node": {"id": "u", "entity": "User", "node_ids": [1]},
            "limit": 5,
        },
    },
    "search (projects)": {
        "type": "query",
        "body": {
            "query_type": "traversal",
            "node": {"id": "p", "entity": "Project", "node_ids": [1]},
            "limit": 10,
        },
    },
    "search (groups)": {
        "type": "query",
        "body": {
            "query_type": "traversal",
            "node": {"id": "g", "entity": "Group", "node_ids": [2]},
            "limit": 10,
        },
    },
    "search (notes 100)": {
        "type": "query",
        "body": {
            "query_type": "traversal",
            "node": {"id": "n", "entity": "Note", "node_ids": [1]},
            "limit": 100,
        },
    },
    "neighbors": {
        "type": "query",
        "body": {
            "query_type": "neighbors",
            "node": {"id": "p", "entity": "Project", "node_ids": [1]},
            "neighbors": {"node": "p", "direction": "both"},
            "limit": 50,
        },
    },
    # 0.83.1: Group->Project edge is CONTAINS; needs node_ids/filters on >=1 node.
    "traversal (1 join)": {
        "type": "query",
        "body": {
            "query_type": "traversal",
            "nodes": [
                {"id": "g", "entity": "Group", "node_ids": [2]},
                {"id": "p", "entity": "Project"},
            ],
            "relationships": [{"type": "CONTAINS", "from": "g", "to": "p", "direction": "outgoing"}],
            "limit": 50,
        },
    },
    # Note->Project edge is IN_PROJECT (no Project->Note edge in 0.83.1).
    "traversal (2 joins)": {
        "type": "query",
        "body": {
            "query_type": "traversal",
            "nodes": [
                {"id": "g", "entity": "Group", "node_ids": [2]},
                {"id": "p", "entity": "Project"},
                {"id": "n", "entity": "Note"},
            ],
            "relationships": [
                {"type": "CONTAINS", "from": "g", "to": "p", "direction": "outgoing"},
                {"type": "IN_PROJECT", "from": "n", "to": "p", "direction": "outgoing"},
            ],
            "limit": 50,
        },
    },
    # 0.83.1: group_by is top-level (not inside aggregations[]).
    "aggregation": {
        "type": "query",
        "body": {
            "query_type": "aggregation",
            "nodes": [
                {"id": "g", "entity": "Group", "node_ids": [2]},
                {"id": "p", "entity": "Project"},
            ],
            "relationships": [{"type": "CONTAINS", "from": "g", "to": "p", "direction": "outgoing"}],
            "aggregations": [{"function": "count", "target": "p", "alias": "project_count"}],
            "group_by": [{"kind": "node", "node": "g", "alias": "group"}],
        },
    },
    # 0.83.1: path_finding requires rel_types (goes inside `path`).
    "path (depth 2)": {
        "type": "query",
        "body": {
            "query_type": "path_finding",
            "nodes": [
                {"id": "u1", "entity": "User", "node_ids": [1]},
                {"id": "u2", "entity": "User", "node_ids": [2]},
            ],
            "path": {"type": "shortest", "from": "u1", "to": "u2", "max_depth": 2,
                     "rel_types": ["MEMBER_OF", "CONTAINS", "IN_PROJECT"]},
        },
    },
    "path (depth 3)": {
        "type": "query",
        "body": {
            "query_type": "path_finding",
            "nodes": [
                {"id": "u1", "entity": "User", "node_ids": [1]},
                {"id": "u2", "entity": "User", "node_ids": [3]},
            ],
            "path": {"type": "shortest", "from": "u1", "to": "u2", "max_depth": 3,
                     "rel_types": ["MEMBER_OF", "CONTAINS", "IN_PROJECT"]},
        },
    },
    # ---- stress queries: heavier scans to surface ClickHouse memory / large
    # result-set behaviour (analogues of staging's stress_search_1000 /
    # stress_neighbors_wide / stress_traversal_wide). The 0.83.1 DSL requires
    # node_ids on >=1 node, so these scale the *limit* rather than going fully
    # unbounded (an unbounded aggregation is rejected as a compile error on v72).
    "stress: search notes (1000)": {
        "type": "query",
        "body": {
            "query_type": "traversal",
            "node": {"id": "n", "entity": "Note", "node_ids": [1]},
            "limit": 1000,
        },
    },
    "stress: neighbors (wide)": {
        "type": "query",
        "body": {
            "query_type": "neighbors",
            "node": {"id": "g", "entity": "Group", "node_ids": [2]},
            "neighbors": {"node": "g", "direction": "both"},
            "limit": 1000,
        },
    },
    "stress: traversal (wide 2-join)": {
        "type": "query",
        "body": {
            "query_type": "traversal",
            "nodes": [
                {"id": "g", "entity": "Group", "node_ids": [2]},
                {"id": "p", "entity": "Project"},
                {"id": "n", "entity": "Note"},
            ],
            "relationships": [
                {"type": "CONTAINS", "from": "g", "to": "p", "direction": "outgoing"},
                {"type": "IN_PROJECT", "from": "n", "to": "p", "direction": "outgoing"},
            ],
            "limit": 1000,  # DSL caps /limit at 1000
        },
    },
}

# Friendly names mapping for --query flag
QUERY_ALIASES = {k.replace(" ", "_").replace("(", "").replace(")", ""): k for k in QUERIES}


def main():
    parser = argparse.ArgumentParser(description="Direct gRPC load test against GKG webserver")
    parser.add_argument("--endpoint", default="[::1]:50054", help="GKG gRPC endpoint (default: [::1]:50054)")
    parser.add_argument("--tls", action="store_true", help="Use TLS channel credentials")
    parser.add_argument("--rounds", type=int, default=3, help="Rounds per query type (default: 3)")
    parser.add_argument("--concurrency", type=int, default=20, help="Concurrent requests per round (default: 20)")
    parser.add_argument("--user-id", type=int, default=1, help="User ID for JWT (default: 1)")
    parser.add_argument("--username", default="root", help="Username for JWT (default: root)")
    parser.add_argument("--org-id", type=int, default=1, help="Organization ID (default: 1)")
    parser.add_argument("--admin", action="store_true", default=True, help="Set admin=true in JWT (default: true)")
    parser.add_argument("--no-admin", dest="admin", action="store_false", help="Use traversal IDs instead of admin")
    parser.add_argument("--preset", choices=list(STAGING_PRESETS.keys()), help="Use a predefined user identity + traversal paths")
    parser.add_argument("--debug", action="store_true", help="Single-shot debug mode (1 req, verbose)")
    parser.add_argument("--query", help="Run only this query (use --list to see names)")
    parser.add_argument("--list", action="store_true", help="List available query names")
    args = parser.parse_args()

    if args.list:
        for name in QUERIES:
            qtype = QUERIES[name]["type"]
            print(f"  {name:<25} ({qtype})")
        return

    jwt_secret = os.environ.get("GKG_JWT_SECRET")
    if not jwt_secret:
        print("ERROR: GKG_JWT_SECRET environment variable required (base64-encoded)")
        sys.exit(1)

    traversal_ids = []
    user_id, username, org_id = args.user_id, args.username, args.org_id

    if args.preset:
        p = STAGING_PRESETS[args.preset]
        user_id, username, org_id = p["user_id"], p["username"], p["org_id"]
        traversal_ids = p["traversal_ids"]
        print(f"  Preset: {args.preset} -> {username} (id={user_id}), {len(traversal_ids)} traversal paths")
    elif os.environ.get("STAGING_PAT") and not args.admin:
        print("Fetching user info from staging...")
        info = fetch_user_info()
        user_id, username = info["id"], info["username"]
        groups = fetch_groups()
        traversal_ids = [f"{org_id}/{g['id']}/" for g in groups]
        print(f"  User: {username} (id={user_id}), {len(traversal_ids)} traversal paths")

    jwt_token = build_jwt(jwt_secret, user_id, username, org_id, traversal_ids, args.admin)

    script_dir = os.path.dirname(os.path.abspath(__file__))
    repo_proto = os.path.join(script_dir, "..", "crates", "gkg-server", "proto", "gkg.proto")
    local_proto = os.path.join(script_dir, "gkg.proto")
    proto_path = repo_proto if os.path.exists(repo_proto) else local_proto
    pb2, pb2_grpc = compile_proto(proto_path)

    if args.tls:
        channel = grpc.secure_channel(args.endpoint, grpc.ssl_channel_credentials())
    else:
        channel = grpc.insecure_channel(args.endpoint)

    stub = pb2_grpc.KnowledgeGraphServiceStub(channel)
    metadata = [("authorization", f"Bearer {jwt_token}")]

    queries_to_run = QUERIES
    if args.query:
        resolved = QUERY_ALIASES.get(args.query, args.query)
        if resolved not in QUERIES:
            print(f"ERROR: unknown query '{args.query}'. Use --list to see available names.")
            sys.exit(1)
        queries_to_run = {resolved: QUERIES[resolved]}

    if args.debug:
        concurrency, rounds = 1, 1
    else:
        concurrency, rounds = args.concurrency, args.rounds

    total = concurrency * rounds

    print(f"Endpoint:    {args.endpoint}")
    print(f"Concurrency: {concurrency}")
    print(f"Rounds:      {rounds}")
    print(f"Samples:     {total} per query type")
    print(f"JWT:         user_id={user_id} username={username} admin={args.admin}")
    print()

    results = []
    for label, spec in queries_to_run.items():
        if spec["type"] == "unary":
            fire_fn = lambda rpc=spec["rpc"]: fire_unary(stub, pb2, metadata, rpc, verbose=args.debug)
        else:
            fire_fn = lambda body=spec["body"]: fire_query(stub, pb2, metadata, body, verbose=args.debug)

        r = bench(label, fire_fn, concurrency, rounds)
        results.append(r)
        status = "OK" if r["errors"] == 0 else f"{r['errors']} errors"
        print(f"  done: {label} ({r['n']} reqs, {status})")

    print()
    header = f"{'Query':<25} {'N':>4} {'Err':>4} {'Min':>7} {'Mean':>7} {'Med':>7} {'p90':>7} {'p99':>7} {'Max':>7} {'Stdev':>7}"
    print(header)
    print("-" * len(header))
    for r in results:
        print(
            f"{r['label']:<25} {r['n']:>4} {r['errors']:>4}"
            f" {r['min']:>6.0f}ms {r['mean']:>6.0f}ms {r['median']:>6.0f}ms"
            f" {r['p90']:>6.0f}ms {r['p99']:>6.0f}ms {r['max']:>6.0f}ms"
            f" {r['stdev']:>6.0f}ms"
        )

    errored = [r for r in results if r["errors"] > 0]
    if errored:
        print()
        print("ERROR DETAILS")
        print("-" * 60)
        for r in errored:
            print(f"\n  {r['label']} ({r['errors']}/{r['n']} failed):")
            for body, count in r["error_bodies"].items():
                print(f"    [{count}x] {body}")

    channel.close()


if __name__ == "__main__":
    main()
