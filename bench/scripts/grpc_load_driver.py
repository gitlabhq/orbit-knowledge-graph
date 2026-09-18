#!/usr/bin/env python3
"""
Concurrent query load driver for the Orbit gRPC webserver.

Mints an HS256 JWT, opens a channel to the webserver, and fires N concurrent
requests per query type across R rounds. It answers the bidirectional
redaction exchange by approving every resource, so query latency reflects the
full pipeline (compile, ClickHouse execute, hydration, redaction round-trip)
minus a real Rails authorization backend.

The driver is environment-agnostic. It runs against an insecure Caproni
endpoint or a TLS-terminated bench webserver, selected by --tls.

Usage (bench, via query-load.sh which sets up the port-forward and secrets):
  GKG_JWT_SECRET=<base64-secret> python3 grpc_load_driver.py \
    --endpoint 127.0.0.1:50054 --tls --tls-ca /tmp/ca.crt \
    --tls-server-name gkg-webserver.e2e-<run>-gkg.svc.cluster.local \
    --rounds 5 --concurrency 4

Environment variables:
  GKG_JWT_SECRET   Base64-encoded HS256 signing key (required). It is the value
                   of the gkg-secrets `gitlab-jwt-signing-key` field, which the
                   server also base64-decodes before use.

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

import grpc
from grpc_tools import protoc


def _b64url(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode()


def _jwt_encode(payload: dict, secret: bytes) -> str:
    header = _b64url(json.dumps({"alg": "HS256", "typ": "JWT"}).encode())
    body = _b64url(json.dumps(payload).encode())
    sig = hmac.new(secret, f"{header}.{body}".encode(), hashlib.sha256).digest()
    return f"{header}.{body}.{_b64url(sig)}"


def build_jwt(secret_b64: str, user_id: int, username: str, org_id: int, admin: bool) -> str:
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
    return _jwt_encode(payload, base64.b64decode(secret_b64.strip()))


def compile_proto(proto_path: str):
    import importlib
    import tempfile

    out_dir = tempfile.mkdtemp(prefix="orbit_proto_")
    result = protoc.main([
        "grpc_tools.protoc",
        f"-I{os.path.dirname(proto_path)}",
        f"--python_out={out_dir}",
        f"--grpc_python_out={out_dir}",
        os.path.basename(proto_path),
    ])
    if result != 0:
        sys.exit(f"ERROR: protoc failed with code {result} on {proto_path}")
    sys.path.insert(0, out_dir)
    return importlib.import_module("orbit_pb2"), importlib.import_module("orbit_pb2_grpc")


def _approve_all(pb2, req):
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


def fire_query(stub, pb2, metadata, query: dict):
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
        for msg in stub.ExecuteQuery(request_iter(), metadata=metadata):
            field = msg.WhichOneof("content")
            if field == "redaction" and msg.redaction.WhichOneof("content") == "required":
                request_queue.put(pb2.ExecuteQueryMessage(
                    redaction=_approve_all(pb2, msg.redaction.required)
                ))
                continue
            if field == "result":
                return 200, (time.perf_counter() - start) * 1000, None
            if field == "error":
                err = f"{msg.error.code}: {msg.error.message}"
                return err, (time.perf_counter() - start) * 1000, err
        return "no_result", (time.perf_counter() - start) * 1000, "stream ended without result"
    except grpc.RpcError as e:
        return str(e.code()), (time.perf_counter() - start) * 1000, e.details()
    finally:
        request_queue.put(None)


def fire_unary(stub, pb2, metadata, rpc_name: str):
    start = time.perf_counter()
    try:
        if rpc_name == "get_cluster_health":
            stub.GetClusterHealth(pb2.GetClusterHealthRequest(format=pb2.RESPONSE_FORMAT_RAW), metadata=metadata)
        elif rpc_name == "list_tools":
            stub.ListTools(pb2.ListToolsRequest(), metadata=metadata)
        elif rpc_name == "get_graph_schema":
            stub.GetGraphSchema(pb2.GetGraphSchemaRequest(format=pb2.RESPONSE_FORMAT_RAW), metadata=metadata)
        return 200, (time.perf_counter() - start) * 1000, None
    except grpc.RpcError as e:
        return str(e.code()), (time.perf_counter() - start) * 1000, e.details()


def pct(sorted_vals, p):
    return sorted_vals[min(int(len(sorted_vals) * p), len(sorted_vals) - 1)]


def bench(label, fire_fn, concurrency, rounds):
    all_times, errors, error_bodies = [], 0, {}
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
        "label": label, "n": len(all_times), "errors": errors, "error_bodies": error_bodies,
        "min": min(s), "mean": statistics.mean(s), "median": statistics.median(s),
        "p90": pct(s, 0.9), "p99": pct(s, 0.99), "max": max(s),
        "stdev": statistics.stdev(s) if len(s) > 1 else 0,
    }


# SDLC query corpus. node_ids are fixed low IDs so every query compiles and
# executes against a real-data-seeded graph; empty result sets are expected and
# still exercise the full pipeline. Sampling real IDs is a future enhancement.
QUERIES = {
    "get_cluster_health": {"type": "unary", "rpc": "get_cluster_health"},
    "list_tools": {"type": "unary", "rpc": "list_tools"},
    "get_graph_schema": {"type": "unary", "rpc": "get_graph_schema"},
    "search (user)": {"type": "query", "body": {
        "query_type": "traversal",
        "nodes": [{"id": "u", "entity": "User", "node_ids": [1]}], "limit": 5}},
    "search (projects)": {"type": "query", "body": {
        "query_type": "traversal",
        "nodes": [{"id": "p", "entity": "Project", "node_ids": [1]}], "limit": 10}},
    "search (groups)": {"type": "query", "body": {
        "query_type": "traversal",
        "nodes": [{"id": "g", "entity": "Group", "node_ids": [2]}], "limit": 10}},
    "search (notes 100)": {"type": "query", "body": {
        "query_type": "traversal",
        "nodes": [{"id": "n", "entity": "Note", "node_ids": [1]}], "limit": 100}},
    # NeighborsConfig carries only direction/rel_types; the center is the single
    # entry in `nodes`.
    "neighbors": {"type": "query", "body": {
        "query_type": "neighbors",
        "nodes": [{"id": "p", "entity": "Project", "node_ids": [1]}],
        "neighbors": {"direction": "both"}, "limit": 50}},
    "traversal (1 join)": {"type": "query", "body": {
        "query_type": "traversal",
        "nodes": [{"id": "g", "entity": "Group", "node_ids": [2]},
                  {"id": "p", "entity": "Project"}],
        "relationships": [{"type": "CONTAINS", "from": "g", "to": "p", "direction": "outgoing"}],
        "limit": 50}},
    "traversal (2 joins)": {"type": "query", "body": {
        "query_type": "traversal",
        "nodes": [{"id": "g", "entity": "Group", "node_ids": [2]},
                  {"id": "p", "entity": "Project"},
                  {"id": "n", "entity": "Note"}],
        "relationships": [{"type": "CONTAINS", "from": "g", "to": "p", "direction": "outgoing"},
                          {"type": "IN_PROJECT", "from": "n", "to": "p", "direction": "outgoing"}],
        "limit": 50}},
    "aggregation": {"type": "query", "body": {
        "query_type": "aggregation",
        "nodes": [{"id": "g", "entity": "Group", "node_ids": [2]},
                  {"id": "p", "entity": "Project"}],
        "relationships": [{"type": "CONTAINS", "from": "g", "to": "p", "direction": "outgoing"}],
        "aggregations": [{"count": "p", "as": "project_count"}],
        "group_by": ["g"]}},
    "path (depth 2)": {"type": "query", "body": {
        "query_type": "path_finding",
        "nodes": [{"id": "u1", "entity": "User", "node_ids": [1]},
                  {"id": "u2", "entity": "User", "node_ids": [2]}],
        "path": {"type": "shortest", "from": "u1", "to": "u2", "max_depth": 2,
                 "rel_types": ["MEMBER_OF", "CONTAINS", "IN_PROJECT"]}}},
    "path (depth 3)": {"type": "query", "body": {
        "query_type": "path_finding",
        "nodes": [{"id": "u1", "entity": "User", "node_ids": [1]},
                  {"id": "u2", "entity": "User", "node_ids": [3]}],
        "path": {"type": "shortest", "from": "u1", "to": "u2", "max_depth": 3,
                 "rel_types": ["MEMBER_OF", "CONTAINS", "IN_PROJECT"]}}},
    # Stress queries scale `limit` (the DSL caps it at 1000) to surface
    # ClickHouse memory and large result-set behaviour.
    "stress: search notes (1000)": {"type": "query", "body": {
        "query_type": "traversal",
        "nodes": [{"id": "n", "entity": "Note", "node_ids": [1]}], "limit": 1000}},
    "stress: neighbors (wide)": {"type": "query", "body": {
        "query_type": "neighbors",
        "nodes": [{"id": "g", "entity": "Group", "node_ids": [2]}],
        "neighbors": {"direction": "both"}, "limit": 1000}},
    "stress: traversal (wide 2-join)": {"type": "query", "body": {
        "query_type": "traversal",
        "nodes": [{"id": "g", "entity": "Group", "node_ids": [2]},
                  {"id": "p", "entity": "Project"},
                  {"id": "n", "entity": "Note"}],
        "relationships": [{"type": "CONTAINS", "from": "g", "to": "p", "direction": "outgoing"},
                          {"type": "IN_PROJECT", "from": "n", "to": "p", "direction": "outgoing"}],
        "limit": 1000}},
}


def default_proto_path() -> str:
    repo_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
    return os.path.join(repo_root, "crates", "orbit-server", "proto", "orbit.proto")


def build_channel(args):
    if not args.tls:
        return grpc.insecure_channel(args.endpoint)
    root = None
    if args.tls_ca:
        with open(args.tls_ca, "rb") as fh:
            root = fh.read()
    creds = grpc.ssl_channel_credentials(root_certificates=root)
    options = []
    if args.tls_server_name:
        options.append(("grpc.ssl_target_name_override", args.tls_server_name))
    return grpc.secure_channel(args.endpoint, creds, options=options)


def main():
    parser = argparse.ArgumentParser(description="Concurrent query load driver for the Orbit gRPC webserver")
    parser.add_argument("--endpoint", default="127.0.0.1:50054")
    parser.add_argument("--proto", default=default_proto_path(), help="Path to orbit.proto")
    parser.add_argument("--rounds", type=int, default=5)
    parser.add_argument("--concurrency", type=int, default=4)
    parser.add_argument("--user-id", type=int, default=1)
    parser.add_argument("--username", default="root")
    parser.add_argument("--org-id", type=int, default=1)
    parser.add_argument("--admin", action="store_true", default=True)
    parser.add_argument("--tls", action="store_true", help="Use a TLS channel")
    parser.add_argument("--tls-ca", help="PEM CA bundle for TLS verification")
    parser.add_argument("--tls-server-name", help="Override the TLS SNI / cert name")
    args = parser.parse_args()

    jwt_secret = os.environ.get("GKG_JWT_SECRET")
    if not jwt_secret:
        sys.exit("ERROR: GKG_JWT_SECRET environment variable required (base64-encoded)")

    jwt_token = build_jwt(jwt_secret, args.user_id, args.username, args.org_id, args.admin)
    pb2, pb2_grpc = compile_proto(args.proto)
    channel = build_channel(args)
    stub = pb2_grpc.OrbitServiceStub(channel)
    metadata = [("authorization", f"Bearer {jwt_token}")]

    total = args.concurrency * args.rounds
    print(f"Endpoint:    {args.endpoint} (tls={args.tls})")
    print(f"Concurrency: {args.concurrency}")
    print(f"Rounds:      {args.rounds}")
    print(f"Samples:     {total} per query type")
    print(f"JWT:         user_id={args.user_id} username={args.username} admin={args.admin}")
    print()

    results = []
    for label, spec in QUERIES.items():
        if spec["type"] == "unary":
            fire_fn = lambda rpc=spec["rpc"]: fire_unary(stub, pb2, metadata, rpc)
        else:
            fire_fn = lambda body=spec["body"]: fire_query(stub, pb2, metadata, body)
        r = bench(label, fire_fn, args.concurrency, args.rounds)
        results.append(r)
        status = "OK" if r["errors"] == 0 else f"{r['errors']} errors"
        print(f"  done: {label} ({r['n']} reqs, {status})")

    print()
    header = (f"{'Query':<28} {'N':>4} {'Err':>4} {'Min':>7} {'Mean':>7} {'Med':>7}"
              f" {'p90':>7} {'p99':>7} {'Max':>7} {'Stdev':>7}")
    print(header)
    print("-" * len(header))
    for r in results:
        print(f"{r['label']:<28} {r['n']:>4} {r['errors']:>4}"
              f" {r['min']:>6.0f}ms {r['mean']:>6.0f}ms {r['median']:>6.0f}ms"
              f" {r['p90']:>6.0f}ms {r['p99']:>6.0f}ms {r['max']:>6.0f}ms {r['stdev']:>6.0f}ms")

    errored = [r for r in results if r["errors"] > 0]
    if errored:
        print("\nERROR DETAILS\n" + "-" * 60)
        for r in errored:
            print(f"\n  {r['label']} ({r['errors']}/{r['n']} failed):")
            for body, count in r["error_bodies"].items():
                print(f"    [{count}x] {body}")

    channel.close()

    total_reqs = sum(r["n"] for r in results)
    total_errs = sum(r["errors"] for r in results)
    if total_reqs > 0 and total_errs == total_reqs:
        sys.exit("ERROR: every request failed; check auth, TLS, and endpoint")


if __name__ == "__main__":
    main()
