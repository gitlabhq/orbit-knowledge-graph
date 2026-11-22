#!/usr/bin/env python3
import json
import os
import subprocess
import sys
import threading
import unittest
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path

SCRIPT = Path(__file__).with_name("docs-write-merge-order.py")
MARKER = "<!-- docs-write: source=!12,!13 -->"


def serve(mrs):
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            iid = int(self.path.rsplit("/", 1)[1])
            body = json.dumps(mrs[iid]).encode() if iid in mrs else b"{}"
            self.send_response(200 if iid in mrs else 404)
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *_):
            pass

    server = HTTPServer(("127.0.0.1", 0), Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    return server


def run_check(mrs, branch="docs-write/mr-12"):
    server = serve(mrs)
    try:
        env = dict(os.environ, CI_API_V4_URL="http://127.0.0.1:{}".format(server.server_port),
                   CI_MERGE_REQUEST_PROJECT_ID="7", CI_MERGE_REQUEST_IID="50",
                   CI_MERGE_REQUEST_SOURCE_BRANCH_NAME=branch)
        return subprocess.run([sys.executable, str(SCRIPT)], capture_output=True, text=True, env=env)
    finally:
        server.shutdown()


def docs_mr(description=MARKER):
    return {50: {"description": description}}


class DocsWriteMergeOrderTest(unittest.TestCase):
    def test_every_source_merged_passes(self):
        result = run_check({**docs_mr(), 12: {"state": "merged"}, 13: {"state": "merged"}})
        self.assertEqual(result.returncode, 0, result.stdout)

    def test_an_open_source_fails(self):
        result = run_check({**docs_mr(), 12: {"state": "merged"}, 13: {"state": "opened"}})
        self.assertEqual(result.returncode, 1)
        self.assertIn("!13 (opened)", result.stdout)

    def test_a_closed_host_fails_and_a_closed_sibling_passes(self):
        self.assertEqual(run_check({**docs_mr(), 12: {"state": "closed"}, 13: {"state": "merged"}}).returncode, 1)
        self.assertEqual(run_check({**docs_mr(), 12: {"state": "merged"}, 13: {"state": "closed"}}).returncode, 0)

    def test_the_branch_names_a_source_without_a_marker(self):
        result = run_check({**docs_mr(""), 12: {"state": "opened"}})
        self.assertEqual(result.returncode, 1)

    def test_no_marker_and_no_flow_branch_passes(self):
        self.assertEqual(run_check(docs_mr(""), branch="feature").returncode, 0)

    def test_an_unreadable_api_fails(self):
        self.assertNotEqual(run_check({}).returncode, 0)


if __name__ == "__main__":
    unittest.main()
