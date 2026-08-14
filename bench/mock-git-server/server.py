#!/usr/bin/env python3
"""
Mock GitLab internal API for code indexing benchmarks.

Serves two endpoints the code indexer needs:
  GET /api/v4/internal/orbit/project/{id}/info
  GET /api/v4/internal/orbit/project/{id}/repository/archive

Archives are read from a local directory (mounted via GCS FUSE).
"""

import argparse
import json
import re
from http.server import HTTPServer, BaseHTTPRequestHandler
from pathlib import Path

INFO_RE = re.compile(r"/api/v4/internal/orbit/project/(\d+)/info$")
ARCHIVE_RE = re.compile(r"/api/v4/internal/orbit/project/(\d+)/repository/archive")


class CorpusHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        m = INFO_RE.match(self.path)
        if m:
            pid = int(m.group(1))
            body = json.dumps({"project_id": pid, "default_branch": "main"}).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return

        m = ARCHIVE_RE.match(self.path)
        if m:
            pid = int(m.group(1))
            archive = self.server.corpus_dir / f"{pid}.tar.gz"
            if not archive.exists():
                self.send_error(404, f"no archive for project {pid}")
                return
            size = archive.stat().st_size
            self.send_response(200)
            self.send_header("Content-Type", "application/gzip")
            self.send_header("Content-Length", str(size))
            self.end_headers()
            with open(archive, "rb") as f:
                while chunk := f.read(65536):
                    self.wfile.write(chunk)
            return

        self.send_error(404)

    def log_message(self, fmt, *args):
        pass


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--corpus-dir", type=Path, default=Path("/data/corpus"))
    p.add_argument("--port", type=int, default=8090)
    args = p.parse_args()

    server = HTTPServer(("0.0.0.0", args.port), CorpusHandler)
    server.corpus_dir = args.corpus_dir
    count = sum(1 for _ in args.corpus_dir.glob("*.tar.gz")) if args.corpus_dir.exists() else 0
    print(f"corpus server on :{args.port}  dir={args.corpus_dir}  archives={count}")
    server.serve_forever()


if __name__ == "__main__":
    main()
