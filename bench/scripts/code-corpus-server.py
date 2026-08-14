#!/usr/bin/env python3
"""
Mock GitLab internal API for code indexing.

Serves two endpoints the code indexer needs:
  GET /api/v4/internal/orbit/project/{id}/info
  GET /api/v4/internal/orbit/project/{id}/repository/archive

Archives are read from a local directory or GCS bucket.

Usage:
  # From local directory:
  python3 code-corpus-server.py --corpus-dir /tmp/code-corpus --port 8090

  # From GCS (downloads on first request, caches locally):
  python3 code-corpus-server.py --gcs-bucket gs://gkg-code-corpus --port 8090
"""

import argparse
import json
import os
import re
import subprocess
import sys
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
            archive = self._resolve_archive(pid)
            if archive is None:
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

    def _resolve_archive(self, pid: int) -> Path | None:
        local = self.server.corpus_dir / f"{pid}.tar.gz"
        if local.exists():
            return local

        if self.server.gcs_bucket:
            gcs = f"{self.server.gcs_bucket}/{pid}.tar.gz"
            try:
                subprocess.run(
                    ["gsutil", "-q", "cp", gcs, str(local)],
                    check=True, capture_output=True,
                )
                return local
            except subprocess.CalledProcessError:
                return None

        return None

    def log_message(self, fmt, *args):
        pass  # quiet


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--corpus-dir", type=Path, default=Path("/tmp/code-corpus"))
    p.add_argument("--gcs-bucket", type=str, default=os.environ.get("CORPUS_BUCKET"))
    p.add_argument("--port", type=int, default=8090)
    args = p.parse_args()

    args.corpus_dir.mkdir(parents=True, exist_ok=True)

    server = HTTPServer(("0.0.0.0", args.port), CorpusHandler)
    server.corpus_dir = args.corpus_dir
    server.gcs_bucket = args.gcs_bucket
    print(f"corpus server on :{args.port}  dir={args.corpus_dir}  gcs={args.gcs_bucket or 'none'}")
    server.serve_forever()


if __name__ == "__main__":
    main()
