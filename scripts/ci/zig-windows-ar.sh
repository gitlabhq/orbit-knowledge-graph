#!/usr/bin/env bash
# Static archive helper for the zig-based windows-gnu build.
set -euo pipefail
exec zig ar "$@"
