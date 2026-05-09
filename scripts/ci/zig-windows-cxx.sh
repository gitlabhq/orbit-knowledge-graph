#!/usr/bin/env bash
# C++ counterpart of zig-windows-cc.sh.
set -euo pipefail
args=()
for a in "$@"; do
    case "$a" in
        -Wp,*) ;;
        *) args+=("$a") ;;
    esac
done
exec zig c++ -target x86_64-windows-gnu "${args[@]}"
