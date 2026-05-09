#!/usr/bin/env bash
# Wrapper around `zig cc -target x86_64-windows-gnu` that filters out the
# GCC-flavored `-Wp,*` preprocessor passthroughs aws-lc-sys hard-codes;
# zig's clang rejects them with "unsupported preprocessor arg".
set -euo pipefail
args=()
for a in "$@"; do
    case "$a" in
        -Wp,*) ;;
        *) args+=("$a") ;;
    esac
done
exec zig cc -target x86_64-windows-gnu "${args[@]}"
