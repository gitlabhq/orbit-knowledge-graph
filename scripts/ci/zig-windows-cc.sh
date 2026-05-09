#!/usr/bin/env bash
# Wrapper around `zig cc -target x86_64-windows-gnu` that filters out the
# GCC-only `-Wp,*` flags aws-lc-sys hard-codes (zig's clang rejects them) and
# the cc-rs-supplied `--target=x86_64-pc-windows-gnu` (zig's clang doesn't
# accept the `pc` vendor in clang-style triples — we already pass our own
# `-target` flag).
set -e
args=()
skip_next=0
for a in "$@"; do
    if [ "$skip_next" -eq 1 ]; then
        skip_next=0
        continue
    fi
    case "$a" in
        -Wp,*) ;;
        --target=*) ;;
        -target) skip_next=1 ;;
        *) args+=("$a") ;;
    esac
done
exec zig cc -target x86_64-windows-gnu "${args[@]}"
