#!/usr/bin/env bash
# C++ counterpart of zig-windows-cc.sh. Same filters apply.
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
exec zig c++ -target x86_64-windows-gnu "${args[@]}"
