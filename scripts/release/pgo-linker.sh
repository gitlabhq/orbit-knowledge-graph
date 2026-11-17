#!/bin/sh
# Linker wrapper for the PGO instrumented build under cargo-zigbuild. rustc links
# the profiler runtime with a separate `-u __llvm_profile_runtime` argument, which
# `zig cc` reads as an input file ("unrecognized file extension"); join it to `-u<sym>`.
# Delegates to the linker cargo-zigbuild configured for PGO_TARGET.
real=$(printenv "CARGO_TARGET_$(echo "$PGO_TARGET" | tr 'a-z-' 'A-Z_')_LINKER")
[ -n "$real" ] || { echo "pgo-linker: no zigbuild linker for $PGO_TARGET" >&2; exit 1; }
n=$#
while [ "$n" -gt 0 ]; do
    arg=$1
    shift
    n=$((n - 1))
    if [ "$arg" = "-u" ] && [ "$n" -gt 0 ]; then
        arg="-u$1"
        shift
        n=$((n - 1))
    fi
    set -- "$@" "$arg"
done
exec "$real" "$@"
