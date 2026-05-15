#!/usr/bin/env bash
#
# Install the llvm-mingw cross toolchain (UCRT, libc++) under /opt/llvm-mingw
# so a Linux runner can build Windows x86_64 binaries with Rust's
# `x86_64-pc-windows-gnullvm` target. Pin the upstream release with
# LLVM_MINGW_VERSION; defaults to the most recent release we've validated.
#
# We delete libc++ / libunwind import libs and DLLs after extraction. The
# reason is subtle: clang++ (the linker driver) and `cc-rs` (used by
# libduckdb-sys to compile the bundled C++ amalgamation) both inject
# `-lc++` / `-lstdc++` / `-lunwind` near the end of the link line, after any
# `-static-libstdc++` / `-static-libgcc` scope has already closed. With the
# shared `.dll.a` import libs on disk the linker resolves the late `-l*`
# references to those, producing duplicate symbols against the static
# archives `-static-libstdc++` requested — and an orbit.exe that needs
# libc++.dll / libunwind.dll alongside it at runtime.
#
# Removing the dynamic forms leaves the linker no choice but `libc++.a` and
# `libunwind.a`, yielding a fully self-contained binary (only Windows system
# DLLs imported).
set -euo pipefail

VERSION="${LLVM_MINGW_VERSION:-20260505}"
ROOT="/opt/llvm-mingw"
TARBALL="llvm-mingw-${VERSION}-ucrt-ubuntu-22.04-x86_64.tar.xz"

curl -fsSL "https://github.com/mstorsjo/llvm-mingw/releases/download/${VERSION}/${TARBALL}" \
    | tar -xJ -C /opt
mv "/opt/llvm-mingw-${VERSION}-ucrt-ubuntu-22.04-x86_64" "$ROOT"

rm -f \
    "$ROOT/x86_64-w64-mingw32/lib/libc++.dll.a" \
    "$ROOT/x86_64-w64-mingw32/lib/libunwind.dll.a" \
    "$ROOT/x86_64-w64-mingw32/bin/libc++.dll" \
    "$ROOT/x86_64-w64-mingw32/bin/libunwind.dll"

"$ROOT/bin/x86_64-w64-mingw32-clang" --version | head -1
