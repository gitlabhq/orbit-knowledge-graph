#!/usr/bin/env bash
# Install llvm-mingw under /opt/llvm-mingw. We delete libc++/libunwind
# import libs so the linker can't pick them up alongside the static
# archives `-static-libstdc++` requested — without this the build links
# both and orbit.exe ends up needing libc++.dll/libunwind.dll at runtime.
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
