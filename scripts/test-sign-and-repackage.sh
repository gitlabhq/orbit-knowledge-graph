#!/usr/bin/env bash
set -euo pipefail

# Verifies that macOS signing threads the library-validation entitlement into
# the signer, and that Windows signing does not. The real signing job needs the
# protected PKCS11 cert and is release-gated, so this substitutes stub signers
# and asserts the arguments sign-and-repackage.sh builds. See
# scripts/macos-entitlements.plist for why the entitlement exists.

script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
sign_script="${script_dir}/sign-and-repackage.sh"
entitlements="${script_dir}/macos-entitlements.plist"
failures=0

fail() {
    echo "FAIL: $1" >&2
    failures=$((failures + 1))
}

assert_eq() {
    if [ "$1" != "$2" ]; then
        fail "$3: expected <$2>, got <$1>"
    fi
}

run_signer() {
    local platform=$1 binary=$2 archive=$3
    local stub_dir dump
    stub_dir=$(mktemp -d)
    dump="${stub_dir}/args"
    for signer in sign-macos-binaries sign-windows-binaries; do
        cat > "${stub_dir}/${signer}" <<STUB
#!/usr/bin/env bash
printf '%s\n' "\$#" > "${dump}"
for a in "\$@"; do printf '%s\n' "\$a" >> "${dump}"; done
STUB
        chmod +x "${stub_dir}/${signer}"
    done
    PATH="${stub_dir}:${PATH}" bash "$sign_script" "$archive" "$platform" "$binary" >/dev/null
    cat "${dump}"
    rm -rf "${stub_dir}"
}

test_plist_declares_disable_library_validation() {
    [ -f "$entitlements" ] || { fail "missing $entitlements"; return; }
    if command -v xmllint >/dev/null 2>&1; then
        xmllint --noout "$entitlements" || fail "plist is not well-formed XML"
    fi
    if ! grep -A1 'com.apple.security.cs.disable-library-validation' "$entitlements" | grep -q '<true/>'; then
        fail "plist does not set disable-library-validation to true"
    fi
}

test_macos_threads_entitlement_arg() {
    local work archive out
    work=$(mktemp -d)
    echo binary > "${work}/orbit"
    archive="${work}/orbit-local-darwin-aarch64.tar.gz"
    tar -czf "$archive" -C "$work" orbit
    mapfile -t out < <(run_signer macos orbit "$archive")
    rm -rf "$work"

    assert_eq "${out[0]}" "3" "macos argc"
    assert_eq "${out[1]}" "--rcodesign-args" "macos arg0"
    assert_eq "${out[2]}" "--entitlements-xml-file ${entitlements}" "macos arg1"
    case "${out[3]}" in
        */orbit) ;;
        *) fail "macos arg2 should be the extracted binary, got <${out[3]}>" ;;
    esac
}

test_windows_omits_entitlement_arg() {
    if ! command -v zip >/dev/null 2>&1 || ! command -v unzip >/dev/null 2>&1; then
        echo "SKIP: zip/unzip unavailable, cannot exercise the Windows archive path"
        return
    fi
    local work archive out
    work=$(mktemp -d)
    echo binary > "${work}/orbit.exe"
    archive="${work}/orbit-local-windows-x86_64.zip"
    (cd "$work" && zip -q "$archive" orbit.exe)
    mapfile -t out < <(run_signer windows orbit.exe "$archive")
    rm -rf "$work"

    assert_eq "${out[0]}" "1" "windows argc"
    case "${out[1]}" in
        */orbit.exe) ;;
        *) fail "windows arg0 should be the extracted binary, got <${out[1]}>" ;;
    esac
}

test_plist_declares_disable_library_validation
test_macos_threads_entitlement_arg
test_windows_omits_entitlement_arg

if [ "$failures" -ne 0 ]; then
    echo "sign-and-repackage tests failed: ${failures}" >&2
    exit 1
fi
echo "sign-and-repackage tests passed"
