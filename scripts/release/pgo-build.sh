# shellcheck shell=bash
# Sourced by build-local-cli.sh when PGO=1. Expects BUILD, TOOLCHAIN, TARGET and
# BIN_DIR. Rewrites BIN_DIR to the PGO-optimized build.
#
# Inputs:
#   PGO_TRAIN_DIRS   space-separated checkouts to index for training (required)
#   PGO_REPORT_DIR   where stages.tsv (stage, seconds, status) is appended (default pgo-report)
#   PGO_WORK_DIR     raw/merged profiles and the instrumented binary (default target/pgo)
#
# sccache is bypassed: it keys on the -Cprofile-use path, not the profile contents.

PGO_REPORT_DIR="${PGO_REPORT_DIR:-pgo-report}"
PGO_WORK_DIR="${PGO_WORK_DIR:-target/pgo}"
PGO_HOST=$(rustc -vV | awk '/^host:/ { print $2 }')

pgo_stage() {
    local name="$1" started status=ok
    shift
    started=$(date +%s)
    "$@" || status=failed
    printf '%s\t%s\t%s\n' "$name" "$(($(date +%s) - started))" "$status" >>"$PGO_REPORT_DIR/stages.tsv"
    [ "$status" = ok ] || {
        echo "error: PGO stage '$name' failed" >&2
        return 1
    }
}

pgo_cargo_build() {
    RUSTC_WRAPPER="" CARGO_TARGET_DIR="$1" RUSTFLAGS="${RUSTFLAGS:-} $2" "${BUILD[@]}"
}

# The instrumented binary is built for the host with the native linker and only
# runs on this machine: with -Cprofile-generate it segfaults at startup when linked
# by zig (glibc 2.28 and musl). The profile is keyed on source, so it still applies
# to the zigbuild -Cprofile-use build of $TARGET.
pgo_instrumented_build() {
    RUSTC_WRAPPER="" CARGO_TARGET_DIR="$PGO_WORK_DIR/gen" RUSTFLAGS="${RUSTFLAGS:-} -Cprofile-generate=$PGO_WORK_DIR/raw" \
        rustup run "$TOOLCHAIN" cargo build --release --locked -p orbit-cli --bin orbit \
        --target "$PGO_HOST" --features duckdb-client/bundled
}

pgo_train() {
    local dir
    rm -rf "$PGO_WORK_DIR/raw"
    for dir in $PGO_TRAIN_DIRS; do
        rm -rf "$PGO_WORK_DIR/data"
        ORBIT_TELEMETRY_ENABLED=false ORBIT_DATA_DIR="$PGO_WORK_DIR/data" \
            "$PGO_WORK_DIR/gen/${PGO_HOST}/release/orbit" workspace index "$dir" >/dev/null || return 1
    done
    rm -rf "$PGO_WORK_DIR/data"
}

pgo_merge() {
    local llvm_profdata
    # llvm-profdata must come from the same LLVM as rustc.
    rustup component add --toolchain "$TOOLCHAIN" llvm-tools || return 1
    llvm_profdata="$(rustc --print sysroot)/lib/rustlib/${PGO_HOST}/bin/llvm-profdata"
    "$llvm_profdata" merge -o "$PGO_WORK_DIR/merged.profdata" "$PGO_WORK_DIR/raw"
}

pgo_build() {
    if [ -z "${PGO_TRAIN_DIRS:-}" ]; then
        echo "PGO=1 requires PGO_TRAIN_DIRS" >&2
        return 1
    fi
    mkdir -p "$PGO_REPORT_DIR" "$PGO_WORK_DIR"
    PGO_WORK_DIR=$(cd "$PGO_WORK_DIR" && pwd)

    pgo_stage instrumented-build pgo_instrumented_build
    pgo_stage training pgo_train
    pgo_stage merge pgo_merge
    pgo_stage optimized-build pgo_cargo_build "$PGO_WORK_DIR/use" "-Cprofile-use=$PGO_WORK_DIR/merged.profdata"
    # shellcheck disable=SC2034 # read by build-local-cli.sh
    BIN_DIR="$PGO_WORK_DIR/use/${TARGET}/release"
}
