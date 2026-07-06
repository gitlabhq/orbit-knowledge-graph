#!/usr/bin/env bash
# Sourced (not executed) by the e2e CI job: it exports E2E_GKG_IMAGE/E2E_GKG_TAG
# and backgrounds the MR image build as a child of the job shell so the job can
# `wait "$E2E_BUILD_PID"` after setup.sh.

E2E_BUILD_PID=""
if [ "$CI_COMMIT_BRANCH" = "$CI_DEFAULT_BRANCH" ]; then
  # Reuse multi-arch dev image published by docker-manifest.
  export E2E_GKG_IMAGE="${CI_REGISTRY_IMAGE}/gkg"
  export E2E_GKG_TAG="${DEV_PREFIX}-${CI_COMMIT_SHORT_SHA}"
else
  # MR: build a debug image inline for faster iteration.
  export E2E_GKG_IMAGE="${CI_REGISTRY_IMAGE}/gkg-e2e"
  export E2E_GKG_TAG="${CI_COMMIT_SHORT_SHA}"
  # Skip rebuild on job retry if image for this SHA already exists.
  if docker manifest inspect "${E2E_GKG_IMAGE}:${E2E_GKG_TAG}" >/dev/null 2>&1; then
    echo "Reusing existing image ${E2E_GKG_IMAGE}:${E2E_GKG_TAG}"
  else
    # The stack deploy absorbs the build; the nudger below respawns gkg
    # pods stuck in pull backoff once the push lands.
    (
      set -e
      # Drop DWARF: ~5x smaller image; backtraces keep function names.
      CARGO_PROFILE_DEV_DEBUG=0 cargo build -p gkg-server --locked
      cp target/debug/gkg-server .
      docker build -t "${E2E_GKG_IMAGE}:${E2E_GKG_TAG}" -f e2e/Dockerfile.e2e .
      docker push "${E2E_GKG_IMAGE}:${E2E_GKG_TAG}"
    ) > e2e-image-build.log 2>&1 &
    E2E_BUILD_PID=$!
    (
      while kill -0 "$E2E_BUILD_PID" 2>/dev/null; do sleep 5; done
      docker manifest inspect "${E2E_GKG_IMAGE}:${E2E_GKG_TAG}" >/dev/null 2>&1 || exit 0
      kubectl -n "e2e-${CI_COMMIT_SHORT_SHA}-gkg" delete pods \
        --field-selector=status.phase=Pending 2>/dev/null || true
    ) &
  fi
fi
