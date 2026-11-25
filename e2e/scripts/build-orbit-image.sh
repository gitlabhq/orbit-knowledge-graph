#!/usr/bin/env bash

E2E_BUILD_PID=""
if [ "$CI_COMMIT_BRANCH" = "$CI_DEFAULT_BRANCH" ]; then
  export E2E_GKG_IMAGE="${CI_REGISTRY_IMAGE}/gkg"
  export E2E_GKG_TAG="${DEV_PREFIX}-${CI_COMMIT_SHORT_SHA}-amd64"
  docker manifest inspect "${E2E_GKG_IMAGE}:${E2E_GKG_TAG}" >/dev/null 2>&1 \
    || { echo "${E2E_GKG_IMAGE}:${E2E_GKG_TAG} is not in the registry; docker-build-amd64 for this commit has not published it"; exit 1; }
else
  export E2E_GKG_IMAGE="${CI_REGISTRY_IMAGE}/gkg-e2e"
  export E2E_GKG_TAG="${CI_COMMIT_SHORT_SHA}"
  if docker manifest inspect "${E2E_GKG_IMAGE}:${E2E_GKG_TAG}" >/dev/null 2>&1; then
    echo "Reusing existing image ${E2E_GKG_IMAGE}:${E2E_GKG_TAG}"
  else
    (
      set -e
      CARGO_PROFILE_DEV_DEBUG=0 cargo build -p orbit-server --locked --features libduckdb-sys/bundled
      cp target/debug/gkg-server .
      docker build -t "${E2E_GKG_IMAGE}:${E2E_GKG_TAG}" -f e2e/Dockerfile.e2e .
      docker push "${E2E_GKG_IMAGE}:${E2E_GKG_TAG}"
    ) > e2e-image-build.log 2>&1 &
    E2E_BUILD_PID=$!
    (
      while kill -0 "$E2E_BUILD_PID" 2>/dev/null; do sleep 5; done
      docker manifest inspect "${E2E_GKG_IMAGE}:${E2E_GKG_TAG}" >/dev/null 2>&1 || exit 0
      kubectl -n "e2e-${E2E_SHA}-gkg" delete pods \
        --field-selector=status.phase=Pending 2>/dev/null || true
    ) &
  fi
fi

export E2E_CLI_IMAGE="${CI_REGISTRY_IMAGE}/orbit-cli-e2e:${CI_COMMIT_SHORT_SHA}"
E2E_CLI_PID=""
if docker manifest inspect "$E2E_CLI_IMAGE" >/dev/null 2>&1; then
  echo "Reusing existing image ${E2E_CLI_IMAGE}"
else
  bash scripts/ci/fetch-orbit-cli.sh "$E2E_CLI_IMAGE" > e2e-cli-image.log 2>&1 &
  E2E_CLI_PID=$!
fi
