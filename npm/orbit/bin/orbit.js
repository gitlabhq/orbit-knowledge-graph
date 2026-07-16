#!/usr/bin/env node
"use strict";

const { spawnSync } = require("node:child_process");

const packageName = `@gitlab/orbit-${process.platform}-${process.arch}`;
const binaryName = process.platform === "win32" ? "orbit.exe" : "orbit";

let binaryPath;
try {
  binaryPath = require.resolve(`${packageName}/${binaryName}`);
} catch {
  console.error(
    `@gitlab/orbit: no prebuilt orbit binary for ${process.platform}-${process.arch}.\n` +
      `The optional dependency ${packageName} is not installed. Reinstall without ` +
      `--no-optional / --omit=optional, or use another install method: ` +
      `https://docs.gitlab.com/orbit/local/getting-started/`
  );
  process.exit(1);
}

const result = spawnSync(binaryPath, process.argv.slice(2), {
  stdio: "inherit",
});

if (result.error) {
  console.error(`@gitlab/orbit: failed to run ${binaryPath}: ${result.error.message}`);
  process.exit(1);
}

process.exit(result.status ?? 1);
