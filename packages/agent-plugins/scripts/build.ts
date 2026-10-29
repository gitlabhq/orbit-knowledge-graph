import { copyFile, mkdir } from 'node:fs/promises'
import { join } from 'node:path'

import type { BunPlugin } from 'bun'

const root = join(import.meta.dir, '..')
const NOTICE = '/*! es-toolkit, MIT License, https://github.com/toss/es-toolkit/blob/main/LICENSE */\n'

const OPENCODE_UI = '../../config/setup/agents/opencode_ui'

// OpenCode resolves @opencode/plugin at runtime, and the server and TUI parts share rpc.ts as
// installed, so the bundle imports them instead of inlining them.
const keepExternal: BunPlugin = {
  name: 'keep-external',
  setup(build) {
    build.onResolve({ filter: /^@opencode\/plugin(\/.*)?$/ }, (args) => ({ path: args.path, external: true }))
    build.onResolve({ filter: /^\.\/rpc\.ts$/ }, (args) => ({ path: args.path, external: true }))
  },
}

async function bundle(entry: string, out: string, minify: boolean, plugins: BunPlugin[] = []): Promise<void> {
  const result = await Bun.build({ entrypoints: [join(root, entry)], target: 'bun', minify, plugins })
  const [output] = result.outputs
  if (!result.success || !output) throw new AggregateError(result.logs, entry + ' did not build')
  const lines = (await output.text()).split('\n').filter((line) => !/^\/\/ (@bun|\S+\.[jt]s)$/.test(line))
  await Bun.write(join(root, out), NOTICE + lines.join('\n'))
}

await bundle('vendor/lib.ts', 'core/lib.js', true)

await mkdir(join(root, OPENCODE_UI), { recursive: true })
await bundle('opencode/index.ts', join(OPENCODE_UI, 'index.js'), false, [keepExternal])
// The TUI part ships as source: OpenCode compiles TUI JSX with its own OpenTUI.
for (const file of ['rpc.ts', 'tui.tsx']) {
  await copyFile(join(root, 'opencode', file), join(root, OPENCODE_UI, file))
}
