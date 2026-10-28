import { join } from 'node:path'

const root = join(import.meta.dir, '..')
const NOTICE = '/*! es-toolkit, MIT License, https://github.com/toss/es-toolkit/blob/main/LICENSE */\n'

async function bundle(entry: string, out: string, minify: boolean): Promise<void> {
  const result = await Bun.build({ entrypoints: [join(root, entry)], target: 'bun', minify })
  const [output] = result.outputs
  if (!result.success || !output) throw new AggregateError(result.logs, entry + ' did not build')
  const lines = (await output.text()).split('\n').filter((line) => !/^\/\/ (@bun|\S+\.[jt]s)$/.test(line))
  await Bun.write(join(root, out), NOTICE + lines.join('\n'))
}

await bundle('vendor/lib.ts', 'core/lib.js', true)
await bundle('opencode/index.ts', '../../config/setup/opencode_ui_plugin.js', false)
