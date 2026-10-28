// Copies the declarations Claude Code writes on a `claude --plugin-dir .` load into types/,
// so typechecking and CI need no Claude Code session.
import { join } from 'node:path'

const root = join(import.meta.dir, '..')
for (const name of ['claude-code', 'claude-code-mcp', 'claude-code-tools']) {
  const written = Bun.file(join(root, '.claude-plugin/types', name, 'index.d.ts'))
  if (!(await written.exists())) throw new Error('Run claude --plugin-dir . once to write .claude-plugin/types first')
  await Bun.write(join(root, 'types', name, 'index.d.ts'), written)
}
