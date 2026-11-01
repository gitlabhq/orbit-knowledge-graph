import { readFileSync } from 'node:fs'

import { lookUpMain, main } from './hook.ts'

// The bundle uses Node 20 APIs. An older Node skips the hook instead of failing on some commands.
if (Number(process.versions.node.split('.')[0]) >= 20) {
  let stdin = ''
  try {
    stdin = readFileSync(0, 'utf8')
  } catch {}
  if (process.argv[2] === 'lookups') lookUpMain(stdin)
  else process.stdout.write(main(stdin))
}
