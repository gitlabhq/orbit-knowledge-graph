import { existsSync } from 'node:fs'
import { homedir } from 'node:os'
import { join } from 'node:path'

import { MARK, describeOrbitCall } from '../core/calls.ts'

type Toast = { body: { message: string; variant: 'info'; duration: number } }
type Input = { client: { tui: { showToast(toast: Toast): Promise<unknown> } } }

const REMINDER = '{{reminder}}'

export const OrbitPlugin = async ({ client }: Input) => {
  let reminded = false
  const root = process.env['ORBIT_DATA_DIR'] || join(homedir(), '.gitlab', 'orbit')
  return {
    'tool.execute.before': async (input: { tool: string }, output: { args: unknown }) => {
      const call = describeOrbitCall(input.tool, output.args)
      if (!call) return
      const message = MARK + ' ' + call.active + (call.detail ? '  ' + call.detail : '')
      Promise.resolve()
        .then(() => client.tui.showToast({ body: { message, variant: 'info', duration: 2000 } }))
        .catch(() => {})
    },
    'tool.execute.after': async (input: { tool: string }, output: { output: string }) => {
      if (reminded || input.tool !== 'bash') return
      if (!existsSync(join(root, 'local', 'manifest.duckdb'))) return
      output.output = output.output + '\n\n' + REMINDER
      reminded = true
    },
  }
}
