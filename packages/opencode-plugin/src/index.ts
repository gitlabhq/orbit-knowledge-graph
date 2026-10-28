import { existsSync } from 'node:fs'
import { homedir } from 'node:os'
import { join } from 'node:path'

import { MARK, describeOrbitCall } from '@orbit/agent-plugin-core'
import type { Plugin } from '@opencode-ai/plugin'

// orbit setup writes the reminder text from config/setup/setup.yaml in place of this placeholder
const REMINDER = '{{reminder}}'

// OpenCode loads every exported function of a plugin file as a plugin, so this is the only export
export const OrbitPlugin: Plugin = async ({ client }) => {
  let reminded = false
  const root = process.env['ORBIT_DATA_DIR'] || join(homedir(), '.gitlab', 'orbit')
  return {
    'tool.execute.before': async (input, output) => {
      // OpenCode draws tool rows itself, so a toast is the one place a plugin can show an Orbit call
      const call = describeOrbitCall(input.tool, output.args)
      if (!call) return
      const message = MARK + ' ' + call.active + (call.detail ? '  ' + call.detail : '')
      // Not awaited: OpenCode runs the command only after this hook returns
      Promise.resolve()
        .then(() => client.tui.showToast({ body: { message, variant: 'info', duration: 2000 } }))
        .catch(() => {})
    },
    'tool.execute.after': async (input, output) => {
      if (reminded || input.tool !== 'bash') return
      if (!existsSync(join(root, 'local', 'manifest.duckdb'))) return
      output.output = output.output + '\n\n' + REMINDER
      reminded = true
    },
  }
}
