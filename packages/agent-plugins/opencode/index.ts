import { MARK, describeOrbitCall, detailText } from '../core/calls.ts'

type Toast = { body: { message: string; variant: 'info'; duration: number } }
type Input = { client: { tui: { showToast(toast: Toast): Promise<unknown> } } }

export const OrbitUiPlugin = async ({ client }: Input) => ({
  'tool.execute.before': async (input: { tool: string }, output: { args: unknown }) => {
    try {
      const call = describeOrbitCall(input.tool, output.args)
      if (!call) return
      const detail = detailText(call)
      const message = MARK + ' ' + call.active + (detail ? '  ' + detail : '')
      client.tui.showToast({ body: { message, variant: 'info', duration: 2000 } }).catch(() => {})
    } catch {}
  },
})
