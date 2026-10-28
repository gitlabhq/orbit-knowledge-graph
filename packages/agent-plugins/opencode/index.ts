import { MARK, describeOrbitCall } from '../core/calls.ts'

type Toast = { body: { message: string; variant: 'info'; duration: number } }
type Input = { client: { tui: { showToast(toast: Toast): Promise<unknown> } } }

export const OrbitUiPlugin = async ({ client }: Input) => ({
  'tool.execute.before': async (input: { tool: string }, output: { args: unknown }) => {
    const call = describeOrbitCall(input.tool, output.args)
    if (!call) return
    const message = MARK + ' ' + call.active + (call.detail ? '  ' + call.detail : '')
    Promise.resolve()
      .then(() => client.tui.showToast({ body: { message, variant: 'info', duration: 2000 } }))
      .catch(() => {})
  },
})
