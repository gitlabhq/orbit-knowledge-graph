import { expect, test } from 'bun:test'

import { OrbitPlugin } from './index.ts'

type ShowToast = (options: { body: { message: string } }) => unknown

async function hooksWith(showToast: ShowToast) {
  return OrbitPlugin({ client: { tui: { showToast: async (toast) => showToast(toast) } } })
}

async function toastsFor(commands: string[], tool = 'bash'): Promise<string[]> {
  const toasts: string[] = []
  const hooks = await hooksWith(async ({ body }) => toasts.push(body.message))
  for (const command of commands) {
    await hooks['tool.execute.before']({ tool }, { args: { command } })
  }
  await new Promise((resolve) => setTimeout(resolve, 0))
  return toasts
}

test('an orbit call shows its running label and what it reads', async () => {
  expect(
    await toastsFor(["glab orbit grep 'auth|login'", 'orbit context src/main.rs:10-40', "orbit workspace sql 'SELECT 1'"]),
  ).toEqual(['◎ Searching code  auth|login', '◎ Reading code  src/main.rs:10-40', '◎ Querying local graph  SELECT 1'])
})

test('other commands and other tools show no toast', async () => {
  expect(await toastsFor(['git status', "echo 'glab orbit grep x'", 'glab orbit dsl'])).toEqual([])
  expect(await toastsFor(['glab orbit grep x'], 'read')).toEqual([])
})

test('the hook returns before the toast does', async () => {
  let release = () => {}
  const shown = new Promise<void>((resolve) => (release = resolve))
  const hooks = await hooksWith(() => shown)
  const before = hooks['tool.execute.before']({ tool: 'bash' }, { args: { command: 'orbit grep x' } })
  const waited = new Promise((resolve) => setTimeout(() => resolve('waited'), 50))
  expect(await Promise.race([Promise.resolve(before).then(() => 'returned'), waited])).toBe('returned')
  release()
})

test('a toast that throws or rejects never fails the call', async () => {
  for (const showToast of [() => { throw new Error('no tui') }, async () => { throw new Error('no tui') }]) {
    const hooks = await hooksWith(showToast)
    await hooks['tool.execute.before']({ tool: 'bash' }, { args: { command: 'orbit grep x' } })
  }
  await new Promise((resolve) => setTimeout(resolve, 0))
})

test('the module exports only the plugin', async () => {
  expect(Object.keys(await import('./index.ts'))).toEqual(['OrbitPlugin'])
})
