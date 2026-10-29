import type { ClientModule } from 'claude-code'

import { MARK, fade } from './theme.ts'

export type MarkProps = { label: string; detail: string }

const TICK_MS = 50
const FADE_MS = 2000

const OrbitMark: ClientModule<MarkProps, number> = (props, surface) => {
  const { Box, Text } = surface.elements
  if (surface.state === undefined) {
    surface.every(TICK_MS, () => surface.setState((surface.state ?? 0) + 1))
  }

  const phase = (Date.now() % FADE_MS) / FADE_MS
  const color = fade((1 - Math.cos(phase * 2 * Math.PI)) / 2)

  const children = [Text({ color, bold: true, children: [MARK + ' ' + (props.label || 'Exploring Orbit')] })]
  if (props.detail) children.push(Text({ dimColor: true, children: ['  ' + props.detail] }))
  return Box({ flexDirection: 'row', children })
}

export default OrbitMark
