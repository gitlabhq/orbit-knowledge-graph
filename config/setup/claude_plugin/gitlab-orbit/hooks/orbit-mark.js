// The animated Orbit mark, drawn by a Client so it runs on the surface's frame clock
// instead of redrawing the hooks module's sites.
import { MARK, fade } from './orbit.js'

// 20 frames a second keeps the fade smooth; one trip light to dark and back takes 2 s
const TICK_MS = 50
const FADE_MS = 2000

export default function OrbitMark(props, surface) {
  const { Box, Text } = surface.elements
  if (surface.state === undefined) {
    surface.every(TICK_MS, () => surface.setState((surface.state ?? 0) + 1))
  }

  // The row's color eases light, Orbit blue, dark, and back again, on the clock rather than
  // the tick count, so a late frame never makes it jump
  const phase = (Date.now() % FADE_MS) / FADE_MS
  const color = fade((1 - Math.cos(phase * 2 * Math.PI)) / 2)

  const label = props.label || 'Exploring Orbit'
  const children = [Text({ color, bold: true, children: [MARK + ' ' + label] })]
  if (props.detail) children.push(Text({ dimColor: true, children: ['  ' + props.detail] }))
  return Box({ flexDirection: 'row', children })
}
