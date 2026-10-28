import type { TokenKind } from '../core/cypher.ts'

export { MARK } from '../core/calls.ts'

export const BLUE = '#949dcb'
export const FADE_STOPS = ['#b5dedc', BLUE, '#6851ac'] as const

function channels(hex: string): [number, number, number] {
  const n = parseInt(hex.slice(1), 16)
  return [(n >> 16) & 255, (n >> 8) & 255, n & 255]
}

export function fade(t: number): string {
  const span = Math.min(1, Math.max(0, t)) * (FADE_STOPS.length - 1)
  const i = Math.min(FADE_STOPS.length - 2, Math.floor(span))
  const a = channels(FADE_STOPS[i] ?? BLUE)
  const b = channels(FADE_STOPS[i + 1] ?? BLUE)
  const f = span - i
  return '#' + a.map((c, k) => Math.round(c + ((b[k] ?? c) - c) * f).toString(16).padStart(2, '0')).join('')
}

type TokenStyle = { color?: string; bold?: boolean; italic?: boolean; dimColor?: boolean }

export const TOKEN_STYLE: Readonly<Record<TokenKind, TokenStyle>> = {
  keyword: { color: BLUE, bold: true },
  label: { color: FADE_STOPS[0] },
  relationship: { color: FADE_STOPS[0], italic: true },
  string: { color: 'success' },
  number: { color: 'warning' },
  function: { bold: true },
  property: {},
  variable: {},
  punctuation: { dimColor: true },
  comment: { dimColor: true, italic: true },
  space: {},
}
