import { BLUE } from '../hooks/theme.ts'

export type Style = { color?: 'blue' | 'warning' | 'error'; bold?: boolean; dim?: boolean }

function rgb(hex: string): string {
  const n = parseInt(hex.slice(1), 16)
  return [(n >> 16) & 255, (n >> 8) & 255, n & 255].join(';')
}

const COLOR: Readonly<Record<NonNullable<Style['color']>, string>> = { blue: '38;2;' + rgb(BLUE), warning: '33', error: '31' }

// The Codex TUI draws ANSI colors in a hook message, but other Codex clients may show the escapes as
// text, so colors follow the terminal the hook runs in.
export function colorsOn(env: Record<string, string | undefined> = process.env): boolean {
  return !env['NO_COLOR'] && Boolean(env['TERM']) && env['TERM'] !== 'dumb'
}

export function paint(text: string, style: Style, colors: boolean): string {
  const codes = [style.bold ? '1' : '', style.dim ? '2' : '', style.color ? COLOR[style.color] : ''].filter(Boolean)
  return colors && text && codes.length ? `\x1b[${codes.join(';')}m${text}\x1b[0m` : text
}
