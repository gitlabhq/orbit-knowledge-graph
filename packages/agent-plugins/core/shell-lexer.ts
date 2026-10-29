export type Token =
  | { kind: 'word'; text: string }
  | { kind: 'separator'; pipe: boolean }
  | { kind: 'redirect'; operator: '<' | '>' }
  | { kind: 'substitution' }

export type Lexed = { tokens: Token[]; unparsed: boolean }

type Heredoc = { delimiter: string; stripTabs: boolean; quoted: boolean }
type QuoteForm = { close: string; escapes: boolean; substitutions: boolean }

const QUOTE_FORMS = {
  single: { close: "'", escapes: false, substitutions: false },
  ansi: { close: "'", escapes: true, substitutions: false },
  double: { close: '"', escapes: true, substitutions: true },
} as const satisfies Record<string, QuoteForm>

const HARMLESS_REDIRECT = /^(?:&>>?|>>?)(?:&\d+|\s*\/dev\/null)(?=[\s;&|)]|$)/
const SUBSTITUTION = /\$\(|`/
const KEYWORDS_BEFORE_ARITHMETIC = new Set(['do', 'then', 'else', 'elif', 'if', 'while', 'until', '!', 'time', '{'])

function arithmeticEnd(src: string, open: number): number {
  let depth = 2
  for (let i = open + 2; i < src.length; i++) {
    if (src[i] === '(') depth++
    else if (src[i] === ')') {
      if (depth === 2 && src[i + 1] === ')') return i + 2
      if (--depth < 2) return -1
    }
  }
  return -1
}

function startsArithmetic(src: string, open: number): boolean {
  return src[open] === '(' && src[open + 1] === '(' && arithmeticEnd(src, open) >= 0
}

function startsSubstitution(src: string, at: number): boolean {
  if (src[at] === '`') return true
  return src[at] === '$' && src[at + 1] === '(' && !startsArithmetic(src, at + 1)
}

type Quoted = { text: string; end: number; closed: boolean; substitutes: boolean }

function scanQuoted(src: string, start: number, form: QuoteForm): Quoted {
  let text = ''
  let substitutes = false
  for (let i = start; i < src.length; i++) {
    const c = src[i] ?? ''
    if (c === form.close) return { text, end: i + 1, closed: true, substitutes }
    if (form.escapes && c === '\\' && i + 1 < src.length) text += src[++i]
    else {
      if (form.substitutions && startsSubstitution(src, i)) substitutes = true
      text += c
    }
  }
  return { text, end: src.length, closed: false, substitutes }
}

function scanHeredocOperand(src: string, start: number): { delimiter: string; quoted: boolean; end: number } {
  let i = start
  let delimiter = ''
  let quoted = false
  while (i < src.length && !/[\s;&|<>()]/.test(src[i] ?? '')) {
    const c = src[i] ?? ''
    if (c === "'" || c === '"') {
      const { text, end } = scanQuoted(src, i + 1, c === "'" ? QUOTE_FORMS.single : QUOTE_FORMS.double)
      quoted = true
      delimiter += text
      i = end
    } else if (c === '\\') {
      quoted = true
      delimiter += src[i + 1] ?? ''
      i += 2
    } else {
      delimiter += c
      i++
    }
  }
  return { delimiter, quoted, end: i }
}

function scanHeredocBody(src: string, start: number, heredoc: Heredoc): { end: number; closed: boolean; substitutes: boolean } {
  let at = start
  let substitutes = false
  while (at < src.length) {
    const newline = src.indexOf('\n', at)
    const line = src.slice(at, newline < 0 ? src.length : newline)
    at = newline < 0 ? src.length : newline + 1
    if ((heredoc.stripTabs ? line.replace(/^\t+/, '') : line) === heredoc.delimiter) return { end: at, closed: true, substitutes }
    if (!heredoc.quoted && SUBSTITUTION.test(line)) substitutes = true
  }
  return { end: at, closed: false, substitutes }
}

class Lexer {
  private pos = 0
  private word: string | null = null
  private unparsed = false
  private readonly tokens: Token[] = []
  private readonly heredocs: Heredoc[] = []

  constructor(private readonly src: string) {}

  run(): Lexed {
    while (this.pos < this.src.length) this.step(this.src[this.pos] ?? '')
    this.endWord()
    return { tokens: this.tokens, unparsed: this.unparsed }
  }

  private step(c: string): void {
    if (c === "'") this.quoted('single', 1)
    else if (c === '"') this.quoted('double', 1)
    else if (c === '\\') this.escape()
    else if (c === '$') this.dollar()
    else if (c === '`') this.substitution(1)
    else if (c === '(' && this.startsArithmeticCommand()) this.arithmetic(this.pos, 0)
    else if (c === '<' || c === '>' || (c === '&' && this.src[this.pos + 1] === '>')) this.redirect(c)
    else if (c === '|' && this.src[this.pos + 1] !== '|') this.pipe()
    else if (c === '\n' || ';&|()'.includes(c)) this.separator(c)
    else if (c === '#' && this.word === null) this.comment()
    else if (/\s/.test(c)) this.whitespace()
    else this.plain(c)
  }

  private append(text: string): void {
    this.word = (this.word ?? '') + text
  }

  private endWord(): void {
    if (this.word !== null) this.tokens.push({ kind: 'word', text: this.word })
    this.word = null
  }

  private currentCommandWords(): string[] {
    const words: string[] = []
    for (const token of this.tokens.toReversed()) {
      if (token.kind === 'separator') break
      if (token.kind === 'word') words.unshift(token.text)
    }
    return words
  }

  private startsArithmeticCommand(): boolean {
    if (this.word !== null || !startsArithmetic(this.src, this.pos)) return false
    const words = this.currentCommandWords()
    return words.every((word) => KEYWORDS_BEFORE_ARITHMETIC.has(word)) || words[0] === 'for'
  }

  private quoted(form: keyof typeof QUOTE_FORMS, openingLength: number): void {
    const { text, end, closed, substitutes } = scanQuoted(this.src, this.pos + openingLength, QUOTE_FORMS[form])
    this.append(text)
    if (substitutes) this.tokens.push({ kind: 'substitution' })
    if (!closed) this.unparsed = true
    this.pos = end
  }

  private escape(): void {
    const next = this.src[this.pos + 1]
    if (next === '\n') {
      this.endWord()
      this.pos += 2
    } else if (next === undefined) this.plain('\\')
    else {
      this.append(next)
      this.pos += 2
    }
  }

  private dollar(): void {
    const next = this.src[this.pos + 1]
    if (next === "'") this.quoted('ansi', 2)
    else if (startsSubstitution(this.src, this.pos)) this.substitution(1)
    else if (next === '(' && startsArithmetic(this.src, this.pos + 1)) this.arithmetic(this.pos + 1, 1)
    else this.plain('$')
  }

  private substitution(length: number): void {
    this.tokens.push({ kind: 'substitution' })
    this.pos += length
  }

  private arithmetic(open: number, dollar: 0 | 1): void {
    const end = arithmeticEnd(this.src, open)
    if (SUBSTITUTION.test(this.src.slice(open + 2, end - 2))) this.tokens.push({ kind: 'substitution' })
    if (dollar) this.append('')
    this.pos = end
  }

  private redirect(c: string): void {
    const harmless = this.src.slice(this.pos).match(HARMLESS_REDIRECT)
    this.endWordOrDescriptor()
    if (harmless) {
      this.pos += harmless[0].length
      return
    }
    if (c === '&') this.pos++
    if (this.src[this.pos] === '<' && this.src[this.pos + 1] === '<' && this.src[this.pos + 2] !== '<') this.heredocOperator()
    else this.redirectOperator()
  }

  private endWordOrDescriptor(): void {
    const isDescriptor = this.word !== null && /^\d+$/.test(this.word) && /\d/.test(this.src[this.pos - 1] ?? '')
    if (isDescriptor) this.word = null
    else this.endWord()
  }

  private redirectOperator(): void {
    const operator = this.src[this.pos] === '<' ? '<' : '>'
    this.pos += operator === '<' && this.src.startsWith('<<<', this.pos) ? 3 : 1
    if (operator === '>') {
      if (this.src[this.pos] === '>') this.pos++
      if (this.src[this.pos] === '|' || this.src[this.pos] === '&') this.pos++
    }
    this.tokens.push({ kind: 'redirect', operator })
  }

  private heredocOperator(): void {
    const stripTabs = this.src[this.pos + 2] === '-'
    const operandStart = this.pos + (stripTabs ? 3 : 2)
    const { delimiter, quoted, end } = scanHeredocOperand(this.src, operandStart + (this.src.slice(operandStart).match(/^[ \t]*/)?.[0].length ?? 0))
    if (delimiter) this.heredocs.push({ delimiter, stripTabs, quoted })
    else this.unparsed = true
    this.pos = end
  }

  private pipe(): void {
    this.endWord()
    this.tokens.push({ kind: 'separator', pipe: true })
    this.pos += this.src[this.pos + 1] === '&' ? 2 : 1
  }

  private separator(c: string): void {
    if (c === '\n') this.skipHeredocBodies()
    this.endWord()
    this.tokens.push({ kind: 'separator', pipe: false })
    this.pos += (c === '&' || c === '|') && this.src[this.pos + 1] === c ? 2 : 1
  }

  private skipHeredocBodies(): void {
    let at = this.pos + 1
    for (const heredoc of this.heredocs.splice(0)) {
      const body = scanHeredocBody(this.src, at, heredoc)
      if (body.substitutes) this.tokens.push({ kind: 'substitution' })
      if (!body.closed) this.unparsed = true
      at = body.end
    }
    this.pos = at - 1
  }

  private comment(): void {
    const newline = this.src.indexOf('\n', this.pos)
    this.pos = newline < 0 ? this.src.length : newline
  }

  private whitespace(): void {
    this.endWord()
    this.pos++
  }

  private plain(c: string): void {
    this.append(c)
    this.pos++
  }
}

export function lex(src: string): Lexed {
  return new Lexer(src).run()
}
