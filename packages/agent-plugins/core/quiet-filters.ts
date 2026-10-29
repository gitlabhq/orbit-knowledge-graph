type PatternOption = 'attached' | 'separate' | null

type FilterSpec = {
  maxOperands: number
  numberOption?: RegExp
  fileOptions?: RegExp
  patternOption?: (arg: string) => PatternOption
  notQuiet?: RegExp
}

function grepPatternOption(arg: string): PatternOption {
  if (arg === '--regexp') return 'separate'
  if (arg.startsWith('--regexp=')) return 'attached'
  const letters = arg.startsWith('--') ? null : arg.match(/^-[A-Za-z]*?e(.*)$/)
  if (!letters) return null
  return letters[1] ? 'attached' : 'separate'
}

const LINES_OR_BYTES = /^-[A-Za-z]*[nc]$|^--(?:lines|bytes)$/

const FILTERS: Readonly<Record<string, FilterSpec>> = {
  head: { maxOperands: 0, numberOption: LINES_OR_BYTES },
  tail: { maxOperands: 0, numberOption: LINES_OR_BYTES },
  wc: { maxOperands: 0, fileOptions: /^--files0-from/ },
  cut: { maxOperands: 0, numberOption: /^-[A-Za-z]*[bcf]$|^--(?:bytes|characters|fields)$/ },
  grep: {
    maxOperands: 1,
    numberOption: /^-[A-Za-z]*[mABC]$|^--(?:max-count|after-context|before-context|context)$/,
    fileOptions: /^-[A-Za-z]*[rRdf]|^--(?:recursive|dereference-recursive|directories|file)/,
    patternOption: grepPatternOption,
  },
  jq: {
    maxOperands: 1,
    fileOptions: /^-[A-Za-z]*[fL]|^--(?:from-file|slurpfile|rawfile|argfile)/,
    notQuiet: /\b(?:import|include|env)\b|\$ENV|^-[A-Za-z]*n|^--null-input$/,
  },
}

function operandCount(spec: FilterSpec, args: readonly string[], patternOptions: readonly PatternOption[]): number {
  let operands = 0
  let optionsEnded = false
  for (let i = 0; i < args.length; i++) {
    const arg = args[i] ?? ''
    if (optionsEnded || !arg.startsWith('-')) {
      const isNumberValue = !optionsEnded && /^\d+$/.test(arg) && spec.numberOption?.test(args[i - 1] ?? '')
      if (!isNumberValue) operands++
    } else if (arg === '--') optionsEnded = true
    else if (spec.fileOptions?.test(arg)) return Infinity
    else if (patternOptions[i] === 'separate') i++
  }
  return operands
}

export function isQuietFilter(program: string, args: readonly string[], readsInput: boolean): boolean {
  const spec = Object.hasOwn(FILTERS, program) ? FILTERS[program] : undefined
  if (!spec || readsInput || args.some((arg) => spec.notQuiet?.test(arg))) return false
  const optionsEnd = args.includes('--') ? args.indexOf('--') : args.length
  const patternOptions = args.map((arg, i) => (i < optionsEnd ? spec.patternOption?.(arg) : null) ?? null)
  const maxOperands = patternOptions.some(Boolean) ? 0 : spec.maxOperands
  return operandCount(spec, args, patternOptions) <= maxOperands
}
