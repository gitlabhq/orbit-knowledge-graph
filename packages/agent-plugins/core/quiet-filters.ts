type PatternOption = 'attached' | 'separate' | null

type FilterSpec = {
  maxOperands: number
  fileOptions?: RegExp
  patternOption?: (arg: string) => PatternOption
  notQuiet?: RegExp
}

const NUMBER_OPTION = /^-[A-Za-z]*[nNcmABCfb]$|^--(?:lines|bytes|max-count|after-context|before-context|context)$/

function grepPatternOption(arg: string): PatternOption {
  if (arg === '--regexp') return 'separate'
  if (arg.startsWith('--regexp=')) return 'attached'
  const letters = arg.startsWith('--') ? null : arg.match(/^-[A-Za-z]*?e(.*)$/)
  if (!letters) return null
  return letters[1] ? 'attached' : 'separate'
}

const FILTERS: Readonly<Record<string, FilterSpec>> = {
  head: { maxOperands: 0 },
  tail: { maxOperands: 0 },
  wc: { maxOperands: 0, fileOptions: /^--files0-from/ },
  cut: { maxOperands: 0 },
  grep: {
    maxOperands: 1,
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
      const isNumberValue = !optionsEnded && /^\d+$/.test(arg) && NUMBER_OPTION.test(args[i - 1] ?? '')
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
  const patternOptions = args.map((arg) => spec.patternOption?.(arg) ?? null)
  const maxOperands = patternOptions.some(Boolean) ? 0 : spec.maxOperands
  return operandCount(spec, args, patternOptions) <= maxOperands
}
