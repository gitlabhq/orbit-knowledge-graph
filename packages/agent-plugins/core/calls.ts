import { clip, displayName, orbitInvocations, otherCommands, subcommandOf } from './command.ts'
import { NO_ENTITIES, entitiesOf } from './cypher.ts'
import labels from './labels.ts'

export const MARK = '◎'

export type Label = { done: string; running: string }

export type OrbitCall = { label: string; active: string; detail: string; others?: readonly string[] }

export const COMMAND_LABELS: Readonly<Record<string, Label>> = labels.commands
export const TOOL_LABELS: Readonly<Record<string, Label>> = labels.tools

const QUERY = labels.commands.query
const SCHEMA = labels.commands.schema

export const QUERY_LABEL = QUERY.done

const VALUE_FLAGS = new Set([
  '--db', '--file', '--response-format', '--full-path', '--namespace-id', '--project-id', '--repo', '--budget', '--timeout',
])
const VALUE_FLAGS_BY_KEY: Readonly<Record<string, ReadonlySet<string>>> = {
  grep: new Set(['-A', '-B', '-C', '-m', '-g', '--glob', '--include', '--exclude', '--exclude-dir', '-t', '--type', '-T', '--type-not', '-M', '--max-columns']),
  'workspace sql': new Set(['-f', '-F', '--format']),
}
const SHOWS_ARGUMENT = new Set(['grep', 'context', 'workspace sql', 'sql', 'schema', 'ontology', 'graph-status', 'skills', 'skill'])

function labelFor(table: Readonly<Record<string, Label>>, key: string): Label | undefined {
  return Object.hasOwn(table, key) ? table[key] : undefined
}

function callOf(label: Label, detail: string): OrbitCall {
  return { label: label.done, active: label.running, detail }
}

function firstArg(args: readonly string[], valueFlags: ReadonlySet<string>): string {
  for (let i = 0; i < args.length; i++) {
    const arg = args[i] ?? ''
    if (arg === '-e' || arg === '--regexp') return args[i + 1] ?? ''
    if (VALUE_FLAGS.has(arg) || valueFlags.has(arg)) i++
    else if (!arg.startsWith('-')) return arg
  }
  return ''
}

function detailFor(key: string, args: readonly string[], known: ReadonlySet<string>): string {
  if (key === 'query') {
    const text = args.join(' ')
    const schema = text.match(/CALL\s+db\.schema\(\s*'?(\w*)/i)
    if (schema) return schema[1] ?? ''
    const file = args.indexOf('--file')
    return entitiesOf(text, known) || (file >= 0 ? (args[file + 1] ?? '') : '')
  }
  if (!SHOWS_ARGUMENT.has(key)) return ''
  const valueFlags = VALUE_FLAGS_BY_KEY[key] ?? new Set<string>()
  return clip(firstArg(key.startsWith('skill') && args[0] === 'get' ? args.slice(1) : args, valueFlags), 48)
}

function stringField(input: unknown, key: string): string | undefined {
  const value = typeof input === 'object' && input !== null ? (input as Record<string, unknown>)[key] : undefined
  return typeof value === 'string' ? value : undefined
}

const MAX_DETAIL = 96
const MAX_OTHERS_SHOWN = 6

type Invocation = { call: OrbitCall | null; name: string }

function invocationOf(words: readonly string[], known: ReadonlySet<string>): Invocation | null {
  if (words.length === 0 || words.some((arg) => arg === '--help' || arg === '-h')) return null
  const sub = subcommandOf(words, (key) => labelFor(COMMAND_LABELS, key) !== undefined)
  if (!sub) return { call: null, name: 'orbit ' + displayName(words[0] ?? '') }
  const isSchemaQuery = sub.key === 'query' && sub.args.some((arg) => /^\s*CALL\s+db\.schema\(/i.test(arg))
  const label = isSchemaQuery ? SCHEMA : (labelFor(COMMAND_LABELS, sub.key) ?? QUERY)
  return { call: callOf(label, detailFor(sub.key, sub.args, known)), name: 'orbit ' + sub.key }
}

export function describeCommand(command: string, known: ReadonlySet<string> = NO_ENTITIES): OrbitCall | null {
  const invocations = orbitInvocations(command).flatMap((words) => invocationOf(words, known) ?? [])
  const calls = invocations.flatMap(({ call }) => call ?? [])
  const [first] = calls
  if (!first) return null
  const detail = [...new Set(calls.filter((call) => call.label === first.label).map((call) => call.detail).filter(Boolean))].join(', ')
  const otherOrbit = invocations.filter(({ call }) => call?.label !== first.label).map(({ name }) => name)
  const others = otherCommands(command, [...new Set(otherOrbit)])
  return { ...first, detail: clip(detail, MAX_DETAIL), ...(others.length ? { others } : {}) }
}

export function othersText(call: OrbitCall): string {
  if (!call.others?.length) return ''
  const shown = call.others.slice(0, MAX_OTHERS_SHOWN)
  const hidden = call.others.length - shown.length
  return '  + ' + shown.join(', ') + (hidden > 0 ? `, … +${hidden} more` : '')
}

export function describeOrbitCall(tool: string, input: unknown, known: ReadonlySet<string> = NO_ENTITIES): OrbitCall | null {
  const command = stringField(input, 'command')
  if (tool.toLowerCase() === 'bash') return command === undefined ? null : describeCommand(command, known)
  const [, server = '', name = tool] = tool.match(/^mcp__(.+?)__(.+)$/) ?? []
  if (!/orbit|knowledge[-_]?graph|gkg/i.test(server) && !/orbit|query_graph|get_graph_schema/i.test(name)) return null
  const invoked = stringField(input, 'command_name') ?? command ?? name
  const key = Object.keys(TOOL_LABELS).find((k) => invoked.endsWith(k))
  return callOf((key && labelFor(TOOL_LABELS, key)) || QUERY, input === undefined ? '' : entitiesOf(JSON.stringify(input), known))
}
