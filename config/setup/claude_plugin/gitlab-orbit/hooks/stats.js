// One Orbit response, in the CLI's default llm format or the raw JSON format:
// { nodes, edges, byEntity, aggregated }. Schema and other non-query output reads as empty.
export function parseResponse(text) {
  const out = { nodes: 0, edges: 0, byEntity: {}, aggregated: 0 }
  const raw = text.trim().startsWith('{') ? tryJson(text) : null
  if (raw) {
    const r = raw.result ?? raw
    for (const n of Array.isArray(r.nodes) ? r.nodes : []) {
      out.byEntity[n.type] = (out.byEntity[n.type] ?? 0) + 1
      out.nodes += 1
    }
    out.edges = Array.isArray(r.edges) ? r.edges.length : 0
    return out
  }
  if (/^query_type:\s*aggregation/m.test(text)) {
    // A row's count columns say how many nodes the aggregate covered; sum, avg, min, and max don't
    const columns = text.split(/^columns\[\d+\]\{[^}]*\}:\s*$/m)[1]?.split(/^\S/m)[0] ?? ''
    const counts = [...columns.matchAll(/^\s+(\w+),count,/gm)].map((m) => m[1])
    const header = text.match(/^rows\[\d+\]\{([^}]*)\}:\s*$/m)
    if (!header || counts.length === 0) return out
    const at = header[1].split(',').map((name) => counts.includes(name))
    const rows = text.slice(header.index + header[0].length).split(/^\S/m)[0]
    for (const line of rows.split('\n').filter((l) => l.trim())) {
      line.trim().split(',').forEach((cell, i) => {
        if (at[i] && /^\d+$/.test(cell)) out.aggregated += Number(cell)
      })
    }
    return out
  }
  const nodeBlock = text.split(/^nodes:\s*$/m)[1]?.split(/^\S/m)[0] ?? ''
  for (const m of nodeBlock.matchAll(/^\s+(\w+)\[(\d+)\]\{/gm)) {
    out.byEntity[m[1]] = (out.byEntity[m[1]] ?? 0) + Number(m[2])
    out.nodes += Number(m[2])
  }
  const edges = text.match(/^edges\[(\d+)\]/m)
  out.edges = edges ? Number(edges[1]) : 0
  return out
}

function tryJson(text) {
  try {
    return JSON.parse(text)
  } catch {
    return null
  }
}

export function compact(n) {
  if (n < 1000) return String(n)
  if (n < 10000) return (n / 1000).toFixed(1).replace(/\.0$/, '') + 'k'
  if (n < 1e6) return Math.round(n / 1000) + 'k'
  return (n / 1e6).toFixed(1).replace(/\.0$/, '') + 'M'
}
