import { MarkerType } from 'reactflow'

export const FUNC_STYLE = {
  background:  '#1e1529',
  border:      '1px solid #7c3aed',
  color:       '#a78bfa',
  borderRadius: 6,
  fontSize:    11,
  fontFamily:  'JetBrains Mono, monospace',
  padding:     '4px 10px',
  minWidth:    150,
}

export const SCHEMA_STYLE = {
  background:  '#0f1a2e',
  border:      '1px solid #3b82f6',
  color:       '#93c5fd',
  borderRadius: 5,
  fontSize:    10,
  fontFamily:  'JetBrains Mono, monospace',
  padding:     '3px 8px',
  minWidth:    160,
}

export const EDGE_COLORS = { READS_FROM: '#3b82f6', WRITES_TO: '#f59e0b' }

function makeJobNodes(sortedJobs) {
  return sortedJobs.map((j, i) => ({
    id:       j.id,
    data:     { label: j.name },
    position: { x: 0, y: i * 52 },
    style:    FUNC_STYLE,
  }))
}

function makeDatasetNodes(datasets) {
  const COL_X   = [400, 660, 920]
  const ROW_GAP = 48
  return datasets.map((d, i) => ({
    id:       d.id,
    data:     { label: d.name },
    position: { x: COL_X[i % 3], y: Math.floor(i / 3) * ROW_GAP },
    style:    SCHEMA_STYLE,
  }))
}

function makeEdges(edges) {
  return edges.map((e, i) => {
    const color = EDGE_COLORS[e.rel] || '#6b7280'
    return {
      id:         `e-${i}`,
      source:      e.src,
      target:      e.tgt,
      label:       e.rel,
      labelStyle:  { fill: color, fontSize: 8, fontFamily: 'monospace' },
      style:       { stroke: color, strokeWidth: 1, opacity: 0.55 },
      markerEnd:   { type: MarkerType.ArrowClosed, color },
    }
  })
}

export function buildLayout(jobs, datasets, rawEdges) {
  const jobSet  = new Set(jobs.map(j => j.id))
  const dsMap   = new Map(datasets.map(d => [d.id, d]))
  const edges   = rawEdges.filter(e => jobSet.has(e.src) && dsMap.has(e.tgt))

  const usedDsIds  = new Set(edges.map(e => e.tgt))
  const connDs     = datasets.filter(d => usedDsIds.has(d.id))

  const sortedJobs = [...jobs].sort((a, b) => {
    const ap = a.path || ''
    const bp = b.path || ''
    return ap !== bp ? ap.localeCompare(bp) : (a.name || '').localeCompare(b.name || '')
  })

  return {
    nodes: [...makeJobNodes(sortedJobs), ...makeDatasetNodes(connDs)],
    edges: makeEdges(edges),
    stats: {
      jobs:         jobs.length,
      datasets:     connDs.length,
      reads_from:   rawEdges.filter(e => e.rel === 'READS_FROM').length,
      writes_to:    rawEdges.filter(e => e.rel === 'WRITES_TO').length,
      disconnected: datasets.length - connDs.length,
    },
  }
}
