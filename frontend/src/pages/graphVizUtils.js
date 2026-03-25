import { MarkerType } from 'reactflow'

export const EDGE_COLORS = { READS_FROM: '#3b82f6', WRITES_TO: '#f59e0b' }

// Card dimensions
const CARD_W  = 520   // node width
const CARD_H  = 110   // node height
const ROW_GAP = 40    // vertical gap between cards in same column
const ROW_H   = CARD_H + ROW_GAP
const JOB_X   = 0
const DS_X    = 1100  // horizontal separation

function makeEdges(validEdges) {
  return validEdges.map((e, i) => {
    const color = EDGE_COLORS[e.rel] || '#6b7280'
    return {
      id:        `e-${i}`,
      source:     e.src,
      target:     e.tgt,
      sourceHandle: 'right',
      targetHandle: 'left',
      type:       'smoothstep',
      style:      { stroke: color, strokeWidth: 1.5, opacity: 0.45 },
      markerEnd:  { type: MarkerType.ArrowClosed, color, width: 14, height: 14 },
      data:       { rel: e.rel },
    }
  })
}

export function buildLayout(jobs, datasets, rawEdges) {
  const jobSet     = new Set(jobs.map(j => j.id))
  const dsMap      = new Map(datasets.map(d => [d.id, d]))
  const validEdges = rawEdges.filter(e => jobSet.has(e.src) && dsMap.has(e.tgt))

  const usedDsIds  = new Set(validEdges.map(e => e.tgt))
  const usedJobIds = new Set(validEdges.map(e => e.src))

  const sortedJobs = [...jobs]
    .filter(j => usedJobIds.has(j.id))
    .sort((a, b) => {
      const ap = a.path || ''
      const bp = b.path || ''
      return ap !== bp ? ap.localeCompare(bp) : (a.name || a.id).localeCompare(b.name || b.id)
    })

  // Sort datasets by average row index of connected jobs to reduce edge crossings
  const jobIndex = new Map(sortedJobs.map((j, i) => [j.id, i]))
  const connDs = datasets
    .filter(d => usedDsIds.has(d.id))
    .sort((a, b) => {
      const aE = validEdges.filter(e => e.tgt === a.id)
      const bE = validEdges.filter(e => e.tgt === b.id)
      const avgA = aE.reduce((s, e) => s + (jobIndex.get(e.src) ?? 0), 0) / (aE.length || 1)
      const avgB = bE.reduce((s, e) => s + (jobIndex.get(e.src) ?? 0), 0) / (bE.length || 1)
      return avgA - avgB
    })

  // Vertically center both columns relative to each other
  const jobTotalH = sortedJobs.length * ROW_H
  const dsTotalH  = connDs.length * ROW_H
  const maxH      = Math.max(jobTotalH, dsTotalH)
  const jobOff    = Math.round((maxH - jobTotalH) / 2)
  const dsOff     = Math.round((maxH - dsTotalH) / 2)

  const jobNodes = sortedJobs.map((j, i) => ({
    id:       j.id,
    type:     'job',
    data:     { label: j.name || j.id, sub: j.path || '', dimmed: false, highlighted: null, focal: false },
    position: { x: JOB_X, y: jobOff + i * ROW_H },
    style:    { width: CARD_W, height: CARD_H },
  }))

  const dsNodes = connDs.map((d, i) => ({
    id:       d.id,
    type:     'dataset',
    data:     { label: d.name || d.id, sub: d.layer || d.domain || '', dimmed: false, highlighted: null, focal: false },
    position: { x: DS_X, y: dsOff + i * ROW_H },
    style:    { width: CARD_W, height: CARD_H },
  }))

  return {
    nodes: [...jobNodes, ...dsNodes],
    edges: makeEdges(validEdges),
    stats: {
      jobs:         sortedJobs.length,
      datasets:     connDs.length,
      reads_from:   validEdges.filter(e => e.rel === 'READS_FROM').length,
      writes_to:    validEdges.filter(e => e.rel === 'WRITES_TO').length,
      disconnected: datasets.length - connDs.length,
    },
  }
}
