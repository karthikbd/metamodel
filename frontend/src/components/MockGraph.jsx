/**
 * MockGraph.jsx
 * Renders the PNC mock relationship graph using vis-network directly —
 * no backend / AuraDB required.  Same Bloom-style UI as NeoVisGraph.
 *
 * Props:
 *   mode    {'all'|'process'|'data'|'fk'|'col'|'schema'}
 *   height  {number}  canvas height px (default 560)
 */

import { useEffect, useRef, useState, useCallback } from 'react'
import { buildGraphForMode, buildExpandGraph }       from '../data/mockGraph'

// ── colour palette (mirrors BloomGraph) ───────────────────────────────────

const GROUP_STYLE = {
  Job:     { bg: '#2d1b4e', border: '#9333ea', font: '#e9d5ff' },
  Dataset: { bg: '#1e3a5f', border: '#3b82f6', font: '#bfdbfe' },
  Column:  { bg: '#0f3835', border: '#14b8a6', font: '#99f6e4' },
}
const DEFAULT_STYLE = { bg: '#1c1c20', border: '#52525b', font: '#e4e4e7' }

const REL_COLOR = {
  READS_FROM:   '#3b82f6',
  WRITES_TO:    '#f59e0b',
  DEPENDS_ON:   '#a78bfa',
  GOVERNED_BY:  '#10b981',
  DERIVED_FROM: '#f472b6',
  REFERENCES:   '#fb923c',
  JOINS_WITH:   '#38bdf8',
  HAS_COLUMN:   '#52525b',
}

function nodeStyle(group) {
  const s = GROUP_STYLE[group] || DEFAULT_STYLE
  return {
    color: {
      background:       s.bg,
      border:           s.border,
      hover:            { background: s.bg, border: s.border },
      highlight:        { background: s.bg, border: '#ffffff' },
    },
    font: { color: s.font, size: 13, face: 'JetBrains Mono, monospace' },
    borderWidth:        2,
    borderWidthSelected: 3,
    shape: 'dot',
    size:  20,
  }
}

function edgeStyle(rel) {
  const color = REL_COLOR[rel] || '#71717a'
  return {
    color:   { color, hover: color, highlight: '#ffffff', opacity: 0.9 },
    font:    { color: '#71717a', size: 10, face: 'JetBrains Mono, monospace', align: 'middle' },
    arrows:  { to: { enabled: true, scaleFactor: 0.55 } },
    smooth:  { type: 'dynamic' },
    width:   1.5,
  }
}

const VIS_OPTIONS = {
  nodes: { shape: 'dot', size: 20, borderWidth: 2 },
  edges: {
    arrows: { to: { enabled: true, scaleFactor: 0.55 } },
    smooth: { type: 'dynamic' },
    width:  1.5,
    font:   { color: '#71717a', size: 10, face: 'JetBrains Mono, monospace', align: 'middle' },
  },
  physics: {
    enabled: true,
    solver:  'forceAtlas2Based',
    forceAtlas2Based: {
      gravitationalConstant: -55,
      centralGravity:        0.01,
      springLength:          140,
      springConstant:        0.08,
    },
    stabilization: { iterations: 250, updateInterval: 25 },
  },
  interaction: {
    hover:                true,
    tooltipDelay:         200,
    navigationButtons:    true,
    keyboard:             true,
    multiselect:          true,
    selectConnectedEdges: true,
  },
  background: { color: 'transparent' },
}

let _mc = 0

export default function MockGraph({ mode = 'all', height = 560 }) {
  const containerId = useRef(`mock-graph-${++_mc}`)
  const networkRef  = useRef(null)
  const DataSet     = useRef(null)

  const [ready,    setReady]    = useState(false)
  const [selected, setSelected] = useState(null)   // { id, name, group, props }
  const [history,  setHistory]  = useState([])     // [{ label, mode, expandId }]
  const [curMode,  setCurMode]  = useState(mode)
  const [expandId, setExpandId] = useState(null)   // null = show mode-graph

  // sync when parent mode prop changes
  useEffect(() => {
    setCurMode(mode)
    setExpandId(null)
    setHistory([])
    setSelected(null)
  }, [mode])

  // ── build data for current view ──────────────────────────────────────────
  const graphData = expandId
    ? buildExpandGraph(expandId)
    : buildGraphForMode(curMode)

  // ── mount vis-network ────────────────────────────────────────────────────
  useEffect(() => {
    let cancelled = false

    async function init() {
      const visNet  = await import('vis-network/standalone/esm/vis-network')
      const visData = await import('vis-data')
      if (cancelled) return

      DataSet.current = visData.DataSet

      const { nodes, edges } = graphData

      const nodeDS = new visData.DataSet(
        nodes.map(n => ({ ...n, ...nodeStyle(n.group) }))
      )
      const edgeDS = new visData.DataSet(
        edges.map(e => ({ ...e, ...edgeStyle(e.rel) }))
      )

      const container = document.getElementById(containerId.current)
      if (!container || cancelled) return

      const net = new visNet.Network(container, { nodes: nodeDS, edges: edgeDS }, VIS_OPTIONS)
      networkRef.current = net

      net.once('stabilizationIterationsDone', () => {
        if (!cancelled) { net.fit(); setReady(true) }
      })

      net.on('click', params => {
        if (cancelled) return
        if (params.nodes.length === 0) { setSelected(null); return }
        const id = params.nodes[0]
        const nd = nodes.find(n => n.id === id)
        if (!nd) return
        setSelected({ id, name: nd.label, group: nd.group, props: nd.props || {} })
        const neighbours = net.getConnectedNodes(id)
        net.selectNodes([id, ...neighbours])
      })

      net.on('doubleClick', params => {
        if (cancelled || params.nodes.length === 0) return
        net.focus(params.nodes[0], { scale: 1.6, animation: { duration: 500, easingFunction: 'easeInOutQuad' } })
      })
    }

    setReady(false)
    setSelected(null)
    init().catch(console.error)

    return () => {
      cancelled = true
      try { networkRef.current?.destroy?.() } catch {}
      networkRef.current = null
    }
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [curMode, expandId])

  // ── expand neighbourhood ──────────────────────────────────────────────────
  const handleExpand = useCallback(() => {
    if (!selected) return
    setHistory(h => [...h, { label: selected.name, mode: curMode, expandId }])
    setExpandId(selected.id)
    setSelected(null)
  }, [selected, curMode, expandId])

  // ── back ──────────────────────────────────────────────────────────────────
  const handleBack = useCallback(() => {
    setHistory(h => {
      if (h.length === 0) return h
      const prev = h[h.length - 1]
      setCurMode(prev.mode)
      setExpandId(prev.expandId)
      return h.slice(0, -1)
    })
    setSelected(null)
  }, [])

  // ── colour lookups for UI ─────────────────────────────────────────────────
  const gs = (g) => GROUP_STYLE[g] || DEFAULT_STYLE

  return (
    <div className="flex rounded-lg border border-surface-border overflow-hidden"
         style={{ height, background: '#0c0c0e' }}>

      {/* Canvas */}
      <div className="relative flex-1 min-w-0">

        {!ready && (
          <div className="absolute inset-0 flex items-center justify-center z-10 pointer-events-none">
            <span className="text-sm text-zinc-600 animate-pulse">Rendering mock graph…</span>
          </div>
        )}

        {/* vis-network mount point */}
        <div id={containerId.current} style={{ width: '100%', height: '100%' }} />

        {/* Offline banner */}
        <div className="absolute top-3 right-3 z-20 flex items-center gap-1.5
                        bg-zinc-900/90 border border-zinc-700 rounded px-2 py-1">
          <span className="w-1.5 h-1.5 rounded-full bg-amber-500" />
          <span className="text-[10px] text-zinc-400">mock data · offline</span>
        </div>

        {/* Breadcrumb */}
        {history.length > 0 && (
          <div className="absolute top-3 left-3 z-20 flex items-center gap-1 flex-wrap">
            <button
              onClick={handleBack}
              className="text-[10px] bg-zinc-800/90 border border-zinc-700 rounded px-2 py-1
                         text-zinc-300 hover:text-white hover:border-zinc-500 transition-colors"
            >
              back
            </button>
            {history.map((h, i) => (
              <span key={i} className="text-[10px] text-zinc-600">
                {i > 0 && <span className="mx-1 text-zinc-700">&rsaquo;</span>}
                {h.label}
              </span>
            ))}
            {selected && (
              <span className="text-[10px] text-zinc-400">
                <span className="mx-1 text-zinc-700">&rsaquo;</span>{selected.name}
              </span>
            )}
          </div>
        )}

        {/* Legend */}
        {ready && (
          <div className="absolute bottom-3 left-3 flex flex-col gap-1.5 pointer-events-none z-10">
            {Object.entries(GROUP_STYLE).map(([lbl, s]) => (
              <span key={lbl} className="flex items-center gap-1.5 text-[10px]">
                <span className="w-2.5 h-2.5 rounded-full border"
                      style={{ background: s.bg, borderColor: s.border }} />
                <span className="text-zinc-500">{lbl}</span>
              </span>
            ))}
          </div>
        )}

        {/* Hint */}
        {ready && !selected && (
          <div className="absolute bottom-3 right-3 text-[10px] text-zinc-700
                          pointer-events-none z-10 text-right leading-relaxed">
            click node to inspect<br />double-click to zoom<br />drag and scroll
          </div>
        )}
      </div>

      {/* Property panel */}
      {selected && (
        <div className="w-60 shrink-0 border-l border-surface-border bg-zinc-950
                        flex flex-col overflow-y-auto text-xs">

          <div className="flex items-start justify-between gap-2 px-3 pt-3 pb-2 border-b border-zinc-800">
            <div className="flex flex-col gap-1 min-w-0">
              <span className="self-start px-1.5 py-0.5 rounded text-[10px] font-mono border"
                    style={{ color: gs(selected.group).border, borderColor: gs(selected.group).border, background: gs(selected.group).bg }}>
                {selected.group}
              </span>
              <span className="font-semibold text-zinc-100 font-mono break-all">{selected.name}</span>
            </div>
            <button onClick={() => setSelected(null)}
                    className="shrink-0 mt-0.5 text-zinc-600 hover:text-zinc-300 text-base leading-none">
              x
            </button>
          </div>

          <div className="flex-1 px-3 py-2 space-y-1.5 overflow-y-auto">
            {Object.entries(selected.props).length === 0
              ? <p className="text-zinc-600 italic">No properties</p>
              : Object.entries(selected.props).map(([k, v]) => (
                  <div key={k} className="grid grid-cols-[auto_1fr] gap-x-2 items-start">
                    <span className="text-zinc-500 font-mono shrink-0 pt-px">{k}</span>
                    <span className="text-zinc-300 font-mono break-all">{String(v)}</span>
                  </div>
                ))
            }
          </div>

          <div className="px-3 py-3 border-t border-zinc-800 space-y-2">
            <button onClick={handleExpand}
                    className="w-full px-3 py-1.5 rounded bg-violet-500/10 border border-violet-500/30
                               text-violet-400 hover:bg-violet-500/20 transition-colors font-medium text-[11px]">
              Expand neighbourhood
            </button>
            {history.length > 0 && (
              <button onClick={handleBack}
                      className="w-full px-3 py-1.5 rounded bg-zinc-800 border border-zinc-700
                                 text-zinc-400 hover:text-zinc-200 transition-colors text-[11px]">
                Back
              </button>
            )}
            <p className="text-zinc-700 text-center text-[10px] pt-1">double-click to zoom</p>
          </div>
        </div>
      )}
    </div>
  )
}
