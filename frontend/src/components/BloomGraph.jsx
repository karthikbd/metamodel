/**
 * BloomGraph  (Bloom-style graph — vis-network + backend proxy)
 *
 * Instead of a direct browser→AuraDB Bolt/WebSocket connection (blocked by
 * corporate proxies, causing the "Encryption/trust" neo4j-driver error), this
 * component calls  POST /api/graph/vis-query  on the FastAPI backend which runs
 * the Cypher server-side and returns  { nodes, edges }  ready for vis-network.
 *
 * The backend handles ALL Neo4j I/O — the browser never touches port 7687.
 *
 * Interactivity:
 *   Click a node         → property panel slides in
 *   "Expand" button      → re-runs Cypher centred on that node
 *   "Back" breadcrumb    → restores previous query
 *   Double-click         → zoom + highlight direct neighbours
 *   Click empty canvas   → deselect / close panel
 *   Drag / scroll-zoom / keyboard nav (vis-network built-in)
 *   Multi-select (ctrl+click) natively supported
 *
 * Props:
 *   cypher   {string}   Cypher to visualise; changing this resets the view
 *   height   {number}   Canvas height px (default 560)
 *   onStable {fn}       Called once physics stabilises
 */

// ── colour palette ──────────────────────────────────────────────────────────

const LABEL_COLORS = {
  Job:          { background: '#2d1b4e', border: '#9333ea', hover: { background: '#3b1f63', border: '#a855f7' } },
  Dataset:      { background: '#1e3a5f', border: '#3b82f6', hover: { background: '#25476e', border: '#60a5fa' } },
  BusinessRule: { background: '#083d30', border: '#10b981', hover: { background: '#0a4d3b', border: '#34d399' } },
  Column:       { background: '#0f3835', border: '#14b8a6', hover: { background: '#154845', border: '#2dd4bf' } },
  Script:       { background: '#4a0e2e', border: '#ec4899', hover: { background: '#5e1239', border: '#f472b6' } },
}

const LABEL_FONT = {
  Job:          '#e9d5ff',
  Dataset:      '#bfdbfe',
  BusinessRule: '#a7f3d0',
  Column:       '#99f6e4',
  Script:       '#fce7f3',
}

const REL_COLORS = {
  READS_FROM:   '#3b82f6',
  WRITES_TO:    '#f59e0b',
  DEPENDS_ON:   '#a78bfa',
  GOVERNED_BY:  '#10b981',
  DERIVED_FROM: '#f472b6',
  REFERENCES:   '#fb923c',
  JOINS_WITH:   '#38bdf8',
  HAS_COLUMN:   '#52525b',
}

//  helpers 

/** Build a vis-network node object from a backend vis-query node */
function buildVisNode(n) {
  const col  = LABEL_COLORS[n.label] || { background: '#1c1c20', border: '#52525b', hover: { background: '#1c1c20', border: '#71717a' } }
  const font = LABEL_FONT[n.label]   || '#e4e4e7'
  const name = n.props?.name || n.props?.id || n.id
  return {
    id:          n.id,
    label:       String(name),
    group:       n.label,
    color:       { background: col.background, border: col.border, hover: col.hover, highlight: { background: col.background, border: '#ffffff' } },
    font:        { color: font, size: 13, face: 'JetBrains Mono, monospace' },
    borderWidth: 2,
    borderWidthSelected: 3,
    shape:       'dot',
    size:        22,
    // private metadata exposed to the sidebar panel
    _name:  String(name),
    _type:  n.label,
    _props: n.props || {},
  }
}

/** Build a vis-network edge object from a backend vis-query edge */
function buildVisEdge(e) {
  const color = REL_COLORS[e.type] || '#71717a'
  return {
    id:     e.id,
    from:   e.from,
    to:     e.to,
    label:  e.type,
    color:  { color, hover: color, opacity: 0.85 },
    font:   { color: '#71717a', size: 10, face: 'JetBrains Mono, monospace', align: 'middle' },
    arrows: { to: { enabled: true, scaleFactor: 0.6 } },
    smooth: { type: 'dynamic' },
    width:  1.5,
  }
}

/** Cypher to fetch the immediate neighbourhood of a node by name */
function buildExpandCypher(name, type) {
  const safe = String(name).replace(/'/g, "\\'")
  if (type && type !== 'Node') {
    return `MATCH (n:${type} {name:'${safe}'})-[r]-(m) RETURN n, r, m LIMIT 80`
  }
  return `MATCH (n {name:'${safe}'})-[r]-(m) RETURN n, r, m LIMIT 80`
}

/** Call POST /api/graph/vis-query — returns { nodes, edges } or throws on error */
async function fetchVizData(cypher, params = {}) {
  const res = await fetch('/api/graph/vis-query', {
    method:  'POST',
    headers: { 'Content-Type': 'application/json' },
    body:    JSON.stringify({ cypher, params }),
  })
  if (!res.ok) {
    const detail = await res.json().catch(() => ({}))
    throw new Error(detail?.detail || `HTTP ${res.status}`)
  }
  return res.json()
}

/** vis-network global options */
const VIS_OPTIONS = {
  nodes: {
    shape: 'dot', size: 22, borderWidth: 2,
    font:  { color: '#e4e4e7', size: 13, face: 'JetBrains Mono, monospace' },
  },
  edges: {
    arrows: { to: { enabled: true, scaleFactor: 0.6 } },
    smooth: { type: 'dynamic' },
    width:  1.5,
    font:   { color: '#71717a', size: 10, face: 'JetBrains Mono, monospace', align: 'middle' },
  },
  physics: {
    enabled: true,
    solver:  'forceAtlas2Based',
    forceAtlas2Based: {
      gravitationalConstant: -50,
      centralGravity:        0.01,
      springLength:          130,
      springConstant:        0.08,
    },
    stabilization: { iterations: 200, updateInterval: 25 },
  },
  interaction: {
    hover:                true,
    tooltipDelay:         300,
    navigationButtons:    true,
    keyboard:             true,
    multiselect:          true,
    selectConnectedEdges: true,
  },
}

//  component 

import { useEffect, useRef, useState, useCallback } from 'react'
import MockGraph from './MockGraph'

/** Derive a mock-graph mode from a Cypher string so the fallback renders
 *  the most appropriate subgraph view. */
function deriveModeFromCypher(cypher) {
  if (!cypher) return 'all'
  const q = cypher.toUpperCase()
  if (q.includes('HAS_COLUMN'))                          return 'schema'
  if (q.includes('DERIVED_FROM') && q.includes('COLUMN')) return 'col'
  if (q.includes('REFERENCES') || q.includes('JOINS_WITH')) return 'fk'
  if (q.includes('READS_FROM') || q.includes('WRITES_TO'))  return 'data'
  if (q.includes('DEPENDS_ON'))                          return 'process'
  return 'all'
}

let _instanceCounter = 0

export default function NeoVisGraph({ cypher, height = 560, onStable }) {
  const idRef      = useRef(`bloom-${++_instanceCounter}`)
  const networkRef = useRef(null)
  const nodeDSRef  = useRef(null)
  const edgeDSRef  = useRef(null)
  const cancelRef  = useRef(false)

  const [status,       setStatus]       = useState('loading')
  const [msg,          setMsg]          = useState('')
  const [selected,     setSelected]     = useState(null)
  const [history,      setHistory]      = useState([])
  const [activeCypher, setActiveCypher] = useState(cypher)
  const [useMock,      setUseMock]      = useState(false)

  // Sync activeCypher when parent changes the cypher prop
  useEffect(() => {
    setActiveCypher(cypher)
    setHistory([])
    setSelected(null)
  }, [cypher])

  //  1. Mount vis-network once on component mount 
  useEffect(() => {
    cancelRef.current = false

    async function init() {
      // Confirm API is reachable; fall back to offline mock if not
      const res = await fetch('/api/neo4j-creds').catch(() => null)
      if (!res || !res.ok) { setUseMock(true); return }
      if (cancelRef.current) return

      // Load vis-network (installed via neovis.js transitive deps)
      const [visNet, visData] = await Promise.all([
        import('vis-network/standalone/esm/vis-network'),
        import('vis-data'),
      ])
      if (cancelRef.current) return

      // Fetch initial graph from the backend
      const initCypher = cypher || 'MATCH (n)-[r]->(m) RETURN n, r, m LIMIT 80'
      let data
      try {
        data = await fetchVizData(initCypher)
      } catch (err) {
        if (!cancelRef.current) { setStatus('error'); setMsg(err.message) }
        return
      }
      if (cancelRef.current) return

      const nodeDS = new visData.DataSet(data.nodes.map(buildVisNode))
      const edgeDS = new visData.DataSet(data.edges.map(buildVisEdge))
      nodeDSRef.current = nodeDS
      edgeDSRef.current = edgeDS

      const container = document.getElementById(idRef.current)
      if (!container || cancelRef.current) return

      const net = new visNet.Network(container, { nodes: nodeDS, edges: edgeDS }, VIS_OPTIONS)
      networkRef.current = net

      net.once('stabilizationIterationsDone', () => {
        if (cancelRef.current) return
        net.fit()
        setStatus('ready')
        if (onStable) onStable()
      })

      net.on('click', params => {
        if (cancelRef.current) return
        if (params.nodes.length === 0) { setSelected(null); net.unselectAll(); return }
        const visId = params.nodes[0]
        const nd    = nodeDS.get(visId)
        if (!nd) return
        setSelected({ visId, name: nd._name, type: nd._type, props: nd._props })
        net.selectNodes([visId, ...net.getConnectedNodes(visId)])
      })

      net.on('doubleClick', params => {
        if (cancelRef.current || params.nodes.length === 0) return
        net.focus(params.nodes[0], {
          scale: 1.6,
          animation: { duration: 500, easingFunction: 'easeInOutQuad' },
        })
      })
    }

    init().catch(err => {
      if (!cancelRef.current) { setStatus('error'); setMsg(err.message) }
    })

    return () => {
      cancelRef.current = true
      try { networkRef.current?.destroy?.() } catch {}
      networkRef.current = null
      nodeDSRef.current  = null
      edgeDSRef.current  = null
    }
  }, []) // eslint-disable-line react-hooks/exhaustive-deps

  //  2. Reload when activeCypher changes (expand / back / preset switch) 
  useEffect(() => {
    if (!networkRef.current || !nodeDSRef.current || !edgeDSRef.current) return
    if (!activeCypher) return

    setStatus('loading')
    setSelected(null)

    fetchVizData(activeCypher)
      .then(data => {
        if (cancelRef.current) return
        nodeDSRef.current.clear()
        edgeDSRef.current.clear()
        nodeDSRef.current.add(data.nodes.map(buildVisNode))
        edgeDSRef.current.add(data.edges.map(buildVisEdge))
        networkRef.current.startSimulation()
        networkRef.current.once('stabilizationIterationsDone', () => {
          networkRef.current?.fit()
          setStatus('ready')
        })
      })
      .catch(err => {
        if (!cancelRef.current) { setStatus('error'); setMsg(err.message) }
      })
  }, [activeCypher])

  //  Actions 

  const handleExpand = useCallback(() => {
    if (!selected) return
    setHistory(h => [...h, { label: selected.name, cypher: activeCypher }])
    setActiveCypher(buildExpandCypher(selected.name, selected.type))
  }, [selected, activeCypher])

  const handleBack = useCallback(() => {
    setHistory(h => {
      if (h.length === 0) return h
      const prev = h[h.length - 1]
      setActiveCypher(prev.cypher)
      return h.slice(0, -1)
    })
    setSelected(null)
  }, [])

  //  render 
  if (useMock) {
    return <MockGraph mode={deriveModeFromCypher(activeCypher)} height={height} />
  }

  return (
    <div className="flex rounded-lg border border-surface-border overflow-hidden"
         style={{ height, background: '#0c0c0e' }}>

      {/*  Canvas  */}
      <div className="relative flex-1 min-w-0">

        {status === 'loading' && (
          <div className="absolute inset-0 flex items-center justify-center z-10 pointer-events-none">
            <span className="text-sm text-zinc-600 animate-pulse">Loading graph from backend…</span>
          </div>
        )}
        {status === 'error' && (
          <div className="absolute inset-0 flex flex-col items-center justify-center gap-2 z-10">
            <p className="text-sm text-red-400 font-medium">Graph query failed</p>
            <p className="text-xs text-zinc-600 font-mono max-w-xs text-center">{msg}</p>
            <p className="text-xs text-zinc-700">Check backend logs or <code>.env</code> credentials</p>
          </div>
        )}

        {/* vis-network canvas target */}
        <div id={idRef.current} style={{ width: '100%', height: '100%' }} />

        {/* Breadcrumb / traversal history */}
        {history.length > 0 && (
          <div className="absolute top-3 left-3 z-20 flex items-center gap-1 flex-wrap">
            <button
              onClick={handleBack}
              className="flex items-center gap-1 text-[10px] bg-zinc-800/90 border border-zinc-700
                         rounded px-2 py-1 text-zinc-300 hover:text-white hover:border-zinc-500 transition-colors"
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

        {/* Node-type legend */}
        {status === 'ready' && (
          <div className="absolute bottom-3 left-3 flex flex-col gap-1.5 pointer-events-none z-10">
            {Object.entries(LABEL_COLORS).map(([lbl, c]) => (
              <span key={lbl} className="flex items-center gap-1.5 text-[10px]">
                <span className="w-2.5 h-2.5 rounded-full border"
                      style={{ background: c.background, borderColor: c.border }} />
                <span className="text-zinc-500">{lbl}</span>
              </span>
            ))}
          </div>
        )}

        {/* Interaction hint */}
        {status === 'ready' && !selected && (
          <div className="absolute bottom-3 right-3 text-[10px] text-zinc-700
                          pointer-events-none z-10 text-right leading-relaxed">
            click node to inspect
            <br />double-click to zoom
            <br />drag and scroll
          </div>
        )}
      </div>

      {/*  Property panel  slides in on node click  */}
      {selected && (
        <div className="w-60 shrink-0 border-l border-surface-border bg-zinc-950
                        flex flex-col overflow-y-auto text-xs">

          {/* Header: type badge + name */}
          <div className="flex items-start justify-between gap-2 px-3 pt-3 pb-2 border-b border-zinc-800">
            <div className="flex flex-col gap-1 min-w-0">
              <span
                className="self-start px-1.5 py-0.5 rounded text-[10px] font-mono border"
                style={{
                  color:       LABEL_COLORS[selected.type]?.border     || '#a1a1aa',
                  borderColor: LABEL_COLORS[selected.type]?.border     || '#52525b',
                  background:  LABEL_COLORS[selected.type]?.background || '#18181b',
                }}
              >
                {selected.type}
              </span>
              <span className="font-semibold text-zinc-100 font-mono break-all" title={selected.name}>
                {selected.name}
              </span>
            </div>
            <button
              onClick={() => setSelected(null)}
              className="shrink-0 mt-0.5 text-zinc-600 hover:text-zinc-300 text-base leading-none"
            >×</button>
          </div>

          {/* Property key-value list */}
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

          {/* Actions */}
          <div className="px-3 py-3 border-t border-zinc-800 space-y-2">
            <button
              onClick={handleExpand}
              className="w-full px-3 py-1.5 rounded bg-violet-500/10 border border-violet-500/30
                         text-violet-400 hover:bg-violet-500/20 transition-colors font-medium text-[11px]"
            >
              Expand neighbourhood
            </button>
            {history.length > 0 && (
              <button
                onClick={handleBack}
                className="w-full px-3 py-1.5 rounded bg-zinc-800 border border-zinc-700
                           text-zinc-400 hover:text-zinc-200 transition-colors text-[11px]"
              >
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