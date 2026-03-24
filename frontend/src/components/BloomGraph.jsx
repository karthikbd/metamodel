/**
 * NeoVisGraph — wraps neovis.js to render a real Neo4j Bloom-style force graph
 * directly from your AuraDB instance via Bolt WebSocket.
 *
 * NOTE: This file is named BloomGraph.jsx for backwards-compat with existing imports.
 *
 * Props:
 *   cypher    {string}   Cypher query to visualise (re-renders on change)
 *   height    {number}   Canvas height in px  (default 560)
 *   onStable  {fn}       Called once the physics stabilises
 *
 * Legacy props (nodes/edges/selectedId/onNodeClick) are accepted but ignored —
 * NeoVis fetches data directly from AuraDB via the Cypher prop.
 */
// ── visuals ───────────────────────────────────────────────────────────────────

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

// ── component ─────────────────────────────────────────────────────────────────

import { useEffect, useRef, useState } from 'react'

let _instanceCounter = 0

export default function NeoVisGraph({ cypher, height = 560, onStable }) {
  // stable unique DOM id per mount
  const idRef    = useRef(`neovis-${++_instanceCounter}`)
  const vizRef   = useRef(null)
  const [status, setStatus] = useState('loading')   // loading | ready | error
  const [msg,    setMsg]    = useState('')

  // ── 1. Build and mount NeoVis ─────────────────────────────────────────────
  useEffect(() => {
    let cancelled = false

    async function init() {
      // Fetch Neo4j credentials from backend (avoids baking them into the bundle)
      const res = await fetch('/api/neo4j-creds')
      if (!res.ok) throw new Error(`/api/neo4j-creds returned ${res.status}`)
      const creds = await res.json()
      if (cancelled) return

      // Dynamic import keeps neovis.js out of the SSR/build critical path
      const { default: NeoVis } = await import('neovis.js')
      const DC = NeoVis.NEOVIS_DEFAULT_CONFIG   // symbol key for default per-label options
      if (cancelled) return

      // Build per-label config
      const labels = {}
      Object.entries(LABEL_COLORS).forEach(([lbl, col]) => {
        labels[lbl] = {
          label: 'name',
          [DC]: {
            color: col,
            font:  { color: LABEL_FONT[lbl] || '#e4e4e7', size: 13, face: 'JetBrains Mono, monospace' },
            size:  22,
            shape: 'dot',
          },
        }
      })

      // Build per-relationship config
      const relationships = {}
      Object.entries(REL_COLORS).forEach(([rel, color]) => {
        relationships[rel] = {
          [DC]: {
            color: { color, hover: color, opacity: 0.85 },
            font:  { color: '#71717a', size: 10, face: 'JetBrains Mono, monospace', align: 'middle' },
          },
        }
      })

      const viz = new NeoVis({
        containerId: idRef.current,
        neo4j: {
          serverUrl:      creds.uri,
          serverUser:     creds.user,
          serverPassword: creds.password,
          ...(creds.database ? { serverDatabase: creds.database } : {}),
        },
        labels,
        relationships,
        initialCypher: cypher || 'MATCH (n)-[r]->(m) RETURN n, r, m LIMIT 80',
        visConfig: {
          nodes: {
            shape:       'dot',
            size:        22,
            borderWidth: 2,
            font: { color: '#e4e4e7', size: 13, face: 'JetBrains Mono, monospace' },
          },
          edges: {
            arrows:  { to: { enabled: true, scaleFactor: 0.6 } },
            smooth:  { type: 'dynamic' },
            width:   1.5,
            font:    { color: '#71717a', size: 10, face: 'JetBrains Mono, monospace', align: 'middle' },
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
          interaction: { hover: true, tooltipDelay: 300, navigationButtons: true, keyboard: true },
          background:  { color: '#0c0c0e' },
        },
      })

      viz.registerOnEvent(NeoVis.NeoVisEvents.CompletionEvent, () => {
        if (!cancelled) {
          setStatus('ready')
          if (onStable) onStable()
        }
      })

      viz.render()
      vizRef.current = viz
    }

    init().catch(err => {
      if (!cancelled) { setStatus('error'); setMsg(err.message) }
    })

    return () => {
      cancelled = true
      try { vizRef.current?.clearNetwork?.() } catch {}
      vizRef.current = null
    }
  }, []) // eslint-disable-line react-hooks/exhaustive-deps

  // ── 2. Re-run when query changes ─────────────────────────────────────────
  useEffect(() => {
    if (!vizRef.current || !cypher) return
    setStatus('loading')
    vizRef.current.renderWithCypher(cypher)
  }, [cypher])

  // ── render ────────────────────────────────────────────────────────────────
  return (
    <div className="relative rounded-lg border border-surface-border overflow-hidden"
         style={{ height, background: '#0c0c0e' }}>

      {status === 'loading' && (
        <div className="absolute inset-0 flex items-center justify-center z-10 pointer-events-none">
          <span className="text-sm text-zinc-600 animate-pulse">Connecting to Neo4j AuraDB…</span>
        </div>
      )}
      {status === 'error' && (
        <div className="absolute inset-0 flex flex-col items-center justify-center gap-2 z-10">
          <p className="text-sm text-red-400 font-medium">Neo4j connection failed</p>
          <p className="text-xs text-zinc-600 font-mono max-w-xs text-center">{msg}</p>
          <p className="text-xs text-zinc-700">Check NEO4J_URI / NEO4J_PASSWORD in <code>.env</code></p>
        </div>
      )}

      {/* NeoVis canvas target */}
      <div id={idRef.current} style={{ width: '100%', height: '100%' }} />

      {/* Legend */}
      {status === 'ready' && (
        <div className="absolute bottom-3 left-3 flex flex-col gap-1.5 pointer-events-none">
          {Object.entries(LABEL_COLORS).map(([lbl, c]) => (
            <span key={lbl} className="flex items-center gap-1.5 text-[10px]">
              <span className="w-2.5 h-2.5 rounded-full border"
                    style={{ background: c.background, borderColor: c.border }} />
              <span className="text-zinc-500">{lbl}</span>
            </span>
          ))}
        </div>
      )}
    </div>
  )
}

