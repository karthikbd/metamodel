/**
 * BloomGraph — Neo4j Bloom-style force-directed graph using react-force-graph-2d.
 *
 * Props:
 *   nodes        [{id, name, type, ...}]
 *   edges        [{src, tgt, rel, ...}]   (also accepts {source, target} format)
 *   height       number (default 560)
 *   selectedId   string | null — externally controlled selection (optional)
 *   onNodeClick  (node | null) => void — when provided, selection is fully controlled
 *                from outside; when absent, BloomGraph manages its own info-panel
 */

import { useRef, useEffect, useState, useMemo, useCallback } from 'react'
import ForceGraph2D from 'react-force-graph-2d'
import { X, Info } from 'lucide-react'

// ── Palette ────────────────────────────────────────────────────────────────────

const NODE_PALETTE = {
  Job:          { fill: '#3b0764', stroke: '#9333ea', text: '#e9d5ff' },
  Dataset:      { fill: '#1e3a5f', stroke: '#3b82f6', text: '#bfdbfe' },
  BusinessRule: { fill: '#064e3b', stroke: '#10b981', text: '#a7f3d0' },
  Column:       { fill: '#134e4a', stroke: '#14b8a6', text: '#99f6e4' },
  Script:       { fill: '#831843', stroke: '#ec4899', text: '#fce7f3' },
  _default:     { fill: '#27272a', stroke: '#71717a', text: '#d4d4d8' },
}

const LINK_COLORS = {
  READS_FROM:   '#3b82f6',
  WRITES_TO:    '#f59e0b',
  DEPENDS_ON:   '#a78bfa',
  GOVERNED_BY:  '#10b981',
  DERIVED_FROM: '#f472b6',
  REFERENCES:   '#fb923c',
  JOINS_WITH:   '#38bdf8',
}

const NODE_R = 9
const LABEL_MAX = 18

// ── Component ──────────────────────────────────────────────────────────────────

export default function BloomGraph({
  nodes = [],
  edges = [],
  height = 560,
  selectedId = null,
  onNodeClick,
}) {
  const containerRef = useRef(null)
  const [cWidth, setCWidth] = useState(900)

  // Internal selection state — only used when caller does NOT pass onNodeClick
  const [internalSel, setInternalSel] = useState(null)
  const effectiveSel = onNodeClick ? selectedId : internalSel

  // ── Responsive width ─────────────────────────────────────────────────────────
  useEffect(() => {
    if (!containerRef.current) return
    const ro = new ResizeObserver(([entry]) => {
      setCWidth(Math.floor(entry.contentRect.width))
    })
    ro.observe(containerRef.current)
    return () => ro.disconnect()
  }, [])

  // ── graphData — shallow-clone so ForceGraph2D can mutate x/y freely ──────────
  const graphData = useMemo(() => ({
    nodes: nodes.map(n => ({ ...n })),
    links: edges.map(e => ({
      ...e,
      source: e.src    || e.source || '',
      target: e.tgt    || e.target || '',
    })),
  }), [nodes, edges])  // eslint-disable-line react-hooks/exhaustive-deps
  // NOTE: intentional key-equality — we want a new simulation whenever the
  // node/edge arrays themselves change (new fetch), not on every render.

  // ── Custom node canvas renderer ───────────────────────────────────────────────
  const paintNode = useCallback((node, ctx, gs) => {
    const pal   = NODE_PALETTE[node.type] || NODE_PALETTE._default
    const label = node.name || node.id || ''
    const fs    = Math.max(6, 10 / gs)

    // White ring around the selected node
    if (node.id === effectiveSel) {
      ctx.beginPath()
      ctx.arc(node.x, node.y, NODE_R + 5, 0, 2 * Math.PI)
      ctx.strokeStyle = 'rgba(255,255,255,0.9)'
      ctx.lineWidth   = 2 / gs
      ctx.stroke()
    }

    // Filled circle
    ctx.beginPath()
    ctx.arc(node.x, node.y, NODE_R, 0, 2 * Math.PI)
    ctx.fillStyle   = pal.fill
    ctx.fill()
    ctx.strokeStyle = pal.stroke
    ctx.lineWidth   = 1.5 / gs
    ctx.stroke()

    // Label below node
    ctx.font         = `${fs}px 'JetBrains Mono', monospace`
    ctx.textAlign    = 'center'
    ctx.textBaseline = 'top'
    ctx.fillStyle    = pal.text
    const trunc = label.length > LABEL_MAX ? label.slice(0, LABEL_MAX - 1) + '…' : label
    ctx.fillText(trunc, node.x, node.y + NODE_R + 2 / gs)
  }, [effectiveSel])

  // Pointer hit area (must be larger than the visual circle to be easy to click)
  const paintPointer = useCallback((node, color, ctx) => {
    ctx.fillStyle = color
    ctx.beginPath()
    ctx.arc(node.x, node.y, NODE_R + 4, 0, 2 * Math.PI)
    ctx.fill()
  }, [])

  // ── Click handlers ────────────────────────────────────────────────────────────
  const handleNodeClick = useCallback(node => {
    if (onNodeClick) {
      onNodeClick(node)
    } else {
      setInternalSel(prev => prev === node.id ? null : node.id)
    }
  }, [onNodeClick])

  const handleBgClick = useCallback(() => {
    if (onNodeClick) onNodeClick(null)
    else setInternalSel(null)
  }, [onNodeClick])

  // ── Internal info panel (only when not controlled from outside) ───────────────
  const internalSelNode = useMemo(() => {
    if (onNodeClick || !internalSel) return null
    return nodes.find(n => n.id === internalSel) || null
  }, [onNodeClick, internalSel, nodes])

  // ── Render ────────────────────────────────────────────────────────────────────
  return (
    <div
      className="flex rounded-lg border border-surface-border overflow-hidden"
      style={{ height }}
    >
      {/* ── Canvas panel ──────────────────────────────────────────────────────── */}
      <div
        ref={containerRef}
        className="flex-1 relative"
        style={{ background: '#09090b', minWidth: 0 }}
      >
        {nodes.length === 0 ? (
          <div className="h-full flex flex-col items-center justify-center gap-2 text-zinc-600 text-sm">
            <span>No graph data</span>
            <span className="text-xs text-zinc-700">Run Phase 1 Pipeline to hydrate the graph</span>
          </div>
        ) : (
          <ForceGraph2D
            graphData={graphData}
            width={cWidth}
            height={height}
            backgroundColor="#09090b"
            /* node rendering */
            nodeCanvasObject={paintNode}
            nodePointerAreaPaint={paintPointer}
            nodeRelSize={NODE_R}
            /* edge rendering */
            linkColor={link => LINK_COLORS[link.rel] || '#52525b'}
            linkWidth={1.5}
            linkLabel={link => link.rel || ''}
            linkDirectionalArrowLength={5}
            linkDirectionalArrowRelPos={0.9}
            /* interactions */
            onNodeClick={handleNodeClick}
            onBackgroundClick={handleBgClick}
            /* physics — fade out fast for snappy feel */
            cooldownTicks={150}
            d3AlphaDecay={0.025}
            d3VelocityDecay={0.3}
          />
        )}

        {/* ── Overlay legend ─────────────────────────────────────────────────── */}
        {nodes.length > 0 && (
          <div
            className="absolute top-3 left-3 flex flex-col gap-1 pointer-events-none
                       bg-black/50 backdrop-blur-sm rounded-md px-2.5 py-2"
          >
            <p className="text-[9px] uppercase tracking-widest text-zinc-600 mb-0.5">Nodes</p>
            {Object.entries(NODE_PALETTE)
              .filter(([k]) => k !== '_default')
              .map(([type, pal]) => (
                <div key={type} className="flex items-center gap-1.5">
                  <span
                    className="w-3 h-3 rounded-full border shrink-0"
                    style={{ background: pal.fill, borderColor: pal.stroke }}
                  />
                  <span className="text-[10px] text-zinc-400">{type}</span>
                </div>
              ))}
            <p className="text-[9px] uppercase tracking-widest text-zinc-600 mt-1.5 mb-0.5">Edges</p>
            {Object.entries(LINK_COLORS).map(([rel, color]) => (
              <div key={rel} className="flex items-center gap-1.5">
                <span className="w-4 h-[2px] rounded shrink-0" style={{ background: color }} />
                <span className="text-[9px] text-zinc-500">{rel}</span>
              </div>
            ))}
          </div>
        )}

        {/* ── Hint ───────────────────────────────────────────────────────────── */}
        {nodes.length > 0 && (
          <div className="absolute bottom-3 right-3 text-[9px] text-zinc-700 pointer-events-none">
            drag · scroll zoom · click node
          </div>
        )}
      </div>

      {/* ── Internal info panel (uncontrolled mode) ───────────────────────────── */}
      {internalSelNode && (
        <div className="w-64 shrink-0 border-l border-surface-border bg-surface-card p-4 text-xs space-y-3 overflow-y-auto">
          <div className="flex items-center justify-between">
            <span className="flex items-center gap-1.5 text-zinc-300 font-medium">
              <Info size={12} /> Node Details
            </span>
            <button
              onClick={() => setInternalSel(null)}
              className="text-zinc-600 hover:text-zinc-300"
            >
              <X size={13} />
            </button>
          </div>
          {Object.entries(internalSelNode)
            .filter(([k]) =>
              !['x', 'y', 'vx', 'vy', 'fx', 'fy', 'index', '__indexColor',
                '__controlPoints', 'src', 'tgt'].includes(k)
            )
            .map(([k, v]) => (
              <div key={k} className="space-y-0.5">
                <div className="text-zinc-500 uppercase tracking-wide text-[10px]">{k}</div>
                <div className="font-mono text-zinc-300 break-all">
                  {Array.isArray(v)
                    ? v.length ? v.join(', ') : '—'
                    : String(v ?? '—')}
                </div>
              </div>
            ))}
        </div>
      )}
    </div>
  )
}
