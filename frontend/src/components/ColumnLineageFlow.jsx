/**
 * ColumnLineageFlow.jsx
 *
 * Informatica-style column lineage diagram — works fully offline from mock data.
 *
 * Layout:
 *   • Datasets are parent group nodes (colored boxes with header label)
 *   • Columns that participate in lineage are child nodes stacked inside
 *   • DERIVED_FROM arrows cross dataset boundaries left-to-right
 *   • Click a column → highlight full upstream + downstream chain
 *   • PII columns flagged in red
 *
 * Bridge between higher-dim (Dataset) and lower-dim (Column):
 *   Every column is placed INSIDE its owning dataset container so you can see
 *   both the dataset it belongs to AND the cross-dataset derivation flow.
 */

import { useState, useMemo, useCallback } from 'react'
import ReactFlow, {
  Background, BackgroundVariant, Controls, MiniMap, Panel,
  Handle, Position, MarkerType,
  useNodesState, useEdgesState,
} from 'reactflow'
import 'reactflow/dist/style.css'
import { Lock } from 'lucide-react'
import { COLUMN_EDGES, COLUMNS_BY_DS, DATASETS } from '../data/mockGraph'

// ── Layout constants ───────────────────────────────────────────────────────

const DS_W = 210, COL_H = 36, DS_HDR = 32, DS_PAD = 8, TIER_W = 270, TIER_GAP = 30

// Tier assignment: 0 = pure source, 4 = final derived
const DS_TIER = { 'ds-004':0,'ds-002':0,'ds-001':0,'ds-003':1,'ds-005':2,'ds-006':2,'ds-009':2,'ds-010':2,'ds-008':3,'ds-011':4 }
const DS_VTIER = { 'ds-004':0,'ds-002':1,'ds-001':2,'ds-003':0,'ds-005':0,'ds-006':1,'ds-009':2,'ds-010':3,'ds-008':0,'ds-011':0 }

const DS_COL_DATA = [['ds-001','#091820','#06b6d4','#67e8f9'],['ds-002','#091820','#3b82f6','#93c5fd'],['ds-003','#130e28','#8b5cf6','#c4b5fd'],['ds-004','#0c1919','#14b8a6','#5eead4'],['ds-005','#1a0e0e','#ef4444','#fca5a5'],['ds-006','#1a0e0e','#f97316','#fdba74'],['ds-008','#1a1005','#f59e0b','#fcd34d'],['ds-009','#091a10','#22c55e','#86efac'],['ds-010','#091a10','#10b981','#6ee7b7'],['ds-011','#1a1505','#eab308','#fde047']]
const DS_COL_MAP = Object.fromEntries(DS_COL_DATA.map(([id, bg, border, text]) => [id, { bg, border, text }]))
function dsCol(id) { return DS_COL_MAP[id] || { bg:'#111116', border:'#52525b', text:'#a1a1aa' } }

// ── Build flat COL_BY_ID ─────────────────────────────────────────────────

function buildLayoutMaps() {
  const colById = {}
  for (const ds of DATASETS) {
    ;(COLUMNS_BY_DS[ds.name] || []).forEach(c => { colById[c.id] = { col:c, ds } })
  }
  const involved = new Set()
  COLUMN_EDGES.forEach(e => { involved.add(e.src); involved.add(e.tgt) })
  const grouped = {}
  for (const cid of involved) {
    const entry = colById[cid]
    if (!entry) continue
    const dsId = entry.ds.id
    if (!grouped[dsId]) grouped[dsId] = []
    grouped[dsId].push(cid)
  }
  const yPos = {}, byTier = {}
  Object.entries(DS_TIER).forEach(([id, t]) => {
    if (!byTier[t]) byTier[t] = []
    byTier[t].push({ id, vslot: DS_VTIER[id] || 0 })
  })
  Object.values(byTier).forEach(items => {
    items.sort((a, b) => a.vslot - b.vslot)
    let y = 0
    items.forEach(({ id }) => {
      yPos[id] = y
      y += (DS_HDR + DS_PAD * 2 + (grouped[id]?.length || 0) * COL_H) + TIER_GAP
    })
  })
  return { colById, involved, grouped, yPos }
}

const { colById: COL_BY_ID, involved: involvedIds, grouped: byDs, yPos: dsY } = buildLayoutMaps()

// ── Pure style helpers (extracted to reduce Lizard CCN) ─────────────────

function colNodeBg(isFocal, hl) {
  if (isFocal)       return '#2a1f00'
  if (hl === 'up')   return '#0e1e28'
  if (hl === 'down') return '#1e0e0e'
  return '#111116'
}
function colNodeBorder(isFocal, hl, pii, fallback) {
  if (isFocal)       return '#f59e0b'
  if (hl === 'up')   return '#3b82f6'
  if (hl === 'down') return '#ef4444'
  if (pii)           return '#7f1d1d'
  return fallback
}
function colNodeColor(isFocal, hl, pii) {
  if (isFocal)       return '#fde047'
  if (hl === 'up')   return '#93c5fd'
  if (hl === 'down') return '#fca5a5'
  if (pii)           return '#fca5a5'
  return '#a1a1aa'
}
function colIdOnPath(colId, up, down, id) { return id === colId || up.has(id) || down.has(id) }
function edgeOnPath(e, colId, up, down) {
  return colIdOnPath(colId, up, down, e.source) && colIdOnPath(colId, up, down, e.target)
}
function edgeSW(onPath, conf) { return onPath && conf === 'verified' ? 2.4 : onPath ? 1.8 : 1 }
function applyEdgeHighlight(e, colId, up, down, conf) {
  const onPath = edgeOnPath(e, colId, up, down)
  const confOk = conf === 'all' || e._conf === conf
  return { ...e, hidden: !confOk, style: { ...e.style, opacity: onPath ? 1 : 0.07, strokeWidth: edgeSW(onPath, e._conf) } }
}

// ── Custom nodes ──────────────────────────────────────────────────────────

function DatasetGroupNode({ data }) {
  const c = dsCol(data.dsId)
  return (
    <div style={{
      width: '100%', height: '100%',
      background: c.bg,
      border: `1.5px solid ${c.border}`,
      borderRadius: 10,
      boxShadow: `0 0 18px ${c.border}22`,
      fontFamily: 'JetBrains Mono, monospace',
    }}>
      {/* Header */}
      <div style={{
        height: DS_HDR, borderBottom: `1px solid ${c.border}44`,
        display: 'flex', alignItems: 'center', gap: 6,
        padding: '0 10px',
      }}>
        <span style={{ width:8, height:8, borderRadius:'50%', background: c.border, flexShrink:0 }} />
        <span style={{ color: c.text, fontSize:10, fontWeight:700, letterSpacing:'0.04em' }}>
          {data.name}
        </span>
        <span style={{ color:'#3f3f46', fontSize:9, marginLeft:'auto' }}>
          {data.domain}
        </span>
      </div>
    </div>
  )
}

function ColumnNode({ data }) {
  const c      = dsCol(data.dsId)
  const hl     = data.highlighted
  const focal  = data.focal
  const bg     = colNodeBg(focal, hl)
  const border = colNodeBorder(focal, hl, data.pii, `${c.border}44`)
  const color  = colNodeColor(focal, hl, data.pii)

  return (
    <div style={{
      width: '100%', height: '100%',
      background: bg,
      border: `1px solid ${border}`,
      borderRadius: 5,
      display: 'flex', alignItems: 'center', gap: 6,
      padding: '0 8px',
      opacity: data.dimmed ? 0.18 : 1,
      fontFamily: 'JetBrains Mono, monospace',
      cursor: 'pointer',
      transition: 'opacity 0.15s',
    }}>
      <Handle type="target" position={Position.Left}
        style={{ background: border, width:7, height:7, border:'none', left:-4 }} />
      {data.pii && <Lock size={8} color="#f87171" />}
      <span style={{ color, fontSize: 10, fontWeight: focal ? 700 : 400 }}
            className="truncate">{data.name}</span>
      <span style={{ color:'#52525b', fontSize:9, marginLeft:'auto', flexShrink:0 }}>
        {data.dtype}
      </span>
      <Handle type="source" position={Position.Right}
        style={{ background: border, width:7, height:7, border:'none', right:-4 }} />
    </div>
  )
}

const NODE_TYPES = { dsGroup: DatasetGroupNode, col: ColumnNode }

// ── Build initial nodes + edges ───────────────────────────────────────────

function buildInitialFlow() {
  const nodes = []
  const edges = []

  // Dataset group nodes
  Object.entries(byDs).forEach(([dsId, colIds]) => {
    const ds = DATASETS.find(d => d.id === dsId)
    if (!ds || !(dsId in DS_TIER)) return
    const x = DS_TIER[dsId] * TIER_W
    const y = dsY[dsId] || 0
    const h = DS_HDR + DS_PAD * 2 + colIds.length * COL_H

    nodes.push({
      id: dsId,
      type: 'dsGroup',
      position: { x, y },
      data: { dsId, name: ds.name, domain: ds.domain },
      style: { width: DS_W, height: h, pointerEvents: 'none' },
      selectable: false,
    })

    // Column child nodes
    colIds.forEach((cid, idx) => {
      const entry = COL_BY_ID[cid]
      if (!entry) return
      const { col } = entry
      nodes.push({
        id: cid,
        type: 'col',
        parentNode: dsId,
        extent: 'parent',
        position: { x: DS_PAD, y: DS_HDR + DS_PAD + idx * COL_H },
        data: {
          dsId, name: col.name, dtype: col.dtype,
          pii: col.pii, pk: col.pk,
          dimmed: false, highlighted: null, focal: false,
        },
        style: { width: DS_W - DS_PAD * 2, height: COL_H - 4 },
      })
    })
  })

  // DERIVED_FROM edges — direction: tgt → src (source feeds into derived)
  COLUMN_EDGES.forEach((e, i) => {
    if (!involvedIds.has(e.src) || !involvedIds.has(e.tgt)) return
    const exprShort = e.expression ? e.expression.slice(0, 30) : null
    edges.push({
      id: `ce-${i}`,
      source: e.tgt,   // tgt = source column (older)
      target: e.src,   // src = derived column (newer)
      label: exprShort,
      labelStyle: { fill:'#f472b6', fontSize:7.5, fontFamily:'JetBrains Mono, monospace' },
      labelBgStyle: { fill:'#0c0c0e', fillOpacity:0.9, borderRadius:3 },
      style: {
        stroke: e.confidence === 'verified' ? '#f472b6' : '#f472b688',
        strokeWidth: e.confidence === 'verified' ? 1.6 : 1.2,
        strokeDasharray: e.confidence === 'verified' ? undefined : '4 3',
      },
      markerEnd: { type: MarkerType.ArrowClosed, color:'#f472b6', width:13, height:13 },
      _src: e.src, _tgt: e.tgt,
      _conf: e.confidence,
      _expr: e.expression,
    })
  })

  return { nodes, edges }
}

const { nodes: INIT_NODES, edges: INIT_EDGES } = buildInitialFlow()

// ── Lineage BFS helpers ───────────────────────────────────────────────────

function getLineage(colId) {
  const up = new Set(), down = new Set()
  const upQ = [colId], downQ = [colId]
  const uvU = new Set([colId]), uvD = new Set([colId])
  while (upQ.length) {
    const id = upQ.shift()
    INIT_EDGES.forEach(e => {
      if (e.target === id && !uvU.has(e.source)) {
        up.add(e.source); uvU.add(e.source); upQ.push(e.source)
      }
    })
  }
  while (downQ.length) {
    const id = downQ.shift()
    INIT_EDGES.forEach(e => {
      if (e.source === id && !uvD.has(e.target)) {
        down.add(e.target); uvD.add(e.target); downQ.push(e.target)
      }
    })
  }
  return { up, down }
}

// ── Component ─────────────────────────────────────────────────────────────

export default function ColumnLineageFlow() {
  const [nodes, setNodes, onNodesChange] = useNodesState(INIT_NODES)
  const [edges, setEdges, onEdgesChange] = useEdgesState(INIT_EDGES)
  const [focal,  setFocal]  = useState(null)   // colId
  const [panel,  setPanel]  = useState(null)   // { name, dsName, dtype, pii, expr, up, down }
  const [conf,   setConf]   = useState('all')  // 'all' | 'verified' | 'inferred'

  const applyFocus = useCallback((colId) => {
    if (!colId) {
      setNodes(ns => ns.map(n =>
        n.type === 'col' ? { ...n, data: { ...n.data, dimmed:false, highlighted:null, focal:false } } : n
      ))
      setEdges(es => es.map(e => ({
        ...e,
        hidden: conf !== 'all' && e._conf !== conf,
        style: {
          ...e.style, opacity: 1,
          strokeWidth: e._conf === 'verified' ? 1.6 : 1.2,
        },
      })))
      return
    }
    const { up, down } = getLineage(colId)
    setNodes(ns => ns.map(n => {
      if (n.type !== 'col') return n
      const isUp   = up.has(n.id)
      const isDown = down.has(n.id)
      const isFocal = n.id === colId
      const rel = isFocal ? 'focal'
                : isUp    ? 'up'
                : isDown  ? 'down'
                : null
      return {
        ...n,
        data: {
          ...n.data,
          dimmed:      !isFocal && !isUp && !isDown,
          highlighted: rel === 'focal' ? null : rel,
          focal:       isFocal,
        },
      }
    }))
    setEdges(es => es.map(e => applyEdgeHighlight(e, colId, up, down, conf)))
  }, [setNodes, setEdges, conf])

  const onNodeClick = useCallback((_ev, node) => {
    if (node.type !== 'col') return
    if (focal === node.id) {
      setFocal(null); setPanel(null); applyFocus(null)
    } else {
      const entry = COL_BY_ID[node.id]
      const { up, down } = getLineage(node.id)
      setFocal(node.id)
      setPanel({
        name:   node.data.name, dsName: entry?.ds.name || '',
        dtype:  node.data.dtype, pii: node.data.pii,
        up: up.size, down: down.size,
        // immediate sources / targets
        sources: INIT_EDGES
          .filter(e => e.target === node.id)
          .map(e => ({ id: e.source, expr: e._expr, conf: e._conf })),
        derived: INIT_EDGES
          .filter(e => e.source === node.id)
          .map(e => ({ id: e.target, expr: e._expr, conf: e._conf })),
      })
      applyFocus(node.id)
    }
  }, [focal, applyFocus])

  const onPaneClick = useCallback(() => {
    setFocal(null); setPanel(null); applyFocus(null)
  }, [applyFocus])

  // confidence filter toggle
  const toggleConf = useCallback((v) => {
    setConf(v)
    setEdges(es => es.map(e => ({
      ...e,
      hidden: v !== 'all' && e._conf !== v,
    })))
  }, [setEdges])

  // resolve col name from id for panel
  const colName = (id) => {
    const e = COL_BY_ID[id]
    return e ? `${e.ds.name}.${e.col.name}` : id
  }

  return (
    <div className="flex rounded-lg border border-surface-border overflow-hidden"
         style={{ height: 660, background: '#070709' }}>

      <div className="flex-1 relative min-w-0">
        <ReactFlow
          nodes={nodes}
          edges={edges}
          nodeTypes={NODE_TYPES}
          onNodesChange={onNodesChange}
          onEdgesChange={onEdgesChange}
          onNodeClick={onNodeClick}
          onPaneClick={onPaneClick}
          fitView
          fitViewOptions={{ padding: 0.12 }}
          minZoom={0.2}
          maxZoom={2.5}
          proOptions={{ hideAttribution: true }}
          style={{ background: '#070709' }}
        >
          <Background variant={BackgroundVariant.Dots} gap={22} size={0.8} color="#1a1a24" />
          <Controls style={{ background:'#111116', border:'1px solid #27272a', borderRadius:8 }} />
          <MiniMap
            nodeColor={n => {
              if (n.type === 'dsGroup') return dsCol(n.data.dsId).border
              if (n.data?.focal) return '#f59e0b'
              if (n.data?.highlighted === 'up')   return '#3b82f6'
              if (n.data?.highlighted === 'down')  return '#ef4444'
              return '#27272a'
            }}
            style={{ background:'#111116', border:'1px solid #27272a', borderRadius:8 }}
            maskColor="#0c0c0e99"
          />

          {/* Top controls */}
          <Panel position="top-left">
            <div className="flex gap-1.5 items-center">
              <div className="flex rounded-md overflow-hidden border border-zinc-800 text-[10px]">
                {[['all','All'], ['verified','Verified ✓'], ['inferred','Inferred ~']].map(([v, lbl]) => (
                  <button key={v} onClick={() => toggleConf(v)}
                    className={`px-2.5 py-1.5 transition-colors ${
                      conf === v ? 'bg-pink-900/50 text-pink-300' : 'bg-zinc-900/80 text-zinc-500 hover:text-zinc-300'
                    }`}>{lbl}</button>
                ))}
              </div>
              {focal && (
                <button onClick={() => { setFocal(null); setPanel(null); applyFocus(null) }}
                  className="text-[10px] px-2.5 py-1.5 bg-zinc-900/80 border border-zinc-700
                             rounded text-zinc-400 hover:text-zinc-200">
                  ✕ clear
                </button>
              )}
            </div>
          </Panel>

          {/* Legend */}
          <Panel position="bottom-left">
            <div className="bg-zinc-950/90 border border-zinc-800 rounded-md px-2.5 py-2
                            flex flex-col gap-1 text-[10px] font-mono">
              <span className="text-zinc-600 text-[9px] uppercase tracking-widest mb-0.5">Column role</span>
              {[
                { color:'#f59e0b', label:'focal (selected)' },
                { color:'#3b82f6', label:'upstream source' },
                { color:'#ef4444', label:'downstream derived' },
                { color:'#f472b6', label:'─── DERIVED_FROM' },
                { color:'#f472b688', label:'- - - inferred' },
              ].map(({ color, label }) => (
                <span key={label} className="flex items-center gap-2">
                  <span style={{ color, fontWeight:700 }}>━</span>
                  <span style={{ color:'#71717a' }}>{label}</span>
                </span>
              ))}
              <span className="text-zinc-700 text-[9px] mt-1">
                Datasets = containers · columns inside
              </span>
            </div>
          </Panel>

          {/* Hint */}
          {!focal && (
            <Panel position="bottom-right">
              <span className="text-[10px] text-zinc-700 font-mono">
                click a column to trace its lineage
              </span>
            </Panel>
          )}
        </ReactFlow>
      </div>

      {/* Info panel */}
      {panel && (
        <div className="w-64 shrink-0 border-l border-surface-border bg-zinc-950
                        flex flex-col text-xs font-mono overflow-y-auto">

          <div className="px-3 pt-3 pb-2 border-b border-zinc-800">
            <p className="text-[9px] text-zinc-600 uppercase tracking-widest mb-1">Focal column</p>
            <p className="font-bold text-amber-300 break-all leading-snug">{panel.dsName}.{panel.name}</p>
            <div className="flex gap-2 mt-1.5">
              <span className="px-1.5 py-0.5 rounded bg-zinc-800 text-zinc-400 text-[9px]">{panel.dtype}</span>
              {panel.pii && (
                <span className="px-1.5 py-0.5 rounded bg-red-950 border border-red-800 text-red-400 text-[9px]">
                  PII
                </span>
              )}
              <span className="px-1.5 py-0.5 rounded bg-zinc-800 text-blue-400 text-[9px]">
                {panel.up} upstream
              </span>
              <span className="px-1.5 py-0.5 rounded bg-zinc-800 text-red-400 text-[9px]">
                {panel.down} downstream
              </span>
            </div>
          </div>

          {panel.sources.length > 0 && (
            <div className="px-3 py-2 border-b border-zinc-800">
              <p className="text-[9px] text-blue-400 uppercase tracking-widest mb-1.5">
                Source columns ({panel.sources.length})
              </p>
              {panel.sources.map((s, i) => (
                <div key={i} className="mb-2">
                  <p className="text-blue-300 text-[10px] break-all">{colName(s.id)}</p>
                  {s.expr && (
                    <p className="text-zinc-600 text-[9px] mt-0.5 break-all leading-relaxed italic">
                      {s.expr}
                    </p>
                  )}
                  <span className={`text-[8px] ${s.conf === 'verified' ? 'text-green-500' : 'text-amber-600'}`}>
                    {s.conf}
                  </span>
                </div>
              ))}
            </div>
          )}

          {panel.derived.length > 0 && (
            <div className="px-3 py-2">
              <p className="text-[9px] text-red-400 uppercase tracking-widest mb-1.5">
                Derived into ({panel.derived.length})
              </p>
              {panel.derived.map((s, i) => (
                <div key={i} className="mb-2">
                  <p className="text-red-300 text-[10px] break-all">{colName(s.id)}</p>
                  {s.expr && (
                    <p className="text-zinc-600 text-[9px] mt-0.5 break-all leading-relaxed italic">
                      {s.expr}
                    </p>
                  )}
                  <span className={`text-[8px] ${s.conf === 'verified' ? 'text-green-500' : 'text-amber-600'}`}>
                    {s.conf}
                  </span>
                </div>
              ))}
            </div>
          )}

          {panel.sources.length === 0 && panel.derived.length === 0 && (
            <p className="px-3 py-4 text-zinc-600 italic text-[11px]">No direct lineage edges.</p>
          )}
        </div>
      )}
    </div>
  )
}
