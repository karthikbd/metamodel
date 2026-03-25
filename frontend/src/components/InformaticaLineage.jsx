/**
 * InformaticaLineage.jsx
 *
 * Informatica-style horizontal data lineage diagram built with React Flow v11.
 *   • Left-to-right tier layout: Sources → Ingest → Core Tables → Transforms
 *     → Derived → Orchestration → Risk Outputs → Report Jobs → Reports
 *   • Custom Dataset nodes (colored by domain) and Job nodes (colored by role)
 *   • READS_FROM (blue), WRITES_TO (amber), DEPENDS_ON (dashed purple) edges
 *   • Click any node → property + column panel slides in on the right
 *   • Filter strip: All / Risk & Capital / Compliance / Reporting
 *   • Upstream / downstream path highlighting on click
 */

import { useState, useCallback, useMemo } from 'react'
import ReactFlow, {
  Background, BackgroundVariant, Controls, MiniMap, Panel,
  Handle, Position, MarkerType,
  useNodesState, useEdgesState,
} from 'reactflow'
import 'reactflow/dist/style.css'
import { Database, Cpu, FileBarChart2, X, ChevronDown, ChevronRight } from 'lucide-react'
import {
  DATASETS, JOBS, JOB_EDGES, COLUMNS_BY_DS,
} from '../data/mockGraph'

// ── Layout ─────────────────────────────────────────────────────────────────

const NW = 210, NH = 76, HSTEP = 310, VGAP = 18

// Tier index per node — defines the horizontal column
const TIER = {
  'ds-src-crm': 0, 'ds-src-pay': 0, 'ds-src-sanc': 0,
  'job-001': 1, 'job-002': 1,
  'ds-001': 2, 'ds-002': 2, 'ds-003': 2, 'ds-004': 2,
  'job-003': 3, 'job-004': 3, 'job-007': 3, 'job-008': 3, 'job-011': 3,
  'ds-005': 4, 'ds-006': 4, 'ds-009': 4, 'ds-010': 4, 'ds-012': 4,
  'job-005': 5, 'job-006': 5,
  'ds-007': 6, 'ds-008': 6,
  'job-009': 7, 'job-010': 7,
  'ds-011': 8,
}

const TIER_LABEL = [
  'Source Systems',
  'Ingest Jobs',
  'Core Tables',
  'Transform Jobs',
  'Derived Tables',
  'Orchestration',
  'Risk Outputs',
  'Report Jobs',
  'Reports',
]

// Vertical order within each tier
const TIER_ORDER = {
  'ds-src-crm': 0, 'ds-src-pay': 1, 'ds-src-sanc': 2,
  'job-001': 0, 'job-002': 1,
  'ds-001': 0, 'ds-002': 1, 'ds-003': 2, 'ds-004': 3,
  'job-003': 0, 'job-004': 1, 'job-007': 2, 'job-008': 3, 'job-011': 4,
  'ds-005': 0, 'ds-006': 1, 'ds-009': 2, 'ds-010': 3, 'ds-012': 4,
  'job-005': 0, 'job-006': 1,
  'ds-007': 0, 'ds-008': 1,
  'job-009': 0, 'job-010': 1,
  'ds-011': 0,
}

function nodeXY(id) {
  return {
    x: TIER[id] * HSTEP,
    y: (TIER_ORDER[id] ?? 0) * (NH + VGAP),
  }
}

// ── Color palettes ─────────────────────────────────────────────────────────

const DS_COLOR = {
  Source:      { bg:'#0c1929', border:'#3b82f6', text:'#93c5fd', sub:'#1d4ed8' },
  Reference:   { bg:'#0c1929', border:'#3b82f6', text:'#93c5fd', sub:'#1d4ed8' },
  Customer:    { bg:'#0d2030', border:'#06b6d4', text:'#67e8f9', sub:'#0891b2' },
  Transaction: { bg:'#100f25', border:'#8b5cf6', text:'#c4b5fd', sub:'#7c3aed' },
  Risk:        { bg:'#1a0e0e', border:'#ef4444', text:'#fca5a5', sub:'#b91c1c' },
  Compliance:  { bg:'#0a1c14', border:'#10b981', text:'#6ee7b7', sub:'#059669' },
  Reporting:   { bg:'#1a1505', border:'#f59e0b', text:'#fcd34d', sub:'#d97706' },
}
function dsColor(domain) { return DS_COLOR[domain] || DS_COLOR.Reference }

const JOB_COLOR = {
  ETL:        { bg:'#120d28', border:'#7c3aed', text:'#c4b5fd' },
  Risk:       { bg:'#1a0e0e', border:'#dc2626', text:'#fca5a5' },
  Compliance: { bg:'#0a1c14', border:'#059669', text:'#6ee7b7' },
  Reporting:  { bg:'#1a1705', border:'#d97706', text:'#fcd34d' },
}
function jobColor(domain) { return JOB_COLOR[domain] || JOB_COLOR.ETL }

const EDGE_STYLE = {
  READS_FROM:  { stroke:'#3b82f6', strokeWidth:1.8 },
  WRITES_TO:   { stroke:'#f59e0b', strokeWidth:2   },
  DEPENDS_ON:  { stroke:'#8b5cf6', strokeWidth:1.5, strokeDasharray:'5,4' },
}
const EDGE_MARKER = { type: MarkerType.ArrowClosed, width:14, height:14 }

// ── Custom node: Dataset ────────────────────────────────────────────────────

function DatasetNode({ data, selected }) {
  const c = dsColor(data.domain)
  return (
    <div
      style={{
        width: NW, background: c.bg,
        border: `1.5px solid ${selected ? '#ffffff' : c.border}`,
        boxShadow: selected ? `0 0 0 2px ${c.border}` : `0 0 12px ${c.border}22`,
        borderRadius: 8,
        fontFamily: "'JetBrains Mono', monospace",
      }}
    >
      <Handle type="target" position={Position.Left}
        style={{ background: c.border, width:8, height:8, border:'none' }} />
      <div className="px-3 py-2">
        <div className="flex items-center gap-1.5 mb-1">
          <Database size={11} color={c.border} />
          <span style={{ color: c.text, fontSize:11, fontWeight:600 }}
                className="truncate leading-tight">{data.name}</span>
        </div>
        <div className="flex items-center gap-1.5 flex-wrap">
          <span style={{ background: c.sub+'33', color: c.text, border:`1px solid ${c.border}44`,
                         fontSize:9, padding:'1px 5px', borderRadius:3 }}>
            {data.domain}
          </span>
          <span style={{ color:'#52525b', fontSize:9 }}>{data.format}</span>
          {data.pii && (
            <span style={{ background:'#450a0a', color:'#fca5a5',
                           border:'1px solid #7f1d1d', fontSize:9,
                           padding:'1px 5px', borderRadius:3 }}>PII</span>
          )}
        </div>
      </div>
      <Handle type="source" position={Position.Right}
        style={{ background: c.border, width:8, height:8, border:'none' }} />
    </div>
  )
}

// ── Custom node: Job ────────────────────────────────────────────────────────

function JobNode({ data, selected }) {
  const c = jobColor(data.domain)
  return (
    <div
      style={{
        width: NW, background: c.bg,
        border: `1.5px solid ${selected ? '#ffffff' : c.border}`,
        boxShadow: selected ? `0 0 0 2px ${c.border}` : `0 0 12px ${c.border}22`,
        borderRadius: 8,
        fontFamily: "'JetBrains Mono', monospace",
      }}
    >
      <Handle type="target" position={Position.Left}
        style={{ background: c.border, width:8, height:8, border:'none' }} />
      <div className="px-3 py-2">
        <div className="flex items-center gap-1.5 mb-1">
          <Cpu size={11} color={c.border} />
          <span style={{ color: c.text, fontSize:11, fontWeight:600 }}
                className="truncate leading-tight">{data.name}</span>
        </div>
        <div className="flex items-center gap-1.5 flex-wrap">
          <span style={{ background: c.border+'22', color: c.text, border:`1px solid ${c.border}44`,
                         fontSize:9, padding:'1px 5px', borderRadius:3 }}>
            {data.job_type}
          </span>
          {(data.risk_tags || []).map(t => (
            <span key={t} style={{ background:'#450a0a', color:'#fca5a5',
                                    border:'1px solid #7f1d1d', fontSize:9,
                                    padding:'1px 5px', borderRadius:3 }}>
              {t}
            </span>
          ))}
        </div>
      </div>
      <Handle type="source" position={Position.Right}
        style={{ background: c.border, width:8, height:8, border:'none' }} />
    </div>
  )
}

const NODE_TYPES = { dataset: DatasetNode, job: JobNode }

// ── Build initial nodes + edges ─────────────────────────────────────────────

function detectPii(dsName) {
  const cols = COLUMNS_BY_DS[dsName] || []
  return cols.some(c => c.pii)
}

function buildFlow() {
  const nodes = []
  const edges = []

  // Dataset nodes
  DATASETS.forEach(ds => {
    if (TIER[ds.id] === undefined) return
    const { x, y } = nodeXY(ds.id)
    nodes.push({
      id: ds.id,
      type: 'dataset',
      position: { x, y },
      data: {
        name:   ds.name,
        domain: ds.domain,
        format: ds.format,
        owner:  ds.owner,
        pii:    detectPii(ds.name),
        _kind:  'dataset',
      },
    })
  })

  // Job nodes
  JOBS.forEach(j => {
    if (TIER[j.id] === undefined) return
    const { x, y } = nodeXY(j.id)
    nodes.push({
      id: j.id,
      type: 'job',
      position: { x, y },
      data: {
        name:      j.name,
        domain:    j.domain,
        job_type:  j.type,
        path:      j.path,
        risk_tags: j.risk_tags,
        _kind:     'job',
      },
    })
  })

  // Edges: READS_FROM / WRITES_TO only (data flow)
  JOB_EDGES.forEach((e, i) => {
    if (e.rel === 'DEPENDS_ON') return  // shown in process mode
    if (TIER[e.src] === undefined || TIER[e.tgt] === undefined) return
    const style = EDGE_STYLE[e.rel] || EDGE_STYLE.READS_FROM
    edges.push({
      id: `je-${i}`,
      source: e.rel === 'WRITES_TO' ? e.src : e.src,
      target: e.rel === 'WRITES_TO' ? e.tgt : e.tgt,
      label:  e.rel === 'READS_FROM' ? 'reads' : 'writes',
      labelStyle: { fill: style.stroke, fontSize: 9, fontFamily: 'JetBrains Mono, monospace' },
      labelBgStyle: { fill: '#0c0c0e', fillOpacity: 0.85 },
      style,
      markerEnd: { ...EDGE_MARKER, color: style.stroke },
      _rel: e.rel,
    })
  })

  return { nodes, edges }
}


// ── Pure helpers (reduce cyclomatic complexity in useMemo) ────────────────

function applyNodeVisibility(n, domainSet, selectedId, hl) {
  const hidden       = domainSet ? !domainSet.has(n.id) : false
  const isSelected   = n.id === selectedId
  const isNeighbour  = hl && (hl.up.has(n.id) || hl.down.has(n.id))
  const dimmed       = !!hl && !isSelected && !isNeighbour
  return {
    ...n,
    hidden,
    selected: isSelected,
    style: dimmed ? { opacity: 0.25 } : undefined,
  }
}

function edgeOnPath(e, selectedId, hl) {
  if (!hl) return false
  if (e.source === selectedId || e.target === selectedId) return true
  const bothUp   = hl.up.has(e.source)   && hl.up.has(e.target)
  const bothDown = hl.down.has(e.source) && hl.down.has(e.target)
  return bothUp || bothDown
}

function applyEdgeVisibility(e, domainSet, selectedId, hl) {
  const hidden    = domainSet
    ? (!domainSet.has(e.source) || !domainSet.has(e.target))
    : false
  const onPath    = edgeOnPath(e, selectedId, hl)
  const dimmed    = !!hl && !onPath
  const baseWidth = e.style?.strokeWidth || 1.5
  const style     = dimmed  ? { ...e.style, opacity: 0.12 }
                 : onPath   ? { ...e.style, strokeWidth: baseWidth + 0.8 }
                 : e.style
  return { ...e, hidden, style }
}

// ── Main component ──────────────────────────────────────────────────────────

const FILTERS = [
  { key: 'all',        label: 'All' },
  { key: 'risk',       label: 'Risk & Capital' },
  { key: 'compliance', label: 'Compliance' },
  { key: 'reporting',  label: 'Reporting' },
]

// IDs relevant to each domain filter
const DOMAIN_IDS = {
  risk:        new Set(['ds-001','ds-002','ds-003','ds-004','job-001','job-002','job-003','job-004','ds-005','ds-006','job-005','job-006','ds-007','ds-008','job-009','job-010','ds-011']),
  compliance:  new Set(['ds-001','ds-002','ds-003','ds-src-crm','ds-src-sanc','job-001','job-002','job-007','job-008','ds-009','ds-010']),
  reporting:   new Set(['ds-001','ds-002','ds-003','ds-005','ds-006','ds-008','job-005','job-009','job-010','job-011','ds-011','ds-012']),
}

const { nodes: INIT_NODES, edges: INIT_EDGES } = buildFlow()

export default function InformaticaLineage() {
  const [nodes, ,]          = useNodesState(INIT_NODES)
  const [edges, ,]          = useEdgesState(INIT_EDGES)
  const [selected, setSelected] = useState(null)
  const [filter,   setFilter]   = useState('all')
  const [showDeps, setShowDeps] = useState(false)
  const [colExpanded, setColExpanded] = useState(false)

  // ── Downstream/upstream highlight on selection ──────────────────────────
  const highlighted = useMemo(() => {
    if (!selected) return null
    const up = new Set(), down = new Set()
    const traverse = (id, dir, visited = new Set()) => {
      if (visited.has(id)) return
      visited.add(id)
      INIT_EDGES.forEach(e => {
        if (dir === 'down' && e.source === id) { down.add(e.target); traverse(e.target, dir, visited) }
        if (dir === 'up'   && e.target === id) { up.add(e.source);   traverse(e.source, dir, visited) }
      })
    }
    traverse(selected.id, 'down')
    traverse(selected.id, 'up')
    return { up, down }
  }, [selected])

  // ── Dep edges (DEPENDS_ON) toggled separately ───────────────────────────
  const depEdges = useMemo(() => {
    if (!showDeps) return []
    return JOB_EDGES
      .filter(e => e.rel === 'DEPENDS_ON' && TIER[e.src] !== undefined && TIER[e.tgt] !== undefined)
      .map((e, i) => ({
        id: `dep-${i}`,
        source: e.src, target: e.tgt,
        label: 'depends',
        labelStyle: { fill: '#8b5cf6', fontSize: 9, fontFamily: 'JetBrains Mono, monospace' },
        labelBgStyle: { fill: '#0c0c0e', fillOpacity: 0.85 },
        style: EDGE_STYLE.DEPENDS_ON,
        markerEnd: { ...EDGE_MARKER, color: '#8b5cf6' },
        animated: true,
      }))
  }, [showDeps])

  // ── Filtered view ──────────────────────────────────────────────────────
  const domainSet = filter !== 'all' ? DOMAIN_IDS[filter] : null

  const visNodes = useMemo(() => {
    const sid = selected?.id ?? null
    return nodes.map(n => applyNodeVisibility(n, domainSet, sid, highlighted))
  }, [nodes, domainSet, selected, highlighted])

  const visEdges = useMemo(() => {
    const sid = selected?.id ?? null
    return [...edges, ...depEdges].map(e => applyEdgeVisibility(e, domainSet, sid, highlighted))
  }, [edges, depEdges, domainSet, selected, highlighted])

  // ── Node click ────────────────────────────────────────────────────────
  const onNodeClick = useCallback((_, node) => {
    setSelected(prev => prev?.id === node.id ? null : node)
    setColExpanded(false)
  }, [])

  const onPaneClick = useCallback(() => setSelected(null), [])

  // ── Tier swimlane label positions ─────────────────────────────────────
  const swimlaneY = useMemo(() => {
    const maxRow = { 0:2, 1:1, 2:3, 3:4, 4:4, 5:1, 6:1, 7:1, 8:0 }
    return TIER_LABEL.map((lbl, t) => ({
      lbl, t,
      height: ((maxRow[t] ?? 0) + 1) * (NH + VGAP) + VGAP,
    }))
  }, [])

  // ── Column data for panel ─────────────────────────────────────────────
  const columns = useMemo(() => {
    if (!selected || selected.data?._kind !== 'dataset') return null
    return COLUMNS_BY_DS[selected.data.name] || null
  }, [selected])

  return (
    <div className="flex rounded-lg border border-surface-border overflow-hidden"
         style={{ height: 640, background: '#070709' }}>

      {/* ── React Flow canvas ── */}
      <div className="flex-1 relative min-w-0">
        <ReactFlow
          nodes={visNodes}
          edges={visEdges}
          nodeTypes={NODE_TYPES}
          onNodeClick={onNodeClick}
          onPaneClick={onPaneClick}
          fitView
          fitViewOptions={{ padding: 0.18 }}
          minZoom={0.15}
          maxZoom={2}
          proOptions={{ hideAttribution: true }}
          style={{ background: '#070709' }}
        >
          <Background variant={BackgroundVariant.Dots} gap={24} size={1} color="#1c1c24" />
          <Controls style={{ background: '#111116', border: '1px solid #27272a', borderRadius: 8 }} />
          <MiniMap
            nodeColor={n => {
              const d = n.data || {}
              if (d._kind === 'job') return jobColor(d.domain).border
              return dsColor(d.domain).border
            }}
            style={{ background: '#111116', border: '1px solid #27272a', borderRadius: 8 }}
            maskColor="#0c0c0e99"
          />

          {/* ── Top panel ── */}
          <Panel position="top-left">
            <div className="flex flex-wrap gap-1.5 items-center">
              {/* Domain filter */}
              <div className="flex rounded-md overflow-hidden border border-zinc-800 text-[10px]">
                {FILTERS.map(f => (
                  <button key={f.key} onClick={() => setFilter(f.key)}
                    className={`px-2.5 py-1.5 transition-colors ${
                      filter === f.key
                        ? 'bg-violet-900/70 text-violet-300'
                        : 'bg-zinc-900/80 text-zinc-500 hover:text-zinc-300'
                    }`}
                  >{f.label}</button>
                ))}
              </div>
              {/* Dep toggle */}
              <button
                onClick={() => setShowDeps(v => !v)}
                className={`px-2.5 py-1.5 rounded-md border text-[10px] transition-colors ${
                  showDeps
                    ? 'bg-violet-900/50 border-violet-700 text-violet-300'
                    : 'bg-zinc-900/80 border-zinc-800 text-zinc-500 hover:text-zinc-300'
                }`}
              >
                {showDeps ? '▶ deps on' : '▶ deps off'}
              </button>
            </div>
          </Panel>

          {/* ── Tier labels ── */}
          <Panel position="top-center" style={{ pointerEvents:'none', left:0, right:0, width:'100%' }}>
            <div className="relative" style={{ height: 1 }}>
              {swimlaneY.map(({ lbl, t }) => (
                <div
                  key={t}
                  style={{
                    position: 'absolute',
                    left: t * HSTEP - 2,
                    width: NW + 4,
                    top: -28,
                    textAlign: 'center',
                    pointerEvents: 'none',
                  }}
                >
                  <span style={{
                    fontSize: 9, color: '#3f3f46',
                    fontFamily: 'JetBrains Mono, monospace',
                    letterSpacing: '0.06em',
                    textTransform: 'uppercase',
                  }}>{lbl}</span>
                </div>
              ))}
            </div>
          </Panel>

          {/* ── Legend ── */}
          <Panel position="bottom-left">
            <div className="bg-zinc-950/90 border border-zinc-800 rounded-md px-2.5 py-2
                            flex flex-col gap-1 text-[10px] font-mono">
              <span className="text-zinc-600 text-[9px] uppercase tracking-widest mb-0.5">Legend</span>
              {[
                { color: '#3b82f6', label: '→  reads from' },
                { color: '#f59e0b', label: '→  writes to'  },
                { color: '#8b5cf6', label: '- -  depends on' },
              ].map(({ color, label }) => (
                <span key={label} className="flex items-center gap-2">
                  <span style={{ color, fontWeight: 700 }}>━</span>
                  <span style={{ color: '#71717a' }}>{label}</span>
                </span>
              ))}
            </div>
          </Panel>
        </ReactFlow>
      </div>

      {/* ── Property panel ── */}
      {selected && (
        <div className="w-64 shrink-0 border-l border-surface-border bg-zinc-950
                        flex flex-col overflow-hidden text-xs font-mono">

          {/* Header */}
          <div className="flex items-start justify-between gap-2 px-3 pt-3 pb-2
                          border-b border-zinc-800">
            <div className="flex flex-col gap-1 min-w-0">
              {selected.data._kind === 'dataset' ? (
                <>
                  <span className="flex items-center gap-1.5 px-1.5 py-0.5 rounded border w-fit text-[10px]"
                        style={{
                          color:       dsColor(selected.data.domain).text,
                          borderColor: dsColor(selected.data.domain).border,
                          background:  dsColor(selected.data.domain).bg,
                        }}>
                    <Database size={9} /> Dataset
                  </span>
                  <span className="font-semibold text-zinc-100 break-all leading-snug">
                    {selected.data.name}
                  </span>
                </>
              ) : (
                <>
                  <span className="flex items-center gap-1.5 px-1.5 py-0.5 rounded border w-fit text-[10px]"
                        style={{
                          color:       jobColor(selected.data.domain).text,
                          borderColor: jobColor(selected.data.domain).border,
                          background:  jobColor(selected.data.domain).bg,
                        }}>
                    <Cpu size={9} /> Job
                  </span>
                  <span className="font-semibold text-zinc-100 break-all leading-snug">
                    {selected.data.name}
                  </span>
                </>
              )}
            </div>
            <button onClick={() => setSelected(null)}
                    className="shrink-0 mt-0.5 text-zinc-600 hover:text-zinc-300">
              <X size={14} />
            </button>
          </div>

          {/* Properties */}
          <div className="px-3 py-2 border-b border-zinc-800 space-y-1.5 text-[11px]">
            {Object.entries(selected.data)
              .filter(([k]) => !k.startsWith('_') && k !== 'name')
              .map(([k, v]) => (
                <div key={k} className="grid grid-cols-[auto_1fr] gap-x-2 items-start">
                  <span className="text-zinc-600 shrink-0 pt-px capitalize">{k.replace(/_/g,' ')}</span>
                  {Array.isArray(v)
                    ? <div className="flex flex-wrap gap-1">
                        {v.length === 0
                          ? <span className="text-zinc-700">—</span>
                          : v.map(t => (
                              <span key={t} className="px-1 py-0 border border-red-800 rounded
                                                        bg-red-950 text-red-400 text-[10px]">{t}</span>
                            ))
                        }
                      </div>
                    : <span className="text-zinc-300 break-all">{v === true ? 'yes' : v === false ? 'no' : String(v)}</span>
                  }
                </div>
              ))
            }
          </div>

          {/* Columns accordion (datasets only) */}
          {columns && (
            <div className="flex-1 overflow-y-auto">
              <button
                onClick={() => setColExpanded(v => !v)}
                className="w-full flex items-center justify-between px-3 py-2
                           hover:bg-zinc-900 transition-colors border-b border-zinc-800"
              >
                <span className="text-zinc-400 font-medium text-[11px]">
                  Columns ({columns.length})
                </span>
                {colExpanded ? <ChevronDown size={12} className="text-zinc-500" />
                              : <ChevronRight size={12} className="text-zinc-500" />}
              </button>

              {colExpanded && (
                <div className="divide-y divide-zinc-900">
                  {columns.map(col => (
                    <div key={col.id} className="px-3 py-1.5 flex items-start gap-2">
                      <span className="text-zinc-200 flex-1 min-w-0 truncate text-[10px]">
                        {col.name}
                        {col.pk && <span className="ml-1 text-[9px] text-yellow-500">PK</span>}
                        {col.deprecated && <span className="ml-1 text-[9px] text-zinc-600 line-through">dep</span>}
                      </span>
                      <span className="text-zinc-600 shrink-0 text-[9px]">{col.dtype}</span>
                      {col.pii && <span className="text-[9px] text-red-400 shrink-0">PII</span>}
                    </div>
                  ))}
                </div>
              )}
            </div>
          )}

          {/* Path info */}
          {selected.data._kind === 'job' && (
            <div className="px-3 py-2 border-t border-zinc-800 text-[10px]">
              <p className="text-zinc-600 mb-0.5">script path</p>
              <p className="text-zinc-400 break-all font-mono">{selected.data.path}</p>
            </div>
          )}

          {/* Lineage summary */}
          {highlighted && (
            <div className="px-3 py-2 border-t border-zinc-800 space-y-1 text-[10px]">
              <p className="text-zinc-500">
                <span className="text-blue-400">{highlighted.up.size}</span> upstream ·{' '}
                <span className="text-amber-400">{highlighted.down.size}</span> downstream
              </p>
            </div>
          )}
        </div>
      )}
    </div>
  )
}
