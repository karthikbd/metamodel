import { useState, useCallback, useRef } from 'react'
import ReactFlow, {
  Background, BackgroundVariant, Controls, MiniMap, Panel,
  useNodesState, useEdgesState, Handle, Position,
} from 'reactflow'
import 'reactflow/dist/style.css'
import { Network, Loader2, RefreshCw, Filter, Database, Cpu, X, ArrowRight } from 'lucide-react'
import { fetchJobs, fetchDatasets, runCypher } from '../services/api'
import toast from 'react-hot-toast'
import { buildLayout, EDGE_COLORS } from './graphVizUtils'

const CYPHER = 'MATCH (j:Job)-[r:READS_FROM|WRITES_TO]->(d:Dataset) RETURN j.id AS src, type(r) AS rel, d.id AS tgt'
const MONO   = 'JetBrains Mono, monospace'

// ── Colour helpers ────────────────────────────────────────────────────────

function nodeColors(type, focal, hl) {
  const base = type === 'job'
    ? { bg: '#190d2e', border: '#7c3aed', hdr: '#1e1040', dot: '#a78bfa', text: '#c4b5fd' }
    : { bg: '#0a1628', border: '#3b82f6', hdr: '#0d1e38', dot: '#60a5fa', text: '#93c5fd' }
  if (focal)         return { ...base, border: '#f59e0b', bg: '#1c1200', hdr: '#231700', dot: '#fbbf24', text: '#fde68a' }
  if (hl === 'up')   return { ...base, border: '#3b82f6', bg: '#071628', hdr: '#0a1f3a', dot: '#60a5fa', text: '#93c5fd' }
  if (hl === 'down') return { ...base, border: '#ef4444', bg: '#1a0808', hdr: '#220c0c', dot: '#f87171', text: '#fca5a5' }
  return base
}

// ── Card node (shared for job + dataset) ─────────────────────────────────

function CardNode({ data, type }) {
  const { label, sub, dimmed, highlighted, focal } = data
  const c = nodeColors(type, focal, highlighted)
  const Icon = type === 'job' ? Cpu : Database

  return (
    <div style={{
      width: '100%', height: '100%',
      background: c.bg,
      border: `1.5px solid ${c.border}`,
      borderRadius: 9,
      boxShadow: focal
        ? `0 0 18px ${c.border}55, inset 0 0 0 1px ${c.border}33`
        : highlighted
          ? `0 0 10px ${c.border}44`
          : `0 2px 12px #00000055`,
      opacity: dimmed ? 0.1 : 1,
      fontFamily: MONO,
      overflow: 'hidden',
      transition: 'opacity 0.18s, box-shadow 0.18s, border-color 0.18s',
      display: 'flex', flexDirection: 'column',
      cursor: 'pointer',
    }}>
      {/* Header strip */}
      <div style={{
        background: c.hdr,
        borderBottom: `1px solid ${c.border}44`,
        display: 'flex', alignItems: 'center', gap: 10,
        padding: '0 18px', height: 58, flexShrink: 0,
      }}>
        <span style={{ width: 11, height: 11, borderRadius: '50%', background: c.dot, flexShrink: 0 }} />
        <Icon size={16} color={c.dot} style={{ flexShrink: 0 }} />
        <span style={{
          color: c.text, fontSize: 16, fontWeight: 700,
          letterSpacing: '0.02em', overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap',
        }}>{label}</span>
        <span style={{ color: '#3f3f46', fontSize: 11, marginLeft: 'auto', flexShrink: 0, textTransform: 'uppercase', letterSpacing: '0.08em' }}>
          {type === 'job' ? 'JOB' : 'DS'}
        </span>
      </div>

      {/* Sub-label row */}
      <div style={{ flex: 1, display: 'flex', alignItems: 'center', padding: '0 18px' }}>
        <span style={{
          color: '#6b7280', fontSize: 13, overflow: 'hidden',
          textOverflow: 'ellipsis', whiteSpace: 'nowrap', width: '100%',
        }}>{sub || '\u00a0'}</span>
      </div>

      <Handle id="left"  type="target" position={Position.Left}
        style={{ background: c.border, width: 13, height: 13, border: `3px solid ${c.bg}`, left: -7 }} />
      <Handle id="right" type="source" position={Position.Right}
        style={{ background: c.border, width: 13, height: 13, border: `3px solid ${c.bg}`, right: -7 }} />
    </div>
  )
}

function JobNode(props)     { return <CardNode {...props} type="job" /> }
function DatasetNode(props) { return <CardNode {...props} type="dataset" /> }

const NODE_TYPES = { job: JobNode, dataset: DatasetNode }

// ── Legend ────────────────────────────────────────────────────────────────

function Legend() {
  return (
    <div style={{ fontFamily: MONO }}
      className="flex flex-wrap items-center gap-4 text-[9px] text-zinc-500">
      {[
        { bg: '#190d2e', border: '#7c3aed', label: 'Job' },
        { bg: '#0a1628', border: '#3b82f6', label: 'Dataset' },
        { bg: '#1c1200', border: '#f59e0b', label: 'Selected' },
        { bg: '#071628', border: '#3b82f6', label: 'Upstream' },
        { bg: '#1a0808', border: '#ef4444', label: 'Downstream' },
      ].map(s => (
        <span key={s.label} className="flex items-center gap-1.5">
          <span style={{ width: 14, height: 14, borderRadius: 3, background: s.bg, border: `1.5px solid ${s.border}`, display: 'inline-block' }} />
          {s.label}
        </span>
      ))}
      <span className="flex items-center gap-1.5">
        <span style={{ width: 18, height: 2, borderRadius: 1, background: EDGE_COLORS.READS_FROM, display: 'inline-block' }} />
        READS_FROM
      </span>
      <span className="flex items-center gap-1.5">
        <span style={{ width: 18, height: 2, borderRadius: 1, background: EDGE_COLORS.WRITES_TO, display: 'inline-block' }} />
        WRITES_TO
      </span>
    </div>
  )
}

// ── Stats bar ─────────────────────────────────────────────────────────────

function StatsBar({ stats }) {
  const items = [
    { label: 'Jobs',       value: stats.jobs,         color: '#a78bfa' },
    { label: 'Datasets',   value: stats.datasets,     color: '#60a5fa' },
    { label: 'READS_FROM', value: stats.reads_from,   color: '#93c5fd' },
    { label: 'WRITES_TO',  value: stats.writes_to,    color: '#fbbf24' },
    { label: 'Isolated',   value: stats.disconnected, color: '#52525b' },
  ]
  return (
    <div className="flex flex-wrap gap-2 flex-shrink-0">
      {items.map(s => (
        <div key={s.label} style={{ fontFamily: MONO }}
          className="bg-surface-card border border-surface-border rounded-md px-3 py-1.5">
          <p className="text-sm font-bold" style={{ color: s.color }}>{s.value}</p>
          <p className="text-[9px] text-zinc-600 mt-0.5 uppercase tracking-wider">{s.label}</p>
        </div>
      ))}
    </div>
  )
}

// ── Empty / loading canvas ────────────────────────────────────────────────

function EmptyCanvas({ loading }) {
  if (loading) return (
    <div className="h-full flex flex-col items-center justify-center gap-3 text-zinc-600">
      <Loader2 size={28} className="animate-spin text-violet-500" />
      <p className="text-sm" style={{ fontFamily: MONO }}>Fetching lineage from Neo4j…</p>
    </div>
  )
  return (
    <div className="h-full flex flex-col items-center justify-center gap-4 text-zinc-600">
      <Network size={44} className="opacity-10" />
      <div className="text-center">
        <p className="text-sm text-zinc-400">Click <strong className="text-violet-400">Load Graph</strong> to render the diagram</p>
        <p className="text-xs text-zinc-700 mt-1" style={{ fontFamily: MONO }}>Requires Phase 1 pipeline data in Neo4j</p>
      </div>
    </div>
  )
}

// ── Detail panel ──────────────────────────────────────────────────────────

function DetailPanel({ panel, onClose, nodeMap }) {
  if (!panel) return null
  const isJob = panel.type === 'job'
  const c     = nodeColors(panel.type, false, null)

  return (
    <div style={{
      position: 'absolute', top: 12, right: 12, zIndex: 20,
      background: '#111116', border: `1px solid ${c.border}55`,
      borderRadius: 10, padding: '14px 16px', width: 268,
      boxShadow: `0 8px 32px #00000099, 0 0 0 1px ${c.border}22`,
      fontFamily: MONO,
    }}>
      <div className="flex items-start justify-between mb-3">
        <div style={{ overflow: 'hidden', flex: 1, paddingRight: 8 }}>
          <div className="flex items-center gap-1.5 mb-1">
            {isJob ? <Cpu size={10} color={c.dot} /> : <Database size={10} color={c.dot} />}
            <span style={{ color: c.dot, fontSize: 8, textTransform: 'uppercase', letterSpacing: '0.1em', fontWeight: 700 }}>
              {isJob ? 'Job' : 'Dataset'}
            </span>
          </div>
          <p style={{ color: '#e4e4e7', fontSize: 11, wordBreak: 'break-all', lineHeight: 1.4 }}>{panel.label}</p>
          {panel.sub && <p style={{ color: '#52525b', fontSize: 9, marginTop: 2 }}>{panel.sub}</p>}
        </div>
        <button onClick={onClose} style={{ color: '#52525b', flexShrink: 0 }}
          className="hover:text-zinc-300 transition-colors">
          <X size={13} />
        </button>
      </div>

      {panel.connections.length > 0 ? (
        <>
          <p style={{ color: '#3f3f46', fontSize: 8.5, textTransform: 'uppercase', letterSpacing: '0.1em', marginBottom: 8 }}>
            {isJob ? 'Datasets' : 'Jobs'} ({panel.connections.length})
          </p>
          <div className="space-y-1 max-h-56 overflow-y-auto">
            {panel.connections.map((conn, i) => {
              const n   = nodeMap.get(conn.id)
              const lbl = n?.data?.label || conn.id
              const rc  = conn.rel === 'WRITES_TO' ? '#f59e0b' : '#60a5fa'
              return (
                <div key={i} style={{
                  display: 'flex', alignItems: 'center', gap: 6,
                  background: '#18181b', borderRadius: 5, padding: '5px 8px',
                }}>
                  <ArrowRight size={9} style={{ color: rc, flexShrink: 0 }} />
                  <span style={{ color: '#a1a1aa', fontSize: 9.5, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap', flex: 1 }}>
                    {lbl}
                  </span>
                  <span style={{ color: rc, fontSize: 8.5, flexShrink: 0 }}>{conn.rel}</span>
                </div>
              )
            })}
          </div>
        </>
      ) : (
        <p style={{ color: '#52525b', fontSize: 10 }}>No connections in current view.</p>
      )}
    </div>
  )
}

// ── Main page ─────────────────────────────────────────────────────────────

export default function GraphVizPage() {
  const [nodes, setNodes, onNodesChange] = useNodesState([])
  const [edges, setEdges, onEdgesChange] = useEdgesState([])
  const [loading,  setLoading]  = useState(false)
  const [loaded,   setLoaded]   = useState(false)
  const [stats,    setStats]    = useState(null)
  const [filter,   setFilter]   = useState('')
  const [filtered, setFiltered] = useState(false)
  const [panel,    setPanel]    = useState(null)
  const [focal,    setFocal]    = useState(null)

  const rawEdgesRef = useRef([])
  const nodeMapRef  = useRef(new Map())

  const loadGraph = useCallback(async () => {
    setLoading(true); setLoaded(false); setPanel(null); setFocal(null)
    try {
      const [jobs, datasets, edgeRes] = await Promise.all([
        fetchJobs(), fetchDatasets(), runCypher(CYPHER),
      ])
      const { nodes: n, edges: e, stats: s } = buildLayout(
        Array.isArray(jobs)     ? jobs     : [],
        Array.isArray(datasets) ? datasets : [],
        edgeRes?.rows || [],
      )
      rawEdgesRef.current = e
      nodeMapRef.current  = new Map(n.map(nd => [nd.id, nd]))
      setNodes(n); setEdges(e); setStats(s)
      setLoaded(true); setFiltered(false); setFilter('')
    } catch {
      toast.error('Failed to load graph — run Phase 1 first')
    } finally {
      setLoading(false)
    }
  }, [setNodes, setEdges])

  const applyFocus = useCallback((nodeId, nodeType) => {
    if (!nodeId) {
      setNodes(nds => nds.map(n => ({
        ...n, data: { ...n.data, dimmed: false, highlighted: null, focal: false },
      })))
      setEdges(eds => eds.map(e => ({
        ...e, animated: false,
        style: { ...e.style, strokeWidth: 1.5, opacity: 0.45 },
      })))
      return
    }
    const up = new Set(), down = new Set()
    rawEdgesRef.current.forEach(e => {
      if (nodeType === 'job') {
        if (e.source === nodeId) {
          if (e.data?.rel === 'WRITES_TO')  down.add(e.target)
          if (e.data?.rel === 'READS_FROM') up.add(e.target)
        }
      } else {
        if (e.target === nodeId) {
          if (e.data?.rel === 'WRITES_TO')  up.add(e.source)
          if (e.data?.rel === 'READS_FROM') down.add(e.source)
        }
      }
    })
    const relevant = new Set([nodeId, ...up, ...down])
    setNodes(nds => nds.map(n => ({
      ...n,
      data: {
        ...n.data,
        focal:       n.id === nodeId,
        highlighted: up.has(n.id) ? 'up' : down.has(n.id) ? 'down' : null,
        dimmed:      !relevant.has(n.id),
      },
    })))
    setEdges(eds => eds.map(e => {
      const active = e.source === nodeId || e.target === nodeId
      return {
        ...e, animated: active,
        style: {
          ...e.style,
          strokeWidth: active ? 2.2 : 1.2,
          opacity:     relevant.has(e.source) && relevant.has(e.target) ? 0.9 : 0.05,
        },
      }
    }))
  }, [setNodes, setEdges])

  const onNodeClick = useCallback((_ev, node) => {
    if (focal === node.id) {
      setFocal(null); setPanel(null); applyFocus(null)
      return
    }
    setFocal(node.id)
    applyFocus(node.id, node.type)
    const connections = rawEdgesRef.current
      .filter(e => e.source === node.id || e.target === node.id)
      .map(e => ({
        id:  e.source === node.id ? e.target : e.source,
        rel: e.data?.rel || '',
      }))
    setPanel({ type: node.type, label: node.data.label, sub: node.data.sub, connections })
  }, [focal, applyFocus])

  const onPaneClick = useCallback(() => {
    setFocal(null); setPanel(null); applyFocus(null)
  }, [applyFocus])

  const applyFilter = useCallback((q) => {
    const lower = q.toLowerCase().trim()
    if (!lower) {
      setNodes(nds => nds.map(n => ({
        ...n, data: { ...n.data, dimmed: false, highlighted: null, focal: false },
      })))
      setEdges(eds => eds.map(e => ({ ...e, animated: false, style: { ...e.style, opacity: 0.45 } })))
      setFiltered(false)
      return
    }
    const matched = new Set()
    setNodes(nds => nds.map(n => {
      const hit = (n.data?.label || '').toLowerCase().includes(lower)
      if (hit) matched.add(n.id)
      return { ...n, data: { ...n.data, dimmed: !hit, focal: false, highlighted: null } }
    }))
    setEdges(eds => eds.map(e => ({
      ...e, animated: false,
      style: { ...e.style, opacity: matched.has(e.source) || matched.has(e.target) ? 0.75 : 0.04 },
    })))
    setFiltered(true); setFocal(null); setPanel(null)
  }, [setNodes, setEdges])

  return (
    <div className="p-6 space-y-3 h-full flex flex-col overflow-hidden">

      {/* ── Header ── */}
      <div className="flex items-start justify-between flex-shrink-0">
        <div>
          <h1 className="text-xl font-semibold text-zinc-100 flex items-center gap-2">
            <Network size={18} className="text-violet-400" />
            Graph Visualiser
          </h1>
          <p className="text-xs text-zinc-500 mt-0.5" style={{ fontFamily: MONO }}>
            Job → Dataset lineage · click any node to trace connections
          </p>
        </div>
        <div className="flex items-center gap-2">
          {loaded && (
            <div className="flex items-center gap-1.5">
              <input
                value={filter}
                onChange={e => { setFilter(e.target.value); applyFilter(e.target.value) }}
                placeholder="Filter nodes…"
                className="bg-surface border border-surface-border rounded-md px-3 py-1.5 text-xs
                           text-zinc-200 placeholder-zinc-600 focus:outline-none focus:border-accent w-44"
                style={{ fontFamily: MONO }}
              />
              {filtered && (
                <button onClick={() => { setFilter(''); applyFilter('') }}
                  className="text-xs text-zinc-500 hover:text-zinc-300 flex items-center gap-1">
                  <Filter size={11} /> Clear
                </button>
              )}
            </div>
          )}
          <button onClick={loadGraph} disabled={loading}
            className="flex items-center gap-1.5 px-3 py-1.5 rounded-md bg-violet-700 hover:bg-violet-600
                       text-sm text-white border border-violet-600 transition-colors disabled:opacity-50"
            style={{ fontFamily: MONO }}>
            {loading ? <Loader2 size={13} className="animate-spin" /> : <RefreshCw size={13} />}
            {loaded ? 'Reload' : 'Load Graph'}
          </button>
        </div>
      </div>

      {stats && <StatsBar stats={stats} />}
      {loaded && <div className="flex-shrink-0"><Legend /></div>}

      {/* ── Canvas ── */}
      <div className="flex-1 min-h-0 rounded-xl border border-surface-border overflow-hidden relative"
        style={{ background: '#070709' }}>
        {!loaded
          ? <EmptyCanvas loading={loading} />
          : (
            <>
              <ReactFlow
                nodes={nodes}
                edges={edges}
                nodeTypes={NODE_TYPES}
                onNodesChange={onNodesChange}
                onEdgesChange={onEdgesChange}
                onNodeClick={onNodeClick}
                onPaneClick={onPaneClick}
                fitView
                fitViewOptions={{ padding: 0.22 }}
                minZoom={0.05}
                maxZoom={3}
                proOptions={{ hideAttribution: true }}
                style={{ background: '#070709' }}
              >
                <Background variant={BackgroundVariant.Dots} gap={24} size={0.7} color="#1c1c28" />
                <Controls style={{ background: '#111116', border: '1px solid #27272a', borderRadius: 8 }} />
                <MiniMap
                  nodeColor={n => {
                    if (n.data?.focal)                  return '#f59e0b'
                    if (n.data?.highlighted === 'up')   return '#3b82f6'
                    if (n.data?.highlighted === 'down') return '#ef4444'
                    return n.type === 'job' ? '#7c3aed' : '#3b82f6'
                  }}
                  nodeStrokeWidth={0}
                  style={{ background: '#111116', border: '1px solid #27272a', borderRadius: 8 }}
                  maskColor="#07070988"
                />
                <Panel position="top-left">
                  <div style={{ fontFamily: MONO, pointerEvents: 'none', display: 'flex', gap: 284 }}
                    className="text-[8.5px] text-zinc-600 uppercase tracking-widest select-none">
                    <span className="border border-zinc-800 rounded px-2 py-0.5 bg-zinc-950/60">Jobs</span>
                    <span className="border border-zinc-800 rounded px-2 py-0.5 bg-zinc-950/60">Datasets</span>
                  </div>
                </Panel>
              </ReactFlow>

              <DetailPanel
                panel={panel}
                onClose={() => { setPanel(null); setFocal(null); applyFocus(null) }}
                nodeMap={nodeMapRef.current}
              />
            </>
          )
        }
      </div>
    </div>
  )
}
