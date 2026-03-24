import { useState, useEffect, useCallback } from 'react'
import ReactFlow, {
  Background,
  Controls,
  MiniMap,
  useNodesState,
  useEdgesState,
  MarkerType,
} from 'reactflow'
import 'reactflow/dist/style.css'
import { GitBranch, Database, Network } from 'lucide-react'
import { fetchAllJobGraph } from '../services/api'
import ColumnLineageTab from './ColumnLineageTab'
import FunctionalLineageTab from './FunctionalLineageTab'
import toast from 'react-hot-toast'

// ─── Constants ────────────────────────────────────────────────────────────────

const EDGE_COLORS = {
  READS_FROM:   '#3b82f6',
  WRITES_TO:    '#f59e0b',
  DEPENDS_ON:   '#a78bfa',
  GOVERNED_BY:  '#34d399',
  DERIVED_FROM: '#f472b6',
  REFERENCES:   '#fb923c',
  JOINS_WITH:   '#38bdf8',
}

const MONO = 'JetBrains Mono, monospace'

// ─── Layout helper ────────────────────────────────────────────────────────────

function layoutAllJobGraph(rawNodes, rawEdges) {
  const jobs     = [...rawNodes].filter(n => n.type === 'Job')
                                .sort((a, b) => (a.name || '').localeCompare(b.name || ''))
  const datasets = [...rawNodes].filter(n => n.type === 'Dataset')
                                .sort((a, b) => (a.name || '').localeCompare(b.name || ''))
  const rules    = [...rawNodes].filter(n => n.type === 'BusinessRule')
  const Y = 90

  const rfNodes = [
    ...jobs.map((n, i) => ({
      id: n.id, _type: 'Job', _name: n.name || '',
      data: { label: n.name || n.id.slice(0, 14) },
      position: { x: 60, y: i * Y },
      style: {
        background: '#1e1529', border: '1px solid #7c3aed', color: '#a78bfa',
        borderRadius: 6, fontSize: 11, fontFamily: MONO,
        padding: '5px 12px', minWidth: 140,
      },
    })),
    ...datasets.map((n, i) => ({
      id: n.id, _type: 'Dataset', _name: n.name || '',
      data: { label: n.name || n.id.slice(0, 14) },
      position: { x: 520, y: i * Y },
      style: {
        background: '#0f1a2e', border: '1px solid #3b82f6', color: '#93c5fd',
        borderRadius: 6, fontSize: 11, fontFamily: MONO,
        padding: '5px 12px', minWidth: 140,
      },
    })),
    ...rules.map((n, i) => ({
      id: n.id, _type: 'BusinessRule', _name: n.name || '',
      data: { label: n.name || n.id.slice(0, 14) },
      position: { x: 980, y: i * Y },
      style: {
        background: '#0e1e18', border: '1px solid #34d399', color: '#6ee7b7',
        borderRadius: 6, fontSize: 11, fontFamily: MONO,
        padding: '5px 12px', minWidth: 140,
      },
    })),
  ]

  const rfEdges = rawEdges.map((e, i) => {
    const color = EDGE_COLORS[e.rel] || '#6b7280'
    return {
      id: `e-${i}`, source: e.src, target: e.tgt, _rel: e.rel,
      label: e.rel,
      labelStyle: { fill: color, fontSize: 9, fontFamily: MONO },
      style: { stroke: color, strokeWidth: 1.5 },
      markerEnd: { type: MarkerType.ArrowClosed, color },
    }
  })

  return { rfNodes, rfEdges }
}

// ─── Job Lineage Tab ──────────────────────────────────────────────────────────

function JobLineageTab() {
  const [edgeMode,    setEdgeMode]   = useState('all')
  const [loading,     setLoading]    = useState(true)
  const [baseNodes,   setBaseNodes]  = useState([])
  const [baseEdges,   setBaseEdges]  = useState([])
  const [selectedId,  setSelectedId] = useState(null)
  const [focusInfo,   setFocusInfo]  = useState(null)   // { node, incoming, outgoing }
  const [nodes, setNodes, onNodesChange] = useNodesState([])
  const [edges, setEdges, onEdgesChange] = useEdgesState([])

  useEffect(() => {
    fetchAllJobGraph()
      .then(data => {
        const { rfNodes, rfEdges } = layoutAllJobGraph(data.nodes || [], data.edges || [])
        setBaseNodes(rfNodes)
        setBaseEdges(rfEdges)
        setNodes(rfNodes)
        setEdges(rfEdges)
      })
      .catch(() => toast.error('Could not load job graph'))
      .finally(() => setLoading(false))
  }, [])

  // Compute which edges are visible under the current edge-mode filter
  const visibleEdges = useCallback((mode, bEdges) =>
    mode === 'process' ? bEdges.filter(e => e._rel === 'DEPENDS_ON')
    : mode === 'data'  ? bEdges.filter(e => e._rel === 'READS_FROM' || e._rel === 'WRITES_TO')
    : bEdges
  , [])

  // Apply selection highlight: dims everything not connected to selectedId
  const applySelection = useCallback((selId, mode, bNodes, bEdges) => {
    const vEdges = visibleEdges(mode, bEdges)

    if (!selId) {
      setNodes(bNodes.map(n => ({ ...n, style: { ...n.style, opacity: 1, boxShadow: 'none' } })))
      setEdges(vEdges.map(e => ({ ...e, style: { ...e.style, opacity: 1, strokeWidth: 1.5 } })))
      setFocusInfo(null)
      return
    }

    // All edges touching this node
    const connectedEdgeIds = new Set(
      vEdges.filter(e => e.source === selId || e.target === selId).map(e => e.id)
    )
    // All nodes reachable via those edges (1-hop neighbourhood)
    const neighbourIds = new Set([selId])
    vEdges.forEach(e => {
      if (e.source === selId) neighbourIds.add(e.target)
      if (e.target === selId) neighbourIds.add(e.source)
    })

    setNodes(bNodes.map(n => {
      const isSel  = n.id === selId
      const isNeig = neighbourIds.has(n.id)
      return {
        ...n,
        style: {
          ...n.style,
          opacity:   isNeig ? 1 : 0.1,
          boxShadow: isSel  ? '0 0 0 2px #fff, 0 0 16px 4px rgba(255,255,255,.25)' : 'none',
          border:    isSel  ? `2px solid #fff` : n.style.border,
        },
      }
    }))
    setEdges(vEdges.map(e => ({
      ...e,
      style: {
        ...e.style,
        opacity:     connectedEdgeIds.has(e.id) ? 1 : 0.05,
        strokeWidth: connectedEdgeIds.has(e.id) ? 2.5 : 1.5,
      },
    })))

    // Build info panel data
    const selNode  = bNodes.find(n => n.id === selId)
    const incoming = vEdges
      .filter(e => e.target === selId)
      .map(e => ({ id: e.source, rel: e._rel, node: bNodes.find(n => n.id === e.source) }))
    const outgoing = vEdges
      .filter(e => e.source === selId)
      .map(e => ({ id: e.target, rel: e._rel, node: bNodes.find(n => n.id === e.target) }))
    setFocusInfo({ node: selNode, incoming, outgoing })
  }, [visibleEdges, setNodes, setEdges])

  // Re-apply whenever selection or edge mode changes
  useEffect(() => {
    if (baseNodes.length) applySelection(selectedId, edgeMode, baseNodes, baseEdges)
  }, [selectedId, edgeMode, baseNodes, baseEdges, applySelection])

  const handleNodeClick = useCallback((_evt, node) => {
    setSelectedId(prev => prev === node.id ? null : node.id)
  }, [])

  const handlePaneClick = useCallback(() => {
    setSelectedId(null)
  }, [])

  const clearSelection = () => setSelectedId(null)

  return (
    <div className="space-y-3 flex flex-col flex-1">

      {/* Controls */}
      <div className="flex flex-wrap gap-2 items-center justify-between">
        <div className="flex gap-1 bg-surface border border-surface-border rounded-lg p-0.5">
          {[
            { key: 'all',     label: 'All edges' },
            { key: 'process', label: 'Process (DEPENDS_ON)' },
            { key: 'data',    label: 'Data (READS/WRITES)' },
          ].map(({ key, label }) => (
            <button
              key={key}
              onClick={() => setEdgeMode(key)}
              className={`px-3 py-1 rounded-md text-xs font-medium transition-colors ${
                edgeMode === key ? 'bg-accent text-white' : 'text-zinc-400 hover:text-zinc-200'
              }`}
            >
              {label}
            </button>
          ))}
        </div>
        {selectedId && (
          <button
            onClick={clearSelection}
            className="text-xs text-zinc-400 hover:text-zinc-100 border border-zinc-700
                       bg-zinc-800 rounded-md px-3 py-1 transition-colors"
          >
            x Clear selection
          </button>
        )}
        <span className="text-xs text-zinc-600 ml-auto">
          Click any node to focus its flow &nbsp;·&nbsp; click again or background to clear
        </span>
      </div>

      {/* Legend */}
      <div className="flex flex-wrap gap-3 text-xs">
        {Object.entries(EDGE_COLORS).map(([rel, color]) => (
          <span key={rel} className="flex items-center gap-1.5">
            <span className="w-4 h-0.5 rounded" style={{ background: color }} />
            <span className="text-zinc-500">{rel}</span>
          </span>
        ))}
        {[
          { label: 'Job',          color: '#7c3aed' },
          { label: 'Dataset',      color: '#3b82f6' },
          { label: 'BusinessRule', color: '#34d399' },
        ].map(({ label, color }) => (
          <span key={label} className="flex items-center gap-1.5">
            <span className="w-2.5 h-2.5 rounded border" style={{ borderColor: color }} />
            <span className="text-zinc-500">{label}</span>
          </span>
        ))}
      </div>

      {/* Graph */}
      <div className="flex-1 min-h-[480px] rounded-lg border border-surface-border overflow-hidden">
        {loading ? (
          <div className="h-full flex items-center justify-center text-sm text-zinc-600">
            Loading full lineage graph...
          </div>
        ) : nodes.length === 0 ? (
          <div className="h-full flex flex-col items-center justify-center gap-2">
            <p className="text-sm text-zinc-600">No jobs or datasets found.</p>
            <p className="text-xs text-zinc-700">Run Phase 1 Pipeline first.</p>
          </div>
        ) : (
          <ReactFlow
            nodes={nodes}
            edges={edges}
            onNodesChange={onNodesChange}
            onEdgesChange={onEdgesChange}
            onNodeClick={handleNodeClick}
            onPaneClick={handlePaneClick}
            fitView
            fitViewOptions={{ padding: 0.15 }}
            style={{ background: '#0c0c0e' }}
          >
            <Background color="#27272a" />
            <Controls />
            <MiniMap
              nodeColor={n => n.style?.border?.match(/#[0-9a-f]{6}/i)?.[0] || '#52525b'}
              style={{ background: '#18181b', border: '1px solid #27272a' }}
            />
          </ReactFlow>
        )}
      </div>

      {/* Focus info panel — shown on node click */}
      {focusInfo && (
        <div className="rounded-lg border border-zinc-700 bg-zinc-900/80 px-4 py-3 space-y-2">
          <div className="flex items-center justify-between">
            <div className="flex items-center gap-2">
              <span
                className="text-xs font-mono px-2 py-0.5 rounded border"
                style={{
                  color:        focusInfo.node?._type === 'Job' ? '#a78bfa'
                                : focusInfo.node?._type === 'Dataset' ? '#93c5fd' : '#6ee7b7',
                  borderColor:  focusInfo.node?._type === 'Job' ? '#7c3aed'
                                : focusInfo.node?._type === 'Dataset' ? '#3b82f6' : '#34d399',
                  background:   focusInfo.node?._type === 'Job' ? '#1e1529'
                                : focusInfo.node?._type === 'Dataset' ? '#0f1a2e' : '#0e1e18',
                }}
              >
                {focusInfo.node?._type}
              </span>
              <span className="text-sm font-semibold text-zinc-100 font-mono">
                {focusInfo.node?._name || focusInfo.node?.id}
              </span>
            </div>
            <span className="text-xs text-zinc-600">
              {focusInfo.incoming.length} incoming &nbsp;·&nbsp; {focusInfo.outgoing.length} outgoing
            </span>
          </div>

          <div className="grid grid-cols-2 gap-4 text-xs">
            {/* Incoming */}
            {focusInfo.incoming.length > 0 && (
              <div>
                <p className="text-zinc-500 mb-1 font-medium">Incoming</p>
                <ul className="space-y-1">
                  {focusInfo.incoming.map((c, i) => (
                    <li key={i} className="flex items-center gap-1.5">
                      <span className="w-2 h-2 rounded-sm" style={{ background: EDGE_COLORS[c.rel] || '#6b7280' }} />
                      <span className="text-zinc-400 italic">{c.rel}</span>
                      <span className="text-zinc-200 font-mono truncate max-w-[180px]">
                        {c.node?._name || c.id}
                      </span>
                      <button
                        onClick={() => setSelectedId(c.id)}
                        className="ml-1 text-zinc-600 hover:text-zinc-200 underline"
                      >
                        focus
                      </button>
                    </li>
                  ))}
                </ul>
              </div>
            )}

            {/* Outgoing */}
            {focusInfo.outgoing.length > 0 && (
              <div>
                <p className="text-zinc-500 mb-1 font-medium">Outgoing</p>
                <ul className="space-y-1">
                  {focusInfo.outgoing.map((c, i) => (
                    <li key={i} className="flex items-center gap-1.5">
                      <span className="w-2 h-2 rounded-sm" style={{ background: EDGE_COLORS[c.rel] || '#6b7280' }} />
                      <span className="text-zinc-400 italic">{c.rel}</span>
                      <span className="text-zinc-200 font-mono truncate max-w-[180px]">
                        {c.node?._name || c.id}
                      </span>
                      <button
                        onClick={() => setSelectedId(c.id)}
                        className="ml-1 text-zinc-600 hover:text-zinc-200 underline"
                      >
                        focus
                      </button>
                    </li>
                  ))}
                </ul>
              </div>
            )}
          </div>
        </div>
      )}
    </div>
  )
}

// ─── Page ─────────────────────────────────────────────────────────────────────

export default function LineagePage() {
  const [tab, setTab] = useState('job')

  return (
    <div className="p-6 space-y-4 h-full flex flex-col">
      <div>
        <h1 className="text-xl font-semibold text-zinc-100">Lineage Visualiser</h1>
        <p className="text-sm text-zinc-500 mt-0.5">
          <strong className="text-zinc-400">Process lineage</strong> - Job to Job call chains (DEPENDS_ON)
          &nbsp;·&nbsp;
          <strong className="text-zinc-400">Data lineage</strong> - Job to Dataset read/write (READS_FROM / WRITES_TO)
          &nbsp;·&nbsp;
          <strong className="text-zinc-400">Column lineage</strong> - Column-level DERIVED_FROM chains
          &nbsp;·&nbsp;
          <strong className="text-zinc-400">Functional lineage</strong> - Full call graph, all functions
        </p>
      </div>

      {/* Tab switcher */}
      <div className="flex gap-1 bg-surface border border-surface-border rounded-lg p-1 w-fit">
        <button
          onClick={() => setTab('job')}
          className={`flex items-center gap-1.5 px-4 py-1.5 rounded-md text-sm font-medium transition-colors ${
            tab === 'job' ? 'bg-accent text-white' : 'text-zinc-400 hover:text-zinc-200'
          }`}
        >
          <GitBranch size={14} /> Process &amp; Data Lineage
        </button>
        <button
          onClick={() => setTab('column')}
          className={`flex items-center gap-1.5 px-4 py-1.5 rounded-md text-sm font-medium transition-colors ${
            tab === 'column' ? 'bg-accent text-white' : 'text-zinc-400 hover:text-zinc-200'
          }`}
        >
          <Database size={14} /> Column Lineage
        </button>
        <button
          onClick={() => setTab('functional')}
          className={`flex items-center gap-1.5 px-4 py-1.5 rounded-md text-sm font-medium transition-colors ${
            tab === 'functional' ? 'bg-accent text-white' : 'text-zinc-400 hover:text-zinc-200'
          }`}
        >
          <Network size={14} /> Functional Lineage
        </button>
      </div>

      {/* Tab content */}
      {tab === 'job'        && <JobLineageTab />}
      {tab === 'column'     && <ColumnLineageTab />}
      {tab === 'functional' && <FunctionalLineageTab />}
    </div>
  )
}
