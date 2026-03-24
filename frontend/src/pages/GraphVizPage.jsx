import { useState, useCallback } from 'react'
import ReactFlow, { Background, Controls, MiniMap, useNodesState, useEdgesState } from 'reactflow'
import 'reactflow/dist/style.css'
import { Network, Loader2, RefreshCw, Filter } from 'lucide-react'
import { fetchJobs, fetchDatasets, runCypher } from '../services/api'
import toast from 'react-hot-toast'
import { buildLayout, EDGE_COLORS } from './graphVizUtils'

const CYPHER = 'MATCH (j:Job)-[r:READS_FROM|WRITES_TO]->(d:Dataset) RETURN j.id AS src, type(r) AS rel, d.id AS tgt'

function Legend() {
  return (
    <div className="flex flex-wrap items-center gap-4 text-xs text-zinc-500">
      <span className="flex items-center gap-1.5">
        <span className="w-3 h-3 rounded-sm" style={{ background: '#1e1529', border: '1px solid #7c3aed' }} />
        Job
      </span>
      <span className="flex items-center gap-1.5">
        <span className="w-3 h-3 rounded-sm" style={{ background: '#0f1a2e', border: '1px solid #3b82f6' }} />
        Dataset
      </span>
      <span className="flex items-center gap-1.5">
        <span className="w-4 h-0.5 rounded" style={{ background: EDGE_COLORS.READS_FROM }} />
        READS_FROM
      </span>
      <span className="flex items-center gap-1.5">
        <span className="w-4 h-0.5 rounded" style={{ background: EDGE_COLORS.WRITES_TO }} />
        WRITES_TO
      </span>
    </div>
  )
}

function StatsBar({ stats }) {
  const items = [
    { label: 'Jobs',             value: stats.jobs,         color: 'text-violet-400' },
    { label: 'Datasets',         value: stats.datasets,     color: 'text-blue-400'   },
    { label: 'READS_FROM edges', value: stats.reads_from,   color: 'text-blue-300'   },
    { label: 'WRITES_TO edges',  value: stats.writes_to,    color: 'text-amber-400'  },
    { label: 'Disconnected',     value: stats.disconnected, color: 'text-zinc-500'   },
  ]
  return (
    <div className="flex flex-wrap gap-3 flex-shrink-0">
      {items.map(s => (
        <div key={s.label} className="bg-surface-card border border-surface-border rounded-md px-3 py-2">
          <p className={`text-lg font-bold ${s.color}`}>{s.value}</p>
          <p className="text-[10px] text-zinc-500 mt-0.5">{s.label}</p>
        </div>
      ))}
    </div>
  )
}

function EmptyCanvas({ loading }) {
  if (loading) {
    return (
      <div className="h-full flex flex-col items-center justify-center gap-3 text-zinc-600">
        <Loader2 size={28} className="animate-spin text-violet-500" />
        <p className="text-sm">Loading graph data from Neo4j…</p>
      </div>
    )
  }
  return (
    <div className="h-full flex flex-col items-center justify-center gap-3 text-zinc-600">
      <Network size={40} className="opacity-20" />
      <p className="text-sm">
        Click <strong className="text-zinc-400">Load Graph</strong> to render the node-edge diagram.
      </p>
      <p className="text-xs text-zinc-700">Requires Phase 1 pipeline to have run first.</p>
    </div>
  )
}

export default function GraphVizPage() {
  const [nodes, setNodes, onNodesChange] = useNodesState([])
  const [edges, setEdges, onEdgesChange] = useEdgesState([])
  const [loading,  setLoading]  = useState(false)
  const [loaded,   setLoaded]   = useState(false)
  const [stats,    setStats]    = useState(null)
  const [filter,   setFilter]   = useState('')
  const [filtered, setFiltered] = useState(false)

  const loadGraph = useCallback(async () => {
    setLoading(true)
    setLoaded(false)
    try {
      const [jobs, datasets, edgeRes] = await Promise.all([
        fetchJobs(), fetchDatasets(), runCypher(CYPHER),
      ])
      const { nodes: n, edges: e, stats: s } = buildLayout(
        Array.isArray(jobs)     ? jobs     : [],
        Array.isArray(datasets) ? datasets : [],
        edgeRes?.rows || [],
      )
      setNodes(n); setEdges(e); setStats(s)
      setLoaded(true); setFiltered(false); setFilter('')
    } catch {
      toast.error('Failed to load graph — run Phase 1 first')
    } finally {
      setLoading(false)
    }
  }, [setNodes, setEdges])

  const applyFilter = useCallback((q) => {
    const lower = q.toLowerCase().trim()
    if (!lower) {
      setNodes(nds => nds.map(n => ({ ...n, style: { ...n.style, opacity: 1 } })))
      setEdges(eds => eds.map(e => ({ ...e, style: { ...e.style, opacity: 0.55 } })))
      setFiltered(false)
      return
    }
    const matched = new Set()
    setNodes(nds => nds.map(n => {
      const hit = (n.data?.label || '').toLowerCase().includes(lower)
      if (hit) matched.add(n.id)
      return { ...n, style: { ...n.style, opacity: hit ? 1 : 0.12 } }
    }))
    setEdges(eds => eds.map(e => {
      const hit = matched.has(e.source) || matched.has(e.target)
      return { ...e, style: { ...e.style, opacity: hit ? 0.7 : 0.05 } }
    }))
    setFiltered(true)
  }, [setNodes, setEdges])

  return (
    <div className="p-6 space-y-4 h-full flex flex-col overflow-hidden">
      <div className="flex items-start justify-between flex-shrink-0">
        <div>
          <h1 className="text-xl font-semibold text-zinc-100 flex items-center gap-2">
            <Network size={18} className="text-violet-400" />
            Graph Visualiser
          </h1>
          <p className="text-sm text-zinc-500 mt-0.5">
            Jobs (purple) connected to Datasets (blue) via READS_FROM / WRITES_TO edges
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
                       text-sm text-white border border-violet-600 transition-colors disabled:opacity-50">
            {loading ? <Loader2 size={13} className="animate-spin" /> : <RefreshCw size={13} />}
            {loaded ? 'Reload' : 'Load Graph'}
          </button>
        </div>
      </div>

      {stats && <StatsBar stats={stats} />}
      {loaded && <div className="flex-shrink-0"><Legend /></div>}

      <div className="flex-1 min-h-0 rounded-lg border border-surface-border overflow-hidden">
        {!loaded ? <EmptyCanvas loading={loading} /> : (
          <ReactFlow nodes={nodes} edges={edges}
            onNodesChange={onNodesChange} onEdgesChange={onEdgesChange}
            fitView fitViewOptions={{ padding: 0.15 }}
            minZoom={0.05} maxZoom={2}
            style={{ background: '#0c0c0e' }}
            defaultEdgeOptions={{ type: 'straight' }}>
            <Background color="#27272a" gap={20} />
            <Controls />
            <MiniMap
              nodeColor={n => n.style?.border?.includes('#7c3aed') ? '#7c3aed' : '#3b82f6'}
              style={{ background: '#18181b', border: '1px solid #27272a' }}
              maskColor="rgba(12,12,14,0.7)"
            />
          </ReactFlow>
        )}
      </div>
    </div>
  )
}
