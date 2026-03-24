import { useState, useEffect, useCallback } from 'react'
import { GitBranch, Database, Network } from 'lucide-react'
import { fetchAllJobGraph } from '../services/api'
import ColumnLineageTab from './ColumnLineageTab'
import FunctionalLineageTab from './FunctionalLineageTab'
import BloomGraph from '../components/BloomGraph'
import toast from 'react-hot-toast'

// ─── Edge colour map (used in the focus-info panel) ───────────────────────────

const EDGE_COLORS = {
  READS_FROM:   '#3b82f6',
  WRITES_TO:    '#f59e0b',
  DEPENDS_ON:   '#a78bfa',
  GOVERNED_BY:  '#34d399',
  DERIVED_FROM: '#f472b6',
  REFERENCES:   '#fb923c',
  JOINS_WITH:   '#38bdf8',
}

// ─── Job Lineage Tab ──────────────────────────────────────────────────────────

function JobLineageTab() {
  const [edgeMode,   setEdgeMode]   = useState('all')
  const [loading,    setLoading]    = useState(true)
  const [allNodes,   setAllNodes]   = useState([])
  const [allEdges,   setAllEdges]   = useState([])
  const [selectedId, setSelectedId] = useState(null)

  useEffect(() => {
    fetchAllJobGraph()
      .then(data => {
        setAllNodes(data.nodes || [])
        setAllEdges(data.edges || [])
      })
      .catch(() => toast.error('Could not load job graph'))
      .finally(() => setLoading(false))
  }, [])

  // Edge-mode filter
  const filteredEdges =
    edgeMode === 'process' ? allEdges.filter(e => e.rel === 'DEPENDS_ON')
    : edgeMode === 'data'  ? allEdges.filter(e => e.rel === 'READS_FROM' || e.rel === 'WRITES_TO')
    : allEdges

  // Focus-info data
  const selectedNode = allNodes.find(n => n.id === selectedId) || null
  const incoming = selectedId
    ? filteredEdges
        .filter(e => e.tgt === selectedId)
        .map(e => ({ id: e.src, rel: e.rel, node: allNodes.find(n => n.id === e.src) }))
    : []
  const outgoing = selectedId
    ? filteredEdges
        .filter(e => e.src === selectedId)
        .map(e => ({ id: e.tgt, rel: e.rel, node: allNodes.find(n => n.id === e.tgt) }))
    : []

  const handleNodeClick = useCallback(node => {
    if (!node) { setSelectedId(null); return }
    setSelectedId(prev => prev === node.id ? null : node.id)
  }, [])

  return (
    <div className="space-y-3 flex flex-col flex-1">

      {/* ── Controls ──────────────────────────────────────────────────────────── */}
      <div className="flex flex-wrap gap-2 items-center justify-between">
        <div className="flex gap-1 bg-surface border border-surface-border rounded-lg p-0.5">
          {[
            { key: 'all',     label: 'All edges' },
            { key: 'process', label: 'Process (DEPENDS_ON)' },
            { key: 'data',    label: 'Data (READS / WRITES)' },
          ].map(({ key, label }) => (
            <button
              key={key}
              onClick={() => { setEdgeMode(key); setSelectedId(null) }}
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
            onClick={() => setSelectedId(null)}
            className="text-xs text-zinc-400 hover:text-zinc-100 border border-zinc-700
                       bg-zinc-800 rounded-md px-3 py-1 transition-colors"
          >
            × Clear selection
          </button>
        )}

        <span className="text-xs text-zinc-600 ml-auto">
          Drag nodes · scroll to zoom · click to inspect
        </span>
      </div>

      {/* ── Bloom force graph ─────────────────────────────────────────────────── */}
      {loading ? (
        <div className="h-[560px] flex items-center justify-center text-sm text-zinc-600
                        border border-surface-border rounded-lg">
          Loading lineage graph…
        </div>
      ) : (
        <BloomGraph
          nodes={allNodes}
          edges={filteredEdges}
          height={560}
          selectedId={selectedId}
          onNodeClick={handleNodeClick}
        />
      )}

      {/* ── Focus info panel ──────────────────────────────────────────────────── */}
      {selectedNode && (
        <div className="rounded-lg border border-zinc-700 bg-zinc-900/80 px-4 py-3 space-y-2">
          <div className="flex items-center justify-between">
            <div className="flex items-center gap-2">
              <span
                className="text-xs font-mono px-2 py-0.5 rounded border"
                style={{
                  color:       selectedNode.type === 'Job'     ? '#a78bfa'
                             : selectedNode.type === 'Dataset' ? '#93c5fd' : '#6ee7b7',
                  borderColor: selectedNode.type === 'Job'     ? '#7c3aed'
                             : selectedNode.type === 'Dataset' ? '#3b82f6' : '#34d399',
                  background:  selectedNode.type === 'Job'     ? '#1e1529'
                             : selectedNode.type === 'Dataset' ? '#0f1a2e' : '#0e1e18',
                }}
              >
                {selectedNode.type}
              </span>
              <span className="text-sm font-semibold text-zinc-100 font-mono">
                {selectedNode.name || selectedNode.id}
              </span>
            </div>
            <span className="text-xs text-zinc-600">
              {incoming.length} incoming &nbsp;·&nbsp; {outgoing.length} outgoing
            </span>
          </div>

          <div className="grid grid-cols-2 gap-4 text-xs">
            {incoming.length > 0 && (
              <div>
                <p className="text-zinc-500 mb-1 font-medium">Incoming</p>
                <ul className="space-y-1">
                  {incoming.map((c, i) => (
                    <li key={i} className="flex items-center gap-1.5">
                      <span className="w-2 h-2 rounded-sm shrink-0"
                            style={{ background: EDGE_COLORS[c.rel] || '#6b7280' }} />
                      <span className="text-zinc-400 italic shrink-0">{c.rel}</span>
                      <span className="text-zinc-200 font-mono truncate max-w-[180px]">
                        {c.node?.name || c.id}
                      </span>
                      <button
                        onClick={() => setSelectedId(c.id)}
                        className="ml-1 text-zinc-600 hover:text-zinc-200 underline shrink-0"
                      >focus</button>
                    </li>
                  ))}
                </ul>
              </div>
            )}
            {outgoing.length > 0 && (
              <div>
                <p className="text-zinc-500 mb-1 font-medium">Outgoing</p>
                <ul className="space-y-1">
                  {outgoing.map((c, i) => (
                    <li key={i} className="flex items-center gap-1.5">
                      <span className="w-2 h-2 rounded-sm shrink-0"
                            style={{ background: EDGE_COLORS[c.rel] || '#6b7280' }} />
                      <span className="text-zinc-400 italic shrink-0">{c.rel}</span>
                      <span className="text-zinc-200 font-mono truncate max-w-[180px]">
                        {c.node?.name || c.id}
                      </span>
                      <button
                        onClick={() => setSelectedId(c.id)}
                        className="ml-1 text-zinc-600 hover:text-zinc-200 underline shrink-0"
                      >focus</button>
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
          <strong className="text-zinc-400">Process lineage</strong> — Job call chains (DEPENDS_ON)
          &nbsp;·&nbsp;
          <strong className="text-zinc-400">Data lineage</strong> — Job ↔ Dataset (READS_FROM / WRITES_TO)
          &nbsp;·&nbsp;
          <strong className="text-zinc-400">Dataset joins</strong> — FK / DERIVED_FROM / JOINS_WITH
          &nbsp;·&nbsp;
          <strong className="text-zinc-400">Column lineage</strong> — Column-level DERIVED_FROM chains
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
