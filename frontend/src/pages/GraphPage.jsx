import { useState, useEffect, useCallback } from 'react'
import { Search, Play, Share2, Table2, X, Info } from 'lucide-react'
import ReactFlow, {
  Background, Controls, MiniMap,
  useNodesState, useEdgesState, MarkerType,
} from 'reactflow'
import 'reactflow/dist/style.css'
import { runCypher, runCypherVisual, fetchJobs, fetchDatasets } from '../services/api'
import toast from 'react-hot-toast'

// ── ReactFlow node/edge helpers ───────────────────────────────────────────────

const TYPE_STYLE = {
  Job:          { border: '#9333ea', bg: '#1e1529', color: '#c4b5fd' },
  Dataset:      { border: '#3b82f6', bg: '#0f1228', color: '#93c5fd' },
  Column:       { border: '#22c55e', bg: '#0a1a0e', color: '#86efac' },
  BusinessRule: { border: '#f59e0b', bg: '#1a1200', color: '#fcd34d' },
  Script:       { border: '#ec4899', bg: '#1a0812', color: '#f9a8d4' },
  Node:         { border: '#52525b', bg: '#18181b', color: '#a1a1aa' },
}

const MONO = 'JetBrains Mono, monospace'
const X_GAP = 260
const Y_GAP = 90

function buildRFGraph(nodes, edges) {
  // Assign x positions by type bucket, y by index within bucket
  const buckets = {}
  nodes.forEach(n => {
    const t = n.type || 'Node'
    buckets[t] = buckets[t] || []
    buckets[t].push(n)
  })
  const typeOrder = ['Job', 'Dataset', 'Column', 'BusinessRule', 'Script', 'Node']
  const orderedTypes = [...new Set([...typeOrder, ...Object.keys(buckets)])]

  const rfNodes = []
  orderedTypes.forEach((type, colIdx) => {
    if (!buckets[type]) return
    buckets[type].forEach((n, rowIdx) => {
      const s = TYPE_STYLE[type] || TYPE_STYLE.Node
      const label = n.data?.name || n.id
      rfNodes.push({
        id:   n.id,
        data: { label, raw: n.data },
        position: { x: colIdx * X_GAP + 40, y: rowIdx * Y_GAP + 40 },
        style: {
          background: s.bg,
          border: `1px solid ${s.border}`,
          color: s.color,
          borderRadius: 6,
          fontSize: 11,
          fontFamily: MONO,
          padding: '6px 12px',
          minWidth: 110,
          maxWidth: 200,
          wordBreak: 'break-word',
        },
      })
    })
  })

  const rfEdges = edges.map((e, i) => ({
    id:     e.id || `e-${i}`,
    source: e.source || e.src || '',
    target: e.target || e.tgt || '',
    label:  e.label || e.rel || '',
    style:  { stroke: '#a78bfa', strokeWidth: 1.5 },
    labelStyle: { fill: '#71717a', fontSize: 9, fontFamily: MONO },
    markerEnd: { type: MarkerType.ArrowClosed, color: '#a78bfa' },
  }))

  return { rfNodes, rfEdges }
}

const SAMPLE_QUERIES = [
  { label: 'All jobs',                q: "MATCH (j:Job) RETURN j.name AS name, coalesce(j.path, j.domain, '') AS path, coalesce(j.risk_tags, []) AS risk_tags LIMIT 25" },
  { label: 'All datasets',           q: 'MATCH (d:Dataset) RETURN d.name AS name, d.qualified_name AS qualified_name, d.format AS format, d.status AS status LIMIT 25' },
  { label: 'Job → Dataset',          q: 'MATCH (j:Job)-[r:READS_FROM|WRITES_TO]->(d:Dataset) RETURN j.name, type(r), d.name LIMIT 30' },
  { label: 'All columns',            q: 'MATCH (d:Dataset)-[:HAS_COLUMN]->(c:Column) RETURN d.name AS dataset, c.name AS column, c.data_type AS dtype, c.status AS status LIMIT 25' },
  { label: 'PII-tagged jobs',        q: "MATCH (j:Job) WHERE 'PII' IN j.risk_tags RETURN j.name AS name, coalesce(j.path, j.domain, '') AS path, coalesce(j.risk_tags, []) AS risk_tags LIMIT 25" },
  { label: 'Job dependencies',       q: 'MATCH (a:Job)-[r:DEPENDS_ON]->(b:Job) RETURN a.name AS caller, b.name AS callee LIMIT 25' },
  { label: 'Business rules',         q: 'MATCH (j:Job)-[:GOVERNED_BY]->(b:BusinessRule) RETURN j.name, b.name, b.description LIMIT 25' },
  { label: 'Deprecated columns',     q: "MATCH (c:Column {status:'deprecated'}) RETURN c.name, c.deprecated_at LIMIT 25" },
]

function CellValue({ v }) {
  if (v === null || v === undefined) return <span className="text-zinc-600">null</span>
  if (Array.isArray(v)) {
    if (v.length === 0) return <span className="text-zinc-600">[]</span>
    return (
      <div className="flex flex-wrap gap-1">
        {v.map((item, i) => (
          <span key={i} className="badge-info">{String(item)}</span>
        ))}
      </div>
    )
  }
  return <span>{String(v)}</span>
}

export default function GraphPage() {
  const [query,        setQuery]       = useState(SAMPLE_QUERIES[2].q)   // default: Job→Dataset
  const [results,      setResults]     = useState(null)
  const [loading,      setLoading]     = useState(false)
  const [tab,          setTab]         = useState('query')              // 'query' | 'jobs' | 'datasets'
  const [viewMode,     setViewMode]    = useState('graph')              // 'table' | 'graph'
  const [search,       setSearch]      = useState('')
  const [listData,     setListData]    = useState([])
  const [selectedNode, setSelectedNode] = useState(null)               // for info panel

  // ReactFlow state
  const [rfNodes, setRfNodes, onNodesChange] = useNodesState([])
  const [rfEdges, setRfEdges, onEdgesChange] = useEdgesState([])

  // ── Execute query ─────────────────────────────────────────────────────────
  async function executeQuery() {
    setLoading(true)
    setResults(null)
    setRfNodes([])
    setRfEdges([])
    setSelectedNode(null)
    try {
      if (viewMode === 'graph') {
        const data = await runCypherVisual(query)
        const { rfNodes: n, rfEdges: e } = buildRFGraph(data.nodes || [], data.edges || [])
        setRfNodes(n)
        setRfEdges(e)
        setResults({ type: 'graph', nodeCount: (data.nodes || []).length, edgeCount: (data.edges || []).length })
      } else {
        const data = await runCypher(query)
        setResults({ type: 'table', ...data })
      }
    } catch (err) {
      toast.error(err.response?.data?.detail || 'Query failed')
    } finally {
      setLoading(false)
    }
  }

  async function loadJobs() {
    const data = await fetchJobs(search || null)
    setListData(data)
  }

  async function loadDatasets() {
    const data = await fetchDatasets()
    setListData(data)
  }

  // Re-run when viewMode changes so the result type matches
  useEffect(() => {
    if (tab === 'query') executeQuery()
  }, [viewMode]) // eslint-disable-line react-hooks/exhaustive-deps

  // Auto-run on first load
  useEffect(() => { executeQuery() }, [])  // eslint-disable-line react-hooks/exhaustive-deps

  useEffect(() => {
    if (tab === 'jobs') loadJobs()
    else if (tab === 'datasets') loadDatasets()
  }, [tab])  // eslint-disable-line react-hooks/exhaustive-deps

  const handleNodeClick = useCallback((_ev, node) => {
    setSelectedNode(node)
  }, [])

  const handlePaneClick = useCallback(() => {
    setSelectedNode(null)
  }, [])

  return (
    <div className="p-6 space-y-4">
      <div>
        <h1 className="text-xl font-semibold text-zinc-100">Graph Explorer</h1>
        <p className="text-sm text-zinc-500 mt-0.5">Query the Meta Model graph with Cypher · visualise as graph or table</p>
      </div>

      {/* Top-level tabs */}
      <div className="flex gap-1 border-b border-surface-border pb-0">
        {[['query','Cypher Query'], ['jobs','Jobs'], ['datasets','Datasets']].map(([id, label]) => (
          <button
            key={id}
            onClick={() => setTab(id)}
            className={`px-4 py-2 text-sm border-b-2 transition-colors ${
              tab === id
                ? 'border-accent text-accent-text'
                : 'border-transparent text-zinc-500 hover:text-zinc-300'
            }`}
          >
            {label}
          </button>
        ))}
      </div>

      {tab === 'query' && (
        <div className="space-y-3">
          {/* Sample query chips */}
          <div className="flex flex-wrap gap-2">
            {SAMPLE_QUERIES.map(sq => (
              <button
                key={sq.label}
                onClick={() => setQuery(sq.q)}
                className="text-xs px-2.5 py-1 rounded-full border border-surface-border
                           text-zinc-400 hover:text-zinc-100 hover:border-zinc-600 transition-colors"
              >
                {sq.label}
              </button>
            ))}
          </div>

          {/* Editor */}
          <div className="card p-0 overflow-hidden">
            <div className="flex items-center justify-between px-4 py-2 border-b border-surface-border bg-surface-card">
              {/* View mode toggle */}
              <div className="flex rounded-md overflow-hidden border border-surface-border text-xs">
                <button
                  onClick={() => setViewMode('graph')}
                  className={`flex items-center gap-1.5 px-3 py-1 transition-colors ${
                    viewMode === 'graph'
                      ? 'bg-violet-900/60 text-violet-300'
                      : 'text-zinc-500 hover:text-zinc-300'
                  }`}
                >
                  <Share2 size={11} /> Graph
                </button>
                <button
                  onClick={() => setViewMode('table')}
                  className={`flex items-center gap-1.5 px-3 py-1 transition-colors ${
                    viewMode === 'table'
                      ? 'bg-blue-900/40 text-blue-300'
                      : 'text-zinc-500 hover:text-zinc-300'
                  }`}
                >
                  <Table2 size={11} /> Table
                </button>
              </div>
              <button
                onClick={executeQuery}
                disabled={loading}
                className="btn-primary text-xs flex items-center gap-1.5 py-1"
              >
                <Play size={11} /> {loading ? 'Running…' : 'Run'}
              </button>
            </div>
            <textarea
              value={query}
              onChange={e => setQuery(e.target.value)}
              onKeyDown={e => { if (e.ctrlKey && e.key === 'Enter') executeQuery() }}
              rows={4}
              className="w-full bg-[#0c0c0e] px-4 py-3 font-mono text-sm text-zinc-200
                         focus:outline-none resize-none"
              placeholder="MATCH (n) RETURN n LIMIT 10"
              spellCheck={false}
            />
          </div>

          {/* ── Graph view ── */}
          {results?.type === 'graph' && (
            <div className="space-y-2">
              <div className="flex items-center justify-between">
                <span className="text-xs text-zinc-500">
                  {results.nodeCount} node{results.nodeCount !== 1 ? 's' : ''} · {results.edgeCount} edge{results.edgeCount !== 1 ? 's' : ''}
                  {selectedNode && <span className="ml-2 text-violet-400">· click a node to inspect</span>}
                </span>
                {/* Legend */}
                <div className="flex gap-3 text-[10px]">
                  {Object.entries(TYPE_STYLE).slice(0,4).map(([type, s]) => (
                    <span key={type} className="flex items-center gap-1">
                      <span className="w-2.5 h-2.5 rounded border" style={{ borderColor: s.border }} />
                      <span className="text-zinc-500">{type}</span>
                    </span>
                  ))}
                </div>
              </div>

              <div className="flex gap-3">
                {/* ReactFlow canvas */}
                <div className={`flex-1 rounded-lg border border-surface-border overflow-hidden transition-all ${selectedNode ? 'min-h-[460px]' : 'min-h-[520px]'}`}>
                  <ReactFlow
                    nodes={rfNodes}
                    edges={rfEdges}
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
                      nodeColor={n => {
                        const s = TYPE_STYLE[n.data?.raw?.type || 'Node'] || TYPE_STYLE.Node
                        return s.border
                      }}
                      style={{ background: '#18181b', border: '1px solid #27272a' }}
                    />
                  </ReactFlow>
                </div>

                {/* Info panel */}
                {selectedNode && (
                  <div className="w-64 rounded-lg border border-surface-border bg-surface-card p-4 text-xs space-y-3 shrink-0">
                    <div className="flex items-center justify-between">
                      <span className="flex items-center gap-1.5 text-zinc-300 font-medium">
                        <Info size={12} /> Node Details
                      </span>
                      <button onClick={() => setSelectedNode(null)} className="text-zinc-600 hover:text-zinc-300">
                        <X size={13} />
                      </button>
                    </div>
                    <div className="space-y-1.5">
                      <div className="text-zinc-500">ID</div>
                      <div className="font-mono text-zinc-300 break-all">{selectedNode.id}</div>
                    </div>
                    {selectedNode.data?.raw && Object.entries(selectedNode.data.raw).map(([k, v]) => (
                      <div key={k} className="space-y-0.5">
                        <div className="text-zinc-500">{k}</div>
                        <div className="font-mono text-zinc-300 break-all">
                          {Array.isArray(v) ? v.join(', ') || '—' : String(v ?? '—')}
                        </div>
                      </div>
                    ))}
                  </div>
                )}
              </div>
            </div>
          )}

          {/* ── Table view ── */}
          {results?.type === 'table' && (
            <div className="card p-0 overflow-hidden">
              <div className="px-4 py-2 border-b border-surface-border flex items-center justify-between">
                <span className="text-xs text-zinc-500">{results.count} row{results.count !== 1 ? 's' : ''}</span>
              </div>
              {results.count === 0 ? (
                <p className="px-4 py-6 text-sm text-zinc-600 text-center">No rows returned.</p>
              ) : (
                <div className="overflow-x-auto">
                  <table className="w-full text-xs">
                    <thead>
                      <tr className="border-b border-surface-border bg-surface-card">
                        {Object.keys(results.rows[0]).map(k => (
                          <th key={k} className="text-left px-4 py-2 font-medium text-zinc-500 font-mono">{k}</th>
                        ))}
                      </tr>
                    </thead>
                    <tbody className="divide-y divide-surface-border">
                      {results.rows.map((row, i) => (
                        <tr key={i} className="hover:bg-surface-hover/50 transition-colors">
                          {Object.values(row).map((v, j) => (
                            <td key={j} className="px-4 py-2 text-zinc-300 font-mono">
                              <CellValue v={v} />
                            </td>
                          ))}
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              )}
            </div>
          )}
        </div>
      )}

      {tab === 'jobs' && (
        <div className="space-y-3">
          <div className="flex gap-2">
            <input
              value={search}
              onChange={e => setSearch(e.target.value)}
              onKeyDown={e => e.key === 'Enter' && loadJobs()}
              placeholder="Search job name…"
              className="flex-1 bg-surface border border-surface-border rounded-md px-3 py-2
                         text-sm text-zinc-200 focus:outline-none focus:border-accent"
            />
            <button onClick={loadJobs} className="btn-primary flex items-center gap-1.5">
              <Search size={13} /> Search
            </button>
          </div>
          <JobTable data={listData} />
        </div>
      )}

      {tab === 'datasets' && (
        <div className="space-y-3">
          <button onClick={loadDatasets} className="btn-primary flex items-center gap-1.5">
            <Search size={13} /> Reload Datasets
          </button>
          <DatasetTable data={listData} />
        </div>
      )}
    </div>
  )
}

function JobTable({ data }) {
  if (!data.length) return <p className="text-sm text-zinc-600 py-4 text-center">No jobs found. Run Phase 1 pipeline first.</p>
  return (
    <div className="card p-0 overflow-hidden overflow-x-auto">
      <table className="w-full text-xs">
        <thead>
          <tr className="border-b border-surface-border bg-surface-card">
            <th className="text-left px-4 py-2 text-zinc-500 font-normal">Name</th>
            <th className="text-left px-4 py-2 text-zinc-500 font-normal">Path</th>
            <th className="text-left px-4 py-2 text-zinc-500 font-normal">Lines</th>
            <th className="text-left px-4 py-2 text-zinc-500 font-normal">Risk Tags</th>
          </tr>
        </thead>
        <tbody className="divide-y divide-surface-border">
          {data.map((j, i) => (
            <tr key={i} className="hover:bg-surface-hover/50">
              <td className="px-4 py-2 font-mono text-violet-300">{j.name}</td>
              <td className="px-4 py-2 text-zinc-500 font-mono">{j.path}</td>
              <td className="px-4 py-2 text-zinc-400">{j.line_start}–{j.line_end}</td>
              <td className="px-4 py-2">
                <div className="flex flex-wrap gap-1">
                  {(j.risk_tags || []).map(t => (
                    <span key={t} className={t === 'PII' ? 'badge-error' : 'badge-warn'}>{t}</span>
                  ))}
                </div>
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  )
}

function DatasetTable({ data }) {
  if (!data.length) return <p className="text-sm text-zinc-600 py-4 text-center">No datasets found. Run Phase 1 pipeline first.</p>
  return (
    <div className="card p-0 overflow-hidden overflow-x-auto">
      <table className="w-full text-xs">
        <thead>
          <tr className="border-b border-surface-border bg-surface-card">
            {['Name', 'Qualified Name', 'Format', 'Status'].map(h => (
              <th key={h} className="text-left px-4 py-2 text-zinc-500 font-normal">{h}</th>
            ))}
          </tr>
        </thead>
        <tbody className="divide-y divide-surface-border">
          {data.map((d, i) => (
            <tr key={i} className="hover:bg-surface-hover/50">
              <td className="px-4 py-2 font-mono text-blue-300">{d.name}</td>
              <td className="px-4 py-2 font-mono text-zinc-400">{d.qualified_name || '—'}</td>
              <td className="px-4 py-2 text-zinc-500">{d.format || '—'}</td>
              <td className="px-4 py-2">
                <span className={d.status === 'deprecated' ? 'badge-error' : 'badge-success'}>{d.status || 'active'}</span>
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  )
}
