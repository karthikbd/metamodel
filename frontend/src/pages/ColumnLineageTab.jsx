import { useState, useEffect } from 'react'
import ReactFlow, {
  Background,
  Controls,
  MiniMap,
  useNodesState,
  useEdgesState,
  MarkerType,
} from 'reactflow'
import 'reactflow/dist/style.css'
import { Database, ArrowRight, Lock, Loader2, X } from 'lucide-react'
import { fetchAllColumnLineage, fetchColumnLineageGraph } from '../services/api'
import toast from 'react-hot-toast'

// ─── Layout constants ─────────────────────────────────────────────────────────
const COL_UPSTREAM_X   = 60
const COL_FOCAL_X      = 480
const COL_DOWNSTREAM_X = 900
const COL_ROW_H        = 78
const MONO             = 'JetBrains Mono, monospace'

// ─── Helpers ──────────────────────────────────────────────────────────────────

function classifyNodes(focalId, rawEdges) {
  const upstreamSet   = new Set()
  const downstreamSet = new Set()

  const q1 = [focalId]
  const v1 = new Set([focalId])
  while (q1.length) {
    const curr = q1.shift()
    rawEdges.forEach(e => {
      if (e.src === curr && !v1.has(e.tgt)) {
        upstreamSet.add(e.tgt); v1.add(e.tgt); q1.push(e.tgt)
      }
    })
  }

  const q2 = [focalId]
  const v2 = new Set([focalId])
  while (q2.length) {
    const curr = q2.shift()
    rawEdges.forEach(e => {
      if (e.tgt === curr && !v2.has(e.src)) {
        downstreamSet.add(e.src); v2.add(e.src); q2.push(e.src)
      }
    })
  }

  return { upstreamSet, downstreamSet }
}

function buildColumnGraph(data) {
  const rawNodes = data.nodes || []
  const rawEdges = data.edges || []
  const focus    = data.focus || {}

  const focalId = rawNodes.find(
    n => n.dataset === focus.dataset && n.name === focus.column
  )?.id

  const upstreamSet   = new Set()
  const downstreamSet = new Set()

  if (focalId) {
    const cls = classifyNodes(focalId, rawEdges)
    cls.upstreamSet.forEach(id => upstreamSet.add(id))
    cls.downstreamSet.forEach(id => downstreamSet.add(id))
  }

  const upArr  = rawNodes.filter(n => upstreamSet.has(n.id))
  const dnArr  = rawNodes.filter(n => downstreamSet.has(n.id))
  const others = rawNodes.filter(
    n => n.id !== focalId && !upstreamSet.has(n.id) && !downstreamSet.has(n.id)
  )

  const maxRows = Math.max(upArr.length, dnArr.length, 1)
  const centerY = (maxRows * COL_ROW_H) / 2
  const rfNodes = []

  upArr.forEach((n, i) => {
    const pii = n.pii
    rfNodes.push({
      id: n.id,
      data: { label: `${n.dataset}.${n.name}${pii ? ' \uD83D\uDD12' : ''}` },
      position: { x: COL_UPSTREAM_X, y: i * COL_ROW_H },
      style: {
        background: '#0b1e0f', border: `1px solid ${pii ? '#ef4444' : '#22c55e'}`,
        color: '#86efac', borderRadius: 6, fontSize: 11,
        fontFamily: MONO, padding: '5px 10px', maxWidth: 180, wordBreak: 'break-all',
      },
    })
  })

  if (focalId) {
    const fn = rawNodes.find(n => n.id === focalId)
    if (fn) {
      const pii = fn.pii
      rfNodes.push({
        id: fn.id,
        data: { label: `${fn.dataset}.${fn.name}${pii ? ' \uD83D\uDD12' : ''}` },
        position: { x: COL_FOCAL_X, y: centerY - COL_ROW_H / 2 },
        style: {
          background: '#1a1500', border: `2px solid ${pii ? '#ef4444' : '#eab308'}`,
          color: '#fde047', borderRadius: 6, fontSize: 12,
          fontFamily: MONO, padding: '8px 14px',
          fontWeight: 600, maxWidth: 200, wordBreak: 'break-all',
        },
      })
    }
  }

  dnArr.forEach((n, i) => {
    const pii = n.pii
    rfNodes.push({
      id: n.id,
      data: { label: `${n.dataset}.${n.name}${pii ? ' \uD83D\uDD12' : ''}` },
      position: { x: COL_DOWNSTREAM_X, y: i * COL_ROW_H },
      style: {
        background: '#1e0b0b', border: `1px solid ${pii ? '#ef4444' : '#f97316'}`,
        color: '#fdba74', borderRadius: 6, fontSize: 11,
        fontFamily: MONO, padding: '5px 10px', maxWidth: 180, wordBreak: 'break-all',
      },
    })
  })

  others.forEach((n, i) => {
    rfNodes.push({
      id: n.id,
      data: { label: `${n.dataset}.${n.name}` },
      position: { x: COL_FOCAL_X, y: centerY + (i + 1) * COL_ROW_H + 80 },
      style: {
        background: '#18181b', border: '1px solid #52525b', color: '#a1a1aa',
        borderRadius: 6, fontSize: 11, fontFamily: MONO, padding: '5px 10px',
      },
    })
  })

  const rfEdges = rawEdges.map((e, i) => ({
    id: `ce-${i}`,
    source: e.tgt,
    target: e.src,
    label: e.expression ? e.expression.slice(0, 28) : 'DERIVED_FROM',
    labelStyle: { fill: '#f472b6', fontSize: 8, fontFamily: MONO },
    style: { stroke: '#f472b6', strokeWidth: 1.5, strokeDasharray: '4 2' },
    markerEnd: { type: MarkerType.ArrowClosed, color: '#f472b6' },
  }))

  return {
    nodes: rfNodes,
    edges: rfEdges,
    stats: { upstream: upArr.length, downstream: dnArr.length, edges: rawEdges.length },
  }
}

// ─── Component ────────────────────────────────────────────────────────────────

export default function ColumnLineageTab() {
  const [chains,       setChains]       = useState([])
  const [loading,      setLoading]      = useState(false)
  const [focusInfo,    setFocusInfo]    = useState(null)
  const [graphStats,   setGraphStats]   = useState(null)
  const [graphLoading, setGraphLoading] = useState(false)
  const [nodes, setNodes, onNodesChange] = useNodesState([])
  const [edges, setEdges, onEdgesChange] = useEdgesState([])

  useEffect(() => {
    setLoading(true)
    fetchAllColumnLineage()
      .then(data => setChains(data.chains || []))
      .catch(() => toast.error('Could not load column lineage'))
      .finally(() => setLoading(false))
  }, [])

  async function openGraph(dataset, column) {
    setFocusInfo({ dataset, column })
    setGraphLoading(true)
    try {
      const data = await fetchColumnLineageGraph(dataset, column)
      const { nodes: n, edges: e, stats: s } = buildColumnGraph(data)
      setNodes(n)
      setEdges(e)
      setGraphStats(s)
    } catch {
      toast.error('Failed to load column graph')
    } finally {
      setGraphLoading(false)
    }
  }

  function closeGraph() {
    setFocusInfo(null)
    setGraphStats(null)
    setNodes([])
    setEdges([])
  }

  const piiCount = chains.filter(c => c.src_pii || c.tgt_pii).length

  return (
    <div className="space-y-4 flex flex-col flex-1">

      {/* Overview table */}
      <div className="bg-surface-card rounded-lg border border-surface-border overflow-hidden">
        <div className="px-4 py-3 border-b border-surface-border flex items-center justify-between">
          <div className="flex items-center gap-3">
            <Database size={15} className="text-pink-400" />
            <span className="text-sm font-semibold text-zinc-300">
              All Column Lineage Chains
              {chains.length > 0 && (
                <span className="ml-2 text-xs text-zinc-500 font-normal">
                  ({chains.length} DERIVED_FROM relationships{piiCount > 0 ? `, ${piiCount} PII-flagged` : ''})
                </span>
              )}
            </span>
          </div>
          <span className="text-xs text-zinc-600">Click a row to view the full graph</span>
        </div>

        {loading ? (
          <div className="flex items-center justify-center py-16 text-zinc-500">
            <Loader2 size={20} className="animate-spin mr-2" /> Loading lineage...
          </div>
        ) : chains.length === 0 ? (
          <div className="py-16 text-center text-zinc-600">
            <Database size={32} className="mx-auto mb-3 opacity-20" />
            <p className="text-sm">No DERIVED_FROM relationships found.</p>
            <p className="text-xs mt-1 text-zinc-700">
              Run Phase 1 hydration first to extract column derivation metadata.
            </p>
          </div>
        ) : (
          <div className="overflow-x-auto">
            <table className="w-full text-sm">
              <thead>
                <tr className="border-b border-zinc-800 bg-zinc-900/50">
                  <th className="text-left px-3 py-2.5 text-xs font-medium text-zinc-500 uppercase tracking-wide">
                    Source Column
                  </th>
                  <th className="px-3 py-2.5" />
                  <th className="text-left px-3 py-2.5 text-xs font-medium text-zinc-500 uppercase tracking-wide">
                    Expression
                  </th>
                  <th className="px-3 py-2.5" />
                  <th className="text-left px-3 py-2.5 text-xs font-medium text-zinc-500 uppercase tracking-wide">
                    Derived Into
                  </th>
                </tr>
              </thead>
              <tbody>
                {chains.map((c, i) => (
                  <tr
                    key={i}
                    onClick={() => openGraph(c.src_dataset, c.src_column)}
                    className="border-b border-zinc-800 hover:bg-zinc-800/40 cursor-pointer transition-colors"
                  >
                    <td className="py-2.5 px-3">
                      <div className="flex items-center gap-1.5">
                        {c.src_pii && <Lock size={10} className="text-red-400 flex-shrink-0" />}
                        <span className="font-mono text-xs text-green-300">
                          {c.src_dataset}.{c.src_column}
                        </span>
                      </div>
                    </td>
                    <td className="py-2.5 px-2">
                      <ArrowRight size={13} className="text-zinc-600" />
                    </td>
                    <td className="py-2.5 px-3">
                      {c.expression ? (
                        <code className="text-xs text-amber-300 font-mono bg-zinc-900 px-2 py-0.5 rounded">
                          {c.expression.slice(0, 40)}
                        </code>
                      ) : (
                        <span className="text-xs text-zinc-600 italic">passthrough</span>
                      )}
                    </td>
                    <td className="py-2.5 px-2">
                      <ArrowRight size={13} className="text-zinc-600" />
                    </td>
                    <td className="py-2.5 px-3">
                      <div className="flex items-center gap-1.5">
                        {c.tgt_pii && <Lock size={10} className="text-red-400 flex-shrink-0" />}
                        <span className="font-mono text-xs text-orange-300">
                          {c.tgt_dataset}.{c.tgt_column}
                        </span>
                      </div>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </div>

      {/* Focus Graph — shown when a row is clicked */}
      {focusInfo && (
        <div className="flex flex-col gap-3 flex-1">
          <div className="flex items-center justify-between">
            <div className="flex items-center gap-2 text-sm">
              <span className="text-zinc-500">Graph for</span>
              <span className="font-mono text-yellow-300 bg-yellow-900/20 border border-yellow-800/50 px-2 py-0.5 rounded text-xs">
                {focusInfo.dataset}.{focusInfo.column}
              </span>
            </div>
            <div className="flex items-center gap-3">
              {graphStats && (
                <div className="flex gap-3 text-xs">
                  <span className="text-green-400">{graphStats.upstream} upstream</span>
                  <span className="text-orange-400">{graphStats.downstream} downstream</span>
                  <span className="text-pink-400">{graphStats.edges} edges</span>
                </div>
              )}
              <button
                onClick={closeGraph}
                className="p-1.5 rounded-md bg-zinc-800 hover:bg-zinc-700 text-zinc-400 border border-zinc-700"
              >
                <X size={13} />
              </button>
            </div>
          </div>

          <div className="flex flex-wrap gap-4 text-xs">
            {[
              { label: 'Upstream source',    color: '#22c55e' },
              { label: 'Focal column',       color: '#eab308' },
              { label: 'Downstream derived', color: '#f97316' },
              { label: 'PII flagged',        color: '#ef4444' },
            ].map(({ label, color }) => (
              <span key={label} className="flex items-center gap-1.5">
                <span className="w-3 h-3 rounded-sm border" style={{ borderColor: color }} />
                <span className="text-zinc-500">{label}</span>
              </span>
            ))}
          </div>

          <div className="flex-1 min-h-96 rounded-lg border border-surface-border overflow-hidden">
            {graphLoading ? (
              <div className="h-full flex items-center justify-center text-zinc-500">
                <Loader2 size={20} className="animate-spin mr-2" /> Building graph...
              </div>
            ) : nodes.length === 0 ? (
              <div className="h-full flex items-center justify-center text-zinc-600 text-sm">
                No upstream/downstream nodes found for this column.
              </div>
            ) : (
              <ReactFlow
                nodes={nodes}
                edges={edges}
                onNodesChange={onNodesChange}
                onEdgesChange={onEdgesChange}
                fitView
                style={{ background: '#0c0c0e' }}
              >
                <Background color="#27272a" />
                <Controls />
                <MiniMap
                  nodeColor={n => {
                    const b = n.style?.border || ''
                    if (b.includes('#eab308')) return '#eab308'
                    if (b.includes('#22c55e')) return '#22c55e'
                    if (b.includes('#f97316')) return '#f97316'
                    if (b.includes('#ef4444')) return '#ef4444'
                    return '#52525b'
                  }}
                  style={{ background: '#18181b', border: '1px solid #27272a' }}
                />
              </ReactFlow>
            )}
          </div>
        </div>
      )}
    </div>
  )
}
