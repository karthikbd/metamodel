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
import { GitBranch, Loader2 } from 'lucide-react'
import { fetchAllFunctionalLineage } from '../services/api'
import toast from 'react-hot-toast'

// â”€â”€â”€ BFS topological layout (roots left â†’ leaves right) â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

function buildFunctionalGraph(chains) {
  // Collect unique nodes
  const jobMap = {}
  chains.forEach(c => {
    jobMap[c.caller_id] = { id: c.caller_id, name: c.caller_name, path: c.caller_path || '' }
    jobMap[c.callee_id] = { id: c.callee_id, name: c.callee_name, path: c.callee_path || '' }
  })

  // Build adjacency and in-degree
  const callers = new Set(chains.map(c => c.caller_id))
  const callees = new Set(chains.map(c => c.callee_id))
  const roots   = [...callers].filter(id => !callees.has(id))

  // BFS to assign levels (x = level * 280)
  const levelMap = {}
  const visited  = new Set()
  const queue    = roots.map(id => ({ id, level: 0 }))
  if (queue.length === 0) {
    // No obvious roots â€” just assign everyone level 0
    Object.keys(jobMap).forEach(id => { levelMap[id] = 0 })
  }
  while (queue.length) {
    const { id, level } = queue.shift()
    if (visited.has(id)) continue
    visited.add(id)
    levelMap[id] = Math.max(levelMap[id] ?? 0, level)
    chains
      .filter(c => c.caller_id === id)
      .forEach(c => queue.push({ id: c.callee_id, level: level + 1 }))
  }
  // Any remaining unvisited nodes
  Object.keys(jobMap).forEach(id => { if (!(id in levelMap)) levelMap[id] = 0 })

  // Group by level
  const byLevel = {}
  Object.entries(levelMap).forEach(([id, lvl]) => {
    byLevel[lvl] = byLevel[lvl] || []
    byLevel[lvl].push(id)
  })

  const X_GAP = 280
  const Y_GAP = 110
  const MONO  = 'JetBrains Mono, monospace'

  const rfNodes = Object.entries(byLevel).flatMap(([lvl, ids]) =>
    ids.map((id, i) => ({
      id, _name: jobMap[id].name, _path: jobMap[id].path,
      data: { label: jobMap[id].name || id.slice(0, 14) },
      position: { x: Number(lvl) * X_GAP + 40, y: i * Y_GAP + 40 },
      style: {
        background: callers.has(id) && callees.has(id) ? '#1a1030'
                  : callers.has(id)                    ? '#1e1529'
                  :                                      '#0f1228',
        border: `1px solid ${
          callers.has(id) && callees.has(id) ? '#9333ea'
        : callers.has(id)                    ? '#7c3aed'
        :                                      '#3b82f6'
        }`,
        color: callers.has(id) ? '#c4b5fd' : '#93c5fd',
        borderRadius: 6, fontSize: 11, fontFamily: MONO,
        padding: '6px 12px', minWidth: 120,
      },
    }))
  )

  const rfEdges = chains.map((c, i) => ({
    id: `fe-${i}`, source: c.caller_id, target: c.callee_id,
    _callerId: c.caller_id, _calleeId: c.callee_id,
    style: { stroke: '#a78bfa', strokeWidth: 1.5 },
    markerEnd: { type: MarkerType.ArrowClosed, color: '#a78bfa' },
  }))

  return { rfNodes, rfEdges }
}

// â”€â”€â”€ Component â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

export default function FunctionalLineageTab() {
  const [loading,     setLoading]     = useState(true)
  const [chains,      setChains]      = useState([])
  const [filter,      setFilter]      = useState('')
  const [baseNodes,   setBaseNodes]   = useState([])
  const [baseEdges,   setBaseEdges]   = useState([])
  const [nodes, setNodes, onNodesChange] = useNodesState([])
  const [edges, setEdges, onEdgesChange] = useEdgesState([])

  useEffect(() => {
    fetchAllFunctionalLineage()
      .then(data => {
        const ch = data.chains || []
        setChains(ch)
        if (ch.length) {
          const { rfNodes, rfEdges } = buildFunctionalGraph(ch)
          setBaseNodes(rfNodes)
          setBaseEdges(rfEdges)
          setNodes(rfNodes)
          setEdges(rfEdges)
        }
      })
      .catch(() => toast.error('Failed to load functional lineage'))
      .finally(() => setLoading(false))
  }, [])

  const applyFilter = useCallback((text, bNodes, bEdges) => {
    const lf = text.trim().toLowerCase()
    const matchedIds = new Set(
      lf ? bNodes.filter(n => n._name.toLowerCase().includes(lf)).map(n => n.id)
         : bNodes.map(n => n.id)
    )
    setNodes(bNodes.map(n => ({
      ...n,
      style: { ...n.style, opacity: matchedIds.has(n.id) ? 1 : 0.1 },
    })))
    setEdges(bEdges.map(e => ({
      ...e,
      style: {
        ...e.style,
        opacity: (!lf || (matchedIds.has(e._callerId) && matchedIds.has(e._calleeId))) ? 1 : 0.05,
      },
    })))
  }, [setNodes, setEdges])

  useEffect(() => {
    if (baseNodes.length) applyFilter(filter, baseNodes, baseEdges)
  }, [filter, baseNodes, baseEdges, applyFilter])

  // Stats
  const uniqueCallers = new Set(chains.map(c => c.caller_id)).size
  const uniqueCallees = new Set(chains.map(c => c.callee_id)).size
  const uniqueScripts = new Set([...chains.map(c => c.caller_path), ...chains.map(c => c.callee_path)]).size

  if (loading) {
    return (
      <div className="flex items-center justify-center py-24 text-zinc-500">
        <Loader2 size={28} className="animate-spin mr-3" />
        Loading functional lineageâ€¦
      </div>
    )
  }

  if (chains.length === 0) {
    return (
      <div className="text-center py-24 text-zinc-600">
        <GitBranch size={40} className="mx-auto mb-4 opacity-20" />
        <p className="text-base mb-2">No DEPENDS_ON relationships found.</p>
        <p className="text-xs">Run Phase 1 hydration to build the function call graph.</p>
      </div>
    )
  }

  return (
    <div className="space-y-3 flex flex-col flex-1">

      {/* Stats */}
      <div className="grid grid-cols-2 gap-3 sm:grid-cols-4">
        {[
          { label: 'Calling Functions', value: uniqueCallers, color: 'text-violet-400' },
          { label: 'DEPENDS_ON Edges',  value: chains.length, color: 'text-blue-400'   },
          { label: 'Called Functions',  value: uniqueCallees, color: 'text-emerald-400' },
          { label: 'Scripts Involved',  value: uniqueScripts, color: 'text-amber-400'  },
        ].map(s => (
          <div key={s.label} className="bg-zinc-800/60 rounded-lg p-3 border border-zinc-700">
            <p className={`text-xl font-bold ${s.color}`}>{s.value}</p>
            <p className="text-[10px] text-zinc-500 mt-0.5">{s.label}</p>
          </div>
        ))}
      </div>

      {/* Filter + legend row */}
      <div className="flex flex-wrap gap-3 items-center">
        <input
          value={filter}
          onChange={e => setFilter(e.target.value)}
          placeholder="Highlight functions by nameâ€¦"
          className="bg-surface border border-surface-border rounded-md px-3 py-1.5
                     text-xs text-zinc-200 focus:outline-none focus:border-accent w-56"
        />
        {filter && (
          <button onClick={() => setFilter('')}
            className="text-xs text-zinc-500 hover:text-zinc-200">âœ• clear</button>
        )}
        <div className="flex gap-4 ml-auto text-[10px]">
          {[
            { label: 'Root caller (no parent)',     color: '#7c3aed' },
            { label: 'Caller + callee (both roles)', color: '#9333ea' },
            { label: 'Leaf callee (not a caller)',  color: '#3b82f6' },
          ].map(({ label, color }) => (
            <span key={label} className="flex items-center gap-1.5">
              <span className="w-2.5 h-2.5 rounded border" style={{ borderColor: color }} />
              <span className="text-zinc-500">{label}</span>
            </span>
          ))}
        </div>
      </div>

      {/* Full DEPENDS_ON graph */}
      <div className="flex-1 min-h-[500px] rounded-lg border border-surface-border overflow-hidden">
        <ReactFlow
          nodes={nodes}
          edges={edges}
          onNodesChange={onNodesChange}
          onEdgesChange={onEdgesChange}
          fitView
          fitViewOptions={{ padding: 0.1 }}
          style={{ background: '#0c0c0e' }}
        >
          <Background color="#27272a" />
          <Controls />
          <MiniMap
            nodeColor={n => n.style?.border?.match(/#[0-9a-f]{6}/i)?.[0] || '#52525b'}
            style={{ background: '#18181b', border: '1px solid #27272a' }}
          />
        </ReactFlow>
      </div>
    </div>
  )
}

