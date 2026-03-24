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
import { GitBranch, Loader2, X, Info } from 'lucide-react'
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
  const [selectedId,  setSelectedId]  = useState(null)
  const [focusInfo,   setFocusInfo]   = useState(null)   // { name, path, callers[], callees[] }

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

  // Highlight selected node + its direct neighbours; populate info panel
  const applySelection = useCallback((selId, bNodes, bEdges) => {
    if (!selId) {
      // Just re-apply filter
      applyFilter(filter, bNodes, bEdges)
      setFocusInfo(null)
      return
    }
    const callerEdges  = bEdges.filter(e => e._callerId === selId)
    const calleeEdges  = bEdges.filter(e => e._calleeId === selId)
    const neighbourIds = new Set([
      selId,
      ...callerEdges.map(e => e._calleeId),
      ...calleeEdges.map(e => e._callerId),
    ])

    setNodes(bNodes.map(n => ({
      ...n,
      style: {
        ...n.style,
        opacity: neighbourIds.has(n.id) ? 1 : 0.08,
        outline: n.id === selId ? '2px solid #a78bfa' : 'none',
      },
    })))
    setEdges(bEdges.map(e => {
      const active = e._callerId === selId || e._calleeId === selId
      return {
        ...e,
        style: { ...e.style, opacity: active ? 1 : 0.05, strokeWidth: active ? 2.5 : 1.5 },
      }
    }))

    // Build focus info
    const node = bNodes.find(n => n.id === selId)
    setFocusInfo({
      name:    node?._name   || selId,
      path:    node?._path   || '',
      callees: callerEdges.map(e => {
        const t = bNodes.find(n => n.id === e._calleeId)
        return t?._name || e._calleeId
      }),
      callers: calleeEdges.map(e => {
        const s = bNodes.find(n => n.id === e._callerId)
        return s?._name || e._callerId
      }),
    })
  }, [filter, applyFilter, setNodes, setEdges])

  useEffect(() => {
    if (baseNodes.length) {
      if (selectedId) applySelection(selectedId, baseNodes, baseEdges)
      else applyFilter(filter, baseNodes, baseEdges)
    }
  }, [filter, baseNodes, baseEdges, applyFilter, applySelection, selectedId])

  const handleNodeClick = useCallback((_ev, node) => {
    const newId = node.id === selectedId ? null : node.id
    setSelectedId(newId)
    applySelection(newId, baseNodes, baseEdges)
  }, [selectedId, baseNodes, baseEdges, applySelection])

  const handlePaneClick = useCallback(() => {
    setSelectedId(null)
    applyFilter(filter, baseNodes, baseEdges)
    setFocusInfo(null)
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

      {/* Filter + clear selection row */}
      <div className="flex flex-wrap gap-3 items-center">
        <input
          value={filter}
          onChange={e => setFilter(e.target.value)}
          placeholder="Highlight functions by name…"
          className="bg-surface border border-surface-border rounded-md px-3 py-1.5
                     text-xs text-zinc-200 focus:outline-none focus:border-accent w-56"
        />
        {filter && (
          <button onClick={() => setFilter('')}
            className="text-xs text-zinc-500 hover:text-zinc-200">✕ clear filter</button>
        )}
        {selectedId && (
          <button
            onClick={() => { setSelectedId(null); applyFilter(filter, baseNodes, baseEdges); setFocusInfo(null) }}
            className="flex items-center gap-1 text-xs text-violet-400 hover:text-violet-200 border border-violet-800 rounded px-2 py-1"
          >
            <X size={11} /> Clear selection
          </button>
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
          onNodeClick={handleNodeClick}
          onPaneClick={handlePaneClick}
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

      {/* Info panel — shown when a node is selected */}
      {focusInfo && (
        <div className="rounded-lg border border-violet-800/60 bg-violet-950/30 p-4 text-xs space-y-3">
          <div className="flex items-center justify-between">
            <span className="flex items-center gap-1.5 text-violet-300 font-medium">
              <Info size={13} /> {focusInfo.name}
            </span>
            <button
              onClick={() => { setSelectedId(null); applyFilter(filter, baseNodes, baseEdges); setFocusInfo(null) }}
              className="text-zinc-600 hover:text-zinc-300"
            >
              <X size={13} />
            </button>
          </div>

          {focusInfo.path && (
            <div>
              <p className="text-zinc-500 mb-0.5">Script path</p>
              <p className="font-mono text-zinc-300 break-all">{focusInfo.path}</p>
            </div>
          )}

          <div className="grid grid-cols-2 gap-4">
            <div>
              <p className="text-zinc-500 mb-1">Called by ({focusInfo.callers.length})</p>
              {focusInfo.callers.length === 0
                ? <p className="text-zinc-600 italic">— root (no callers)</p>
                : focusInfo.callers.map(c => (
                    <div key={c} className="font-mono text-violet-300 hover:text-violet-100 cursor-pointer
                                            py-0.5 px-1 rounded hover:bg-violet-900/30 transition-colors"
                         onClick={() => {
                           const n = baseNodes.find(n => n._name === c)
                           if (n) { setSelectedId(n.id); applySelection(n.id, baseNodes, baseEdges) }
                         }}>
                      ↑ {c}
                    </div>
                  ))
              }
            </div>
            <div>
              <p className="text-zinc-500 mb-1">Calls into ({focusInfo.callees.length})</p>
              {focusInfo.callees.length === 0
                ? <p className="text-zinc-600 italic">— leaf (calls nothing)</p>
                : focusInfo.callees.map(c => (
                    <div key={c} className="font-mono text-blue-300 hover:text-blue-100 cursor-pointer
                                            py-0.5 px-1 rounded hover:bg-blue-900/30 transition-colors"
                         onClick={() => {
                           const n = baseNodes.find(n => n._name === c)
                           if (n) { setSelectedId(n.id); applySelection(n.id, baseNodes, baseEdges) }
                         }}>
                      ↓ {c}
                    </div>
                  ))
              }
            </div>
          </div>
        </div>
      )}
    </div>
  )
}

