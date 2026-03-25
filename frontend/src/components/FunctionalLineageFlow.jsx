/**
 * FunctionalLineageFlow.jsx
 *
 * Informatica-style functional / job call-graph diagram.
 * Works fully offline from mock data (JOBS + JOB_EDGES DEPENDS_ON).
 *
 * Layout:
 *   • Jobs are placed in horizontal tiers by BFS depth in the DEPENDS_ON graph
 *   • Tier 0 = root callers (not called by anyone)
 *   • Cards styled by domain: ETL / Risk / Compliance / Reporting
 *   • DEPENDS_ON arrows flow left → right
 *   • READS_FROM / WRITES_TO shown as dashed ambient edges (toggleable)
 *   • Click a job → highlight full upstream + downstream dependency chain
 *
 * Domain filter strip matches InformaticaLineage.jsx style.
 */

import { useState, useMemo, useCallback, useEffect } from 'react'
import ReactFlow, {
  Background, BackgroundVariant, Controls, MiniMap, Panel,
  Handle, Position, MarkerType,
  useNodesState, useEdgesState,
} from 'reactflow'
import 'reactflow/dist/style.css'
import { JOBS, JOB_EDGES } from '../data/mockGraph'

// ── Layout constants ──────────────────────────────────────────────────────

const CARD_W = 210, CARD_H = 74, TIER_X = 290, TIER_Y = 94

const DOM = { ETL:{ border:'#3b82f6',bg:'#0d1b2e',text:'#93c5fd',badge:'#1e3a5f' }, Risk:{ border:'#ef4444',bg:'#1f0e0e',text:'#fca5a5',badge:'#4c1414' }, Compliance:{ border:'#22c55e',bg:'#0a1e10',text:'#86efac',badge:'#14402a' }, Reporting:{ border:'#f59e0b',bg:'#1f1505',text:'#fcd34d',badge:'#4a300a' } }
function domCol(domain) {
  for (const [k, v] of Object.entries(DOM)) { if (domain?.includes(k)) return v }
  return { border:'#52525b', bg:'#111116', text:'#a1a1aa', badge:'#27272a' }
}

// ── JobNode style helpers (extracted to keep CCN < 8) ─────────────────────

function jobBorder(focal, hl, fallback) {
  if (focal)         return '#f59e0b'
  if (hl === 'up')   return '#3b82f6'
  if (hl === 'down') return '#ef4444'
  return fallback
}
function jobBg(focal, hl, fallback) {
  if (focal)         return '#2a1f00'
  if (hl === 'up')   return '#0e1e28'
  if (hl === 'down') return '#1e0e0e'
  return fallback
}
function jobColor(focal, hl, fallback) {
  if (focal) return '#fde047'
  if (hl)    return '#e4e4e7'
  return fallback
}

// ── Build DEPENDS_ON graph ────────────────────────────────────────────────

// ── Build job graph data (all module-level setup in one place) ─────────────────────

function buildJobData() {
  const dependsEdges = JOB_EDGES.filter(e => e.rel === 'DEPENDS_ON')
  const ioEdges      = JOB_EDGES.filter(e => e.rel === 'READS_FROM' || e.rel === 'WRITES_TO')
  const jobById = Object.fromEntries(JOBS.map(j => [j.id, j]))
  const callerMap = {}, calleeMap = {}
  const ids = new Set(JOBS.map(j => j.id))
  dependsEdges.forEach(e => {
    ids.add(e.src); ids.add(e.tgt)
    if (!callerMap[e.src]) callerMap[e.src] = new Set()
    if (!calleeMap[e.tgt]) calleeMap[e.tgt] = new Set()
    callerMap[e.src].add(e.tgt)
    calleeMap[e.tgt].add(e.src)
  })
  const roots = [...ids].filter(id => !calleeMap[id]?.size)
  const tier = {}, q = [], visited = new Set()
  roots.forEach(id => { tier[id] = 0; q.push({ id, t:0 }); visited.add(id) })
  while (q.length) {
    const { id, t } = q.shift()
    ;(callerMap[id] || new Set()).forEach(dep => {
      if (tier[dep] === undefined || tier[dep] < t + 1) tier[dep] = t + 1
      if (!visited.has(dep)) { visited.add(dep); q.push({ id:dep, t:tier[dep] }) }
    })
  }
  ids.forEach(id => { if (tier[id] === undefined) tier[id] = 0 })
  return { dependsEdges, ioEdges, jobById, ids, tier }
}

const { dependsEdges: DEPENDS_EDGES, ioEdges: IO_EDGES, jobById: JOB_BY_ID,
        ids: jobIds, tier: JOB_TIER } = buildJobData()


// ── Custom job node ────────────────────────────────────────────────────────

function JobNode({ data }) {
  const c    = domCol(data.domain)
  const hl   = data.highlighted
  const bord = jobBorder(data.focal, hl, c.border)
  const bg   = jobBg(data.focal, hl, c.bg)

  return (
    <div style={{
      width: '100%', height: '100%',
      background: bg,
      border: `1.5px solid ${bord}`,
      borderRadius: 8,
      boxShadow: `0 0 12px ${bord}22`,
      opacity: data.dimmed ? 0.15 : 1,
      display: 'flex', flexDirection: 'column', justifyContent: 'center',
      padding: '7px 11px',
      fontFamily: 'JetBrains Mono, monospace',
      cursor: 'pointer',
      transition: 'opacity 0.15s',
    }}>
      <Handle type="target" position={Position.Left}
        style={{ background: bord, width:8, height:8, border:'none', left:-5 }} />
      {/* domain badge */}
      <span style={{
        display:'inline-block', fontSize:8, fontWeight:700, letterSpacing:'0.06em',
        background: c.badge, color: c.text, borderRadius:3, padding:'1px 5px',
        marginBottom:4, alignSelf:'flex-start',
      }}>{data.domain}</span>
      {/* name */}
      <span style={{ color: jobColor(data.focal, hl, c.text),
                     fontSize:11, fontWeight:700, lineHeight:1.2, marginBottom:3 }}>
        {data.name}
      </span>
      {/* path hint */}
      <span style={{ color:'#3f3f46', fontSize:8.5, whiteSpace:'nowrap', overflow:'hidden',
                     textOverflow:'ellipsis' }}>
        {data.path}
      </span>
      <Handle type="source" position={Position.Right}
        style={{ background: bord, width:8, height:8, border:'none', right:-5 }} />
    </div>
  )
}

const NODE_TYPES = { job: JobNode }

// ── Build initial nodes + edges ───────────────────────────────────────────

function buildFlow(domain, showIO) {
  const nodes = [], edges = []

  // Position jobs
  const tierSlot = {}
  ;[...jobIds].forEach(id => {
    const job = JOB_BY_ID[id]
    if (!job) return
    const t = JOB_TIER[id] || 0
    if (domain !== 'All' && !job.domain?.includes(domain)) return
    tierSlot[t] = (tierSlot[t] || 0)
    const slot = tierSlot[t]
    tierSlot[t]++
    nodes.push({
      id,
      type: 'job',
      position: { x: t * TIER_X, y: slot * TIER_Y },
      data: {
        name: job.name, domain: job.domain || 'ETL',
        path: job.path, type: job.type,
        dimmed: false, highlighted: null, focal: false,
      },
      style: { width: CARD_W, height: CARD_H },
    })
  })

  const nodeSet = new Set(nodes.map(n => n.id))

  // DEPENDS_ON edges
  DEPENDS_EDGES.forEach((e, i) => {
    if (!nodeSet.has(e.src) || !nodeSet.has(e.tgt)) return
    edges.push({
      id: `dep-${i}`,
      source: e.src,   // caller
      target: e.tgt,   // callee (dependency)
      // We want caller on left, callee on right? Actually in BFS,
      // roots (no callers) are tier 0, and callees (what roots depend on) are tier 1+
      // So the edge direction is src=caller → tgt=callee → flow is L→R which matches BFS
      style: { stroke:'#8b5cf6', strokeWidth:1.8 },
      markerEnd: { type:MarkerType.ArrowClosed, color:'#8b5cf6', width:13, height:13 },
      _rel: 'DEPENDS_ON', _src: e.src, _tgt: e.tgt,
    })
  })

  // Optional IO edges
  if (showIO) {
    IO_EDGES.forEach((e, i) => {
      if (!nodeSet.has(e.src) || !nodeSet.has(e.tgt)) return
      edges.push({
        id: `io-${i}`,
        source: e.src, target: e.tgt,
        style: {
          stroke: e.rel === 'READS_FROM' ? '#3b82f6' : '#f59e0b',
          strokeWidth: 0.9,
          strokeDasharray: '3 4',
        },
        markerEnd: {
          type: MarkerType.Arrow,
          color: e.rel === 'READS_FROM' ? '#3b82f6' : '#f59e0b',
          width:11, height:11,
        },
        label: e.rel === 'READS_FROM' ? 'reads' : 'writes',
        labelStyle: { fill: e.rel === 'READS_FROM' ? '#3b82f666' : '#f59e0b66', fontSize:7.5 },
        _rel: e.rel, _src: e.src, _tgt: e.tgt,
      })
    })
  }

  return { nodes, edges }
}

// ── Lineage helpers ────────────────────────────────────────────────────────

function getJobLineage(jobId, edges) {
  const up = new Set(), down = new Set()
  const upQ = [jobId], downQ = [jobId]
  const uvU = new Set([jobId]), uvD = new Set([jobId])
  const deps = edges.filter(e => e._rel === 'DEPENDS_ON')
  while (upQ.length) {
    const id = upQ.shift()
    deps.forEach(e => {
      if (e.source === id && !uvU.has(e.target)) {
        up.add(e.target); uvU.add(e.target); upQ.push(e.target)
      }
    })
  }
  while (downQ.length) {
    const id = downQ.shift()
    deps.forEach(e => {
      if (e.target === id && !uvD.has(e.source)) {
        down.add(e.source); uvD.add(e.source); downQ.push(e.source)
      }
    })
  }
  return { up, down }
}

// ── Component ─────────────────────────────────────────────────────────────

const DOMAINS = ['All', 'ETL', 'Risk', 'Compliance', 'Reporting']

export default function FunctionalLineageFlow() {
  const [domain,  setDomain]  = useState('All')
  const [showIO,  setShowIO]  = useState(false)
  const [focal,   setFocal]   = useState(null)
  const [panel,   setPanel]   = useState(null)

  // Build on domain / showIO change
  const { nodes: initN, edges: initE } = useMemo(
    () => buildFlow(domain, showIO), [domain, showIO]
  )

  const [nodes, setNodes, onNodesChange] = useNodesState(initN)
  const [edges, setEdges, onEdgesChange] = useEdgesState(initE)

  // Re-sync when domain or showIO changes
  useEffect(() => {
    setNodes(initN)
    setEdges(initE)
    setFocal(null)
    setPanel(null)
  }, [initN, initE, setNodes, setEdges])

  const applyFocus = useCallback((jobId, curEdges) => {
    if (!jobId) {
      setNodes(ns => ns.map(n => ({
        ...n, data: { ...n.data, dimmed:false, highlighted:null, focal:false }
      })))
      setEdges(es => es.map(e => ({ ...e, style: { ...e.style, opacity:1 } })))
      return
    }
    const { up, down } = getJobLineage(jobId, curEdges)
    setNodes(ns => ns.map(n => {
      const isUp   = up.has(n.id)
      const isDown = down.has(n.id)
      const isFocal = n.id === jobId
      return {
        ...n,
        data: {
          ...n.data,
          dimmed:      !isFocal && !isUp && !isDown,
          highlighted: isFocal ? null : isUp ? 'up' : isDown ? 'down' : null,
          focal:       isFocal,
        },
      }
    }))
    setEdges(es => es.map(e => {
      if (e._rel !== 'DEPENDS_ON') return e
      const srcOk = e.source === jobId || up.has(e.source) || down.has(e.source)
      const tgtOk = e.target === jobId || up.has(e.target) || down.has(e.target)
      return { ...e, style: { ...e.style, opacity: (srcOk && tgtOk) ? 1 : 0.07 } }
    }))
  }, [setNodes, setEdges])

  const onNodeClick = useCallback((_ev, node) => {
    if (focal === node.id) {
      setFocal(null); setPanel(null); applyFocus(null, edges)
      return
    }
    const job = JOB_BY_ID[node.id] || {}
    const { up, down } = getJobLineage(node.id, edges)
    setFocal(node.id)
    setPanel({
      name:    node.data.name,
      domain:  node.data.domain,
      type:    job.type,
      path:    job.path,
      upCount: up.size,
      downCount: down.size,
      upNames:   [...up].map(id => JOB_BY_ID[id]?.name || id),
      downNames: [...down].map(id => JOB_BY_ID[id]?.name || id),
      riskTags:  job.risk_tags || [],
    })
    applyFocus(node.id, edges)
  }, [focal, edges, applyFocus])

  const onPaneClick = useCallback(() => {
    setFocal(null); setPanel(null); applyFocus(null, edges)
  }, [edges, applyFocus])

  // Domain filter style helper
  function tabCls(d) {
    const active = domain === d
    const c = DOM[d] || {}
    return `px-3 py-1.5 text-[10px] font-bold rounded-md transition-all cursor-pointer font-mono
      ${active
        ? `bg-zinc-800 border`
        : 'text-zinc-600 hover:text-zinc-300 border border-transparent'
      }`
  }
  function tabStyle(d) {
    if (domain !== d) return {}
    const c = DOM[d]
    if (!c) return { borderColor:'#52525b', color:'#a1a1aa' }
    return { borderColor: c.border, color: c.text }
  }

  return (
    <div className="flex rounded-lg border border-surface-border overflow-hidden"
         style={{ height: 660, background: '#070709' }}>

      <div className="flex-1 relative min-w-0">
        <ReactFlow
          nodes={nodes} edges={edges}
          nodeTypes={NODE_TYPES}
          onNodesChange={onNodesChange}
          onEdgesChange={onEdgesChange}
          onNodeClick={onNodeClick}
          onPaneClick={onPaneClick}
          fitView
          fitViewOptions={{ padding: 0.1 }}
          minZoom={0.15}
          maxZoom={2.5}
          proOptions={{ hideAttribution: true }}
          style={{ background: '#070709' }}
        >
          <Background variant={BackgroundVariant.Dots} gap={22} size={0.8} color="#1a1a24" />
          <Controls style={{ background:'#111116', border:'1px solid #27272a', borderRadius:8 }} />
          <MiniMap
            nodeColor={n => {
              if (n.data?.focal) return '#f59e0b'
              if (n.data?.highlighted === 'up')   return '#3b82f6'
              if (n.data?.highlighted === 'down')  return '#ef4444'
              return domCol(n.data?.domain).border
            }}
            style={{ background:'#111116', border:'1px solid #27272a', borderRadius:8 }}
            maskColor="#0c0c0e99"
          />

          {/* Toolbar */}
          <Panel position="top-left">
            <div className="flex gap-3 items-center">
              {/* Domain filter */}
              <div className="flex gap-0.5 rounded-lg border border-zinc-800 p-0.5 bg-zinc-950/80">
                {DOMAINS.map(d => (
                  <button key={d} onClick={() => setDomain(d)}
                    className={tabCls(d)} style={tabStyle(d)}>
                    {d}
                  </button>
                ))}
              </div>

              {/* IO toggle */}
              <button onClick={() => setShowIO(v => !v)}
                className={`text-[10px] px-2.5 py-1.5 rounded-md border font-mono transition-colors
                  ${showIO
                    ? 'bg-zinc-800 border-zinc-500 text-zinc-300'
                    : 'bg-zinc-950/80 border-zinc-800 text-zinc-600 hover:text-zinc-300'
                  }`}>
                {showIO ? '✓' : '+'} IO edges
              </button>

              {focal && (
                <button onClick={() => { setFocal(null); setPanel(null); applyFocus(null, edges) }}
                  className="text-[10px] px-2.5 py-1.5 bg-zinc-900/80 border border-zinc-700
                             rounded text-zinc-400 hover:text-zinc-200 font-mono">
                  ✕ clear
                </button>
              )}
            </div>
          </Panel>

          {/* Legend */}
          <Panel position="bottom-left">
            <div className="bg-zinc-950/90 border border-zinc-800 rounded-md px-2.5 py-2
                            flex flex-col gap-1 text-[10px] font-mono">
              <span className="text-zinc-600 text-[9px] uppercase tracking-widest mb-0.5">Legend</span>
              {[
                { color:'#8b5cf6', label:'─── DEPENDS_ON (call chain)' },
                { color:'#3b82f6', label:'- - - READS_FROM dataset' },
                { color:'#f59e0b', label:'- - - WRITES_TO dataset' },
                { color:'#3b82f6', label:'▶  upstream dependency' },
                { color:'#ef4444', label:'▶  downstream caller' },
              ].map(({ color, label }) => (
                <span key={label} className="flex items-center gap-2">
                  <span style={{ color, fontWeight:700 }}>━</span>
                  <span style={{ color:'#71717a' }}>{label}</span>
                </span>
              ))}
              <span className="text-zinc-700 text-[9px] mt-1">
                Tier 0 = root callers → right = leaf callees
              </span>
            </div>
          </Panel>

          {!focal && (
            <Panel position="bottom-right">
              <span className="text-[10px] text-zinc-700 font-mono">
                click a job to trace its dependency chain
              </span>
            </Panel>
          )}
        </ReactFlow>
      </div>

      {/* Info panel */}
      {panel && (
        <div className="w-60 shrink-0 border-l border-surface-border bg-zinc-950
                        flex flex-col text-xs font-mono overflow-y-auto">

          <div className="px-3 pt-3 pb-2 border-b border-zinc-800">
            <p className="text-[9px] text-zinc-600 uppercase tracking-widest mb-1">Focal job</p>
            <p className="font-bold text-amber-300 break-all leading-snug">{panel.name}</p>
            <div className="flex flex-wrap gap-1.5 mt-1.5">
              <span className="px-1.5 py-0.5 rounded bg-zinc-800 text-zinc-400 text-[9px]">
                {panel.type}
              </span>
              {panel.riskTags.map(t => (
                <span key={t} className="px-1.5 py-0.5 rounded bg-red-950 border border-red-800
                                         text-red-400 text-[8px]">
                  {t}
                </span>
              ))}
            </div>
            {panel.path && (
              <p className="text-zinc-700 text-[9px] mt-1.5 break-all leading-relaxed">
                {panel.path}
              </p>
            )}
          </div>

          {panel.upNames.length > 0 && (
            <div className="px-3 py-2 border-b border-zinc-800">
              <p className="text-[9px] text-blue-400 uppercase tracking-widest mb-1.5">
                Dependencies ({panel.upCount})
              </p>
              {panel.upNames.map((n, i) => (
                <p key={i} className="text-blue-300 text-[10px] mb-1">• {n}</p>
              ))}
            </div>
          )}

          {panel.downNames.length > 0 && (
            <div className="px-3 py-2">
              <p className="text-[9px] text-red-400 uppercase tracking-widest mb-1.5">
                Called by ({panel.downCount})
              </p>
              {panel.downNames.map((n, i) => (
                <p key={i} className="text-red-300 text-[10px] mb-1">• {n}</p>
              ))}
            </div>
          )}

          {panel.upCount === 0 && panel.downCount === 0 && (
            <p className="px-3 py-4 text-zinc-600 italic text-[11px]">
              No DEPENDS_ON links for this job.
            </p>
          )}
        </div>
      )}
    </div>
  )
}
