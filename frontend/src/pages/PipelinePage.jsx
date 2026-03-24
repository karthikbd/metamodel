import { useState, useRef, useEffect } from 'react'
import { useNavigate } from 'react-router-dom'
import { Play, Square, ChevronDown, ChevronRight, CheckCircle, XCircle, Clock, Loader, ArrowRight } from 'lucide-react'
import { startPhase1Stream, fetchConfig } from '../services/api'
import toast from 'react-hot-toast'

const AGENTS = [
  { id: 'ast_extractor',      label: 'Agent 1 — AST Extraction',            desc: 'Parses Python files via ast module. Produces Function, Class, Argument, Decorator nodes.' },
  { id: 'cross_ref_resolver', label: 'Agent 2 — Cross-Reference Resolution', desc: 'Resolves import chains → CALLS edges, IMPORTS edges, UNRESOLVED nodes.' },
  { id: 'schema_extractor',   label: 'Agent 3 — Schema & Transformation',    desc: 'Extracts Dataset/Column nodes, READS_FROM/WRITES_TO edges, Transformation nodes. Critical for Phase 2.' },
  { id: 'llm_summariser',     label: 'Agent 4 — LLM Summarisation',          desc: 'Generates LLMSummary nodes per function. All output marked confidence: inferred.' },
]

function StatusIcon({ status }) {
  if (status === 'success') return <CheckCircle size={14} className="text-emerald-400 flex-shrink-0" />
  if (status === 'error')   return <XCircle     size={14} className="text-red-400    flex-shrink-0" />
  if (status === 'running') return <Loader      size={14} className="text-violet-400 animate-spin flex-shrink-0" />
  return <Clock size={14} className="text-zinc-600 flex-shrink-0" />
}

function AgentPanel({ agent, agentStatus, logs }) {
  const [open, setOpen] = useState(true)

  return (
    <div className={`border rounded-lg overflow-hidden transition-colors ${
      agentStatus === 'running' ? 'border-violet-500/40' :
      agentStatus === 'success' ? 'border-emerald-500/20' :
      agentStatus === 'error'   ? 'border-red-500/20' :
      'border-surface-border'
    }`}>
      {/* Header */}
      <button
        onClick={() => setOpen(o => !o)}
        className="w-full flex items-center gap-3 px-4 py-3 bg-surface-card hover:bg-surface-hover transition-colors text-left"
      >
        <StatusIcon status={agentStatus} />
        <div className="flex-1">
          <p className="text-sm font-medium text-zinc-200">{agent.label}</p>
          <p className="text-xs text-zinc-500 mt-0.5">{agent.desc}</p>
        </div>
        <span className="text-zinc-600 ml-2">
          {open ? <ChevronDown size={14} /> : <ChevronRight size={14} />}
        </span>
      </button>

      {/* Log stream */}
      {open && (
        <div className="bg-[#0c0c0e] border-t border-surface-border px-4 py-3 max-h-64 overflow-y-auto font-mono text-xs">
          {logs.length === 0 ? (
            <span className="text-zinc-600">Waiting to start…</span>
          ) : (
            logs.map((ev, i) => (
              <div key={i} className={`log-${ev.level || 'info'}`}>
                <span className="text-zinc-600 select-none">
                  {new Date(ev.ts * 1000).toISOString().slice(11, 23)}
                </span>
                <span className={
                  ev.level === 'error'   ? 'text-red-400'    :
                  ev.level === 'warn'    ? 'text-amber-400'  :
                  ev.level === 'success' ? 'text-emerald-400':
                  'text-zinc-300'
                }>
                  {ev.message}
                </span>
              </div>
            ))
          )}
        </div>
      )}
    </div>
  )
}

export default function PipelinePage() {
  const [repoRoot,   setRepoRoot]   = useState('')
  const [force,      setForce]      = useState(false)
  const navigate = useNavigate()

  // Auto-fill repo root from backend config on mount
  useEffect(() => {
    fetchConfig().then(cfg => {
      if (cfg?.repo_scan_root) setRepoRoot(cfg.repo_scan_root)
    }).catch(() => setRepoRoot('./sample_repo'))
  }, [])
  const [selected,   setSelected]   = useState(new Set(AGENTS.map(a => a.id)))
  const [running,    setRunning]    = useState(false)
  const [agentState, setAgentState] = useState({})   // id → { status, logs }
  const [pipelineStatus, setPipelineStatus] = useState(null)
  const abortRef = useRef(false)

  function toggleAgent(id) {
    setSelected(prev => {
      const next = new Set(prev)
      next.has(id) ? next.delete(id) : next.add(id)
      return next
    })
  }

  function initAgentState() {
    const init = {}
    AGENTS.forEach(a => { init[a.id] = { status: 'queued', logs: [] } })
    setAgentState(init)
    setPipelineStatus('running')
  }

  function handleStart() {
    abortRef.current = false
    setRunning(true)
    initAgentState()

    startPhase1Stream(
      { repo_root: repoRoot, agents: [...selected], force },
      (ev) => {
        if (abortRef.current) return

        if (ev.type === 'agent_start') {
          setAgentState(s => ({ ...s, [ev.agent]: { ...s[ev.agent], status: 'running' } }))
        } else if (ev.type === 'agent_end') {
          setAgentState(s => ({ ...s, [ev.agent]: { ...s[ev.agent], status: ev.status } }))
        } else if (ev.type === 'event') {
          setAgentState(s => {
            const prev = s[ev.agent] || { status: 'running', logs: [] }
            return { ...s, [ev.agent]: { ...prev, logs: [...prev.logs, ev] } }
          })
        } else if (ev.type === 'pipeline_end') {
          setPipelineStatus(ev.status)
          toast[ev.status === 'success' ? 'success' : 'error'](
            ev.status === 'success' ? 'Hydration complete — graph is ready' : 'Hydration finished with errors'
          )
        }
      },
      () => setRunning(false)
    )
  }

  function handleStop() {
    abortRef.current = true
    setRunning(false)
    setPipelineStatus('error')
  }

  return (
    <div className="p-6 space-y-5">
      {/* Header */}
      <div>
        <h1 className="text-xl font-semibold text-zinc-100">Pipeline Runner</h1>
        <p className="text-sm text-zinc-500 mt-0.5">Phase 1 — Hydrate the graph from your legacy codebase</p>
      </div>

      {/* Config */}
      <div className="card space-y-4">
        <p className="text-xs font-semibold text-zinc-400 uppercase tracking-widest">Configuration</p>
        <div>
          <label className="text-xs text-zinc-500 mb-1 block">Repository root path</label>
          <input
            type="text"
            value={repoRoot}
            onChange={e => setRepoRoot(e.target.value)}
            className="w-full bg-surface border border-surface-border rounded-md px-3 py-2 text-sm
                       font-mono text-zinc-200 focus:outline-none focus:border-accent"
            placeholder="./sample_repo or /absolute/path"
          />
        </div>

        <div>
          <label className="text-xs text-zinc-500 mb-2 block">Agents to run</label>
          <div className="space-y-2">
            {AGENTS.map(a => (
              <label key={a.id} className="flex items-start gap-3 cursor-pointer group">
                <input
                  type="checkbox"
                  checked={selected.has(a.id)}
                  onChange={() => toggleAgent(a.id)}
                  className="mt-0.5 accent-violet-500"
                />
                <div>
                  <p className="text-sm text-zinc-300 group-hover:text-zinc-100 transition-colors">{a.label}</p>
                  <p className="text-xs text-zinc-600">{a.desc}</p>
                </div>
              </label>
            ))}
          </div>
        </div>

        <div className="flex items-center gap-3 pt-1 pb-1">
          <label className="flex items-center gap-2 cursor-pointer select-none">
            <input
              type="checkbox"
              checked={force}
              onChange={e => setForce(e.target.checked)}
              className="accent-amber-500"
            />
            <span className="text-xs text-amber-400 font-medium">Force re-extract</span>
            <span className="text-xs text-zinc-600">(bypass file-hash cache — re-processes all files)</span>
          </label>
        </div>

        <div className="flex gap-2 pt-1 flex-wrap">
          <button
            onClick={handleStart}
            disabled={running || selected.size === 0}
            className="btn-primary flex items-center gap-2"
          >
            <Play size={13} /> Start Hydration
          </button>
          {running && (
            <button onClick={handleStop} className="btn-ghost flex items-center gap-2 border border-surface-border">
              <Square size={13} /> Abort
            </button>
          )}
          {pipelineStatus && (
            <span className={`text-sm my-auto ml-2 ${
              pipelineStatus === 'success' ? 'text-emerald-400' :
              pipelineStatus === 'error'   ? 'text-red-400' :
              'text-violet-400'
            }`}>
              {pipelineStatus === 'running' ? 'Running…' :
               pipelineStatus === 'success' ? '✓ Complete' : '✗ Finished with errors'}
            </span>
          )}
        </div>

        {/* Post-completion quick navigation */}
        {pipelineStatus === 'success' && (
          <div className="border border-emerald-800/40 bg-emerald-900/10 rounded-lg p-3 mt-2">
            <p className="text-xs font-semibold text-emerald-300 mb-2">Graph hydrated — explore results:</p>
            <div className="flex flex-wrap gap-2">
              {[
                { label: 'View Graph',   path: '/graph'      },
                { label: 'Lineage',      path: '/lineage'    },
                { label: 'Compliance',   path: '/compliance' },
                { label: 'STM Mapping',  path: '/stm'        },
                { label: 'Phase 2',      path: '/phase2'     },
              ].map(({ label, path }) => (
                <button
                  key={path}
                  onClick={() => navigate(path)}
                  className="flex items-center gap-1.5 px-3 py-1.5 rounded-md bg-zinc-800 hover:bg-zinc-700
                             text-xs text-zinc-300 border border-zinc-700 transition-colors"
                >
                  {label} <ArrowRight size={11} />
                </button>
              ))}
            </div>
          </div>
        )}
      </div>

      {/* Agent panels */}
      <div className="space-y-3">
        <p className="text-xs font-semibold text-zinc-400 uppercase tracking-widest">Execution Log</p>
        {AGENTS.map(agent => (
          <AgentPanel
            key={agent.id}
            agent={agent}
            agentStatus={agentState[agent.id]?.status || 'queued'}
            logs={agentState[agent.id]?.logs || []}
          />
        ))}
      </div>
    </div>
  )
}
