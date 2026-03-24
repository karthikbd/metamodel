import { useEffect, useState } from 'react'
import { useParams, Link } from 'react-router-dom'
import { CheckCircle, XCircle, Clock, Loader, ChevronDown, ChevronRight } from 'lucide-react'
import { fetchRuns, fetchRun, fetchAgentRuns, fetchAgentEvents } from '../services/api'

function StatusBadge({ status }) {
  const cls = { success: 'badge-success', error: 'badge-error', running: 'badge-running',
                queued: 'badge-queued', partial: 'badge-warn' }
  return <span className={cls[status] || 'badge-queued'}>{status}</span>
}

function StatusIcon({ status }) {
  if (status === 'success') return <CheckCircle size={13} className="text-emerald-400" />
  if (status === 'error')   return <XCircle     size={13} className="text-red-400"     />
  if (status === 'running') return <Loader      size={13} className="text-violet-400 animate-spin" />
  return <Clock size={13} className="text-zinc-600" />
}

function msLabel(ms) {
  if (!ms) return ''
  return ms < 1000 ? `${ms}ms` : `${(ms / 1000).toFixed(2)}s`
}

function AgentRunRow({ runId, ar }) {
  const [open, setOpen] = useState(false)
  const [events, setEvents] = useState(null)

  async function toggle() {
    if (!open && !events) {
      const evs = await fetchAgentEvents(runId, ar.id)
      setEvents(evs)
    }
    setOpen(o => !o)
  }

  return (
    <>
      <tr
        onClick={toggle}
        className="hover:bg-surface-hover/50 cursor-pointer transition-colors select-none"
      >
        <td className="py-2 pl-4">
          <StatusIcon status={ar.status} />
        </td>
        <td className="py-2 text-sm font-mono text-zinc-300">{ar.agent_name}</td>
        <td className="py-2"><StatusBadge status={ar.status} /></td>
        <td className="py-2 text-xs text-zinc-500">{msLabel(ar.duration_ms)}</td>
        <td className="py-2 text-xs text-zinc-500">{ar.event_count} events</td>
        <td className="py-2 pr-4 text-zinc-600">
          {open ? <ChevronDown size={13} /> : <ChevronRight size={13} />}
        </td>
      </tr>
      {open && (
        <tr>
          <td colSpan={6} className="pb-3 px-4">
            <div className="bg-[#0c0c0e] rounded-md border border-surface-border p-3 max-h-56 overflow-y-auto font-mono text-xs space-y-0.5">
              {events === null ? (
                <p className="text-zinc-600">Loading…</p>
              ) : events.length === 0 ? (
                <p className="text-zinc-600">No events recorded.</p>
              ) : (
                events.map((ev, i) => (
                  <div key={i} className="flex gap-3">
                    <span className="text-zinc-600 flex-shrink-0">
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
          </td>
        </tr>
      )}
    </>
  )
}

function RunDetail({ run }) {
  const [agentRuns, setAgentRuns] = useState([])

  useEffect(() => {
    fetchAgentRuns(run.id).then(setAgentRuns)
  }, [run.id])

  return (
    <div className="space-y-4">
      <div className="card space-y-2">
        <div className="flex items-center gap-3">
          <p className="text-sm font-mono text-zinc-400">{run.id}</p>
          <StatusBadge status={run.status} />
        </div>
        <div className="grid grid-cols-3 gap-4 text-xs">
          <div><p className="text-zinc-500">Phase</p><p className="text-zinc-200 mt-0.5">{run.phase}</p></div>
          <div><p className="text-zinc-500">Repo Root</p><p className="text-zinc-200 mt-0.5 font-mono">{run.repo_root}</p></div>
          <div><p className="text-zinc-500">Started</p><p className="text-zinc-200 mt-0.5">
            {run.started_at ? new Date(run.started_at * 1000).toLocaleString() : '—'}
          </p></div>
        </div>
      </div>

      <div className="card">
        <p className="text-xs font-semibold text-zinc-400 uppercase tracking-widest mb-3">Agent Runs</p>
        <table className="w-full text-xs">
          <thead>
            <tr className="text-zinc-500 border-b border-surface-border">
              <th className="text-left py-2 pl-4 w-8"></th>
              <th className="text-left py-2">Agent</th>
              <th className="text-left py-2">Status</th>
              <th className="text-left py-2">Duration</th>
              <th className="text-left py-2">Events</th>
              <th className="py-2 pr-4 w-8"></th>
            </tr>
          </thead>
          <tbody className="divide-y divide-surface-border">
            {agentRuns.map(ar => (
              <AgentRunRow key={ar.id} runId={run.id} ar={ar} />
            ))}
          </tbody>
        </table>
      </div>
    </div>
  )
}

export default function TracesPage() {
  const { id } = useParams()
  const [runs, setRuns] = useState([])
  const [selected, setSelected] = useState(null)

  useEffect(() => {
    fetchRuns().then(data => {
      setRuns(data)
      if (id) {
        const found = data.find(r => r.id === id)
        if (found) setSelected(found)
      }
    })
  }, [id])

  async function selectRun(run) {
    const full = await fetchRun(run.id)
    setSelected(full)
  }

  return (
    <div className="p-6 space-y-4">
      <div>
        <h1 className="text-xl font-semibold text-zinc-100">Run Traces</h1>
        <p className="text-sm text-zinc-500 mt-0.5">LangSmith-style execution log per pipeline run</p>
      </div>

      <div className="flex gap-4">
        {/* Run list */}
        <div className="w-72 flex-shrink-0 card">
          <p className="text-xs font-semibold text-zinc-400 uppercase tracking-widest mb-3">All Runs</p>
          {runs.length === 0 ? (
            <p className="text-sm text-zinc-600">No runs yet.</p>
          ) : (
            <div className="space-y-1">
              {runs.map(r => (
                <button
                  key={r.id}
                  onClick={() => selectRun(r)}
                  className={`w-full text-left px-3 py-2.5 rounded-md transition-colors text-xs ${
                    selected?.id === r.id
                      ? 'bg-accent/15 text-accent-text'
                      : 'hover:bg-surface-hover text-zinc-400'
                  }`}
                >
                  <div className="flex items-center justify-between">
                    <span className="font-mono">{r.id.slice(0, 8)}…</span>
                    <StatusBadge status={r.status} />
                  </div>
                  <div className="text-zinc-600 mt-0.5">
                    {r.phase} · {r.agent_runs?.length ?? 0} agents
                  </div>
                </button>
              ))}
            </div>
          )}
        </div>

        {/* Detail */}
        <div className="flex-1">
          {selected ? (
            <RunDetail run={selected} />
          ) : (
            <div className="card h-40 flex items-center justify-center">
              <p className="text-sm text-zinc-600">Select a run to inspect its trace.</p>
            </div>
          )}
        </div>
      </div>
    </div>
  )
}
