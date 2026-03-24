import { useEffect, useState } from 'react'
import { Link } from 'react-router-dom'
import { Play, Database, FunctionSquare, Table, GitBranch, BookOpen, Wifi, WifiOff, RefreshCw } from 'lucide-react'
import { fetchStats, fetchRuns } from '../services/api'

function StatCard({ icon: Icon, label, value, color = 'violet' }) {
  const colors = {
    violet:  'bg-violet-500/10  text-violet-400',
    emerald: 'bg-emerald-500/10 text-emerald-400',
    blue:    'bg-blue-500/10    text-blue-400',
    amber:   'bg-amber-500/10   text-amber-400',
    rose:    'bg-rose-500/10    text-rose-400',
  }
  return (
    <div className="card flex items-start gap-4">
      <div className={`p-2 rounded-lg ${colors[color]}`}>
        <Icon size={18} />
      </div>
      <div>
        <p className="text-2xl font-semibold text-zinc-100">{value ?? '—'}</p>
        <p className="text-xs text-zinc-500 mt-0.5">{label}</p>
      </div>
    </div>
  )
}

function StatusBadge({ status }) {
  const cls = {
    success: 'badge-success',
    error:   'badge-error',
    running: 'badge-running',
    queued:  'badge-queued',
    partial: 'badge-warn',
  }
  return <span className={cls[status] || 'badge-queued'}>{status}</span>
}

function formatTs(ts) {
  if (!ts) return '—'
  return new Date(ts * 1000).toLocaleString()
}

export default function Dashboard() {
  const [stats, setStats]   = useState(null)
  const [runs,  setRuns]    = useState([])
  const [online, setOnline] = useState(null)
  const [loading, setLoading] = useState(true)

  const loadData = () => {
    Promise.all([fetchStats(), fetchRuns()]).then(([s, r]) => {
      setStats(s.graph)
      setOnline(s.neo4j_online)
      setRuns(r.slice(0, 5))
    }).finally(() => setLoading(false))
  }

  useEffect(() => {
    loadData()
    const timer = setInterval(loadData, 15000) // live refresh every 15 s
    return () => clearInterval(timer)
  }, [])  // eslint-disable-line react-hooks/exhaustive-deps

  return (
    <div className="p-6 space-y-6">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-xl font-semibold text-zinc-100">Metadata Model Engine</h1>
          <p className="text-sm text-zinc-500 mt-0.5">Living graph — source of truth for code structure, data lineage &amp; compliance</p>
        </div>
        <div className="flex items-center gap-2">
          {online === null ? null : online ? (
            <span className="flex items-center gap-1.5 text-xs text-emerald-400">
              <Wifi size={13} /> Neo4j online
            </span>
          ) : (
            <span className="flex items-center gap-1.5 text-xs text-red-400">
              <WifiOff size={13} /> Neo4j offline
            </span>
          )}
          <button
            onClick={loadData}
            className="btn-ghost flex items-center gap-1.5 text-xs border border-surface-border px-2 py-1.5 rounded-md"
            title="Refresh stats now"
          >
            <RefreshCw size={12} /> Refresh
          </button>
          <Link to="/pipeline" className="btn-primary flex items-center gap-2">
            <Play size={13} /> Run Hydration
          </Link>
        </div>
      </div>

      {/* Stats grid */}
      <div className="grid grid-cols-2 lg:grid-cols-5 gap-3">
        <StatCard icon={FunctionSquare} label="Functions"      value={stats?.functions}     color="violet"  />
        <StatCard icon={Table}          label="Schema Objects" value={stats?.schema_objects} color="blue"    />
        <StatCard icon={Database}       label="Files"          value={stats?.files}          color="emerald" />
        <StatCard icon={GitBranch}      label="Repositories"   value={stats?.repositories}   color="amber"   />
        <StatCard icon={BookOpen}       label="Business Rules" value={stats?.business_rules} color="rose"    />
      </div>

      {/* What is the Metadata Model? */}
      <div className="card border border-violet-500/30 bg-violet-900/10 space-y-3">
        <p className="text-sm font-semibold text-violet-300 uppercase tracking-widest">What is the Metadata Model?</p>
        <p className="text-sm text-zinc-300">
          The <strong className="text-zinc-100">Metadata Model</strong> stores <em>facts about your codebase</em>, not business data itself.
          It tracks which functions read or write which database columns, what compliance tags apply, data lineage paths,
          business governance rules, and STM (Source-to-Target) mappings from code columns to Data Warehouse targets.
        </p>
        <div className="grid grid-cols-3 gap-3 text-xs">
          <div className="bg-zinc-900/50 rounded-md p-3 border border-zinc-700 space-y-1">
            <p className="text-violet-400 font-semibold">Phase 1 — Hydration</p>
            <p className="text-zinc-400">Scan Python source files. Extract every function, column reference, import and risk tag. Write into the graph as a living specification. Run once (or after large refactors).</p>
          </div>
          <div className="bg-zinc-900/50 rounded-md p-3 border border-zinc-700 space-y-1">
            <p className="text-blue-400 font-semibold">STM — Source-to-Target</p>
            <p className="text-zinc-400">Bridges code SchemaObjects (e.g. <code className="text-amber-300">customers.account_id</code>) to DW target columns via MAPS_TO edges. Seed STM <em>after</em> Phase 1 so the graph has schema nodes to link against.</p>
          </div>
          <div className="bg-zinc-900/50 rounded-md p-3 border border-zinc-700 space-y-1">
            <p className="text-emerald-400 font-semibold">Phase 2 — Consumption</p>
            <p className="text-zinc-400">Pipelines query the graph at runtime — no hardcoded column lists. When a schema changes, all downstream pipelines adapt automatically via the graph spec.</p>
          </div>
        </div>
      </div>

      {/* Phase explanation */}
      <div className="grid grid-cols-2 gap-3">
        <div className="card border-l-2 border-violet-500 space-y-1">
          <p className="text-xs font-semibold text-violet-400 uppercase tracking-widest">Phase 1 — Hydration</p>
          <p className="text-sm text-zinc-300">Scan the codebase once. Extract every structural and semantic fact. Write into the graph.</p>
          <p className="text-xs text-zinc-500">Runs infrequently. Output is meant to be stable.</p>
        </div>
        <div className="card border-l-2 border-blue-500 space-y-1">
          <p className="text-xs font-semibold text-blue-400 uppercase tracking-widest">Phase 2 — Consumption</p>
          <p className="text-sm text-zinc-300">Pipelines query the graph at runtime. No hardcoded column names. Schema changes propagate automatically.</p>
          <p className="text-xs text-zinc-500">The graph is the specification.</p>
        </div>
      </div>

      {/* Recent runs */}
      <div className="card space-y-3">
        <div className="flex items-center justify-between">
          <p className="text-sm font-medium text-zinc-300">Recent Runs</p>
          <Link to="/traces" className="text-xs text-accent-text hover:underline">View all</Link>
        </div>
        {runs.length === 0 ? (
          <p className="text-sm text-zinc-600 py-4 text-center">No runs yet. Start a hydration pipeline.</p>
        ) : (
          <table className="w-full text-xs">
            <thead>
              <tr className="text-zinc-500 border-b border-surface-border">
                <th className="text-left py-2 font-normal">ID</th>
                <th className="text-left py-2 font-normal">Phase</th>
                <th className="text-left py-2 font-normal">Status</th>
                <th className="text-left py-2 font-normal">Started</th>
                <th className="text-left py-2 font-normal">Agents</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-surface-border">
              {runs.map(r => (
                <tr key={r.id} className="hover:bg-surface-hover/50 transition-colors">
                  <td className="py-2 font-mono text-zinc-400">{r.id.slice(0, 8)}…</td>
                  <td className="py-2">{r.phase}</td>
                  <td className="py-2"><StatusBadge status={r.status} /></td>
                  <td className="py-2 text-zinc-500">{formatTs(r.started_at)}</td>
                  <td className="py-2 text-zinc-400">{r.agent_runs?.length ?? 0}</td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
      </div>
    </div>
  )
}
