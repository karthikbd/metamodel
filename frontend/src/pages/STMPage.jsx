import { useState, useEffect } from 'react'
import { fetchSTMMappings, fetchSTMLineage, fetchSTMStats, seedSTM } from '../services/api'
import toast from 'react-hot-toast'
import { ArrowRight, Database, GitMerge, RefreshCw, Loader2, ChevronDown, ChevronRight } from 'lucide-react'

const SYSTEM_COLORS = {
  data_warehouse:        { bg: '#0f1a2e', border: '#3b82f6', badge: 'bg-blue-900/40 text-blue-300 border-blue-700' },
  risk_warehouse:        { bg: '#1a1000', border: '#f59e0b', badge: 'bg-amber-900/40 text-amber-300 border-amber-700' },
  regulatory_reporting:  { bg: '#0e1e18', border: '#34d399', badge: 'bg-emerald-900/40 text-emerald-300 border-emerald-700' },
  mds_warehouse:         { bg: '#1a0a2e', border: '#a78bfa', badge: 'bg-violet-900/40 text-violet-300 border-violet-700' },
  unknown:               { bg: '#111827', border: '#6b7280', badge: 'bg-zinc-800 text-zinc-400 border-zinc-600' },
}

function SystemBadge({ system }) {
  const c = SYSTEM_COLORS[system] || SYSTEM_COLORS.unknown
  return (
    <span className={`text-[10px] px-2 py-0.5 rounded border font-mono ${c.badge}`}>
      {system}
    </span>
  )
}

function TransformBox({ expr }) {
  if (!expr) return <span className="text-zinc-600 italic text-xs">passthrough</span>
  return (
    <code className="text-xs text-amber-300 font-mono bg-zinc-900 px-2 py-0.5 rounded">
      {expr}
    </code>
  )
}

function MappingRow({ m }) {
  return (
    <tr className="border-b border-zinc-800 hover:bg-zinc-800/30 transition-colors">
      <td className="py-2.5 px-3">
        <div className="font-mono text-xs text-zinc-200">{m.source_table}</div>
        <div className="font-mono text-[10px] text-zinc-500">{m.source_column}</div>
        {m.source_dtype && (
          <div className="text-[9px] text-zinc-600 mt-0.5">{m.source_dtype}</div>
        )}
      </td>
      <td className="py-2.5 px-3 text-center">
        <ArrowRight size={14} className="text-zinc-600 inline" />
      </td>
      <td className="py-2.5 px-3 max-w-xs">
        <TransformBox expr={m.transform_expr} />
      </td>
      <td className="py-2.5 px-3 text-center">
        <ArrowRight size={14} className="text-zinc-600 inline" />
      </td>
      <td className="py-2.5 px-3">
        <div className="font-mono text-xs text-zinc-200">{m.target_table}</div>
        <div className="font-mono text-[10px] text-zinc-500">{m.target_column}</div>
        <div className="mt-0.5"><SystemBadge system={m.target_system} /></div>
      </td>
      <td className="py-2.5 px-3">
        <div className="text-[10px] text-zinc-500">{m.owner}</div>
      </td>
      <td className="py-2.5 px-3 text-right">
        {m.used_by_functions?.length > 0 ? (
          <div className="flex flex-col items-end gap-0.5">
            {m.used_by_functions.slice(0, 3).map((fn, i) => (
              <span key={i} className="text-[10px] text-violet-400 font-mono whitespace-nowrap">
                {fn}
              </span>
            ))}
            {m.used_by_functions.length > 3 && (
              <span className="text-[10px] text-zinc-600 italic">
                +{m.used_by_functions.length - 3} more
              </span>
            )}
          </div>
        ) : (
          <span className="text-[10px] text-zinc-600 italic">none yet</span>
        )}
      </td>
    </tr>
  )
}

function LineageGroupCard({ system, rows }) {
  const [expanded, setExpanded] = useState(true)
  const c = SYSTEM_COLORS[system] || SYSTEM_COLORS.unknown

  return (
    <div className="rounded-lg border mb-4" style={{ borderColor: c.border, background: c.bg }}>
      <button
        onClick={() => setExpanded(e => !e)}
        className="w-full flex items-center justify-between px-4 py-3 text-left"
      >
        <div className="flex items-center gap-3">
          {expanded ? <ChevronDown size={14} className="text-zinc-400" /> : <ChevronRight size={14} className="text-zinc-400" />}
          <SystemBadge system={system} />
          <span className="text-sm font-semibold text-zinc-200">{system}</span>
          <span className="text-xs text-zinc-500">({rows.length} mappings)</span>
        </div>
        <span className="text-xs text-zinc-600">{[...new Set(rows.map(r => r.target_table))].join(', ')}</span>
      </button>

      {expanded && (
        <div className="px-4 pb-4 space-y-2">
          {rows.map((r, i) => (
            <div key={i} className="flex items-center gap-2 text-xs bg-zinc-900/50 rounded px-3 py-2 flex-wrap">
              <div className="flex items-center gap-1.5">
                {r.function_names && r.function_names.filter(Boolean).length > 0 ? (
                  <span className="text-zinc-400 font-mono text-[10px]">
                    {r.function_names.filter(Boolean).join(', ')}
                  </span>
                ) : (
                  <span className="text-zinc-600 italic text-[10px]">no job</span>
                )}
                <span className="text-zinc-600">→</span>
                <span className="font-mono text-blue-300">{r.source_table}.{r.source_column}</span>
              </div>
              <ArrowRight size={12} className="text-zinc-600" />
              {r.transform_expr ? (
                <code className="text-amber-300 font-mono bg-zinc-800 px-1.5 py-0.5 rounded text-[10px]">
                  {r.transform_expr}
                </code>
              ) : (
                <span className="text-zinc-600 italic text-[10px]">passthrough</span>
              )}
              <ArrowRight size={12} className="text-zinc-600" />
              <span className="font-mono text-emerald-300">{r.target_table}.{r.target_column}</span>
              <span className="ml-auto text-zinc-600 text-[10px] font-mono">{r.interaction}</span>
            </div>
          ))}
        </div>
      )}
    </div>
  )
}

export default function STMPage() {
  const [tab, setTab]           = useState('mappings')
  const [mappings, setMappings] = useState([])
  const [lineage,  setLineage]  = useState({ by_system: {}, total: 0 })
  const [stats,    setStats]    = useState(null)
  const [loading,  setLoading]  = useState(false)
  const [seeding,  setSeeding]  = useState(false)
  const [filter,   setFilter]   = useState('')

  const load = async () => {
    setLoading(true)
    try {
      const [mRes, lRes, sRes] = await Promise.all([
        fetchSTMMappings(),
        fetchSTMLineage(),
        fetchSTMStats(),
      ])
      setMappings(mRes.mappings || [])
      setLineage(lRes)
      setStats(sRes)
    } catch (e) {
      toast.error('Failed to load STM data — try seeding first')
    } finally {
      setLoading(false)
    }
  }

  useEffect(() => { load() }, [])

  const handleSeed = async () => {
    setSeeding(true)
    try {
      const res = await seedSTM()
      toast.success(
        `STM seeded: ${res.stm_nodes_created_or_updated} nodes, ${res.edges_linked_to_schema} edges linked`
      )
      await load()
    } catch (e) {
      toast.error('Seed failed: ' + (e.response?.data?.detail || e.message))
    } finally {
      setSeeding(false)
    }
  }

  const filteredMappings = mappings.filter(m => {
    if (!filter) return true
    const q = filter.toLowerCase()
    return (
      m.source_table?.toLowerCase().includes(q) ||
      m.source_column?.toLowerCase().includes(q) ||
      m.target_table?.toLowerCase().includes(q) ||
      m.target_column?.toLowerCase().includes(q) ||
      m.target_system?.toLowerCase().includes(q)
    )
  })

  return (
    <div className="p-6 space-y-6 min-h-screen bg-surface text-zinc-200">
      {/* Header */}
      <div className="flex items-start justify-between">
        <div>
          <h1 className="text-xl font-bold text-zinc-100 flex items-center gap-2">
            <GitMerge size={20} className="text-violet-400" />
            Source-to-Target Mapping
          </h1>
          <p className="text-sm text-zinc-500 mt-1">
            Bridges code-graph SchemaObjects to downstream DW targets via MAPS_TO edges
          </p>
        </div>
        <div className="flex items-center gap-2">
          <button
            onClick={load}
            disabled={loading}
            className="flex items-center gap-1.5 px-3 py-1.5 rounded-md bg-zinc-800 hover:bg-zinc-700 
                       text-sm text-zinc-300 border border-zinc-700 transition-colors"
          >
            {loading ? <Loader2 size={13} className="animate-spin" /> : <RefreshCw size={13} />}
            Refresh
          </button>
          <button
            onClick={handleSeed}
            disabled={seeding}
            className="flex items-center gap-1.5 px-3 py-1.5 rounded-md bg-violet-700 hover:bg-violet-600 
                       text-sm text-white border border-violet-600 transition-colors"
          >
            {seeding ? <Loader2 size={13} className="animate-spin" /> : <Database size={13} />}
            Seed STM
          </button>
        </div>
      </div>

      {/* Workflow guide */}
      <div className="rounded-lg border border-amber-600/40 bg-amber-900/10 px-4 py-3 text-sm text-zinc-300 flex flex-wrap items-center gap-2">
        <span className="font-semibold text-amber-300">Setup workflow:</span>
        <span className="bg-violet-900/40 text-violet-300 border border-violet-700 px-2 py-0.5 rounded text-xs font-mono">1. Run Phase 1 Hydration</span>
        <ArrowRight size={13} className="text-zinc-500" />
        <span className="bg-amber-900/40 text-amber-300 border border-amber-700 px-2 py-0.5 rounded text-xs font-mono">2. Seed STM &rarr; (button top-right)</span>
        <ArrowRight size={13} className="text-zinc-500" />
        <span className="bg-blue-900/40 text-blue-300 border border-blue-700 px-2 py-0.5 rounded text-xs font-mono">3. Run Phase 2 Pipelines</span>
        <span className="text-zinc-500 text-xs ml-2">STM links code SchemaObjects to DW targets — must seed after Phase 1 so schema nodes exist.</span>
      </div>

      {/* Stats row */}
      {stats && (
        <div className="grid grid-cols-3 gap-4">
          {[
            { label: 'STM Target Nodes',    value: stats.stm_nodes ?? 0,            color: 'text-violet-400' },
            { label: 'MAPS_TO Edges',        value: stats.maps_to_edges ?? 0,        color: 'text-blue-400'   },
            { label: 'Mapped Schema Objects',value: stats.mapped_schema_objects ?? 0,color: 'text-emerald-400'},
          ].map(s => (
            <div key={s.label} className="bg-surface-card rounded-lg border border-surface-border p-4">
              <p className={`text-2xl font-bold ${s.color}`}>{s.value}</p>
              <p className="text-xs text-zinc-500 mt-1">{s.label}</p>
            </div>
          ))}
        </div>
      )}

      {/* Tabs */}
      <div className="flex gap-1 border-b border-zinc-800">
        {[
          { id: 'mappings', label: 'Column Mappings' },
          { id: 'lineage',  label: 'Code → STM Lineage' },
        ].map(t => (
          <button
            key={t.id}
            onClick={() => setTab(t.id)}
            className={`px-4 py-2 text-sm font-medium transition-colors border-b-2 -mb-px
              ${tab === t.id ? 'border-violet-500 text-violet-300' : 'border-transparent text-zinc-500 hover:text-zinc-300'}`}
          >
            {t.label}
          </button>
        ))}
      </div>

      {/* Tab: Mappings */}
      {tab === 'mappings' && (
        <div className="bg-surface-card rounded-lg border border-surface-border overflow-hidden">
          <div className="px-4 py-3 border-b border-surface-border flex items-center justify-between">
            <p className="text-sm font-semibold text-zinc-300">
              SchemaObject → STM Mappings ({filteredMappings.length})
            </p>
            <input
              value={filter}
              onChange={e => setFilter(e.target.value)}
              placeholder="Filter by table / column / system…"
              className="bg-zinc-800 border border-zinc-700 rounded-md px-3 py-1.5 text-xs 
                         text-zinc-300 placeholder-zinc-600 focus:outline-none focus:border-violet-500 w-64"
            />
          </div>
          {loading ? (
            <div className="flex items-center justify-center py-16 text-zinc-500">
              <Loader2 size={24} className="animate-spin mr-3" />Loading mappings…
            </div>
          ) : filteredMappings.length === 0 ? (
            <div className="text-center py-16 text-zinc-600">
              <GitMerge size={32} className="mx-auto mb-3 opacity-30" />
              <p>No STM mappings found.</p>
              <p className="text-sm mt-1">Click <strong>Seed STM</strong> to load default mappings.</p>
            </div>
          ) : (
            <div className="overflow-x-auto">
              <table className="w-full text-sm">
                <thead>
                  <tr className="border-b border-zinc-800 bg-zinc-900/50">
                    <th className="text-left px-3 py-2.5 text-xs font-medium text-zinc-500 uppercase tracking-wide">Source (SchemaObject)</th>
                    <th className="px-3 py-2.5"></th>
                    <th className="text-left px-3 py-2.5 text-xs font-medium text-zinc-500 uppercase tracking-wide">Transform</th>
                    <th className="px-3 py-2.5"></th>
                    <th className="text-left px-3 py-2.5 text-xs font-medium text-zinc-500 uppercase tracking-wide">Target (STM)</th>
                    <th className="text-left px-3 py-2.5 text-xs font-medium text-zinc-500 uppercase tracking-wide">Owner</th>
                    <th className="text-right px-3 py-2.5 text-xs font-medium text-zinc-500 uppercase tracking-wide">Used By</th>
                  </tr>
                </thead>
                <tbody>
                  {filteredMappings.map((m, i) => <MappingRow key={i} m={m} />)}
                </tbody>
              </table>
            </div>
          )}
        </div>
      )}

      {/* Tab: Lineage */}
      {tab === 'lineage' && (
        <div>
          {loading ? (
            <div className="flex items-center justify-center py-16 text-zinc-500">
              <Loader2 size={24} className="animate-spin mr-3" />Loading lineage…
            </div>
          ) : Object.keys(lineage.by_system || {}).length === 0 ? (
            <div className="text-center py-16 text-zinc-600">
              <GitMerge size={32} className="mx-auto mb-3 opacity-30" />
              <p>No code→STM lineage paths found.</p>
              <p className="text-sm mt-1">Seed STM, then run Phase 1 pipeline to generate lineage.</p>
            </div>
          ) : (
            <div>
              <p className="text-sm text-zinc-500 mb-4">
                {lineage.total} lineage paths across {Object.keys(lineage.by_system).length} target systems
              </p>
              {Object.entries(lineage.by_system).map(([system, rows]) => (
                <LineageGroupCard key={system} system={system} rows={rows} />
              ))}
            </div>
          )}
        </div>
      )}
    </div>
  )
}
