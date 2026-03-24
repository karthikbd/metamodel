import { useState, useEffect } from 'react'
import { fetchPhase2Pipelines, resolvePhase2, seedPhase2Pipelines } from '../services/api'
import toast from 'react-hot-toast'
import {
  Cpu, RefreshCw, Loader2, Database, CheckCircle,
  ArrowRight, BookOpen, ChevronDown, ChevronRight, Table2
} from 'lucide-react'

const STATUS_COLORS = {
  active:   'bg-emerald-900/40 text-emerald-300 border-emerald-700',
  inactive: 'bg-zinc-800 text-zinc-400 border-zinc-600',
  error:    'bg-red-900/40 text-red-300 border-red-700',
}

function FieldTable({ title, rows, color }) {
  if (!rows || rows.length === 0)
    return <p className="text-xs text-zinc-600 italic py-2">No {title.toLowerCase()} fields resolved.</p>

  return (
    <div className="mb-4">
      <h4 className="text-xs font-semibold text-zinc-400 uppercase tracking-wide mb-2">{title}</h4>
      <div className="overflow-x-auto rounded border border-zinc-800">
        <table className="w-full text-xs">
          <thead className="bg-zinc-900/60">
            <tr>
              {['Table', 'Column', 'Type', 'Status', 'STM Target', 'STM System'].map(h => (
                <th key={h} className="text-left px-3 py-2 text-zinc-500 font-medium">{h}</th>
              ))}
            </tr>
          </thead>
          <tbody>
            {rows.map((r, i) => (
              <tr key={i} className="border-t border-zinc-800 hover:bg-zinc-800/30">
                <td className={`px-3 py-2 font-mono ${color}`}>{r.table}</td>
                <td className="px-3 py-2 font-mono text-zinc-300">{r.column}</td>
                <td className="px-3 py-2 text-zinc-500">{r.dtype || '—'}</td>
                <td className="px-3 py-2">
                  {r.status === 'deprecated'
                    ? <span className="text-amber-400 text-[10px]">⚠ deprecated</span>
                    : <span className="text-emerald-500 text-[10px]">active</span>}
                </td>
                <td className="px-3 py-2 font-mono text-violet-400">
                  {r.stm_target_table ? `${r.stm_target_table}.${r.stm_target_column}` : <span className="text-zinc-600">—</span>}
                </td>
                <td className="px-3 py-2 text-zinc-500 text-[10px]">{r.target_system || '—'}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  )
}

function STMTargetsPanel({ targets }) {
  if (!targets || targets.length === 0)
    return <p className="text-xs text-zinc-600 italic py-2">No STM targets. Seed STM mappings first.</p>

  return (
    <div className="overflow-x-auto rounded border border-zinc-800">
      <table className="w-full text-xs">
        <thead className="bg-zinc-900/60">
          <tr>
            {['Target Table', 'Target Column', 'System', 'Owner', 'Transform'].map(h => (
              <th key={h} className="text-left px-3 py-2 text-zinc-500 font-medium">{h}</th>
            ))}
          </tr>
        </thead>
        <tbody>
          {targets.map((t, i) => (
            <tr key={i} className="border-t border-zinc-800 hover:bg-zinc-800/30">
              <td className="px-3 py-2 font-mono text-emerald-300">{t.target_table}</td>
              <td className="px-3 py-2 font-mono text-zinc-300">{t.target_column}</td>
              <td className="px-3 py-2 text-zinc-500 text-[10px]">{t.target_system}</td>
              <td className="px-3 py-2 text-zinc-500 text-[10px]">{t.owner}</td>
              <td className="px-3 py-2">
                {t.transform_expr
                  ? <code className="text-amber-300 font-mono bg-zinc-900 px-1.5 py-0.5 rounded">{t.transform_expr}</code>
                  : <span className="text-zinc-600 italic">passthrough</span>}
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  )
}

function PipelineCard({ pipeline, onResolve, autoResolution }) {
  const [expanded,   setExpanded]   = useState(!!autoResolution)
  const [resolving,  setResolving]  = useState(false)
  const [resolution, setResolution] = useState(autoResolution || null)

  const handleResolve = async () => {
    setResolving(true)
    try {
      const data = await onResolve(pipeline.pipeline_id)
      setResolution(data.resolution)
      setExpanded(true)
    } catch (e) {
      toast.error('Resolution failed: ' + (e.response?.data?.detail || e.message))
    } finally {
      setResolving(false)
    }
  }

  const statusCls = STATUS_COLORS[pipeline.status] || STATUS_COLORS.inactive

  return (
    <div className="bg-surface-card rounded-lg border border-surface-border overflow-hidden">
      {/* Header */}
      <div className="px-4 py-3 flex items-start justify-between">
        <div className="flex-1">
          <div className="flex items-center gap-2 mb-1">
            <span className={`text-[10px] px-2 py-0.5 rounded border font-mono ${statusCls}`}>
              {pipeline.status || 'active'}
            </span>
            <h3 className="text-sm font-semibold text-zinc-100">{pipeline.name}</h3>
          </div>
          <p className="text-xs text-zinc-500 mb-2">{pipeline.description}</p>
          <div className="flex items-center gap-4 text-[10px] font-mono text-zinc-600">
            <span>id: {pipeline.pipeline_id}</span>
            <span>fn: {pipeline.function_name} ({pipeline.function_path?.split('/').pop()})</span>
          </div>
        </div>
        <div className="flex items-center gap-2 ml-4">
          <button
            onClick={handleResolve}
            disabled={resolving}
            className="flex items-center gap-1.5 px-3 py-1.5 rounded-md bg-blue-700 hover:bg-blue-600
                       text-xs text-white border border-blue-600 transition-colors whitespace-nowrap"
          >
            {resolving ? <Loader2 size={11} className="animate-spin" /> : <Cpu size={11} />}
            Resolve Fields
          </button>
          <button
            onClick={() => setExpanded(e => !e)}
            className="p-1.5 rounded-md bg-zinc-800 hover:bg-zinc-700 text-zinc-400 border border-zinc-700"
          >
            {expanded ? <ChevronDown size={13} /> : <ChevronRight size={13} />}
          </button>
        </div>
      </div>

      {/* Resolution panel */}
      {expanded && resolution && (
        <div className="border-t border-surface-border px-4 pt-4 pb-5 bg-zinc-900/30">
          {/* Summary counts */}
          <div className="grid grid-cols-3 gap-3 mb-5">
            {[
              { label: 'Source Reads',    value: resolution.total_source_fields, color: 'text-blue-400'   },
              { label: 'Source Writes',   value: resolution.total_target_fields, color: 'text-amber-400'  },
              { label: 'STM DW Targets',  value: resolution.total_stm_targets,   color: 'text-violet-400' },
            ].map(s => (
              <div key={s.label} className="bg-zinc-800/60 rounded-lg p-3 border border-zinc-700">
                <p className={`text-xl font-bold ${s.color}`}>{s.value}</p>
                <p className="text-[10px] text-zinc-500 mt-0.5">{s.label}</p>
              </div>
            ))}
          </div>

          {/* The "before/after" value prop */}
          <div className="mb-5 p-3 rounded-lg bg-zinc-800/40 border border-zinc-700">
            <div className="flex items-center gap-2 mb-2">
              <BookOpen size={13} className="text-emerald-400" />
              <span className="text-xs font-semibold text-emerald-300">Phase 2 Value — No Hardcoded Column Names</span>
            </div>
            <p className="text-[11px] text-zinc-400 leading-relaxed">
              This pipeline dynamically resolved <strong className="text-zinc-200">{resolution.total_source_fields} source fields</strong> and{' '}
              <strong className="text-zinc-200">{resolution.total_stm_targets} DW targets</strong> from the knowledge graph at startup.{' '}
              When columns are deprecated or renamed, the graph updates — the pipeline re-resolves on next startup automatically.
            </p>
          </div>

          <FieldTable
            title="Source Reads (verified confidence only)"
            rows={resolution.reads}
            color="text-blue-300"
          />
          <FieldTable
            title="Source Writes (verified confidence only)"
            rows={resolution.writes}
            color="text-amber-300"
          />

          <div>
            <h4 className="text-xs font-semibold text-zinc-400 uppercase tracking-wide mb-2 flex items-center gap-1.5">
              <Table2 size={12} />
              STM Target Columns
            </h4>
            <STMTargetsPanel targets={resolution.stm_targets} />
          </div>
        </div>
      )}

      {expanded && !resolution && (
        <div className="border-t border-surface-border px-4 py-6 text-center text-zinc-600 text-sm bg-zinc-900/30">
          Click <strong className="text-zinc-400">Resolve Fields</strong> to query the graph
        </div>
      )}
    </div>
  )
}

export default function Phase2Page() {
  const [pipelines,     setPipelines]     = useState([])
  const [loading,       setLoading]       = useState(false)
  const [seeding,       setSeeding]       = useState(false)
  const [resolutions,   setResolutions]   = useState({})   // {pipeline_id: resolution}
  const [autoResolving, setAutoResolving] = useState(false)

  const load = async () => {
    setLoading(true)
    try {
      const data = await fetchPhase2Pipelines()
      const list = data.pipelines || []
      setPipelines(list)

      if (list.length > 0) {
        // Auto-resolve every pipeline so the page is fully populated on load
        setAutoResolving(true)
        const results = await Promise.allSettled(
          list.map(p => resolvePhase2(p.pipeline_id).then(r => [p.pipeline_id, r.resolution]))
        )
        const resolved = {}
        results.forEach(r => { if (r.status === 'fulfilled') resolved[r.value[0]] = r.value[1] })
        setResolutions(resolved)
        setAutoResolving(false)
      }
    } catch {
      toast.error('Failed to load pipelines')
    } finally {
      setLoading(false)
    }
  }

  useEffect(() => { load() }, [])

  const handleSeed = async () => {
    setSeeding(true)
    try {
      const res = await seedPhase2Pipelines()
      toast.success(
        `Seeded ${res.registered?.length ?? 0} pipelines` +
        (res.skipped_functions_not_found?.length
          ? ` (${res.skipped_functions_not_found.length} functions not found — run Phase 1 first)`
          : '')
      )
      await load()
    } catch (e) {
      toast.error('Seed failed: ' + (e.response?.data?.detail || e.message))
    } finally {
      setSeeding(false)
    }
  }

  const handleResolve = async (pipelineId) => {
    return await resolvePhase2(pipelineId)
  }

  return (
    <div className="p-6 space-y-6 min-h-screen bg-surface text-zinc-200">
      {/* Header */}
      <div className="flex items-start justify-between">
        <div>
          <h1 className="text-xl font-bold text-zinc-100 flex items-center gap-2">
            <Cpu size={20} className="text-blue-400" />
            Phase 2 — Runtime Field Resolution
          </h1>
          <p className="text-sm text-zinc-500 mt-1">
            Pipelines query the knowledge graph at startup to resolve source fields and DW targets
            dynamically — eliminating hardcoded column names
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
            className="flex items-center gap-1.5 px-3 py-1.5 rounded-md bg-blue-700 hover:bg-blue-600
                       text-sm text-white border border-blue-600 transition-colors"
          >
            {seeding ? <Loader2 size={13} className="animate-spin" /> : <Database size={13} />}
            Seed Demo Pipelines
          </button>
        </div>
      </div>

      {/* Workflow prerequisites */}
      <div className="rounded-lg border border-amber-600/40 bg-amber-900/10 px-4 py-3 text-sm text-zinc-300 flex flex-wrap items-center gap-2">
        <span className="font-semibold text-amber-300">Prerequisites:</span>
        <span className="bg-violet-900/40 text-violet-300 border border-violet-700 px-2 py-0.5 rounded text-xs font-mono">1. Phase 1 Hydration</span>
        <ArrowRight size={13} className="text-zinc-500" />
        <span className="bg-amber-900/40 text-amber-300 border border-amber-700 px-2 py-0.5 rounded text-xs font-mono">2. Seed STM (STM page)</span>
        <ArrowRight size={13} className="text-zinc-500" />
        <span className="bg-blue-900/40 text-blue-300 border border-blue-700 px-2 py-0.5 rounded text-xs font-mono">3. Seed Demo Pipelines &rarr; (button top-right)</span>
        <span className="text-zinc-500 text-xs ml-2">Pipelines must link to functions that exist in the graph.</span>
      </div>

      {/* TL;DR — plain-English explanation */}
      <div className="rounded-lg border border-zinc-700 bg-zinc-800/40 px-5 py-4">
        <h2 className="text-sm font-semibold text-zinc-200 mb-2 flex items-center gap-2">
          <BookOpen size={15} className="text-zinc-400" />
          What does this page do?
        </h2>
        <p className="text-xs text-zinc-400 leading-relaxed">
          After Phase 1 scans your code,{' '}
          <strong className="text-zinc-200">Phase 2</strong> lets a live pipeline look up
          which columns it needs — directly from the knowledge graph — instead of having
          column names hardcoded in the script.
        </p>
        <div className="mt-3 grid grid-cols-3 gap-3 text-xs">
          <div className="bg-zinc-900/60 rounded-lg px-3 py-2 border border-zinc-700">
            <p className="text-violet-300 font-semibold mb-1">① Register a Pipeline</p>
            <p className="text-zinc-500">Link a pipeline name to a Python function that exists in the graph</p>
          </div>
          <div className="bg-zinc-900/60 rounded-lg px-3 py-2 border border-zinc-700">
            <p className="text-blue-300 font-semibold mb-1">② Resolve Fields</p>
            <p className="text-zinc-500">Click "Resolve Fields" — the graph returns which columns the function reads / writes</p>
          </div>
          <div className="bg-zinc-900/60 rounded-lg px-3 py-2 border border-zinc-700">
            <p className="text-emerald-300 font-semibold mb-1">③ See DW Targets</p>
            <p className="text-zinc-500">See which Data Warehouse tables those columns map to via the STM</p>
          </div>
        </div>
      </div>

      {/* How it works callout */}
      <div className="rounded-lg border border-blue-800/50 bg-blue-900/10 px-5 py-4">
        <h2 className="text-sm font-semibold text-blue-300 mb-2 flex items-center gap-2">
          <CheckCircle size={15} />
          How Phase 2 Works
        </h2>
        <div className="flex items-center gap-3 text-xs text-zinc-400 flex-wrap">
          <div className="flex items-center gap-2">
            <span className="px-2 py-1 rounded bg-zinc-800 text-zinc-300 border border-zinc-700 font-mono">Pipeline starts</span>
            <ArrowRight size={12} className="text-zinc-600" />
            <span className="px-2 py-1 rounded bg-zinc-800 text-zinc-300 border border-zinc-700 font-mono">Queries graph by pipeline_id</span>
            <ArrowRight size={12} className="text-zinc-600" />
            <span className="px-2 py-1 rounded bg-zinc-800 text-zinc-300 border border-zinc-700 font-mono">Gets verified fields only</span>
            <ArrowRight size={12} className="text-zinc-600" />
            <span className="px-2 py-1 rounded bg-blue-800 text-blue-200 border border-blue-700 font-mono">Executes with dynamic column list</span>
          </div>
        </div>
        <p className="text-[11px] text-zinc-500 mt-2">
          Only <code className="text-blue-300">confidence = verified</code> edges are used — inferred edges never drive production execution.
          When schema changes, update the graph → pipelines adapt on next restart.
        </p>
      </div>

      {/* Pipelines */}
      {loading ? (
        <div className="flex items-center justify-center py-24 text-zinc-500">
          <Loader2 size={28} className="animate-spin mr-3" />
          Loading registered pipelines…
        </div>
      ) : pipelines.length === 0 ? (
        <div className="text-center py-24 text-zinc-600">
          <Cpu size={40} className="mx-auto mb-4 opacity-20" />
          <p className="text-base">No pipelines registered yet.</p>
          <p className="text-sm mt-1 mb-5">Run Phase 1 hydration — pipelines are registered automatically at the end.</p>
          <p className="text-xs text-zinc-700">Or click <strong className="text-zinc-400">Seed Demo Pipelines</strong> above to register manually.</p>
        </div>
      ) : (
        <div className="space-y-4">
          <div className="flex items-center gap-3">
            <p className="text-sm text-zinc-500">{pipelines.length} registered pipeline{pipelines.length !== 1 ? 's' : ''}</p>
            {autoResolving && (
              <span className="flex items-center gap-1.5 text-xs text-blue-400">
                <Loader2 size={11} className="animate-spin" />
                Auto-resolving fields…
              </span>
            )}
          </div>
          {pipelines.map(p => (
            <PipelineCard
              key={p.pipeline_id}
              pipeline={p}
              onResolve={handleResolve}
              autoResolution={resolutions[p.pipeline_id]}
            />
          ))}
        </div>
      )}
    </div>
  )
}
