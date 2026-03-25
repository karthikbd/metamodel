import { useState, useEffect } from 'react'
import { Zap, AlertTriangle, ArrowRightLeft } from 'lucide-react'
import { fetchColumnImpact, fetchDatasetImpact, fetchDatasets, fetchColumns } from '../services/api'
import toast from 'react-hot-toast'

function RiskTag({ tag }) {
  const cls = {
    PII:               'badge-error',
    audit_required:    'badge-warn',
    regulatory_report: 'badge-info',
  }
  return <span className={cls[tag] || 'badge-queued'}>{tag}</span>
}

function ImpactLevelBadge({ level }) {
  const cls = {
    critical: 'badge-error',
    high:     'badge-error',
    medium:   'badge-warn',
    low:      'badge-info',
  }
  return <span className={cls[level] || 'badge-info'}>{level || 'low'}</span>
}

function SummaryCard({ label, value, subtle }) {
  return (
    <div className="card py-3 px-4">
      <p className="text-[11px] uppercase tracking-wide text-zinc-500">{label}</p>
      <p className={`mt-1 text-lg font-semibold ${subtle ? 'text-zinc-300' : 'text-zinc-100'}`}>{value}</p>
    </div>
  )
}

function ImpactTable({ rows }) {
  if (!rows || rows.length === 0) {
    return <p className="text-sm text-zinc-600 py-6 text-center">No affected jobs found. Lucky escape.</p>
  }

  return (
    <div className="card p-0 overflow-hidden overflow-x-auto">
      <table className="w-full text-xs">
        <thead>
          <tr className="border-b border-surface-border bg-surface-card">
            {['Job', 'File', 'Touch Type', 'Callers', 'Risk Tags', 'Score'].map(h => (
              <th key={h} className="text-left px-4 py-2 font-normal text-zinc-500">{h}</th>
            ))}
          </tr>
        </thead>
        <tbody className="divide-y divide-surface-border">
          {rows.map((r, i) => (
            <tr key={i} className="hover:bg-surface-hover/50 align-top">
              <td className="px-4 py-2 font-mono text-violet-300">{r.name || r.job_name || '—'}</td>
              <td className="px-4 py-2 font-mono text-zinc-500">{r.path || '—'}</td>
              <td className="px-4 py-2">
                {r.relationship && r.relationship !== '—' ? (
                  <span className={String(r.relationship).includes('WRITES_TO') ? 'badge-warn' : 'badge-info'}>
                    {r.relationship}
                  </span>
                ) : '—'}
              </td>
              <td className="px-4 py-2 text-zinc-400">{(r.callers || []).filter(Boolean).join(', ') || '—'}</td>
              <td className="px-4 py-2">
                <div className="flex flex-wrap gap-1">
                  {(r.risk_tags || []).map(t => <RiskTag key={t} tag={t} />)}
                  {(!r.risk_tags || r.risk_tags.length === 0) && <span className="text-zinc-600">—</span>}
                </div>
              </td>
              <td className="px-4 py-2">
                <div className="flex flex-col gap-1">
                  <span className="font-semibold text-zinc-100">{r.impact_score ?? 0}</span>
                  <ImpactLevelBadge level={r.impact_level} />
                </div>
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  )
}

export default function ImpactPage() {
  const [mode, setMode] = useState('column')
  const [table, setTable] = useState('')
  const [column, setColumn] = useState('')
  const [dataset, setDataset] = useState('')
  const [loading, setLoading] = useState(false)
  const [result, setResult] = useState(null)
  const [tables, setTables] = useState([])
  const [columns, setColumns] = useState([])

  useEffect(() => {
    fetchDatasets().then(datasets => {
      if (!Array.isArray(datasets)) return
      setTables(datasets.map(d => d.name).sort())
    }).catch(() => {})
  }, [])

  async function analyze() {
    setLoading(true)
    setResult(null)
    try {
      if (mode === 'column') {
        if (!table || !column) {
          toast.error('Select dataset and column')
          return
        }
        const data = await fetchColumnImpact(table, column)
        setResult({ ...data, _mode: 'column', title: `${table}.${column}` })
      } else {
        if (!dataset) {
          toast.error('Select a dataset')
          return
        }
        const data = await fetchDatasetImpact(dataset)
        setResult({ ...data, _mode: 'dataset', title: dataset })
      }
    } catch {
      toast.error('Impact analysis failed')
    } finally {
      setLoading(false)
    }
  }

  const summary = result?.summary || {
    affected_jobs: 0,
    writers: 0,
    readers: 0,
    downstream_callers: 0,
    pii_jobs: 0,
    regulatory_jobs: 0,
    audit_jobs: 0,
    impact_score: 0,
    impact_level: 'low',
  }

  return (
    <div className="p-6 space-y-5">
      <div>
        <h1 className="text-xl font-semibold text-zinc-100">Impact Analysis</h1>
        <p className="text-sm text-zinc-500 mt-0.5">
          This tab tells you which jobs will be touched by a schema change and how risky that blast radius is.
        </p>
      </div>

      <div className="grid md:grid-cols-3 gap-3">
        <div className="card border-l-2 border-sky-500 text-sm text-zinc-300">
          <div className="flex items-center gap-2 text-sky-300 font-medium mb-1"><ArrowRightLeft size={14} /> What it checks</div>
          <p>Readers, writers, downstream callers, and high-risk tags such as PII and regulatory reporting.</p>
        </div>
        <div className="card border-l-2 border-amber-500 text-sm text-zinc-300">
          <div className="flex items-center gap-2 text-amber-300 font-medium mb-1"><AlertTriangle size={14} /> How score works</div>
          <p>Writers score higher than readers. More downstream callers and sensitive tags push the score upward.</p>
        </div>
        <div className="card border-l-2 border-violet-500 text-sm text-zinc-300">
          <div className="flex items-center gap-2 text-violet-300 font-medium mb-1"><Zap size={14} /> When to use it</div>
          <p>Use it before renaming columns, changing dataset contracts, or rewiring scheduler-driven pipelines.</p>
        </div>
      </div>

      <div className="flex gap-1 border-b border-surface-border">
        {[['column', 'Column Change'], ['dataset', 'Dataset Change']].map(([id, label]) => (
          <button
            key={id}
            onClick={() => { setMode(id); setResult(null) }}
            className={`px-4 py-2 text-sm border-b-2 transition-colors ${
              mode === id
                ? 'border-accent text-accent-text'
                : 'border-transparent text-zinc-500 hover:text-zinc-300'
            }`}
          >
            {label}
          </button>
        ))}
      </div>

      <div className="card space-y-4">
        {mode === 'column' ? (
          <div className="grid grid-cols-2 gap-3">
            <div>
              <label className="text-xs text-zinc-500 mb-1 block">Dataset name</label>
              <select
                value={table}
                onChange={e => {
                  const t = e.target.value
                  setTable(t)
                  setColumn('')
                  setColumns([])
                  if (t) {
                    fetchColumns(t).then(cols => {
                      setColumns(Array.isArray(cols) ? cols.map(c => c.name).sort() : [])
                    }).catch(() => {})
                  }
                }}
                className="w-full bg-surface border border-surface-border rounded-md px-3 py-2 text-sm text-zinc-200 font-mono focus:outline-none focus:border-accent"
              >
                <option value="">-- select dataset --</option>
                {tables.map(t => <option key={t} value={t}>{t}</option>)}
              </select>
            </div>
            <div>
              <label className="text-xs text-zinc-500 mb-1 block">Column name</label>
              <select
                value={column}
                onChange={e => setColumn(e.target.value)}
                disabled={!table}
                className="w-full bg-surface border border-surface-border rounded-md px-3 py-2 text-sm text-zinc-200 font-mono focus:outline-none focus:border-accent disabled:opacity-50 disabled:cursor-not-allowed"
              >
                <option value="">-- select column --</option>
                {columns.map(c => <option key={c} value={c}>{c}</option>)}
              </select>
            </div>
          </div>
        ) : (
          <div>
            <label className="text-xs text-zinc-500 mb-1 block">Dataset name</label>
            <select
              value={dataset}
              onChange={e => setDataset(e.target.value)}
              className="w-full bg-surface border border-surface-border rounded-md px-3 py-2 text-sm text-zinc-200 font-mono focus:outline-none focus:border-accent"
            >
              <option value="">-- select dataset --</option>
              {tables.map(t => <option key={t} value={t}>{t}</option>)}
            </select>
          </div>
        )}

        <button onClick={analyze} disabled={loading} className="btn-primary flex items-center gap-2">
          <Zap size={13} /> {loading ? 'Analysing…' : 'Analyse Impact'}
        </button>
      </div>

      {result && (
        <div className="space-y-4">
          <div className="flex items-center gap-3 flex-wrap">
            <p className="text-sm text-zinc-300">
              Impact for <span className="font-semibold text-zinc-100 font-mono">{result.title}</span>
            </p>
            <ImpactLevelBadge level={summary.impact_level} />
            <span className="text-sm text-zinc-400">Score {summary.impact_score}/100</span>
          </div>

          <div className="grid sm:grid-cols-2 lg:grid-cols-4 gap-3">
            <SummaryCard label="Affected jobs" value={summary.affected_jobs} />
            <SummaryCard label="Writers / Readers" value={`${summary.writers} / ${summary.readers}`} subtle />
            <SummaryCard label="Downstream callers" value={summary.downstream_callers} subtle />
            <SummaryCard label="Sensitive jobs" value={`${summary.pii_jobs} PII • ${summary.regulatory_jobs} Reg`} subtle />
          </div>

          <div className="card border-l-2 border-accent text-sm text-zinc-300">
            <p>
              <span className="font-semibold text-zinc-100">How to read this:</span>{' '}
              higher scores usually mean the job writes the object, has more downstream dependents, or carries higher-risk tags.
            </p>
          </div>

          <ImpactTable rows={result.affected || []} />
        </div>
      )}
    </div>
  )
}
