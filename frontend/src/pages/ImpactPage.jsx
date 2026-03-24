import { useState, useEffect } from 'react'
import { Zap, Search } from 'lucide-react'
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

function ImpactTable({ rows }) {
  if (!rows || rows.length === 0)
    return <p className="text-sm text-zinc-600 py-6 text-center">No affected functions found.</p>

  return (
    <div className="card p-0 overflow-hidden overflow-x-auto">
      <table className="w-full text-xs">
        <thead>
          <tr className="border-b border-surface-border bg-surface-card">
            {['Job', 'Path', 'Relationship', 'Callers', 'Risk Tags'].map(h => (
              <th key={h} className="text-left px-4 py-2 font-normal text-zinc-500">{h}</th>
            ))}
          </tr>
        </thead>
        <tbody className="divide-y divide-surface-border">
          {rows.map((r, i) => (
            <tr key={i} className="hover:bg-surface-hover/50">
              <td className="px-4 py-2 font-mono text-violet-300">{r.name || r.job_name || '—'}</td>
              <td className="px-4 py-2 font-mono text-zinc-500">{r.path || '—'}</td>
              <td className="px-4 py-2">
                {r.relationship && (
                  <span className={r.relationship === 'WRITES_TO' ? 'badge-warn' : 'badge-info'}>
                    {r.relationship}
                  </span>
                )}
              </td>
              <td className="px-4 py-2 text-zinc-400">
                {(r.callers || []).filter(Boolean).join(', ') || '—'}
              </td>
              <td className="px-4 py-2">
                <div className="flex flex-wrap gap-1">
                  {(r.risk_tags || []).map(t => <RiskTag key={t} tag={t} />)}
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
  const [mode,      setMode]      = useState('column')   // 'column' | 'dataset'
  const [table,     setTable]     = useState('')
  const [column,    setColumn]    = useState('')
  const [dataset,   setDataset]   = useState('')
  const [loading,   setLoading]   = useState(false)
  const [result,    setResult]    = useState(null)
  const [tables,    setTables]    = useState([])
  const [columns,   setColumns]   = useState([])

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
        if (!table || !column) { toast.error('Select dataset and column'); return }
        const data = await fetchColumnImpact(table, column)
        setResult({ ...data, _mode: 'column' })
      } else {
        if (!dataset) { toast.error('Select a dataset'); return }
        const data = await fetchDatasetImpact(dataset)
        setResult({ ...data, _mode: 'dataset', count: (data.result || []).length, affected: data.result || [] })
      }
    } catch {
      toast.error('Impact analysis failed')
    } finally {
      setLoading(false)
    }
  }

  return (
    <div className="p-6 space-y-5">
      <div>
        <h1 className="text-xl font-semibold text-zinc-100">Impact Analysis</h1>
        <p className="text-sm text-zinc-500 mt-0.5">
          Find every job affected by a schema change or dataset modification — before making the change
        </p>
      </div>

      {/* Mode tabs */}
      <div className="flex gap-1 border-b border-surface-border">
        {[['column','Column Rename'], ['dataset','Dataset Impact']].map(([id, label]) => (
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

      {/* Inputs */}
      <div className="card space-y-4">
        {mode === 'column' ? (
          <div className="grid grid-cols-2 gap-3">
            <div>
              <label className="text-xs text-zinc-500 mb-1 block">Table name</label>
              <select
                value={table}
                onChange={e => {
                  const t = e.target.value
                  setTable(t); setColumn('')
                  setColumns([])
                  if (t) fetchColumns(t).then(cols => {
                    setColumns(Array.isArray(cols) ? cols.map(c => c.name).sort() : [])
                  }).catch(() => {})
                }}
                className="w-full bg-surface border border-surface-border rounded-md px-3 py-2
                           text-sm text-zinc-200 font-mono focus:outline-none focus:border-accent"
              >
                <option value="">-- select table --</option>
                {tables.map(t => <option key={t} value={t}>{t}</option>)}
              </select>
            </div>
            <div>
              <label className="text-xs text-zinc-500 mb-1 block">Column name</label>
              <select
                value={column}
                onChange={e => setColumn(e.target.value)}
                disabled={!table}
                className="w-full bg-surface border border-surface-border rounded-md px-3 py-2
                           text-sm text-zinc-200 font-mono focus:outline-none focus:border-accent
                           disabled:opacity-50 disabled:cursor-not-allowed"
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
              className="w-full bg-surface border border-surface-border rounded-md px-3 py-2
                         text-sm text-zinc-200 font-mono focus:outline-none focus:border-accent"
            >
              <option value="">-- select dataset --</option>
              {tables.map(t => <option key={t} value={t}>{t}</option>)}
            </select>
          </div>
        )}

        <button
          onClick={analyze}
          disabled={loading}
          className="btn-primary flex items-center gap-2"
        >
          <Zap size={13} /> {loading ? 'Analysing…' : 'Analyse Impact'}
        </button>
      </div>

      {/* Results */}
      {result && (
        <div className="space-y-3">
          <div className="flex items-center gap-2">
            <p className="text-sm text-zinc-300">
              <span className="font-semibold text-zinc-100">{result.count}</span> affected function{result.count !== 1 ? 's' : ''}
            </p>
            {result.count > 0 && (
              <span className="badge-warn">{result.count} change{result.count !== 1 ? 's' : ''} required</span>
            )}
          </div>
          <ImpactTable rows={result.affected} />
        </div>
      )}
    </div>
  )
}
