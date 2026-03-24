import { useState, useEffect } from 'react'
import { ShieldAlert, Play } from 'lucide-react'
import { fetchComplianceQueries, runComplianceQuery } from '../services/api'
import toast from 'react-hot-toast'

const QUERY_META = {
  pii_without_audit: {
    desc:  'Functions writing to PII-classified columns without an audit decorator.',
    severity: 'high',
  },
  regulatory_report_lineage: {
    desc:  'All functions tagged regulatory_report and their source tables.',
    severity: 'medium',
  },
  deprecated_columns_in_use: {
    desc:  'Functions still reading from DEPRECATED schema objects.',
    severity: 'medium',
  },
}

function SeverityBadge({ s }) {
  const cls = { high: 'badge-error', medium: 'badge-warn', low: 'badge-info' }
  return <span className={cls[s] || 'badge-info'}>{s}</span>
}

function ResultTable({ rows }) {
  if (!rows || rows.length === 0)
    return <p className="text-sm text-zinc-600 py-4 text-center">No issues found. ✓</p>
  const cols = Object.keys(rows[0])
  return (
    <div className="overflow-x-auto">
      <table className="w-full text-xs">
        <thead>
          <tr className="border-b border-surface-border">
            {cols.map(c => (
              <th key={c} className="text-left px-3 py-2 font-normal text-zinc-500 font-mono">{c}</th>
            ))}
          </tr>
        </thead>
        <tbody className="divide-y divide-surface-border">
          {rows.map((row, i) => (
            <tr key={i} className="hover:bg-surface-hover/50">
              {cols.map(c => (
                <td key={c} className="px-3 py-2 text-zinc-300 font-mono">
                  {Array.isArray(row[c]) ? row[c].join(', ') : String(row[c] ?? '—')}
                </td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  )
}

function QueryCard({ query, meta }) {
  const [loading, setLoading] = useState(false)
  const [result,  setResult]  = useState(null)

  async function run() {
    setLoading(true)
    try {
      const res = await runComplianceQuery(query.id)
      setResult(res)
    } catch {
      toast.error('Query failed')
    } finally {
      setLoading(false)
    }
  }

  return (
    <div className="card space-y-3">
      <div className="flex items-start justify-between gap-3">
        <div className="flex items-start gap-3">
          <ShieldAlert size={16} className="text-zinc-500 mt-0.5 flex-shrink-0" />
          <div>
            <p className="text-sm font-medium text-zinc-200">{query.label}</p>
            <p className="text-xs text-zinc-500 mt-0.5">{meta?.desc}</p>
          </div>
        </div>
        <div className="flex items-center gap-2 flex-shrink-0">
          {meta?.severity && <SeverityBadge s={meta.severity} />}
          <button
            onClick={run}
            disabled={loading}
            className="btn-primary text-xs py-1 flex items-center gap-1"
          >
            <Play size={10} /> {loading ? 'Running…' : 'Run'}
          </button>
        </div>
      </div>
      {result && (
        <div className="border-t border-surface-border pt-3">
          <p className="text-xs text-zinc-500 mb-2">{result.count} result{result.count !== 1 ? 's' : ''}</p>
          <ResultTable rows={result.rows} />
        </div>
      )}
    </div>
  )
}

export default function CompliancePage() {
  const [queries, setQueries] = useState([])

  useEffect(() => {
    fetchComplianceQueries().then(setQueries).catch(() => {
      // Fallback to hardcoded list
      setQueries([
        { id: 'pii_without_audit',       label: 'PII Without Audit Decorator' },
        { id: 'regulatory_report_lineage', label: 'Regulatory Report Lineage'  },
        { id: 'deprecated_columns_in_use', label: 'Deprecated Columns In Use'  },
      ])
    })
  }, [])

  return (
    <div className="p-6 space-y-4">
      <div>
        <h1 className="text-xl font-semibold text-zinc-100">Compliance Queries</h1>
        <p className="text-sm text-zinc-500 mt-0.5">
          Regulatory and governance checks answered by single Cypher traversals
        </p>
      </div>

      <div className="card border-l-2 border-amber-500 text-xs text-zinc-400 space-y-1">
        <p className="font-semibold text-amber-400">Answer quality = graph completeness</p>
        <p>These queries are only as accurate as the hydration run that populated the graph. Run Phase 1 on your full codebase before running compliance checks.</p>
      </div>

      <div className="space-y-3">
        {queries.map(q => (
          <QueryCard key={q.id} query={q} meta={QUERY_META[q.id]} />
        ))}
      </div>
    </div>
  )
}
