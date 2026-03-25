import { useState, useEffect } from 'react'
import { ShieldAlert, Play, CheckCircle2, AlertTriangle } from 'lucide-react'
import { fetchComplianceQueries, runComplianceQuery } from '../services/api'
import toast from 'react-hot-toast'

const QUERY_META = {
  pii_without_audit: {
    desc:  'Finds jobs tagged PII that are missing the audit_required control.',
    severity: 'high',
    empty: 'No un-audited PII jobs found.',
    action: 'Add audit coverage before allowing the job further downstream.',
  },
  regulatory_report_lineage: {
    desc:  'Shows regulatory-reporting jobs and the source datasets that feed them.',
    severity: 'medium',
    empty: 'No regulatory-reporting lineage found.',
    action: 'Verify that all report-producing jobs are tagged and linked to source datasets.',
  },
  deprecated_columns_in_use: {
    desc:  'Finds jobs still reading deprecated columns that should be retired.',
    severity: 'medium',
    empty: 'No deprecated columns are currently in use.',
    action: 'Remove or migrate old column references before the next schema cleanup.',
  },
}

const COL_LABELS = {
  job_id: 'Job ID',
  job_name: 'Job',
  name: 'Job',
  path: 'File',
  pii_columns: 'PII Columns',
  pii_column_count: 'PII Count',
  source_datasets: 'Source Datasets',
  source_dataset_count: 'Source Count',
  deprecated_column: 'Deprecated Column',
  risk_tags: 'Risk Tags',
}

function SeverityBadge({ s }) {
  const cls = { high: 'badge-error', medium: 'badge-warn', low: 'badge-info' }
  return <span className={cls[s] || 'badge-info'}>{s}</span>
}

function StatusBadge({ count }) {
  return count > 0
    ? <span className="badge-error">{count} finding{count !== 1 ? 's' : ''}</span>
    : <span className="badge-info">clear</span>
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
              <th key={c} className="text-left px-3 py-2 font-normal text-zinc-500 font-mono">{COL_LABELS[c] || c}</th>
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

  useEffect(() => {
    run()
    // Run once on mount so the tab is immediately useful.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [])

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
          {result && <StatusBadge count={result.count} />}
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
          <div className="flex items-start gap-2 text-xs mb-3">
            {result.count > 0 ? (
              <AlertTriangle size={14} className="text-amber-400 mt-0.5 flex-shrink-0" />
            ) : (
              <CheckCircle2 size={14} className="text-emerald-400 mt-0.5 flex-shrink-0" />
            )}
            <div className="text-zinc-400">
              <p className="mb-1">
                <span className="font-semibold text-zinc-200">{result.count}</span> result{result.count !== 1 ? 's' : ''}.
                {' '}{result.count > 0 ? meta?.action : meta?.empty}
              </p>
            </div>
          </div>
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
          This tab explains whether your graph has governance gaps: missing audit coverage, report lineage, or deprecated schema still in use.
        </p>
      </div>

      <div className="grid md:grid-cols-3 gap-3">
        <div className="card border-l-2 border-sky-500 text-sm text-zinc-300">
          <p className="font-medium text-sky-300 mb-1">What it does</p>
          <p>Runs three pre-built checks against the graph so you can spot control gaps without writing Cypher.</p>
        </div>
        <div className="card border-l-2 border-amber-500 text-sm text-zinc-300">
          <p className="font-medium text-amber-300 mb-1">How to read it</p>
          <p>If a card shows findings, that means some jobs or datasets need cleanup. Zero findings means that check is currently clear.</p>
        </div>
        <div className="card border-l-2 border-violet-500 text-sm text-zinc-300">
          <p className="font-medium text-violet-300 mb-1">Why it matters</p>
          <p>These checks are governance-focused. They support lineage work, but they are not the same thing as scheduler impact.</p>
        </div>
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
