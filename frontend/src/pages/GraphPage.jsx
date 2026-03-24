import { useState, useEffect } from 'react'
import { Search, Play } from 'lucide-react'
import { runCypher, fetchJobs, fetchDatasets } from '../services/api'
import toast from 'react-hot-toast'

const SAMPLE_QUERIES = [
  { label: 'All jobs',                q: "MATCH (j:Job) RETURN j.name AS name, coalesce(j.path, j.domain, '') AS path, coalesce(j.risk_tags, []) AS risk_tags LIMIT 25" },
  { label: 'All datasets',           q: 'MATCH (d:Dataset) RETURN d.name AS name, d.qualified_name AS qualified_name, d.format AS format, d.status AS status LIMIT 25' },
  { label: 'Job → Dataset',          q: 'MATCH (j:Job)-[r:READS_FROM|WRITES_TO]->(d:Dataset) RETURN j.name, type(r), d.name LIMIT 30' },
  { label: 'All columns',            q: 'MATCH (d:Dataset)-[:HAS_COLUMN]->(c:Column) RETURN d.name AS dataset, c.name AS column, c.data_type AS dtype, c.status AS status LIMIT 25' },
  { label: 'PII-tagged jobs',        q: "MATCH (j:Job) WHERE 'PII' IN j.risk_tags RETURN j.name AS name, coalesce(j.path, j.domain, '') AS path, coalesce(j.risk_tags, []) AS risk_tags LIMIT 25" },
  { label: 'Job dependencies',       q: 'MATCH (a:Job)-[r:DEPENDS_ON]->(b:Job) RETURN a.name AS caller, b.name AS callee LIMIT 25' },
  { label: 'Business rules',         q: 'MATCH (j:Job)-[:GOVERNED_BY]->(b:BusinessRule) RETURN j.name, b.name, b.description LIMIT 25' },
  { label: 'Deprecated columns',     q: "MATCH (c:Column {status:'deprecated'}) RETURN c.name, c.deprecated_at LIMIT 25" },
]

function CellValue({ v }) {
  if (v === null || v === undefined) return <span className="text-zinc-600">null</span>
  if (Array.isArray(v)) {
    if (v.length === 0) return <span className="text-zinc-600">[]</span>
    return (
      <div className="flex flex-wrap gap-1">
        {v.map((item, i) => (
          <span key={i} className="badge-info">{String(item)}</span>
        ))}
      </div>
    )
  }
  return <span>{String(v)}</span>
}

export default function GraphPage() {
  const [query,   setQuery]   = useState(SAMPLE_QUERIES[0].q)
  const [results, setResults] = useState(null)
  const [loading, setLoading] = useState(false)
  const [tab,     setTab]     = useState('query')  // 'query' | 'functions' | 'schema'
  const [search,  setSearch]  = useState('')
  const [listData, setListData] = useState([])

  async function executeQuery() {
    setLoading(true)
    setResults(null)
    try {
      const data = await runCypher(query)
      setResults(data)
    } catch (err) {
      toast.error(err.response?.data?.detail || 'Query failed')
    } finally {
      setLoading(false)
    }
  }

  async function loadJobs() {
    const data = await fetchJobs(search || null)
    setListData(data)
  }

  async function loadDatasets() {
    const data = await fetchDatasets()
    setListData(data)
  }

  // Auto-run default Cypher query on first load
  useEffect(() => { executeQuery() }, [])  // eslint-disable-line react-hooks/exhaustive-deps

  // Auto-load list data whenever the user switches tabs
  useEffect(() => {
    if (tab === 'jobs') loadJobs()
    else if (tab === 'datasets') loadDatasets()
  }, [tab])  // eslint-disable-line react-hooks/exhaustive-deps

  return (
    <div className="p-6 space-y-4">
      <div>
        <h1 className="text-xl font-semibold text-zinc-100">Graph Explorer</h1>
        <p className="text-sm text-zinc-500 mt-0.5">Query the Meta Model graph with Cypher</p>
      </div>

      {/* Tabs */}
      <div className="flex gap-1 border-b border-surface-border pb-0">
        {[['query','Cypher Query'], ['jobs','Jobs'], ['datasets','Datasets']].map(([id, label]) => (
          <button
            key={id}
            onClick={() => setTab(id)}
            className={`px-4 py-2 text-sm border-b-2 transition-colors ${
              tab === id
                ? 'border-accent text-accent-text'
                : 'border-transparent text-zinc-500 hover:text-zinc-300'
            }`}
          >
            {label}
          </button>
        ))}
      </div>

      {tab === 'query' && (
        <div className="space-y-3">
          {/* Sample query chips */}
          <div className="flex flex-wrap gap-2">
            {SAMPLE_QUERIES.map(sq => (
              <button
                key={sq.label}
                onClick={() => setQuery(sq.q)}
                className="text-xs px-2.5 py-1 rounded-full border border-surface-border
                           text-zinc-400 hover:text-zinc-100 hover:border-zinc-600 transition-colors"
              >
                {sq.label}
              </button>
            ))}
          </div>

          {/* Editor */}
          <div className="card p-0 overflow-hidden">
            <div className="flex items-center justify-between px-4 py-2 border-b border-surface-border bg-surface-card">
              <span className="text-xs text-zinc-500 font-mono">Cypher</span>
              <button
                onClick={executeQuery}
                disabled={loading}
                className="btn-primary text-xs flex items-center gap-1.5 py-1"
              >
                <Play size={11} /> {loading ? 'Running…' : 'Run'}
              </button>
            </div>
            <textarea
              value={query}
              onChange={e => setQuery(e.target.value)}
              onKeyDown={e => { if (e.ctrlKey && e.key === 'Enter') executeQuery() }}
              rows={5}
              className="w-full bg-[#0c0c0e] px-4 py-3 font-mono text-sm text-zinc-200
                         focus:outline-none resize-none"
              placeholder="MATCH (n) RETURN n LIMIT 10"
              spellCheck={false}
            />
          </div>

          {/* Results */}
          {results && (
            <div className="card p-0 overflow-hidden">
              <div className="px-4 py-2 border-b border-surface-border flex items-center justify-between">
                <span className="text-xs text-zinc-500">{results.count} row{results.count !== 1 ? 's' : ''}</span>
              </div>
              {results.count === 0 ? (
                <p className="px-4 py-6 text-sm text-zinc-600 text-center">No rows returned.</p>
              ) : (
                <div className="overflow-x-auto">
                  <table className="w-full text-xs">
                    <thead>
                      <tr className="border-b border-surface-border bg-surface-card">
                        {Object.keys(results.rows[0]).map(k => (
                          <th key={k} className="text-left px-4 py-2 font-medium text-zinc-500 font-mono">{k}</th>
                        ))}
                      </tr>
                    </thead>
                    <tbody className="divide-y divide-surface-border">
                      {results.rows.map((row, i) => (
                        <tr key={i} className="hover:bg-surface-hover/50 transition-colors">
                          {Object.values(row).map((v, j) => (
                            <td key={j} className="px-4 py-2 text-zinc-300 font-mono">
                              <CellValue v={v} />
                            </td>
                          ))}
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              )}
            </div>
          )}
        </div>
      )}

      {tab === 'jobs' && (
        <div className="space-y-3">
          <div className="flex gap-2">
            <input
              value={search}
              onChange={e => setSearch(e.target.value)}
              onKeyDown={e => e.key === 'Enter' && loadJobs()}
              placeholder="Search job name…"
              className="flex-1 bg-surface border border-surface-border rounded-md px-3 py-2
                         text-sm text-zinc-200 focus:outline-none focus:border-accent"
            />
            <button onClick={loadJobs} className="btn-primary flex items-center gap-1.5">
              <Search size={13} /> Search
            </button>
          </div>
          <JobTable data={listData} />
        </div>
      )}

      {tab === 'datasets' && (
        <div className="space-y-3">
          <button onClick={loadDatasets} className="btn-primary flex items-center gap-1.5">
            <Search size={13} /> Reload Datasets
          </button>
          <DatasetTable data={listData} />
        </div>
      )}
    </div>
  )
}

function JobTable({ data }) {
  if (!data.length) return <p className="text-sm text-zinc-600 py-4 text-center">No jobs found. Run Phase 1 pipeline first.</p>
  return (
    <div className="card p-0 overflow-hidden overflow-x-auto">
      <table className="w-full text-xs">
        <thead>
          <tr className="border-b border-surface-border bg-surface-card">
            <th className="text-left px-4 py-2 text-zinc-500 font-normal">Name</th>
            <th className="text-left px-4 py-2 text-zinc-500 font-normal">Path</th>
            <th className="text-left px-4 py-2 text-zinc-500 font-normal">Lines</th>
            <th className="text-left px-4 py-2 text-zinc-500 font-normal">Risk Tags</th>
          </tr>
        </thead>
        <tbody className="divide-y divide-surface-border">
          {data.map((j, i) => (
            <tr key={i} className="hover:bg-surface-hover/50">
              <td className="px-4 py-2 font-mono text-violet-300">{j.name}</td>
              <td className="px-4 py-2 text-zinc-500 font-mono">{j.path}</td>
              <td className="px-4 py-2 text-zinc-400">{j.line_start}–{j.line_end}</td>
              <td className="px-4 py-2">
                <div className="flex flex-wrap gap-1">
                  {(j.risk_tags || []).map(t => (
                    <span key={t} className={t === 'PII' ? 'badge-error' : 'badge-warn'}>{t}</span>
                  ))}
                </div>
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  )
}

function DatasetTable({ data }) {
  if (!data.length) return <p className="text-sm text-zinc-600 py-4 text-center">No datasets found. Run Phase 1 pipeline first.</p>
  return (
    <div className="card p-0 overflow-hidden overflow-x-auto">
      <table className="w-full text-xs">
        <thead>
          <tr className="border-b border-surface-border bg-surface-card">
            {['Name', 'Qualified Name', 'Format', 'Status'].map(h => (
              <th key={h} className="text-left px-4 py-2 text-zinc-500 font-normal">{h}</th>
            ))}
          </tr>
        </thead>
        <tbody className="divide-y divide-surface-border">
          {data.map((d, i) => (
            <tr key={i} className="hover:bg-surface-hover/50">
              <td className="px-4 py-2 font-mono text-blue-300">{d.name}</td>
              <td className="px-4 py-2 font-mono text-zinc-400">{d.qualified_name || '—'}</td>
              <td className="px-4 py-2 text-zinc-500">{d.format || '—'}</td>
              <td className="px-4 py-2">
                <span className={d.status === 'deprecated' ? 'badge-error' : 'badge-success'}>{d.status || 'active'}</span>
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  )
}
