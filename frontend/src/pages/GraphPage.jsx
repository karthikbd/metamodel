import { useState, useEffect } from 'react'
import { Search, Play, Share2, Table2 } from 'lucide-react'
import { runVisQuery, fetchJobs, fetchDatasets } from '../services/api'
import BloomGraph from '../components/BloomGraph'
import toast from 'react-hot-toast'

const SAMPLE_QUERIES = [
  { label: 'All jobs',                q: 'MATCH (j:Job)\nOPTIONAL MATCH (j)-[r:DEPENDS_ON]->(j2:Job)\nRETURN j, r, j2 LIMIT 60' },
  { label: 'All datasets',            q: 'MATCH (d:Dataset)\nOPTIONAL MATCH (job:Job)-[r:READS_FROM|WRITES_TO]->(d)\nRETURN d, r, job LIMIT 60' },
  { label: 'Job → Dataset',           q: 'MATCH (j:Job)-[r:READS_FROM|WRITES_TO]->(d:Dataset) RETURN j, r, d LIMIT 60' },
  { label: 'All columns',             q: 'MATCH (d:Dataset)-[r:HAS_COLUMN]->(c:Column) RETURN d, r, c LIMIT 80' },
  { label: 'PII-tagged jobs',         q: "MATCH (j:Job) WHERE 'PII' IN j.risk_tags\nOPTIONAL MATCH (j)-[r:READS_FROM|WRITES_TO]->(d:Dataset)\nRETURN j, r, d LIMIT 60" },
  { label: 'Job dependencies',        q: 'MATCH (a:Job)-[r:DEPENDS_ON]->(b:Job) RETURN a, r, b LIMIT 60' },
  { label: 'Business rules',          q: 'MATCH (b:BusinessRule)\nOPTIONAL MATCH (j:Job)-[r:GOVERNED_BY]->(b)\nRETURN b, r, j LIMIT 60' },
  { label: 'Deprecated columns',      q: "MATCH (c:Column) WHERE c.deprecated = true OR c.status = 'deprecated' OR c.deprecated_at IS NOT NULL\nOPTIONAL MATCH (d:Dataset)-[r:HAS_COLUMN]->(c)\nRETURN c, r, d LIMIT 60" },
  { label: 'Column lineage graph',    q: 'MATCH (src:Column)-[r:DERIVED_FROM]->(tgt:Column) RETURN src, r, tgt LIMIT 100' },
  { label: 'Schema (Dataset→Column)', q: 'MATCH (d:Dataset)-[r:HAS_COLUMN]->(c:Column)\nOPTIONAL MATCH (c)-[df:DERIVED_FROM]->(src:Column)\nRETURN d, r, c, df, src LIMIT 300' },
  { label: 'Column lineage (Dataset bridge)', q: 'MATCH (d1:Dataset)-[:HAS_COLUMN]->(c1:Column)-[df:DERIVED_FROM]->(c2:Column)<-[:HAS_COLUMN]-(d2:Dataset)\nRETURN d1, c1, df, c2, d2 LIMIT 200' },
  { label: 'Scheduler DAG',                   q: 'MATCH (a:Job {source:"scheduler"})-[r:DEPENDS_ON]->(b:Job {source:"scheduler"}) RETURN a, r, b LIMIT 50' },
]

/**
 * Rewrites a Cypher query so it can render as a graph:
 *  1. Extracts all named variables from MATCH (var:Label) and [var:TYPE] patterns.
 *  2. Replaces the RETURN clause with RETURN var1, var2, ...
 *  3. Bounds any unbounded variable-length paths  *  →  *1..5  to prevent timeouts.
 *  4. Ensures a LIMIT is present.
 * Returns null when no variables can be extracted.
 */
function fixScalarQuery(cypher) {
  // Extract named node variables — stops before { to handle property maps correctly
  const nodeVarRe = /\(\s*([a-zA-Z_][a-zA-Z0-9_]*)\s*(?::[^{)][^)]*)?\s*[{)]/g
  // Extract named relationship variables
  const relVarRe  = /\[\s*([a-zA-Z_][a-zA-Z0-9_]*)\s*(?::[^\]]+)?\s*\]/g
  const vars = new Set()
  let m
  while ((m = nodeVarRe.exec(cypher)) !== null) vars.add(m[1])
  while ((m = relVarRe.exec(cypher))  !== null) vars.add(m[1])
  if (vars.size === 0) return null

  // Bound any unbounded variable-length rel  [*]  [:T*]  [r:T*]  [:T*3..]  →  *1..5
  // Rule: replace  *  inside [...]  only when NOT already followed by a digit
  let fixed = cypher.replace(/\*(?!\d)(\s*\])/g, '*1..5$1')

  const hasLimit = /\bLIMIT\s+\d+/i.test(fixed)
  const ret = `RETURN ${[...vars].join(', ')}${hasLimit ? '' : ' LIMIT 100'}`
  return fixed.replace(/\bRETURN\b[\s\S]*$/i, ret)
}

function CellValue({ v }) {
  if (v === null || v === undefined) return <span className="text-zinc-600">null</span>
  if (Array.isArray(v)) {
    if (v.length === 0) return <span className="text-zinc-600">[]</span>
    return (
      <div className="flex flex-wrap gap-1">
        {v.map((item, i) => (
          <span key={i} className="badge-info"><CellValue v={item} /></span>
        ))}
      </div>
    )
  }
  // Neo4j Node object (flattened by backend)
  if (v !== null && typeof v === 'object' && v._labels) {
    const label = v._labels[0] || 'Node'
    const name  = v.name || v.qualified_name || v.id || '(unnamed)'
    return (
      <span className="inline-flex items-center gap-1.5">
        <span className="text-[10px] px-1.5 py-px rounded font-mono border
                         bg-violet-900/40 text-violet-300 border-violet-700/40">
          {label}
        </span>
        <span>{String(name)}</span>
      </span>
    )
  }
  // Neo4j Relationship object (flattened by backend)
  if (v !== null && typeof v === 'object' && v._relType) {
    return <span className="font-mono text-amber-400 text-[11px]">&#8594; {v._relType}</span>
  }
  // Generic nested object — compact JSON
  if (v !== null && typeof v === 'object') {
    return <span className="font-mono text-zinc-500 text-[10px]">{JSON.stringify(v)}</span>
  }
  return <span>{String(v)}</span>
}

export default function GraphPage() {
  const [query,        setQuery]       = useState(SAMPLE_QUERIES[2].q)   // default: Job→Dataset
  const [results,      setResults]     = useState(null)
  const [loading,      setLoading]     = useState(false)
  const [tab,          setTab]         = useState('query')
  const [search,       setSearch]      = useState('')
  const [listData,     setListData]    = useState([])
  // activeCypher drives BloomGraph's expand/back internal navigation
  const [activeCypher, setActiveCypher] = useState(SAMPLE_QUERIES[2].q)
  // graphData is the pre-loaded vis-query result passed directly to BloomGraph
  const [graphData,    setGraphData]    = useState(null)
  // viewTab toggles between graph and table within the Cypher Query tab
  const [viewTab,      setViewTab]      = useState('graph')
  // fixedQuery holds an auto-rewritten version of a scalar query (RETURN n.x → RETURN n)
  const [fixedQuery,   setFixedQuery]   = useState(null)

  // ── Execute query ─────────────────────────────────────────────────────────
  // Accepts an optional `override` so chip clicks can pass the query directly
  // without waiting for React to flush the setQuery state update.
  // A single call to vis-query returns {nodes, edges, rows, count, truncated}
  // so the graph and table are always driven by identical data.
  async function executeQuery(override) {
    const cypher = override ?? query
    setQuery(cypher)                // keep textarea in sync
    setLoading(true)
    setResults(null)
    try {
      const data = await runVisQuery(cypher)
      // Set activeCypher AFTER the await so it batches with setGraphData in the same
      // render — this ensures externalCypherRef is set before BloomGraph's activeCypher
      // effect fires, preventing a redundant self-fetch race condition.
      setActiveCypher(cypher)
      // Pass pre-fetched graph data directly to BloomGraph (no second round-trip)
      setGraphData({ nodes: data.nodes, edges: data.edges, truncated: data.truncated })
      // Use the same backend response for the table
      setResults({ type: 'table', rows: data.rows, count: data.count })
      // When the query returns rows but no Node objects (e.g. RETURN n.name instead of
      // RETURN n), auto-switch to the Table tab and offer a one-click fix.
      if (data.nodes.length === 0 && data.count > 0) {
        setViewTab('table')
        const fixed = fixScalarQuery(cypher)
        setFixedQuery(fixed)
      } else {
        setFixedQuery(null)
      }
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

  // Auto-run on first load so both graph and table are populated immediately
  useEffect(() => {
    if (tab === 'query') executeQuery()
  }, [])  // eslint-disable-line react-hooks/exhaustive-deps

  useEffect(() => {
    if (tab === 'jobs') loadJobs()
    else if (tab === 'datasets') loadDatasets()
  }, [tab])  // eslint-disable-line react-hooks/exhaustive-deps

  return (
    <div className="p-6 space-y-4">
      <div>
        <h1 className="text-xl font-semibold text-zinc-100">Graph Explorer</h1>
        <p className="text-sm text-zinc-500 mt-0.5">Query the Meta Model graph with Cypher · visualise as graph or table</p>
      </div>

      {/* Top-level tabs */}
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
                onClick={() => executeQuery(sq.q)}
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
              <div className="flex items-center gap-2 text-[11px] text-zinc-500">
                <Share2 size={11} /><span>Graph</span>
                <span className="text-zinc-700">+</span>
                <Table2 size={11} /><span>Table</span>
              </div>
              <button
                onClick={() => executeQuery()}
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
              rows={4}
              className="w-full bg-[#0c0c0e] px-4 py-3 font-mono text-sm text-zinc-200
                         focus:outline-none resize-none"
              placeholder="MATCH (n) RETURN n LIMIT 10"
              spellCheck={false}
            />
          </div>

          {/* ── Fix banner: shown when a scalar RETURN produced 0 graph nodes ── */}
          {fixedQuery && (
            <div className="flex items-center justify-between gap-3 px-3 py-2 rounded-md
                            bg-amber-950/40 border border-amber-600/30 text-xs">
              <span className="text-amber-200/80">
                Query returned <strong className="text-amber-300">{results?.count ?? 0} rows</strong> but
                no graph nodes — it uses scalar returns like{' '}
                <code className="bg-zinc-800 px-1 rounded">RETURN n.name</code>.
                Auto-fixed to return full node variables.
              </span>
              <button
                onClick={() => { setFixedQuery(null); executeQuery(fixedQuery) }}
                className="shrink-0 px-3 py-1 rounded bg-amber-500/20 border border-amber-500/40
                           text-amber-300 hover:bg-amber-500/30 transition-colors font-medium whitespace-nowrap"
              >
                Fix &amp; show graph
              </button>
            </div>
          )}

          {/* ── Graph / Table view toggle ── */}
          <div className="flex gap-1 border-b border-surface-border">
            {[['graph', 'Graph'], ['table', 'Table']].map(([id, label]) => (
              <button
                key={id}
                onClick={() => setViewTab(id)}
                className={`px-4 py-1.5 text-xs border-b-2 transition-colors ${
                  viewTab === id
                    ? 'border-accent text-accent-text'
                    : 'border-transparent text-zinc-500 hover:text-zinc-300'
                }`}
              >
                {label}{id === 'table' && results?.count != null ? ` (${results.count})` : ''}
              </button>
            ))}
          </div>

          {viewTab === 'graph' && (
            <div className="space-y-1">
              <span className="text-xs text-zinc-600">drag · scroll to zoom · click node for details · double-click to zoom in</span>
              <BloomGraph cypher={activeCypher} graphData={graphData} rowCount={results?.count ?? 0} height={520} />
            </div>
          )}

          {viewTab === 'table' && (
            <div className="card p-0 overflow-hidden">
              {!results ? (
                <p className="px-4 py-6 text-sm text-zinc-600 text-center">Run a query to see results.</p>
              ) : results.count === 0 ? (
                <p className="px-4 py-6 text-sm text-zinc-600 text-center">No rows returned.</p>
              ) : (
                <>
                  <div className="px-4 py-2 border-b border-surface-border">
                    <span className="text-xs text-zinc-500">{results.count} row{results.count !== 1 ? 's' : ''}</span>
                  </div>
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
                </>
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
