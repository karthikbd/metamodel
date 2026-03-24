import { useState } from 'react'
import { GitBranch, Database, Network } from 'lucide-react'
import ColumnLineageTab from './ColumnLineageTab'
import FunctionalLineageTab from './FunctionalLineageTab'
import BloomGraph from '../components/BloomGraph'

// ─── Node badge style helper ─────────────────────────────────────────────────

const NODE_BADGE = {
  Job:     { color: '#a78bfa', borderColor: '#7c3aed', background: '#1e1529' },
  Dataset: { color: '#93c5fd', borderColor: '#3b82f6', background: '#0f1a2e' },
}
function nodeBadgeStyle(type) {
  return NODE_BADGE[type] || { color: '#6ee7b7', borderColor: '#34d399', background: '#0e1e18' }
}

// ─── Cypher queries per edge-mode ────────────────────────────────────────────

const CYPHER = {
  all:     'MATCH (n)-[r:READS_FROM|WRITES_TO|DEPENDS_ON|GOVERNED_BY|REFERENCES|DERIVED_FROM|JOINS_WITH]->(m) RETURN n, r, m LIMIT 120',
  process: 'MATCH (a:Job)-[r:DEPENDS_ON]->(b:Job) RETURN a, r, b LIMIT 100',
  data:    'MATCH (j:Job)-[r:READS_FROM|WRITES_TO]->(d:Dataset) RETURN j, r, d LIMIT 100',
  fk:      'MATCH (a:Dataset)-[r:REFERENCES|DERIVED_FROM|JOINS_WITH]->(b:Dataset) RETURN a, r, b LIMIT 100',
}

// ─── Job Lineage Tab ──────────────────────────────────────────────────────────

function JobLineageTab() {
  const [edgeMode, setEdgeMode] = useState('all')

  return (
    <div className="space-y-3 flex flex-col flex-1">

      {/* ── Controls ── */}
      <div className="flex flex-wrap gap-2 items-center">
        <div className="flex gap-1 bg-surface border border-surface-border rounded-lg p-0.5">
          {[
            { key: 'all',     label: 'All edges' },
            { key: 'process', label: 'Job → Job (DEPENDS_ON)' },
            { key: 'data',    label: 'Job ↔ Dataset' },
            { key: 'fk',      label: 'Dataset FK / Joins' },
          ].map(({ key, label }) => (
            <button
              key={key}
              onClick={() => setEdgeMode(key)}
              className={`px-3 py-1 rounded-md text-xs font-medium transition-colors ${
                edgeMode === key ? 'bg-accent text-white' : 'text-zinc-400 hover:text-zinc-200'
              }`}
            >
              {label}
            </button>
          ))}
        </div>
        <span className="text-xs text-zinc-600 ml-auto">
          Drag · scroll to zoom · hover for details &nbsp;·&nbsp;
          <span className="text-zinc-700">powered by NeoVis → AuraDB</span>
        </span>
      </div>

      {/* ── NeoVis graph (connects directly to AuraDB) ── */}
      <BloomGraph cypher={CYPHER[edgeMode]} height={600} />

    </div>
  )
}

// ─── Page ─────────────────────────────────────────────────────────────────────

export default function LineagePage() {
  const [tab, setTab] = useState('job')

  return (
    <div className="p-6 space-y-4 h-full flex flex-col">
      <div>
        <h1 className="text-xl font-semibold text-zinc-100">Lineage Visualiser</h1>
        <p className="text-sm text-zinc-500 mt-0.5">
          <strong className="text-zinc-400">Process lineage</strong> — Job call chains (DEPENDS_ON)
          &nbsp;·&nbsp;
          <strong className="text-zinc-400">Data lineage</strong> — Job ↔ Dataset (READS_FROM / WRITES_TO)
          &nbsp;·&nbsp;
          <strong className="text-zinc-400">Dataset joins</strong> — FK / DERIVED_FROM / JOINS_WITH
          &nbsp;·&nbsp;
          <strong className="text-zinc-400">Column lineage</strong> — Column-level DERIVED_FROM chains
        </p>
      </div>

      {/* Tab switcher */}
      <div className="flex gap-1 bg-surface border border-surface-border rounded-lg p-1 w-fit">
        <button
          onClick={() => setTab('job')}
          className={`flex items-center gap-1.5 px-4 py-1.5 rounded-md text-sm font-medium transition-colors ${
            tab === 'job' ? 'bg-accent text-white' : 'text-zinc-400 hover:text-zinc-200'
          }`}
        >
          <GitBranch size={14} /> Process &amp; Data Lineage
        </button>
        <button
          onClick={() => setTab('column')}
          className={`flex items-center gap-1.5 px-4 py-1.5 rounded-md text-sm font-medium transition-colors ${
            tab === 'column' ? 'bg-accent text-white' : 'text-zinc-400 hover:text-zinc-200'
          }`}
        >
          <Database size={14} /> Column Lineage
        </button>
        <button
          onClick={() => setTab('functional')}
          className={`flex items-center gap-1.5 px-4 py-1.5 rounded-md text-sm font-medium transition-colors ${
            tab === 'functional' ? 'bg-accent text-white' : 'text-zinc-400 hover:text-zinc-200'
          }`}
        >
          <Network size={14} /> Functional Lineage
        </button>
      </div>

      {/* Tab content */}
      {tab === 'job'        && <JobLineageTab />}
      {tab === 'column'     && <ColumnLineageTab />}
      {tab === 'functional' && <FunctionalLineageTab />}
    </div>
  )
}
