import { useState } from 'react'
import { GitBranch, Database, Network } from 'lucide-react'
import ColumnLineageTab from './ColumnLineageTab'
import FunctionalLineageTab from './FunctionalLineageTab'
import InformaticaLineage from '../components/InformaticaLineage'
import ColumnLineageFlow from '../components/ColumnLineageFlow'
import FunctionalLineageFlow from '../components/FunctionalLineageFlow'
import { seedMockGraph } from '../services/api'

// ─── Job Lineage Tab ───────────────────────────────────────────────────────────

function JobLineageTab() {
  const [seeding, setSeeding] = useState(false)
  const [seedMsg, setSeedMsg] = useState(null)

  const handleSeed = async () => {
    setSeeding(true)
    setSeedMsg(null)
    try {
      const r = await seedMockGraph()
      const s = r.seeded || {}
      setSeedMsg(`✓ Seeded: ${s.datasets ?? 0} datasets, ${s.jobs ?? 0} jobs, ${s.columns ?? 0} columns, ${s.col_edges ?? 0} col edges`)
    } catch (err) {
      setSeedMsg(`✗ Seed failed: ${err?.response?.data?.detail || err.message}`)
    } finally {
      setSeeding(false)
    }
  }

  return (
    <div className="space-y-3 flex flex-col flex-1">

      {/* ── Controls ── */}
      <div className="flex flex-wrap gap-2 items-center">
        <button
          onClick={handleSeed}
          disabled={seeding}
          className="px-3 py-1 rounded-md text-xs font-medium border border-emerald-700 text-emerald-400 hover:bg-emerald-900/20 disabled:opacity-40 transition-colors"
        >
          {seeding ? 'Seeding…' : 'Seed AuraDB'}
        </button>
        <span className="text-xs text-zinc-600 ml-auto">
          Drag · scroll to zoom · hover for details &nbsp;·&nbsp;
          <span className="text-zinc-700">powered by NeoVis → AuraDB</span>
        </span>
      </div>

      {seedMsg && (
        <p className={`text-xs px-3 py-1.5 rounded border ${
          seedMsg.startsWith('✓')
            ? 'text-emerald-400 border-emerald-800 bg-emerald-900/20'
            : 'text-red-400 border-red-800 bg-red-900/20'
        }`}>{seedMsg}</p>
      )}

      {/* ── Informatica-style lineage DAG ── */}
      <div className="text-[10px] text-zinc-600 font-mono">
        Sources → Ingest Jobs → Core Tables → Transforms → Derived Tables → Orchestration → Outputs → Reports
        &nbsp;·&nbsp;click a node to inspect · use filter buttons inside to focus a domain
      </div>
      <InformaticaLineage />

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
      {tab === 'column'     && (
        <div className="space-y-2 flex flex-col flex-1">
          <div className="text-[10px] text-zinc-600 font-mono">
            Dataset containers show their columns ·
            pink arrows = DERIVED_FROM (column-level) ·
            click a column to trace its full upstream / downstream chain
          </div>
          <ColumnLineageFlow />
        </div>
      )}
      {tab === 'functional' && (
        <div className="space-y-2 flex flex-col flex-1">
          <div className="text-[10px] text-zinc-600 font-mono">
            DEPENDS_ON job call-chain ·
            tier 0 = root callers → right = leaf callees ·
            click a job to highlight its dependency path
          </div>
          <FunctionalLineageFlow />
        </div>
      )}
    </div>
  )
}
