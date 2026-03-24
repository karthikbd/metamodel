import { useState } from 'react'
import { NavLink } from 'react-router-dom'
import { shutdownServer } from '../../services/api'
import {
  LayoutDashboard,
  Play,
  ListOrdered,
  Network,
  GitBranch,
  ShieldAlert,
  Zap,
  Database,
  GitMerge,
  Cpu,
  Share2,
  PowerOff,
} from 'lucide-react'

const NAV_PHASE1 = [
  { to: '/',           icon: LayoutDashboard, label: 'Dashboard'   },
  { to: '/pipeline',   icon: Play,            label: 'Pipeline'     },
  { to: '/traces',     icon: ListOrdered,     label: 'Traces'       },
  { to: '/graph',      icon: Network,         label: 'Graph'        },
  { to: '/lineage',    icon: GitBranch,       label: 'Lineage'      },
  { to: '/graphviz',   icon: Share2,          label: 'Graph Viz'    },
  { to: '/compliance', icon: ShieldAlert,     label: 'Compliance'   },
  { to: '/impact',     icon: Zap,             label: 'Impact'       },
  // STM completes Phase 1 — auto-seeds after schema_extractor writes Column nodes
  { to: '/stm',        icon: GitMerge,        label: 'STM'          },
]

const NAV_PHASE2 = [
  { to: '/phase2', icon: Cpu, label: 'Runtime Consume' },
]

function NavSection({ items }) {
  return items.map(({ to, icon: Icon, label }) => (
    <NavLink
      key={to}
      to={to}
      end={to === '/'}
      className={({ isActive }) =>
        `flex items-center gap-3 px-3 py-2 rounded-md text-sm transition-colors ${
          isActive
            ? 'bg-accent/15 text-accent-text'
            : 'text-zinc-400 hover:text-zinc-100 hover:bg-surface-hover'
        }`
      }
    >
      <Icon size={15} />
      {label}
    </NavLink>
  ))
}

function StopServerButton() {
  const [confirming, setConfirming] = useState(false)
  const [stopping, setStopping]   = useState(false)

  async function handleStop() {
    if (!confirming) {
      setConfirming(true)
      // Auto-reset confirmation state after 4s if user doesn't click again
      setTimeout(() => setConfirming(false), 4000)
      return
    }
    setStopping(true)
    try {
      await shutdownServer()
    } catch (_) {
      // Expected — server closes the connection immediately on shutdown
    }
    // Brief delay then show a stopped message
    setTimeout(() => {
      document.body.innerHTML =
        '<div style="display:flex;align-items:center;justify-content:center;height:100vh;' +
        'background:#0f1117;color:#71717a;font-family:sans-serif;font-size:14px;">' +
        'Server stopped. Close this tab or run <code style="color:#a78bfa">start.ps1</code> again to restart.' +
        '</div>'
    }, 800)
  }

  return (
    <button
      onClick={handleStop}
      disabled={stopping}
      title={confirming ? 'Click again to confirm stop' : 'Stop all servers'}
      className={`w-full flex items-center gap-2 px-3 py-1.5 rounded-md text-xs transition-colors ${
        stopping
          ? 'text-zinc-600 cursor-not-allowed'
          : confirming
          ? 'bg-red-900/40 text-red-400 border border-red-800 animate-pulse'
          : 'text-zinc-500 hover:text-red-400 hover:bg-red-900/20'
      }`}
    >
      <PowerOff size={13} />
      {stopping ? 'Stopping…' : confirming ? 'Confirm — Stop Server?' : 'Stop Server'}
    </button>
  )
}

export default function Sidebar() {
  return (
    <aside className="w-56 flex-shrink-0 flex flex-col h-full overflow-hidden bg-surface-card border-r border-surface-border">
      {/* Brand */}
      <div className="flex items-center gap-2.5 px-4 py-5 border-b border-surface-border">
        <div className="w-7 h-7 rounded-md bg-accent flex items-center justify-center">
          <Database size={14} className="text-white" />
        </div>
        <div>
          <p className="text-sm font-semibold text-zinc-100 leading-none">Meta Model</p>
          <p className="text-[10px] text-zinc-500 mt-0.5">Graph Engine</p>
        </div>
      </div>

      {/* Nav */}
      <nav className="flex-1 px-2 py-3 space-y-0.5 overflow-y-auto">
        <p className="px-3 py-1 text-[9px] font-semibold text-zinc-600 uppercase tracking-widest">Phase 1 — Hydrate</p>
        <NavSection items={NAV_PHASE1} />
        <div className="my-2 border-t border-zinc-800" />
        <p className="px-3 py-1 text-[9px] font-semibold text-zinc-600 uppercase tracking-widest">Phase 2 — Consume</p>
        <NavSection items={NAV_PHASE2} />
      </nav>

      {/* Footer — flex-shrink-0 keeps Stop button always pinned at bottom */}
      <div className="flex-shrink-0 px-4 py-3 border-t border-surface-border space-y-2">
        <p className="text-[10px] text-zinc-600">Phase 1 → Hydrate · Phase 2 → Consume</p>
        <StopServerButton />
      </div>
    </aside>
  )
}
