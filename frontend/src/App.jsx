import { Routes, Route, Navigate } from 'react-router-dom'
import Layout from './components/Layout/Layout'
import Dashboard     from './pages/Dashboard'
import PipelinePage  from './pages/PipelinePage'
import TracesPage    from './pages/TracesPage'
import GraphPage     from './pages/GraphPage'
import LineagePage   from './pages/LineagePage'
import CompliancePage from './pages/CompliancePage'
import ImpactPage    from './pages/ImpactPage'
import STMPage       from './pages/STMPage'
import Phase2Page    from './pages/Phase2Page'
import GraphVizPage  from './pages/GraphVizPage'

export default function App() {
  return (
    <Layout>
      <Routes>
        <Route path="/"           element={<Dashboard />} />
        <Route path="/pipeline"   element={<PipelinePage />} />
        <Route path="/traces"     element={<TracesPage />} />
        <Route path="/traces/:id" element={<TracesPage />} />
        <Route path="/graph"      element={<GraphPage />} />
        <Route path="/lineage"    element={<LineagePage />} />
        <Route path="/compliance" element={<CompliancePage />} />
        <Route path="/impact"     element={<ImpactPage />} />
        <Route path="/graphviz"   element={<GraphVizPage />} />
        <Route path="/stm"        element={<STMPage />} />
        <Route path="/phase2"     element={<Phase2Page />} />
        <Route path="*"           element={<Navigate to="/" replace />} />
      </Routes>
    </Layout>
  )
}
