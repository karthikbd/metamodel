import axios from 'axios'

const BASE = import.meta.env.VITE_API_BASE || ''

const api = axios.create({ baseURL: BASE })

// ── Stats ────────────────────────────────────────────────────────────────────
export const fetchStats    = () => api.get('/api/stats/').then(r => r.data)
export const fetchConfig   = () => api.get('/api/config').then(r => r.data)
export const shutdownServer = () => api.post('/api/shutdown').then(r => r.data)

// ── Pipeline ─────────────────────────────────────────────────────────────────
export const fetchPipelineRuns = () => api.get('/api/pipeline/runs').then(r => r.data)

/**
 * Start a Phase 1 pipeline and stream SSE events.
 * @param {object} opts - { repo_root, agents }
 * @param {function} onEvent - called with each parsed event object
 * @param {function} onDone  - called when stream closes
 */
export function startPhase1Stream(opts, onEvent, onDone) {
  const body = JSON.stringify(opts)
  fetch(`${BASE}/api/pipeline/run/phase1`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body,
  }).then(async (response) => {
    const reader = response.body.getReader()
    const decoder = new TextDecoder()
    let buffer = ''

    while (true) {
      const { done, value } = await reader.read()
      if (done) break
      buffer += decoder.decode(value, { stream: true })
      const lines = buffer.split('\n')
      buffer = lines.pop()  // last partial line stays in buffer
      for (const line of lines) {
        if (line.startsWith('data: ')) {
          try {
            const ev = JSON.parse(line.slice(6))
            onEvent(ev)
          } catch (_) {}
        }
      }
    }
    onDone()
  }).catch(() => onDone())
}

// ── Runs ─────────────────────────────────────────────────────────────────────
export const fetchRuns     = () => api.get('/api/runs/').then(r => r.data)
export const fetchRun      = (id) => api.get(`/api/runs/${id}`).then(r => r.data)
export const fetchAgentRuns = (id) => api.get(`/api/runs/${id}/agents`).then(r => r.data)
export const fetchAgentEvents = (runId, agentRunId) =>
  api.get(`/api/runs/${runId}/agents/${agentRunId}/events`).then(r => r.data)

// ── Graph ────────────────────────────────────────────────────────────────────
export const runCypher = (cypher, params) =>
  api.post('/api/graph/query', { cypher, params }).then(r => r.data)

/** Execute Cypher and return ReactFlow-compatible {nodes, edges} for graph rendering. */
export const runCypherVisual = (cypher, params) =>
  api.post('/api/graph/visual', { cypher, params }).then(r => r.data)

export const fetchJobs = (search) =>
  api.get('/api/graph/jobs', { params: search ? { search } : {} }).then(r => r.data)

export const fetchDatasets = (name) =>
  api.get('/api/graph/datasets', { params: name ? { name } : {} }).then(r => r.data)

export const fetchColumns = (dataset) =>
  api.get('/api/graph/columns', { params: dataset ? { dataset } : {} }).then(r => r.data)

export const seedMockGraph = () =>
  api.post('/api/graph/seed-mock').then(r => r.data)

export const fetchJobReads     = (id) => api.get(`/api/graph/job/${id}/reads`).then(r => r.data)
export const fetchJobWrites    = (id) => api.get(`/api/graph/job/${id}/writes`).then(r => r.data)
export const fetchJobDataflows = (id) => api.get(`/api/graph/job/${id}/dataflows`).then(r => r.data)

// ── Lineage ──────────────────────────────────────────────────────────────────
export const fetchJobLineage = (id) =>
  api.get(`/api/lineage/job/${id}`).then(r => r.data)

export const fetchColumnLineage = (table, column) =>
  api.get('/api/lineage/column', { params: { table, column } }).then(r => r.data)

export const fetchColumnLineageGraph = (dataset, column) =>
  api.get('/api/lineage/column-graph', { params: { dataset, column } }).then(r => r.data)

export const fetchDatasetsWithColumns = () =>
  api.get('/api/lineage/datasets-columns').then(r => r.data)

export const fetchAllColumnLineage = () =>
  api.get('/api/lineage/all-column-lineage').then(r => r.data)

export const fetchAllFunctionalLineage = () =>
  api.get('/api/lineage/all-functional-lineage').then(r => r.data)

export const fetchAllJobGraph = () =>
  api.get('/api/lineage/all-job-graph').then(r => r.data)

export const fetchAllColumnGraph = () =>
  api.get('/api/lineage/all-column-graph').then(r => r.data)

// ── Compliance ───────────────────────────────────────────────────────────────
export const fetchComplianceQueries = () =>
  api.get('/api/compliance/queries').then(r => r.data)

export const runComplianceQuery = (id) =>
  api.get(`/api/compliance/run/${id}`).then(r => r.data)

// ── Impact ───────────────────────────────────────────────────────────────────
export const fetchColumnImpact = (table, column) =>
  api.get('/api/impact/column', { params: { table, column } }).then(r => r.data)

export const fetchDatasetImpact = (name) =>
  api.get('/api/impact/dataset', { params: { name } }).then(r => r.data)

// ── STM — Source-to-Target Mapping ───────────────────────────────────────────
export const seedSTM            = ()   => api.post('/api/stm/seed').then(r => r.data)
export const fetchSTMMappings   = ()   => api.get('/api/stm/mappings').then(r => r.data)
export const fetchSTMLineage    = ()   => api.get('/api/stm/lineage').then(r => r.data)
export const fetchSTMBridge     = (id) => api.get(`/api/stm/bridge/${id}`).then(r => r.data)
export const fetchSTMStats      = ()   => api.get('/api/stm/stats').then(r => r.data)

// ── Phase 2 — Runtime Consumption ─────────────────────────────────────────────
export const seedPhase2Pipelines  = ()       => api.post('/api/phase2/seed').then(r => r.data)
export const fetchPhase2Pipelines = ()       => api.get('/api/phase2/pipelines').then(r => r.data)
export const resolvePhase2        = (pipeId) => api.get(`/api/phase2/${pipeId}/resolve`).then(r => r.data)
export const fetchPhase2Stats     = ()       => api.get('/api/phase2/stats').then(r => r.data)
export const registerPipeline     = (body)   => api.post('/api/phase2/register', body).then(r => r.data)
