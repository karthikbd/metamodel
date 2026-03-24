# Meta Model Engine

A living graph database that holds a unified, queryable representation of an organisation's code, data pipelines, and business rules. It answers questions like:

- **Where does this column come from?** Trace any column back through every transformation that produced it.
- **What breaks if I rename this field?** All jobs and dashboards that depend on it  one query.
- **Which jobs process PII without an audit trail?** Compliance violations surfaced automatically.
- **What does this ETL job actually read and write?** Resolved from the codebase, not from documentation.

---

## Business Problem

Large organisations accumulate hundreds of ETL scripts, data pipelines, and reporting jobs over years. The business problems this solves:

1. **No single source of truth**  nobody knows which jobs write to which tables, or where a column originates.
2. **Brittle pipelines**  column lists and schema details are hardcoded. A rename breaks jobs silently.
3. **Compliance risk**  PII-tagged columns flow through jobs that were never audited.
4. **Slow impact analysis**  a schema change triggers a manual hunt through dozens of files.
5. **Regulatory reporting**  teams cannot quickly answer "show the full lineage of this report back to the source system."

The Meta Model Engine solves all five by scanning the codebase once, building a graph, and then answering all lineage, compliance, and impact questions in real time from the graph.

---

## Ontology  What Lives in the Graph

The graph is built on the **DataLineageMetaModel v3** schema. Every node and relationship has a `confidence` of either `verified` (derived from AST/static analysis) or `inferred` (derived from LLM).

### Node Types

| Label | Represents |
|---|---|
| `DataSource` | Root repository or external system (file store, database, API) |
| `Script` | A single source code file (.py) |
| `Job` | A function or pipeline job within a Script |
| `Dataset` | A logical table, file, or topic that data lands in or comes from |
| `Column` | A field within a Dataset, with data type and PII flag |
| `Transformation` | A data transformation step (expression, aggregation, filter) |
| `Dashboard` | A reporting artefact (BI report, regulatory output) |
| `Alias` | An alternate name or mapping for a Dataset or Column |
| `BusinessRule` | A governance rule a Job is subject to (e.g. PII audit required) |
| `LLMSummary` | An AI-generated description of a Job or Dataset |
| `PipelineRun` | A record of a hydration run (when, what, how many nodes written) |

### Relationship Types

| Relationship | Meaning |
|---|---|
| `READS_FROM` | Job reads from Dataset |
| `WRITES_TO` | Job writes to Dataset |
| `HAS_COLUMN` | Dataset contains Column |
| `DERIVED_FROM` | Column value is derived from another Column (with expression) |
| `DEPENDS_ON` | Job depends on another Job (call chain) |
| `PART_OF` | Script is part of a Job hierarchy |
| `GOVERNED_BY` | Job is subject to a BusinessRule |
| `MAPS_TO` | Column is mapped to a target STM entry (Source-to-Target Mapping) |

---

## Two-Phase Architecture

### Phase 1  Hydration (scan once, graph forever)

The hydration pipeline scans a repository and writes everything it finds into Neo4j. It runs four agents in sequence. All progress streams to the UI in real time.

```
Repository on disk
       
       
Agent 1: AST Extractor
  - Walks every .py file
  - Parses functions, arguments, decorators using Python ast module
  - Writes: DataSource, Script, Job nodes
  - SHA-256 per file  skips unchanged files on re-runs
       
       
Agent 2: Cross-Reference Resolver
  - Resolves import chains between files
  - Builds DEPENDS_ON edges between Job nodes
  - Uses qualified import paths, not name matching
       
       
Agent 3: Schema Extractor
  - Identifies read/write operations (SQL, file I/O, DataFrame ops)
  - Writes: Dataset, Column nodes
  - Writes: READS_FROM, WRITES_TO, HAS_COLUMN edges
  - Detects DERIVED_FROM chains from expressions
       
       
Agent 4: LLM Summariser
  - Calls OpenAI to generate human-readable descriptions
  - Writes: LLMSummary nodes with confidence = inferred
  - Gracefully skipped if OPENAI_API_KEY is not set
       
       
Neo4j (AuraDB or local Docker fallback)
  - Full v3 schema applied on first startup
  - All writes are MERGE (idempotent  safe to re-run)
```

### Phase 2  Runtime Consumption (dynamic, not hardcoded)

Once the graph is populated, production pipelines query it at startup instead of hardcoding column lists or schema definitions.

**Before Phase 2:**
```python
# Brittle  breaks silently when schema changes
source_columns = ["customer_id", "trade_date", "amount", "currency"]
```

**After Phase 2:**
```python
# Dynamic  always accurate because it reads from the graph
reads  = GET /api/phase2/pipeline/{id}/reads    # datasets this job reads
writes = GET /api/phase2/pipeline/{id}/writes   # datasets this job writes
fields = GET /api/phase2/pipeline/{id}/resolve  # full column resolution with STM targets
```

Phase 2 only returns edges with `confidence = verified`  LLM-inferred relationships are excluded from runtime resolution.

---

## Source-to-Target Mapping (STM)

STM bridges the gap between source columns in the codebase and target columns in the data warehouse.

```
Source Column (in codebase)
   MAPS_TO  STM entry
                   target_table:  "dim_customer"
                   target_column: "cust_id"
                   target_system: "data_warehouse"
                   transform_expr: "CAST(customer_id AS VARCHAR)"
```

Once STM is seeded, the full lineage from source file  source column  transformation  target warehouse column is resolvable in a single graph traversal.

---

## Compliance & Governance Queries

The graph supports pre-built compliance queries that run against real graph data:

| Query | Business Purpose |
|---|---|
| PII without audit | Jobs that handle PII-flagged columns but are not tagged `audit_required`  regulatory violation |
| Regulatory report lineage | Full upstream lineage of every job tagged `regulatory_report` back to source datasets |
| Deprecated columns in use | Jobs still reading datasets that contain columns marked for deprecation |
| Column impact | All jobs affected if a specific column is renamed or removed |
| Dataset impact | All upstream and downstream jobs for a given dataset |

---

## Graph Integrity Rules

1. **Confidence labelling**  every node and edge carries `confidence: verified` (from AST) or `confidence: inferred` (from LLM). Phase 2 runtime queries filter on `verified` only.
2. **Idempotency**  every write is a `MERGE`. Re-running hydration against the same repo is always safe and only updates changed files.
3. **Deprecation over deletion**  when a node is no longer found in the codebase, it is marked `status: deprecated` and left in the graph. It is never deleted, preserving historical lineage.
4. **Fail loud**  unresolvable imports produce an `UNRESOLVED` marker node. Dynamic SQL that cannot be statically parsed produces a `DYNAMIC_SQL` flag on the edge.

---

## Database  Neo4j Connection

The backend tries AuraDB first. If AuraDB is unreachable, it automatically falls through to a local Docker Neo4j instance. The same v3 schema (constraints + indexes) is applied to whichever database connects first at startup.

```
Backend startup
       
        Try AuraDB (neo4j+s://...)
                Connected?  use it
                Failed?     fall through
       
        Try local Docker Neo4j (bolt://localhost:7687)
                 Connected?  use it
                 Failed?     RuntimeError (both unavailable)
```

The schema init on startup applies 11 UNIQUE constraints and 11 indexes from `meta_modelv3.json` using `IF NOT EXISTS`  safe to run repeatedly.

---

## Running the System

```powershell
# From the repo root  starts everything (Neo4j fallback + backend + frontend)
.\start.ps1
```

What `start.ps1` does, in order:
1. Validates Python venv and npm are present
2. Frees ports 3000 and 8000
3. Starts the local Docker Neo4j container (bolt://localhost:7687) as AuraDB fallback
4. Starts the FastAPI backend (uvicorn, hot-reload), waits for `/health` to pass
5. Starts the frontend dev server
6. Blocks until the backend exits
7. On exit (Ctrl+C or UI Stop button): kills frontend, stops Neo4j container

To stop everything: press **Ctrl+C** in the terminal, or click the **Stop Server** button in the sidebar of the UI.

---

## Environment Variables

| Variable | Description |
|---|---|
| `NEO4J_URI` | AuraDB connection URI |
| `NEO4J_USERNAME` | AuraDB username |
| `NEO4J_PASSWORD` | AuraDB password |
| `NEO4J_DATABASE` | AuraDB named database |
| `NEO4J_LOCAL_URI` | Local Docker Neo4j URI (fallback) |
| `NEO4J_LOCAL_USER` | Local Neo4j username |
| `NEO4J_LOCAL_PASSWORD` | Local Neo4j password |
| `OPENAI_API_KEY` | Required for Agent 4 LLM summaries (Agent 4 skipped if empty) |
| `REPO_SCAN_ROOT` | Filesystem path to the repository to scan (Phase 1) |

---

## Project Structure

```
meta_model/
 start.ps1                        # Single start script  starts and stops everything
 docker-compose.yml               # Local Docker Neo4j service
 meta_modelv3.json                # Ontology definition (v3 schema)
 backend/
    main.py                      # FastAPI app, lifespan (schema init + driver close)
    config.py                    # Settings loaded from .env
    graph/
       neo4j_client.py          # AuraDB-first driver with Docker fallback
       schema_init.py           # Applies v3 constraints + indexes on startup
       writer.py                # All MERGE writes (idempotent, non-fatal on failure)
       queries.py               # All read queries (real Neo4j data, no mocks)
    agents/
       ast_extractor.py         # Agent 1  AST scan
       cross_ref_resolver.py    # Agent 2  import chain resolution
       schema_extractor.py      # Agent 3  dataset/column extraction
       llm_summariser.py        # Agent 4  LLM descriptions
    services/
       run_tracker.py           # Pipeline run history store
    api/routes/
        pipelines.py             # POST /api/pipeline/run/phase1 (SSE stream)
        runs.py                  # GET  /api/runs  run history
        graph.py                 # POST /api/graph/query  raw Cypher
        lineage.py               # GET  /api/lineage/function/{id}
        compliance.py            # Compliance queries (PII, regulatory, deprecated)
        impact.py                # Column + dataset impact analysis
        stats.py                 # GET  /api/stats  graph counts
        stm.py                   # Source-to-Target Mapping
        phase2.py                # Phase 2 pipeline field resolution
 sample_repo/                     # 24-file synthetic PNC codebase used for Phase 1 testing
    compliance/aml_screening.py    # AML checks on PII columns
    compliance/kyc_validator.py    # KYC validation pipeline
    config/database.py             # DB connection config
    etl/customer_ingest.py         # Reads customer_master → writes accounts
    etl/risk_pipeline.py           # Orchestrates credit/market/limit agents
    etl/transaction_feed.py        # Transaction ingestion
    models/customer.py             # Customer domain model
    models/risk.py                 # Risk domain model
    reporting/basel3.py            # Basel III regulatory report
    reporting/ccar_report.py       # CCAR regulatory report (SQL + pandas)
    reporting/mis_daily.py         # Daily MIS report
    risk/credit_risk.py            # Credit risk calculations
    risk/limit_monitor.py          # Limit breach monitoring
    risk/market_risk.py            # Market risk calculations
    utils/db_utils.py              # Shared DB utilities
    utils/decorators.py            # Audit/PII decorators
 frontend/
     src/
         pages/                   # One page per capability
         services/api.js          # All API calls
```
