/**
 * mockGraph.js — pure-JS mirror of backend/graph/mock_data.py
 * Used by MockGraph.jsx to render the relationship graph without a server.
 */

// ── Raw entity data ────────────────────────────────────────────────────────

const DATASETS = [
  { id: 'ds-src-crm',  name: 'crm_feed',            domain: 'Source',      format: 'CSV',     owner: 'data_engineering' },
  { id: 'ds-src-pay',  name: 'payment_feed',         domain: 'Source',      format: 'CSV',     owner: 'data_engineering' },
  { id: 'ds-src-sanc', name: 'sanctions_list',       domain: 'Reference',   format: 'JSON',    owner: 'compliance_team'  },
  { id: 'ds-001',      name: 'customer_master',      domain: 'Customer',    format: 'TABLE',   owner: 'data_engineering' },
  { id: 'ds-002',      name: 'accounts',             domain: 'Customer',    format: 'TABLE',   owner: 'data_engineering' },
  { id: 'ds-003',      name: 'transactions',         domain: 'Transaction', format: 'TABLE',   owner: 'data_engineering' },
  { id: 'ds-004',      name: 'market_data',          domain: 'Reference',   format: 'TABLE',   owner: 'reference_data'   },
  { id: 'ds-005',      name: 'risk_scores',          domain: 'Risk',        format: 'TABLE',   owner: 'risk_team'        },
  { id: 'ds-006',      name: 'var_results',          domain: 'Risk',        format: 'TABLE',   owner: 'risk_team'        },
  { id: 'ds-007',      name: 'limit_breaches',       domain: 'Risk',        format: 'TABLE',   owner: 'risk_team'        },
  { id: 'ds-008',      name: 'capital_requirements', domain: 'Risk',        format: 'TABLE',   owner: 'risk_team'        },
  { id: 'ds-009',      name: 'kyc_screening',        domain: 'Compliance',  format: 'TABLE',   owner: 'compliance_team'  },
  { id: 'ds-010',      name: 'aml_alerts',           domain: 'Compliance',  format: 'TABLE',   owner: 'compliance_team'  },
  { id: 'ds-011',      name: 'ccar_output',          domain: 'Reporting',   format: 'PARQUET', owner: 'reporting_team'   },
  { id: 'ds-012',      name: 'mis_report',           domain: 'Reporting',   format: 'PARQUET', owner: 'reporting_team'   },
]

const JOBS = [
  { id: 'job-001', name: 'customer_ingest',      domain: 'ETL',        type: 'ingest',      path: 'etl/customer_ingest.py',       risk_tags: ['PII', 'audit_required'] },
  { id: 'job-002', name: 'transaction_feed',     domain: 'ETL',        type: 'ingest',      path: 'etl/transaction_feed.py',      risk_tags: ['audit_required'] },
  { id: 'job-003', name: 'credit_risk_scoring',  domain: 'Risk',       type: 'transform',   path: 'risk/credit_risk.py',          risk_tags: ['audit_required', 'regulatory_report'] },
  { id: 'job-004', name: 'market_risk_engine',   domain: 'Risk',       type: 'transform',   path: 'risk/market_risk.py',          risk_tags: ['regulatory_report'] },
  { id: 'job-005', name: 'risk_pipeline_orch',   domain: 'Risk',       type: 'orchestrate', path: 'etl/risk_pipeline.py',         risk_tags: ['audit_required', 'regulatory_report'] },
  { id: 'job-006', name: 'limit_monitor',        domain: 'Risk',       type: 'monitor',     path: 'risk/limit_monitor.py',        risk_tags: ['audit_required'] },
  { id: 'job-007', name: 'kyc_validator',        domain: 'Compliance', type: 'validate',    path: 'compliance/kyc_validator.py',  risk_tags: ['PII', 'audit_required'] },
  { id: 'job-008', name: 'aml_screening_job',    domain: 'Compliance', type: 'screen',      path: 'compliance/aml_screening.py',  risk_tags: ['PII', 'audit_required'] },
  { id: 'job-009', name: 'ccar_report_gen',      domain: 'Reporting',  type: 'report',      path: 'reporting/ccar_report.py',     risk_tags: ['regulatory_report'] },
  { id: 'job-010', name: 'basel3_report_gen',    domain: 'Reporting',  type: 'report',      path: 'reporting/basel3.py',          risk_tags: ['regulatory_report'] },
  { id: 'job-011', name: 'mis_daily_report',     domain: 'Reporting',  type: 'report',      path: 'reporting/mis_daily.py',       risk_tags: [] },
]

/* columns keyed by dataset name */
const COLUMNS_BY_DS = {
  customer_master: [
    { id: 'col-001-01', name: 'customer_id',        dtype: 'BIGINT',   pii: false, pk: true  },
    { id: 'col-001-02', name: 'first_name',         dtype: 'STRING',   pii: true             },
    { id: 'col-001-03', name: 'last_name',          dtype: 'STRING',   pii: true             },
    { id: 'col-001-04', name: 'ssn',                dtype: 'STRING',   pii: true             },
    { id: 'col-001-05', name: 'date_of_birth',      dtype: 'DATE',     pii: true             },
    { id: 'col-001-06', name: 'email',              dtype: 'STRING',   pii: true             },
    { id: 'col-001-07', name: 'nationality',        dtype: 'STRING',   pii: false            },
    { id: 'col-001-08', name: 'customer_segment',   dtype: 'STRING',   pii: false            },
    { id: 'col-001-09', name: 'risk_rating',        dtype: 'STRING',   pii: false            },
    { id: 'col-001-10', name: 'kyc_status',         dtype: 'STRING',   pii: false            },
    { id: 'col-001-11', name: 'legacy_customer_id', dtype: 'STRING',   pii: false, deprecated: true },
  ],
  accounts: [
    { id: 'col-002-01', name: 'account_id',      dtype: 'BIGINT',  pii: false, pk: true },
    { id: 'col-002-02', name: 'customer_id',     dtype: 'BIGINT',  pii: false            },
    { id: 'col-002-03', name: 'account_type',    dtype: 'STRING',  pii: false            },
    { id: 'col-002-04', name: 'current_balance', dtype: 'DECIMAL', pii: false            },
    { id: 'col-002-05', name: 'credit_limit',    dtype: 'DECIMAL', pii: false            },
    { id: 'col-002-06', name: 'status',          dtype: 'STRING',  pii: false            },
  ],
  transactions: [
    { id: 'col-003-01', name: 'transaction_id',   dtype: 'BIGINT',  pii: false, pk: true },
    { id: 'col-003-02', name: 'account_id',       dtype: 'BIGINT',  pii: false            },
    { id: 'col-003-03', name: 'amount',           dtype: 'DECIMAL', pii: false            },
    { id: 'col-003-04', name: 'currency',         dtype: 'STRING',  pii: false            },
    { id: 'col-003-05', name: 'amount_usd',       dtype: 'DECIMAL', pii: false            },
    { id: 'col-003-06', name: 'fx_rate',          dtype: 'DECIMAL', pii: false            },
    { id: 'col-003-07', name: 'transaction_date', dtype: 'DATE',    pii: false            },
    { id: 'col-003-08', name: 'is_suspicious',    dtype: 'BOOLEAN', pii: false            },
    { id: 'col-003-09', name: 'aml_score',        dtype: 'DECIMAL', pii: false            },
    { id: 'col-003-10', name: 'old_amount',       dtype: 'DECIMAL', pii: false, deprecated: true },
  ],
  market_data: [
    { id: 'col-004-01', name: 'market_id',      dtype: 'BIGINT',  pii: false, pk: true },
    { id: 'col-004-02', name: 'currency_pair',  dtype: 'STRING',  pii: false            },
    { id: 'col-004-03', name: 'spot_rate_mid',  dtype: 'DECIMAL', pii: false            },
    { id: 'col-004-04', name: 'volatility_1d',  dtype: 'DECIMAL', pii: false            },
    { id: 'col-004-05', name: 'volatility_10d', dtype: 'DECIMAL', pii: false            },
    { id: 'col-004-06', name: 'spot_rate_old',  dtype: 'DECIMAL', pii: false, deprecated: true },
  ],
  risk_scores: [
    { id: 'col-005-01', name: 'score_id',       dtype: 'BIGINT',  pii: false, pk: true },
    { id: 'col-005-02', name: 'customer_id',    dtype: 'BIGINT',  pii: false            },
    { id: 'col-005-03', name: 'pd_score',       dtype: 'DECIMAL', pii: false            },
    { id: 'col-005-04', name: 'lgd_estimate',   dtype: 'DECIMAL', pii: false            },
    { id: 'col-005-05', name: 'ead',            dtype: 'DECIMAL', pii: false            },
    { id: 'col-005-06', name: 'expected_loss',  dtype: 'DECIMAL', pii: false            },
    { id: 'col-005-07', name: 'scorecard_band', dtype: 'STRING',  pii: false            },
    { id: 'col-005-08', name: 'as_of_date',     dtype: 'DATE',    pii: false            },
  ],
  var_results: [
    { id: 'col-006-01', name: 'var_id',             dtype: 'BIGINT',  pii: false, pk: true },
    { id: 'col-006-02', name: 'as_of_date',         dtype: 'DATE',    pii: false            },
    { id: 'col-006-03', name: 'var_99',             dtype: 'DECIMAL', pii: false            },
    { id: 'col-006-04', name: 'var_95',             dtype: 'DECIMAL', pii: false            },
    { id: 'col-006-05', name: 'expected_shortfall', dtype: 'DECIMAL', pii: false            },
  ],
  limit_breaches: [
    { id: 'col-007-01', name: 'breach_id',     dtype: 'BIGINT',  pii: false, pk: true },
    { id: 'col-007-02', name: 'customer_id',   dtype: 'BIGINT',  pii: false            },
    { id: 'col-007-03', name: 'account_id',    dtype: 'BIGINT',  pii: false            },
    { id: 'col-007-04', name: 'limit_type',    dtype: 'STRING',  pii: false            },
    { id: 'col-007-05', name: 'breach_amount', dtype: 'DECIMAL', pii: false            },
    { id: 'col-007-06', name: 'severity',      dtype: 'STRING',  pii: false            },
    { id: 'col-007-07', name: 'status',        dtype: 'STRING',  pii: false            },
  ],
  capital_requirements: [
    { id: 'col-008-01', name: 'req_id',              dtype: 'BIGINT',  pii: false, pk: true },
    { id: 'col-008-02', name: 'as_of_date',          dtype: 'DATE',    pii: false            },
    { id: 'col-008-03', name: 'portfolio_segment',   dtype: 'STRING',  pii: false            },
    { id: 'col-008-04', name: 'rwa_credit',          dtype: 'DECIMAL', pii: false            },
    { id: 'col-008-05', name: 'rwa_market',          dtype: 'DECIMAL', pii: false            },
    { id: 'col-008-06', name: 'rwa_total',           dtype: 'DECIMAL', pii: false            },
    { id: 'col-008-07', name: 'tier1_ratio',         dtype: 'DECIMAL', pii: false            },
    { id: 'col-008-08', name: 'total_capital_ratio', dtype: 'DECIMAL', pii: false            },
  ],
  kyc_screening: [
    { id: 'col-009-01', name: 'kyc_id',        dtype: 'BIGINT',  pii: false, pk: true },
    { id: 'col-009-02', name: 'customer_id',   dtype: 'BIGINT',  pii: false            },
    { id: 'col-009-03', name: 'kyc_tier',      dtype: 'STRING',  pii: false            },
    { id: 'col-009-04', name: 'risk_category', dtype: 'STRING',  pii: false            },
    { id: 'col-009-05', name: 'verified_at',   dtype: 'DATETIME',pii: false            },
    { id: 'col-009-06', name: 'pep_flag',      dtype: 'BOOLEAN', pii: false            },
  ],
  aml_alerts: [
    { id: 'col-010-01', name: 'alert_id',      dtype: 'BIGINT',  pii: false, pk: true },
    { id: 'col-010-02', name: 'customer_id',   dtype: 'BIGINT',  pii: false            },
    { id: 'col-010-03', name: 'transaction_id',dtype: 'BIGINT',  pii: false            },
    { id: 'col-010-04', name: 'alert_type',    dtype: 'STRING',  pii: false            },
    { id: 'col-010-05', name: 'aml_score',     dtype: 'DECIMAL', pii: false            },
    { id: 'col-010-06', name: 'status',        dtype: 'STRING',  pii: false            },
  ],
  ccar_output: [
    { id: 'col-011-01', name: 'report_id',     dtype: 'STRING',  pii: false, pk: true },
    { id: 'col-011-02', name: 'scenario',      dtype: 'STRING',  pii: false            },
    { id: 'col-011-03', name: 'tier1_capital', dtype: 'DECIMAL', pii: false            },
    { id: 'col-011-04', name: 'rwa_total',     dtype: 'DECIMAL', pii: false            },
    { id: 'col-011-05', name: 'capital_ratio', dtype: 'DECIMAL', pii: false            },
  ],
  mis_report: [
    { id: 'col-012-01', name: 'report_date',       dtype: 'DATE',    pii: false },
    { id: 'col-012-02', name: 'customer_segment',  dtype: 'STRING',  pii: false },
    { id: 'col-012-03', name: 'total_balance',     dtype: 'DECIMAL', pii: false },
    { id: 'col-012-04', name: 'txn_count',         dtype: 'INTEGER', pii: false },
    { id: 'col-012-05', name: 'txn_volume_usd',    dtype: 'DECIMAL', pii: false },
  ],
}

const JOB_EDGES = [
  { src: 'job-001', tgt: 'ds-src-crm', rel: 'READS_FROM' },
  { src: 'job-001', tgt: 'ds-001',     rel: 'WRITES_TO'  },
  { src: 'job-001', tgt: 'ds-002',     rel: 'WRITES_TO'  },
  { src: 'job-002', tgt: 'ds-src-pay', rel: 'READS_FROM' },
  { src: 'job-002', tgt: 'ds-004',     rel: 'READS_FROM' },
  { src: 'job-002', tgt: 'ds-001',     rel: 'READS_FROM' },
  { src: 'job-002', tgt: 'ds-003',     rel: 'WRITES_TO'  },
  { src: 'job-002', tgt: 'job-001',    rel: 'DEPENDS_ON' },
  { src: 'job-003', tgt: 'ds-001',     rel: 'READS_FROM' },
  { src: 'job-003', tgt: 'ds-002',     rel: 'READS_FROM' },
  { src: 'job-003', tgt: 'ds-003',     rel: 'READS_FROM' },
  { src: 'job-003', tgt: 'ds-005',     rel: 'READS_FROM' },
  { src: 'job-003', tgt: 'ds-005',     rel: 'WRITES_TO'  },
  { src: 'job-003', tgt: 'job-002',    rel: 'DEPENDS_ON' },
  { src: 'job-004', tgt: 'ds-004',     rel: 'READS_FROM' },
  { src: 'job-004', tgt: 'ds-006',     rel: 'READS_FROM' },
  { src: 'job-004', tgt: 'ds-006',     rel: 'WRITES_TO'  },
  { src: 'job-004', tgt: 'job-002',    rel: 'DEPENDS_ON' },
  { src: 'job-005', tgt: 'ds-005',     rel: 'READS_FROM' },
  { src: 'job-005', tgt: 'ds-006',     rel: 'READS_FROM' },
  { src: 'job-005', tgt: 'ds-001',     rel: 'READS_FROM' },
  { src: 'job-005', tgt: 'ds-002',     rel: 'READS_FROM' },
  { src: 'job-005', tgt: 'ds-008',     rel: 'WRITES_TO'  },
  { src: 'job-005', tgt: 'job-003',    rel: 'DEPENDS_ON' },
  { src: 'job-005', tgt: 'job-004',    rel: 'DEPENDS_ON' },
  { src: 'job-006', tgt: 'ds-005',     rel: 'READS_FROM' },
  { src: 'job-006', tgt: 'ds-006',     rel: 'READS_FROM' },
  { src: 'job-006', tgt: 'ds-007',     rel: 'READS_FROM' },
  { src: 'job-006', tgt: 'ds-007',     rel: 'WRITES_TO'  },
  { src: 'job-006', tgt: 'job-005',    rel: 'DEPENDS_ON' },
  { src: 'job-007', tgt: 'ds-001',     rel: 'READS_FROM' },
  { src: 'job-007', tgt: 'ds-009',     rel: 'WRITES_TO'  },
  { src: 'job-007', tgt: 'job-001',    rel: 'DEPENDS_ON' },
  { src: 'job-008', tgt: 'ds-003',     rel: 'READS_FROM' },
  { src: 'job-008', tgt: 'ds-001',     rel: 'READS_FROM' },
  { src: 'job-008', tgt: 'ds-src-sanc',rel: 'READS_FROM' },
  { src: 'job-008', tgt: 'ds-010',     rel: 'WRITES_TO'  },
  { src: 'job-008', tgt: 'job-002',    rel: 'DEPENDS_ON' },
  { src: 'job-008', tgt: 'job-007',    rel: 'DEPENDS_ON' },
  { src: 'job-009', tgt: 'ds-008',     rel: 'READS_FROM' },
  { src: 'job-009', tgt: 'ds-005',     rel: 'READS_FROM' },
  { src: 'job-009', tgt: 'ds-006',     rel: 'READS_FROM' },
  { src: 'job-009', tgt: 'ds-011',     rel: 'WRITES_TO'  },
  { src: 'job-009', tgt: 'job-005',    rel: 'DEPENDS_ON' },
  { src: 'job-010', tgt: 'ds-008',     rel: 'READS_FROM' },
  { src: 'job-010', tgt: 'ds-005',     rel: 'READS_FROM' },
  { src: 'job-010', tgt: 'ds-011',     rel: 'WRITES_TO'  },
  { src: 'job-010', tgt: 'job-005',    rel: 'DEPENDS_ON' },
  { src: 'job-011', tgt: 'ds-003',     rel: 'READS_FROM' },
  { src: 'job-011', tgt: 'ds-002',     rel: 'READS_FROM' },
  { src: 'job-011', tgt: 'ds-001',     rel: 'READS_FROM' },
  { src: 'job-011', tgt: 'ds-012',     rel: 'WRITES_TO'  },
  { src: 'job-011', tgt: 'job-002',    rel: 'DEPENDS_ON' },
]

const DATASET_JOINS = [
  { src: 'ds-002', tgt: 'ds-001', rel: 'REFERENCES',  join_key: 'customer_id',    join_type: 'FK'     },
  { src: 'ds-003', tgt: 'ds-002', rel: 'REFERENCES',  join_key: 'account_id',     join_type: 'FK'     },
  { src: 'ds-005', tgt: 'ds-001', rel: 'REFERENCES',  join_key: 'customer_id',    join_type: 'FK'     },
  { src: 'ds-007', tgt: 'ds-001', rel: 'REFERENCES',  join_key: 'customer_id',    join_type: 'FK'     },
  { src: 'ds-007', tgt: 'ds-002', rel: 'REFERENCES',  join_key: 'account_id',     join_type: 'FK'     },
  { src: 'ds-009', tgt: 'ds-001', rel: 'REFERENCES',  join_key: 'customer_id',    join_type: 'FK'     },
  { src: 'ds-010', tgt: 'ds-001', rel: 'REFERENCES',  join_key: 'customer_id',    join_type: 'FK'     },
  { src: 'ds-010', tgt: 'ds-003', rel: 'REFERENCES',  join_key: 'transaction_id', join_type: 'FK'     },
  { src: 'ds-008', tgt: 'ds-005', rel: 'DERIVED_FROM',join_key: 'customer_id',    join_type: 'AGG'    },
  { src: 'ds-008', tgt: 'ds-006', rel: 'DERIVED_FROM',join_key: 'as_of_date',     join_type: 'AGG'    },
  { src: 'ds-003', tgt: 'ds-004', rel: 'JOINS_WITH',  join_key: 'currency_pair',  join_type: 'LOOKUP' },
]

const COLUMN_EDGES = [
  { src: 'col-003-05', tgt: 'col-004-03', expression: 'amount * spot_rate_mid',                         confidence: 'verified' },
  { src: 'col-003-06', tgt: 'col-004-03', expression: 'spot_rate_mid lookup by currency/date',           confidence: 'verified' },
  { src: 'col-005-03', tgt: 'col-003-03', expression: 'logistic_scorecard(amount, dpd_30, dpd_60)',      confidence: 'inferred' },
  { src: 'col-005-03', tgt: 'col-002-04', expression: 'utilisation_ratio = current_balance/credit_limit',confidence: 'inferred' },
  { src: 'col-005-05', tgt: 'col-002-05', expression: 'EAD = max(credit_limit, current_balance)',        confidence: 'verified' },
  { src: 'col-005-06', tgt: 'col-005-03', expression: 'EL = pd_score * lgd_estimate * ead',              confidence: 'verified' },
  { src: 'col-005-06', tgt: 'col-005-04', expression: 'EL = pd_score * lgd_estimate * ead',              confidence: 'verified' },
  { src: 'col-005-06', tgt: 'col-005-05', expression: 'EL = pd_score * lgd_estimate * ead',              confidence: 'verified' },
  { src: 'col-006-03', tgt: 'col-004-04', expression: 'historical_var_99(volatility_1d, portfolio)',     confidence: 'verified' },
  { src: 'col-006-05', tgt: 'col-006-03', expression: 'ES = mean(losses > VaR_99)',                      confidence: 'verified' },
  { src: 'col-008-04', tgt: 'col-005-03', expression: 'RWA_credit = EAD * risk_weight(PD, LGD)',         confidence: 'verified' },
  { src: 'col-008-04', tgt: 'col-005-05', expression: 'RWA_credit = EAD * risk_weight(PD, LGD)',         confidence: 'verified' },
  { src: 'col-008-05', tgt: 'col-006-03', expression: 'RWA_market = var_99 * 12.5',                      confidence: 'verified' },
  { src: 'col-009-04', tgt: 'col-001-07', expression: 'FATF country-risk lookup table',                  confidence: 'inferred' },
  { src: 'col-010-05', tgt: 'col-003-05', expression: 'aml_velocity_score(amount_usd, txn_count)',       confidence: 'inferred' },
  { src: 'col-010-05', tgt: 'col-003-08', expression: 'composite_aml_score(is_suspicious, velocity)',    confidence: 'inferred' },
  { src: 'col-011-03', tgt: 'col-008-07', expression: 'tier1_capital = tier1_ratio * RWA_total',         confidence: 'verified' },
]

// ── Lookups ────────────────────────────────────────────────────────────────

const DS_BY_ID   = Object.fromEntries(DATASETS.map(d => [d.id, d]))
/** flat map: colId → { col, dsName } */
const COL_BY_ID  = (() => {
  const m = {}
  for (const [dsName, cols] of Object.entries(COLUMNS_BY_DS)) {
    for (const c of cols) m[c.id] = { col: c, dsName }
  }
  return m
})()

// ── Graph builder ──────────────────────────────────────────────────────────

/**
 * Build vis-network nodes + edges for a given display mode.
 * @param {'all'|'process'|'data'|'fk'|'col'|'schema'} mode
 * @returns {{ nodes: object[], edges: object[] }}
 */
export function buildGraphForMode(mode) {
  const nodeMap = {}   // id → node
  const edges   = []

  const addDS = (id) => {
    if (nodeMap[id]) return
    const d = DS_BY_ID[id]
    if (!d) return
    nodeMap[id] = {
      id,
      label: d.name,
      group: 'Dataset',
      title: `Dataset · ${d.domain}\nowner: ${d.owner}\nformat: ${d.format}`,
      props: { type: 'Dataset', name: d.name, domain: d.domain, owner: d.owner, format: d.format },
    }
  }

  const addJob = (id) => {
    if (nodeMap[id]) return
    const j = JOBS.find(x => x.id === id)
    if (!j) return
    nodeMap[id] = {
      id,
      label: j.name,
      group: 'Job',
      title: `Job · ${j.domain} / ${j.type}\n${j.path}`,
      props: { type: 'Job', name: j.name, domain: j.domain, job_type: j.type, path: j.path, risk_tags: j.risk_tags.join(', ') },
    }
  }

  const addCol = (id) => {
    if (nodeMap[id]) return
    const entry = COL_BY_ID[id]
    if (!entry) return
    const { col, dsName } = entry
    nodeMap[id] = {
      id,
      label: col.name,
      group: 'Column',
      title: `Column · ${dsName}.${col.name}\ntype: ${col.dtype}${col.pii ? ' · PII' : ''}`,
      props: { type: 'Column', name: col.name, dataset: dsName, data_type: col.dtype, pii_flag: col.pii },
    }
  }

  if (mode === 'all') {
    // All jobs + datasets + job-edges
    JOBS.forEach(j => addJob(j.id))
    DATASETS.forEach(d => addDS(d.id))
    JOB_EDGES.forEach((e, i) => {
      edges.push({ id: `je-${i}`, from: e.src, to: e.tgt, label: e.rel, rel: e.rel })
    })
    DATASET_JOINS.forEach((e, i) => {
      edges.push({ id: `dj-${i}`, from: e.src, to: e.tgt, label: e.rel, rel: e.rel })
    })
  } else if (mode === 'process') {
    // Job → Job DEPENDS_ON only
    JOB_EDGES.filter(e => e.rel === 'DEPENDS_ON').forEach((e, i) => {
      addJob(e.src); addJob(e.tgt)
      edges.push({ id: `dep-${i}`, from: e.src, to: e.tgt, label: 'DEPENDS_ON', rel: 'DEPENDS_ON' })
    })
  } else if (mode === 'data') {
    // Job ↔ Dataset READS_FROM / WRITES_TO
    JOB_EDGES.filter(e => e.rel === 'READS_FROM' || e.rel === 'WRITES_TO').forEach((e, i) => {
      addJob(e.src); addDS(e.tgt)
      edges.push({ id: `rw-${i}`, from: e.src, to: e.tgt, label: e.rel, rel: e.rel })
    })
  } else if (mode === 'fk') {
    // Dataset → Dataset FK / DERIVED_FROM / JOINS_WITH
    DATASET_JOINS.forEach((e, i) => {
      addDS(e.src); addDS(e.tgt)
      edges.push({ id: `fk-${i}`, from: e.src, to: e.tgt, label: `${e.rel}\n${e.join_key}`, rel: e.rel })
    })
  } else if (mode === 'col') {
    // Column → Column DERIVED_FROM
    COLUMN_EDGES.forEach((e, i) => {
      addCol(e.src); addCol(e.tgt)
      edges.push({ id: `ce-${i}`, from: e.src, to: e.tgt, label: 'DERIVED_FROM', title: e.expression, rel: 'DERIVED_FROM' })
    })
  } else if (mode === 'schema') {
    // Dataset → Column HAS_COLUMN  +  Column → Column DERIVED_FROM
    for (const ds of DATASETS) {
      const cols = COLUMNS_BY_DS[ds.name]
      if (!cols) continue
      addDS(ds.id)
      for (const col of cols) {
        addCol(col.id)
        edges.push({ id: `sc-${col.id}`, from: ds.id, to: col.id, label: 'HAS_COLUMN', rel: 'HAS_COLUMN' })
      }
    }
    // Also add DERIVED_FROM edges between columns so derivation lineage is visible
    COLUMN_EDGES.forEach((e, i) => {
      // Only include if both endpoints were already added (they belong to included datasets)
      if (!nodeMap[e.src] || !nodeMap[e.tgt]) return
      edges.push({ id: `sce-${i}`, from: e.src, to: e.tgt, label: 'DERIVED_FROM', title: e.expression, rel: 'DERIVED_FROM' })
    })
  }

  return { nodes: Object.values(nodeMap), edges }
}

/** Expand: return graph centred on a node id (its immediate neighbours) */
export function buildExpandGraph(nodeId) {
  const nodeMap = {}
  const edges   = []

  const isDS  = nodeId.startsWith('ds-')
  const isCol = nodeId.startsWith('col-')

  const allEdges = [
    ...JOB_EDGES.map((e, i) => ({ ...e, _i: `je${i}` })),
    ...DATASET_JOINS.map((e, i) => ({ ...e, _i: `dj${i}` })),
    ...COLUMN_EDGES.map((e, i) => ({ src: e.src, tgt: e.tgt, rel: 'DERIVED_FROM', _i: `ce${i}`, title: e.expression })),
  ]

  // HAS_COLUMN edges for schema context
  if (isDS) {
    const ds = DS_BY_ID[nodeId]
    const cols = ds && COLUMNS_BY_DS[ds.name]
    if (cols) {
      cols.forEach((c, i) => {
        allEdges.push({ src: nodeId, tgt: c.id, rel: 'HAS_COLUMN', _i: `hc${i}` })
      })
    }
  }
  if (isCol) {
    // also add reverse: which dataset owns this column?
    const entry = COL_BY_ID[nodeId]
    if (entry) {
      const ds = DATASETS.find(d => d.name === entry.dsName)
      if (ds) allEdges.push({ src: ds.id, tgt: nodeId, rel: 'HAS_COLUMN', _i: 'hcr' })
    }
  }

  const addNode = (id) => {
    if (nodeMap[id]) return
    if (id.startsWith('job-'))  { JOBS.find(j => j.id === id) && (nodeMap[id] = { id, label: JOBS.find(j => j.id === id).name, group: 'Job', props: { type: 'Job', name: JOBS.find(j => j.id === id).name } }) }
    else if (id.startsWith('ds-'))  { const d = DS_BY_ID[id]; if (d) nodeMap[id] = { id, label: d.name, group: 'Dataset', props: { type: 'Dataset', name: d.name, domain: d.domain } } }
    else if (id.startsWith('col-')) { const e = COL_BY_ID[id]; if (e) nodeMap[id] = { id, label: e.col.name, group: 'Column', props: { type: 'Column', name: e.col.name, dataset: e.dsName, data_type: e.col.dtype } } }
  }

  allEdges.forEach(e => {
    if (e.src === nodeId || e.tgt === nodeId) {
      addNode(e.src); addNode(e.tgt)
      edges.push({ id: e._i, from: e.src, to: e.tgt, label: e.rel, title: e.title || e.rel, rel: e.rel })
    }
  })

  return { nodes: Object.values(nodeMap), edges }
}

export { DATASETS, JOBS, COLUMNS_BY_DS, COLUMN_EDGES, JOB_EDGES, DATASET_JOINS }
