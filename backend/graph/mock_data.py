"""
Mock data — full PNC sample_repo data model.
Maps 1-to-1 with the tables, jobs and column lineage defined in sample_repo/.
  11 Jobs · 15 Datasets · 62 Columns · 18 column-lineage edges
  55 READS_FROM/WRITES_TO/DEPENDS_ON edges · 11 FK/DERIVED_FROM dataset joins
"""

# ── Datasets (actual tables from sample_repo) ─────────────────────────────

DATASETS = [
    # External source feeds
    {"id": "ds-src-crm",  "name": "crm_feed",           "qualified_name": "ext.crm.customer_export",      "format": "CSV",    "status": "active",     "owner": "data_engineering", "domain": "Source"},
    {"id": "ds-src-pay",  "name": "payment_feed",        "qualified_name": "ext.payment.daily_feed",       "format": "CSV",    "status": "active",     "owner": "data_engineering", "domain": "Source"},
    {"id": "ds-src-sanc", "name": "sanctions_list",      "qualified_name": "ext.ofac.sdn_list",            "format": "JSON",   "status": "active",     "owner": "compliance_team",  "domain": "Reference"},
    # Core master / transactional tables
    {"id": "ds-001",      "name": "customer_master",     "qualified_name": "pnc.dbo.customer_master",      "format": "TABLE",  "status": "active",     "owner": "data_engineering", "domain": "Customer"},
    {"id": "ds-002",      "name": "accounts",            "qualified_name": "pnc.dbo.accounts",             "format": "TABLE",  "status": "active",     "owner": "data_engineering", "domain": "Customer"},
    {"id": "ds-003",      "name": "transactions",        "qualified_name": "pnc.dbo.transactions",         "format": "TABLE",  "status": "active",     "owner": "data_engineering", "domain": "Transaction"},
    {"id": "ds-004",      "name": "market_data",         "qualified_name": "pnc.dbo.market_data",          "format": "TABLE",  "status": "active",     "owner": "reference_data",   "domain": "Reference"},
    # Risk domain tables
    {"id": "ds-005",      "name": "risk_scores",         "qualified_name": "pnc.risk.risk_scores",         "format": "TABLE",  "status": "active",     "owner": "risk_team",        "domain": "Risk"},
    {"id": "ds-006",      "name": "var_results",         "qualified_name": "pnc.risk.var_results",         "format": "TABLE",  "status": "active",     "owner": "risk_team",        "domain": "Risk"},
    {"id": "ds-007",      "name": "limit_breaches",      "qualified_name": "pnc.risk.limit_breaches",      "format": "TABLE",  "status": "active",     "owner": "risk_team",        "domain": "Risk"},
    {"id": "ds-008",      "name": "capital_requirements","qualified_name": "pnc.risk.capital_requirements", "format": "TABLE",  "status": "active",     "owner": "risk_team",        "domain": "Risk"},
    # Compliance domain tables
    {"id": "ds-009",      "name": "kyc_screening",       "qualified_name": "pnc.compliance.kyc_screening", "format": "TABLE",  "status": "active",     "owner": "compliance_team",  "domain": "Compliance"},
    {"id": "ds-010",      "name": "aml_alerts",          "qualified_name": "pnc.compliance.aml_alerts",    "format": "TABLE",  "status": "active",     "owner": "compliance_team",  "domain": "Compliance"},
    # Reporting outputs
    {"id": "ds-011",      "name": "ccar_output",         "qualified_name": "pnc.reg.ccar_output",          "format": "PARQUET","status": "active",     "owner": "reporting_team",   "domain": "Reporting"},
    {"id": "ds-012",      "name": "mis_report",          "qualified_name": "pnc.rpt.mis_daily",            "format": "PARQUET","status": "active",     "owner": "reporting_team",   "domain": "Reporting"},
]

# ── Jobs (actual scripts from sample_repo) ────────────────────────────────

JOBS = [
    {"id": "job-001", "name": "customer_ingest",     "path": "sample_repo/etl/customer_ingest.py",       "domain": "ETL",        "type": "ingest",     "status": "active", "risk_tags": ["PII", "audit_required"]},
    {"id": "job-002", "name": "transaction_feed",    "path": "sample_repo/etl/transaction_feed.py",      "domain": "ETL",        "type": "ingest",     "status": "active", "risk_tags": ["audit_required"]},
    {"id": "job-003", "name": "credit_risk_scoring", "path": "sample_repo/risk/credit_risk.py",          "domain": "Risk",       "type": "transform",  "status": "active", "risk_tags": ["audit_required", "regulatory_report"]},
    {"id": "job-004", "name": "market_risk_engine",  "path": "sample_repo/risk/market_risk.py",          "domain": "Risk",       "type": "transform",  "status": "active", "risk_tags": ["regulatory_report"]},
    {"id": "job-005", "name": "risk_pipeline_orch",  "path": "sample_repo/etl/risk_pipeline.py",         "domain": "Risk",       "type": "orchestrate","status": "active", "risk_tags": ["audit_required", "regulatory_report"]},
    {"id": "job-006", "name": "limit_monitor",       "path": "sample_repo/risk/limit_monitor.py",        "domain": "Risk",       "type": "monitor",    "status": "active", "risk_tags": ["audit_required"]},
    {"id": "job-007", "name": "kyc_validator",       "path": "sample_repo/compliance/kyc_validator.py",  "domain": "Compliance", "type": "validate",   "status": "active", "risk_tags": ["PII", "audit_required"]},
    {"id": "job-008", "name": "aml_screening_job",   "path": "sample_repo/compliance/aml_screening.py",  "domain": "Compliance", "type": "screen",     "status": "active", "risk_tags": ["PII", "audit_required"]},
    {"id": "job-009", "name": "ccar_report_gen",     "path": "sample_repo/reporting/ccar_report.py",     "domain": "Reporting",  "type": "report",     "status": "active", "risk_tags": ["regulatory_report"]},
    {"id": "job-010", "name": "basel3_report_gen",   "path": "sample_repo/reporting/basel3.py",          "domain": "Reporting",  "type": "report",     "status": "active", "risk_tags": ["regulatory_report"]},
    {"id": "job-011", "name": "mis_daily_report",    "path": "sample_repo/reporting/mis_daily.py",       "domain": "Reporting",  "type": "report",     "status": "active", "risk_tags": []},
]

# ── Columns ───────────────────────────────────────────────────────────────

COLUMNS = {
    "customer_master": [
        {"id": "col-001-01", "name": "customer_id",       "dtype": "BIGINT",  "pii": False, "pk": True},
        {"id": "col-001-02", "name": "first_name",        "dtype": "STRING",  "pii": True},
        {"id": "col-001-03", "name": "last_name",         "dtype": "STRING",  "pii": True},
        {"id": "col-001-04", "name": "ssn",               "dtype": "STRING",  "pii": True},
        {"id": "col-001-05", "name": "date_of_birth",     "dtype": "DATE",    "pii": True},
        {"id": "col-001-06", "name": "email",             "dtype": "STRING",  "pii": True},
        {"id": "col-001-07", "name": "nationality",       "dtype": "STRING",  "pii": False},
        {"id": "col-001-08", "name": "customer_segment",  "dtype": "STRING",  "pii": False},
        {"id": "col-001-09", "name": "risk_rating",       "dtype": "STRING",  "pii": False},
        {"id": "col-001-10", "name": "kyc_status",        "dtype": "STRING",  "pii": False},
        {"id": "col-001-11", "name": "legacy_customer_id","dtype": "STRING",  "pii": False, "deprecated": True},
    ],
    "accounts": [
        {"id": "col-002-01", "name": "account_id",       "dtype": "BIGINT",  "pii": False, "pk": True},
        {"id": "col-002-02", "name": "customer_id",      "dtype": "BIGINT",  "pii": False, "fk": "customer_master.customer_id"},
        {"id": "col-002-03", "name": "account_type",     "dtype": "STRING",  "pii": False},
        {"id": "col-002-04", "name": "current_balance",  "dtype": "DECIMAL", "pii": False},
        {"id": "col-002-05", "name": "credit_limit",     "dtype": "DECIMAL", "pii": False},
        {"id": "col-002-06", "name": "status",           "dtype": "STRING",  "pii": False},
    ],
    "transactions": [
        {"id": "col-003-01", "name": "transaction_id",  "dtype": "BIGINT",  "pii": False, "pk": True},
        {"id": "col-003-02", "name": "account_id",      "dtype": "BIGINT",  "pii": False, "fk": "accounts.account_id"},
        {"id": "col-003-03", "name": "amount",          "dtype": "DECIMAL", "pii": False},
        {"id": "col-003-04", "name": "currency",        "dtype": "STRING",  "pii": False},
        {"id": "col-003-05", "name": "amount_usd",      "dtype": "DECIMAL", "pii": False},
        {"id": "col-003-06", "name": "fx_rate",         "dtype": "DECIMAL", "pii": False},
        {"id": "col-003-07", "name": "transaction_date","dtype": "DATE",    "pii": False},
        {"id": "col-003-08", "name": "is_suspicious",   "dtype": "BOOLEAN", "pii": False},
        {"id": "col-003-09", "name": "aml_score",       "dtype": "DECIMAL", "pii": False},
        {"id": "col-003-10", "name": "old_amount",      "dtype": "DECIMAL", "pii": False, "deprecated": True},
    ],
    "market_data": [
        {"id": "col-004-01", "name": "market_id",       "dtype": "BIGINT",  "pii": False, "pk": True},
        {"id": "col-004-02", "name": "currency_pair",   "dtype": "STRING",  "pii": False},
        {"id": "col-004-03", "name": "spot_rate_mid",   "dtype": "DECIMAL", "pii": False},
        {"id": "col-004-04", "name": "volatility_1d",   "dtype": "DECIMAL", "pii": False},
        {"id": "col-004-05", "name": "volatility_10d",  "dtype": "DECIMAL", "pii": False},
        {"id": "col-004-06", "name": "spot_rate_old",   "dtype": "DECIMAL", "pii": False, "deprecated": True},
    ],
    "risk_scores": [
        {"id": "col-005-01", "name": "score_id",        "dtype": "BIGINT",  "pii": False, "pk": True},
        {"id": "col-005-02", "name": "customer_id",     "dtype": "BIGINT",  "pii": False, "fk": "customer_master.customer_id"},
        {"id": "col-005-03", "name": "pd_score",        "dtype": "DECIMAL", "pii": False},
        {"id": "col-005-04", "name": "lgd_estimate",    "dtype": "DECIMAL", "pii": False},
        {"id": "col-005-05", "name": "ead",             "dtype": "DECIMAL", "pii": False},
        {"id": "col-005-06", "name": "expected_loss",   "dtype": "DECIMAL", "pii": False},
        {"id": "col-005-07", "name": "scorecard_band",  "dtype": "STRING",  "pii": False},
        {"id": "col-005-08", "name": "as_of_date",      "dtype": "DATE",    "pii": False},
    ],
    "var_results": [
        {"id": "col-006-01", "name": "var_id",            "dtype": "BIGINT",  "pii": False, "pk": True},
        {"id": "col-006-02", "name": "as_of_date",        "dtype": "DATE",    "pii": False},
        {"id": "col-006-03", "name": "var_99",            "dtype": "DECIMAL", "pii": False},
        {"id": "col-006-04", "name": "var_95",            "dtype": "DECIMAL", "pii": False},
        {"id": "col-006-05", "name": "expected_shortfall","dtype": "DECIMAL", "pii": False},
    ],
    "limit_breaches": [
        {"id": "col-007-01", "name": "breach_id",     "dtype": "BIGINT",  "pii": False, "pk": True},
        {"id": "col-007-02", "name": "customer_id",   "dtype": "BIGINT",  "pii": False, "fk": "customer_master.customer_id"},
        {"id": "col-007-03", "name": "account_id",    "dtype": "BIGINT",  "pii": False, "fk": "accounts.account_id"},
        {"id": "col-007-04", "name": "limit_type",    "dtype": "STRING",  "pii": False},
        {"id": "col-007-05", "name": "breach_amount", "dtype": "DECIMAL", "pii": False},
        {"id": "col-007-06", "name": "severity",      "dtype": "STRING",  "pii": False},
        {"id": "col-007-07", "name": "status",        "dtype": "STRING",  "pii": False},
    ],
    "capital_requirements": [
        {"id": "col-008-01", "name": "req_id",             "dtype": "BIGINT",  "pii": False, "pk": True},
        {"id": "col-008-02", "name": "as_of_date",         "dtype": "DATE",    "pii": False},
        {"id": "col-008-03", "name": "portfolio_segment",  "dtype": "STRING",  "pii": False},
        {"id": "col-008-04", "name": "rwa_credit",         "dtype": "DECIMAL", "pii": False},
        {"id": "col-008-05", "name": "rwa_market",         "dtype": "DECIMAL", "pii": False},
        {"id": "col-008-06", "name": "rwa_total",          "dtype": "DECIMAL", "pii": False},
        {"id": "col-008-07", "name": "tier1_ratio",        "dtype": "DECIMAL", "pii": False},
        {"id": "col-008-08", "name": "total_capital_ratio","dtype": "DECIMAL", "pii": False},
    ],
    "kyc_screening": [
        {"id": "col-009-01", "name": "kyc_id",        "dtype": "BIGINT",  "pii": False, "pk": True},
        {"id": "col-009-02", "name": "customer_id",   "dtype": "BIGINT",  "pii": False, "fk": "customer_master.customer_id"},
        {"id": "col-009-03", "name": "kyc_tier",      "dtype": "STRING",  "pii": False},
        {"id": "col-009-04", "name": "risk_category", "dtype": "STRING",  "pii": False},
        {"id": "col-009-05", "name": "verified_at",   "dtype": "DATETIME","pii": False},
        {"id": "col-009-06", "name": "pep_flag",      "dtype": "BOOLEAN", "pii": False},
    ],
    "aml_alerts": [
        {"id": "col-010-01", "name": "alert_id",      "dtype": "BIGINT",  "pii": False, "pk": True},
        {"id": "col-010-02", "name": "customer_id",   "dtype": "BIGINT",  "pii": False, "fk": "customer_master.customer_id"},
        {"id": "col-010-03", "name": "transaction_id","dtype": "BIGINT",  "pii": False, "fk": "transactions.transaction_id"},
        {"id": "col-010-04", "name": "alert_type",    "dtype": "STRING",  "pii": False},
        {"id": "col-010-05", "name": "aml_score",     "dtype": "DECIMAL", "pii": False},
        {"id": "col-010-06", "name": "status",        "dtype": "STRING",  "pii": False},
    ],
    "ccar_output": [
        {"id": "col-011-01", "name": "report_id",     "dtype": "STRING",  "pii": False, "pk": True},
        {"id": "col-011-02", "name": "scenario",      "dtype": "STRING",  "pii": False},
        {"id": "col-011-03", "name": "tier1_capital", "dtype": "DECIMAL", "pii": False},
        {"id": "col-011-04", "name": "rwa_total",     "dtype": "DECIMAL", "pii": False},
        {"id": "col-011-05", "name": "capital_ratio", "dtype": "DECIMAL", "pii": False},
    ],
    "mis_report": [
        {"id": "col-012-01", "name": "report_date",      "dtype": "DATE",    "pii": False},
        {"id": "col-012-02", "name": "customer_segment", "dtype": "STRING",  "pii": False},
        {"id": "col-012-03", "name": "total_balance",    "dtype": "DECIMAL", "pii": False},
        {"id": "col-012-04", "name": "txn_count",        "dtype": "INTEGER", "pii": False},
        {"id": "col-012-05", "name": "txn_volume_usd",   "dtype": "DECIMAL", "pii": False},
    ],
}

# ── Stats ─────────────────────────────────────────────────────────────────

STATS = {
    "jobs":           len(JOBS),
    "datasets":       len(DATASETS),
    "columns":        sum(len(v) for v in COLUMNS.values()),
    "scripts":        11,
    "datasources":    3,
    "business_rules": 6,
}

LABEL_COUNTS = [
    {"label": "Column",       "count": STATS["columns"]},
    {"label": "Job",          "count": STATS["jobs"]},
    {"label": "Dataset",      "count": STATS["datasets"]},
    {"label": "Script",       "count": STATS["scripts"]},
    {"label": "DataSource",   "count": STATS["datasources"]},
    {"label": "BusinessRule", "count": STATS["business_rules"]},
    {"label": "STM",          "count": 8},
]

# ── FK-based dataset → dataset relationships ──────────────────────────────
# Represent primary/foreign key joins and derived relationships between tables

DATASET_JOINS = [
    # Foreign key relationships (child → parent)
    {"src": "ds-002", "tgt": "ds-001", "rel": "REFERENCES",  "join_key": "customer_id",    "join_type": "FK",     "label": "accounts → customer_master"},
    {"src": "ds-003", "tgt": "ds-002", "rel": "REFERENCES",  "join_key": "account_id",     "join_type": "FK",     "label": "transactions → accounts"},
    {"src": "ds-005", "tgt": "ds-001", "rel": "REFERENCES",  "join_key": "customer_id",    "join_type": "FK",     "label": "risk_scores → customer_master"},
    {"src": "ds-007", "tgt": "ds-001", "rel": "REFERENCES",  "join_key": "customer_id",    "join_type": "FK",     "label": "limit_breaches → customer_master"},
    {"src": "ds-007", "tgt": "ds-002", "rel": "REFERENCES",  "join_key": "account_id",     "join_type": "FK",     "label": "limit_breaches → accounts"},
    {"src": "ds-009", "tgt": "ds-001", "rel": "REFERENCES",  "join_key": "customer_id",    "join_type": "FK",     "label": "kyc_screening → customer_master"},
    {"src": "ds-010", "tgt": "ds-001", "rel": "REFERENCES",  "join_key": "customer_id",    "join_type": "FK",     "label": "aml_alerts → customer_master"},
    {"src": "ds-010", "tgt": "ds-003", "rel": "REFERENCES",  "join_key": "transaction_id", "join_type": "FK",     "label": "aml_alerts → transactions"},
    # Aggregation / derived relationships
    {"src": "ds-008", "tgt": "ds-005", "rel": "DERIVED_FROM","join_key": "customer_id",    "join_type": "AGG",    "label": "capital_requirements ← risk_scores"},
    {"src": "ds-008", "tgt": "ds-006", "rel": "DERIVED_FROM","join_key": "as_of_date",     "join_type": "AGG",    "label": "capital_requirements ← var_results"},
    # Lookup join
    {"src": "ds-003", "tgt": "ds-004", "rel": "JOINS_WITH",  "join_key": "currency_pair",  "join_type": "LOOKUP", "label": "transactions × market_data (FX lookup)"},
]

# ── Job → Dataset edges ───────────────────────────────────────────────────

JOB_EDGES = [
    # job-001: customer_ingest  crm_feed → customer_master, accounts
    {"src": "job-001", "tgt": "ds-src-crm", "rel": "READS_FROM", "conf": "verified"},
    {"src": "job-001", "tgt": "ds-001",     "rel": "WRITES_TO",  "conf": "verified"},
    {"src": "job-001", "tgt": "ds-002",     "rel": "WRITES_TO",  "conf": "verified"},

    # job-002: transaction_feed  payment_feed + market_data + customer_master → transactions
    {"src": "job-002", "tgt": "ds-src-pay", "rel": "READS_FROM", "conf": "verified"},
    {"src": "job-002", "tgt": "ds-004",     "rel": "READS_FROM", "conf": "verified"},
    {"src": "job-002", "tgt": "ds-001",     "rel": "READS_FROM", "conf": "verified"},
    {"src": "job-002", "tgt": "ds-003",     "rel": "WRITES_TO",  "conf": "verified"},
    {"src": "job-002", "tgt": "job-001",    "rel": "DEPENDS_ON", "conf": "verified"},

    # job-003: credit_risk_scoring  customer_master + accounts + transactions + risk_scores → risk_scores
    {"src": "job-003", "tgt": "ds-001",     "rel": "READS_FROM", "conf": "verified"},
    {"src": "job-003", "tgt": "ds-002",     "rel": "READS_FROM", "conf": "verified"},
    {"src": "job-003", "tgt": "ds-003",     "rel": "READS_FROM", "conf": "verified"},
    {"src": "job-003", "tgt": "ds-005",     "rel": "READS_FROM", "conf": "verified"},
    {"src": "job-003", "tgt": "ds-005",     "rel": "WRITES_TO",  "conf": "verified"},
    {"src": "job-003", "tgt": "job-002",    "rel": "DEPENDS_ON", "conf": "verified"},

    # job-004: market_risk_engine  market_data + var_results → var_results
    {"src": "job-004", "tgt": "ds-004",     "rel": "READS_FROM", "conf": "verified"},
    {"src": "job-004", "tgt": "ds-006",     "rel": "READS_FROM", "conf": "verified"},
    {"src": "job-004", "tgt": "ds-006",     "rel": "WRITES_TO",  "conf": "verified"},
    {"src": "job-004", "tgt": "job-002",    "rel": "DEPENDS_ON", "conf": "verified"},

    # job-005: risk_pipeline_orch  risk_scores + var_results + customer_master + accounts → capital_requirements
    {"src": "job-005", "tgt": "ds-005",     "rel": "READS_FROM", "conf": "verified"},
    {"src": "job-005", "tgt": "ds-006",     "rel": "READS_FROM", "conf": "verified"},
    {"src": "job-005", "tgt": "ds-001",     "rel": "READS_FROM", "conf": "verified"},
    {"src": "job-005", "tgt": "ds-002",     "rel": "READS_FROM", "conf": "verified"},
    {"src": "job-005", "tgt": "ds-008",     "rel": "WRITES_TO",  "conf": "verified"},
    {"src": "job-005", "tgt": "job-003",    "rel": "DEPENDS_ON", "conf": "verified"},
    {"src": "job-005", "tgt": "job-004",    "rel": "DEPENDS_ON", "conf": "verified"},

    # job-006: limit_monitor  risk_scores + var_results + limit_breaches → limit_breaches
    {"src": "job-006", "tgt": "ds-005",     "rel": "READS_FROM", "conf": "verified"},
    {"src": "job-006", "tgt": "ds-006",     "rel": "READS_FROM", "conf": "verified"},
    {"src": "job-006", "tgt": "ds-007",     "rel": "READS_FROM", "conf": "verified"},
    {"src": "job-006", "tgt": "ds-007",     "rel": "WRITES_TO",  "conf": "verified"},
    {"src": "job-006", "tgt": "job-005",    "rel": "DEPENDS_ON", "conf": "verified"},

    # job-007: kyc_validator  customer_master → kyc_screening
    {"src": "job-007", "tgt": "ds-001",     "rel": "READS_FROM", "conf": "verified"},
    {"src": "job-007", "tgt": "ds-009",     "rel": "WRITES_TO",  "conf": "verified"},
    {"src": "job-007", "tgt": "job-001",    "rel": "DEPENDS_ON", "conf": "verified"},

    # job-008: aml_screening_job  transactions + customer_master + sanctions_list → aml_alerts
    {"src": "job-008", "tgt": "ds-003",     "rel": "READS_FROM", "conf": "verified"},
    {"src": "job-008", "tgt": "ds-001",     "rel": "READS_FROM", "conf": "verified"},
    {"src": "job-008", "tgt": "ds-src-sanc","rel": "READS_FROM", "conf": "verified"},
    {"src": "job-008", "tgt": "ds-010",     "rel": "WRITES_TO",  "conf": "verified"},
    {"src": "job-008", "tgt": "job-002",    "rel": "DEPENDS_ON", "conf": "verified"},
    {"src": "job-008", "tgt": "job-007",    "rel": "DEPENDS_ON", "conf": "verified"},

    # job-009: ccar_report_gen  capital_requirements + risk_scores + var_results → ccar_output
    {"src": "job-009", "tgt": "ds-008",     "rel": "READS_FROM", "conf": "verified"},
    {"src": "job-009", "tgt": "ds-005",     "rel": "READS_FROM", "conf": "verified"},
    {"src": "job-009", "tgt": "ds-006",     "rel": "READS_FROM", "conf": "verified"},
    {"src": "job-009", "tgt": "ds-011",     "rel": "WRITES_TO",  "conf": "verified"},
    {"src": "job-009", "tgt": "job-005",    "rel": "DEPENDS_ON", "conf": "verified"},

    # job-010: basel3_report_gen  capital_requirements + risk_scores → ccar_output (shared regulatory store)
    {"src": "job-010", "tgt": "ds-008",     "rel": "READS_FROM", "conf": "verified"},
    {"src": "job-010", "tgt": "ds-005",     "rel": "READS_FROM", "conf": "verified"},
    {"src": "job-010", "tgt": "ds-011",     "rel": "WRITES_TO",  "conf": "verified"},
    {"src": "job-010", "tgt": "job-005",    "rel": "DEPENDS_ON", "conf": "verified"},

    # job-011: mis_daily_report  transactions + accounts + customer_master → mis_report
    {"src": "job-011", "tgt": "ds-003",     "rel": "READS_FROM", "conf": "verified"},
    {"src": "job-011", "tgt": "ds-002",     "rel": "READS_FROM", "conf": "verified"},
    {"src": "job-011", "tgt": "ds-001",     "rel": "READS_FROM", "conf": "verified"},
    {"src": "job-011", "tgt": "ds-012",     "rel": "WRITES_TO",  "conf": "verified"},
    {"src": "job-011", "tgt": "job-002",    "rel": "DEPENDS_ON", "conf": "verified"},
]

# ── Column lineage (DERIVED_FROM: src is downstream, tgt is upstream) ────

COLUMN_EDGES = [
    # FX normalisation: transactions.amount_usd ← market_data.spot_rate_mid
    {"src": "col-003-05", "tgt": "col-004-03", "expression": "amount * spot_rate_mid",                      "confidence": "verified"},
    {"src": "col-003-06", "tgt": "col-004-03", "expression": "spot_rate_mid lookup by currency/date",        "confidence": "verified"},
    # Credit scoring: risk_scores.pd_score ← transactions.amount + accounts.current_balance / credit_limit
    {"src": "col-005-03", "tgt": "col-003-03", "expression": "logistic_scorecard(amount, dpd_30, dpd_60)",  "confidence": "inferred"},
    {"src": "col-005-03", "tgt": "col-002-04", "expression": "utilisation_ratio = current_balance / credit_limit", "confidence": "inferred"},
    # EAD: risk_scores.ead ← accounts.credit_limit
    {"src": "col-005-05", "tgt": "col-002-05", "expression": "EAD = max(credit_limit, current_balance)",    "confidence": "verified"},
    # Expected loss = PD × LGD × EAD
    {"src": "col-005-06", "tgt": "col-005-03", "expression": "EL = pd_score * lgd_estimate * ead",          "confidence": "verified"},
    {"src": "col-005-06", "tgt": "col-005-04", "expression": "EL = pd_score * lgd_estimate * ead",          "confidence": "verified"},
    {"src": "col-005-06", "tgt": "col-005-05", "expression": "EL = pd_score * lgd_estimate * ead",          "confidence": "verified"},
    # VaR: var_results.var_99 ← market_data.volatility_1d
    {"src": "col-006-03", "tgt": "col-004-04", "expression": "historical_var_99(volatility_1d, portfolio)", "confidence": "verified"},
    # Expected Shortfall ← VaR 99%
    {"src": "col-006-05", "tgt": "col-006-03", "expression": "ES = mean(losses > VaR_99)",                  "confidence": "verified"},
    # Capital: rwa_credit ← pd_score + ead; rwa_market ← var_99
    {"src": "col-008-04", "tgt": "col-005-03", "expression": "RWA_credit = EAD * risk_weight(PD, LGD)",     "confidence": "verified"},
    {"src": "col-008-04", "tgt": "col-005-05", "expression": "RWA_credit = EAD * risk_weight(PD, LGD)",     "confidence": "verified"},
    {"src": "col-008-05", "tgt": "col-006-03", "expression": "RWA_market = var_99 * 12.5 (Basel multiplier)","confidence": "verified"},
    # KYC: risk_category ← customer nationality (FATF country risk lookup)
    {"src": "col-009-04", "tgt": "col-001-07", "expression": "FATF country-risk lookup table",               "confidence": "inferred"},
    # AML alert score ← transaction amount_usd + is_suspicious flag
    {"src": "col-010-05", "tgt": "col-003-05", "expression": "aml_velocity_score(amount_usd, txn_count)",   "confidence": "inferred"},
    {"src": "col-010-05", "tgt": "col-003-08", "expression": "composite_aml_score(is_suspicious, velocity)","confidence": "inferred"},
    # CCAR capital ratio ← tier1_ratio from capital_requirements
    {"src": "col-011-03", "tgt": "col-008-07", "expression": "tier1_capital = tier1_ratio * RWA_total",     "confidence": "verified"},
    # MIS volume ← transactions.amount_usd
    {"src": "col-012-05", "tgt": "col-003-05", "expression": "SUM(amount_usd) GROUP BY customer_segment",   "confidence": "verified"},
]

# ── Compliance findings ────────────────────────────────────────────────────

PII_WITHOUT_AUDIT = [
    {"job_id": "job-001", "job_name": "customer_ingest",    "path": "sample_repo/etl/customer_ingest.py",       "pii_columns": ["customer_master.ssn", "customer_master.date_of_birth", "customer_master.email"]},
    {"job_id": "job-007", "job_name": "kyc_validator",      "path": "sample_repo/compliance/kyc_validator.py",  "pii_columns": ["customer_master.ssn", "customer_master.date_of_birth"]},
    {"job_id": "job-008", "job_name": "aml_screening_job",  "path": "sample_repo/compliance/aml_screening.py",  "pii_columns": ["customer_master.ssn", "customer_master.nationality", "customer_master.email"]},
]

REGULATORY_LINEAGE = [
    {"job_id": "job-005", "name": "risk_pipeline_orch",  "source_datasets": ["customer_master", "accounts", "risk_scores", "var_results"]},
    {"job_id": "job-009", "name": "ccar_report_gen",     "source_datasets": ["capital_requirements", "risk_scores", "var_results"]},
    {"job_id": "job-010", "name": "basel3_report_gen",   "source_datasets": ["capital_requirements", "risk_scores"]},
]

DEPRECATED_COLS = [
    {"job_name": "transaction_feed",    "path": "sample_repo/etl/transaction_feed.py",    "deprecated_column": "transactions.old_amount"},
    {"job_name": "customer_ingest",     "path": "sample_repo/etl/customer_ingest.py",     "deprecated_column": "customer_master.legacy_customer_id"},
    {"job_name": "aml_screening_job",   "path": "sample_repo/compliance/aml_screening.py","deprecated_column": "market_data.spot_rate_old"},
]

# ── Impact ─────────────────────────────────────────────────────────────────

COLUMN_IMPACT = {
    ("transactions", "amount"): [
        {"job_id": "job-003", "name": "credit_risk_scoring","path": "sample_repo/risk/credit_risk.py",    "relationship": "READS_FROM", "callers": ["job-005"], "risk_tags": ["audit_required", "regulatory_report"]},
        {"job_id": "job-011", "name": "mis_daily_report",   "path": "sample_repo/reporting/mis_daily.py", "relationship": "READS_FROM", "callers": [],          "risk_tags": []},
    ],
    ("customer_master", "ssn"): [
        {"job_id": "job-001", "name": "customer_ingest",    "path": "sample_repo/etl/customer_ingest.py",      "relationship": "WRITES_TO",  "callers": [],          "risk_tags": ["PII", "audit_required"]},
        {"job_id": "job-007", "name": "kyc_validator",      "path": "sample_repo/compliance/kyc_validator.py", "relationship": "READS_FROM", "callers": [],          "risk_tags": ["PII", "audit_required"]},
        {"job_id": "job-008", "name": "aml_screening_job",  "path": "sample_repo/compliance/aml_screening.py", "relationship": "READS_FROM", "callers": [],          "risk_tags": ["PII", "audit_required"]},
    ],
}

DATASET_IMPACT = {
    "customer_master": {
        "dataset_id": "ds-001", "dataset_name": "customer_master",
        "read_by":    ["credit_risk_scoring", "transaction_feed", "kyc_validator", "aml_screening_job", "risk_pipeline_orch", "mis_daily_report"],
        "written_by": ["customer_ingest"],
    },
    "accounts": {
        "dataset_id": "ds-002", "dataset_name": "accounts",
        "read_by":    ["credit_risk_scoring", "risk_pipeline_orch", "mis_daily_report"],
        "written_by": ["customer_ingest"],
    },
    "transactions": {
        "dataset_id": "ds-003", "dataset_name": "transactions",
        "read_by":    ["credit_risk_scoring", "aml_screening_job", "mis_daily_report"],
        "written_by": ["transaction_feed"],
    },
    "risk_scores": {
        "dataset_id": "ds-005", "dataset_name": "risk_scores",
        "read_by":    ["risk_pipeline_orch", "limit_monitor", "ccar_report_gen", "basel3_report_gen"],
        "written_by": ["credit_risk_scoring"],
    },
}

# ── STM mappings ──────────────────────────────────────────────────────────

STM_MAPPINGS = [
    {"source_column_id": "col-003-03", "source_table": "transactions",        "source_column": "amount",          "source_dtype": "DECIMAL", "stm_id": "stm-001", "target_table": "dw_transactions",   "target_column": "amount_usd",     "target_system": "data_warehouse", "owner": "data_engineering", "transform_expr": "amount * fx_rate",                       "confidence": "verified", "used_by_jobs": ["transaction_feed"]},
    {"source_column_id": "col-003-05", "source_table": "transactions",        "source_column": "amount_usd",      "source_dtype": "DECIMAL", "stm_id": "stm-002", "target_table": "dw_transactions",   "target_column": "norm_amount",    "target_system": "data_warehouse", "owner": "data_engineering", "transform_expr": "amount_usd",                             "confidence": "verified", "used_by_jobs": ["transaction_feed"]},
    {"source_column_id": "col-005-03", "source_table": "risk_scores",         "source_column": "pd_score",        "source_dtype": "DECIMAL", "stm_id": "stm-003", "target_table": "dw_risk",           "target_column": "pd_score",       "target_system": "data_warehouse", "owner": "risk_team",        "transform_expr": "pd_score",                               "confidence": "verified", "used_by_jobs": ["credit_risk_scoring"]},
    {"source_column_id": "col-005-06", "source_table": "risk_scores",         "source_column": "expected_loss",   "source_dtype": "DECIMAL", "stm_id": "stm-004", "target_table": "dw_risk",           "target_column": "expected_loss",  "target_system": "data_warehouse", "owner": "risk_team",        "transform_expr": "pd_score * lgd_estimate * ead",          "confidence": "verified", "used_by_jobs": ["credit_risk_scoring"]},
    {"source_column_id": "col-008-04", "source_table": "capital_requirements","source_column": "rwa_credit",      "source_dtype": "DECIMAL", "stm_id": "stm-005", "target_table": "dw_capital",        "target_column": "rwa_credit",     "target_system": "data_warehouse", "owner": "risk_team",        "transform_expr": "rwa_credit",                             "confidence": "verified", "used_by_jobs": ["risk_pipeline_orch"]},
    {"source_column_id": "col-011-05", "source_table": "ccar_output",         "source_column": "capital_ratio",   "source_dtype": "DECIMAL", "stm_id": "stm-006", "target_table": "fed_ccar_filing",   "target_column": "capital_ratio",  "target_system": "regulatory",     "owner": "reporting_team",   "transform_expr": "capital_ratio",                          "confidence": "verified", "used_by_jobs": ["ccar_report_gen"]},
    {"source_column_id": "col-001-04", "source_table": "customer_master",     "source_column": "ssn",             "source_dtype": "STRING",  "stm_id": "stm-007", "target_table": "dw_customers",      "target_column": "customer_token", "target_system": "data_warehouse", "owner": "data_engineering", "transform_expr": "hash(ssn)",                              "confidence": "verified", "used_by_jobs": ["customer_ingest"]},
    {"source_column_id": "col-010-05", "source_table": "aml_alerts",          "source_column": "aml_score",       "source_dtype": "DECIMAL", "stm_id": "stm-008", "target_table": "compliance_dw",     "target_column": "aml_risk_score", "target_system": "compliance_dw",  "owner": "compliance_team",  "transform_expr": "aml_score",                              "confidence": "verified", "used_by_jobs": ["aml_screening_job"]},
]

# ── Pipelines ─────────────────────────────────────────────────────────────

PIPELINES = [
    {"pipeline_id": "job-001", "name": "customer_ingest",     "description": "Ingests CRM customer extract into customer_master + accounts. Validates PII, deduplicates.",    "status": "active", "registered_at": 1742000000000, "script_id": "scr-001", "script_name": "customer_ingest.py",    "script_path": "sample_repo/etl/customer_ingest.py"},
    {"pipeline_id": "job-002", "name": "transaction_feed",    "description": "Loads intraday payment feed, applies FX normalisation from market_data, writes to transactions.","status": "active", "registered_at": 1742000100000, "script_id": "scr-002", "script_name": "transaction_feed.py",   "script_path": "sample_repo/etl/transaction_feed.py"},
    {"pipeline_id": "job-003", "name": "credit_risk_scoring", "description": "Basel FIRB logistic scorecard: computes PD/LGD/EAD per customer from accounts + transactions.", "status": "active", "registered_at": 1742000200000, "script_id": "scr-003", "script_name": "credit_risk.py",        "script_path": "sample_repo/risk/credit_risk.py"},
    {"pipeline_id": "job-004", "name": "market_risk_engine",  "description": "Runs historical VaR 99%, stressed VaR, and Expected Shortfall from market_data volatilities.",  "status": "active", "registered_at": 1742000300000, "script_id": "scr-004", "script_name": "market_risk.py",        "script_path": "sample_repo/risk/market_risk.py"},
    {"pipeline_id": "job-005", "name": "risk_pipeline_orch",  "description": "Orchestrates credit scoring → market risk → capital calculation → Basel RWA aggregation.",      "status": "active", "registered_at": 1742000400000, "script_id": "scr-005", "script_name": "risk_pipeline.py",      "script_path": "sample_repo/etl/risk_pipeline.py"},
    {"pipeline_id": "job-006", "name": "limit_monitor",       "description": "Monitors VaR/credit limit breaches, escalates critical, resolves stale alerts.",                "status": "active", "registered_at": 1742000450000, "script_id": "scr-006", "script_name": "limit_monitor.py",      "script_path": "sample_repo/risk/limit_monitor.py"},
    {"pipeline_id": "job-007", "name": "kyc_validator",       "description": "KYC tier assignment, PEP screening, enhanced due diligence classification for all customers.",  "status": "active", "registered_at": 1742000500000, "script_id": "scr-007", "script_name": "kyc_validator.py",      "script_path": "sample_repo/compliance/kyc_validator.py"},
    {"pipeline_id": "job-008", "name": "aml_screening_job",   "description": "AML transaction screening: OFAC SDN match, CTR/SAR generation, FATF country risk flags.",       "status": "active", "registered_at": 1742000600000, "script_id": "scr-008", "script_name": "aml_screening.py",      "script_path": "sample_repo/compliance/aml_screening.py"},
    {"pipeline_id": "job-009", "name": "ccar_report_gen",     "description": "Generates CCAR stress-test capital ratio report from capital_requirements for Fed filing.",     "status": "active", "registered_at": 1742000700000, "script_id": "scr-009", "script_name": "ccar_report.py",        "script_path": "sample_repo/reporting/ccar_report.py"},
]

# ── Helper functions ──────────────────────────────────────────────────────

def _job_by_id(job_id: str) -> dict:
    return next((j for j in JOBS if j["id"] == job_id), {})

def _ds_by_id(ds_id: str) -> dict:
    return next((d for d in DATASETS if d["id"] == ds_id), {})

def _edges_for_job(job_id: str, rel: str | None = None) -> list[dict]:
    return [e for e in JOB_EDGES if e["src"] == job_id and (rel is None or e["rel"] == rel)]


def get_datasets(name: str | None = None, limit: int = 100) -> list[dict]:
    result = [d for d in DATASETS if not name or d["name"] == name]
    return result[:limit]


def get_columns(dataset: str | None = None, limit: int = 100) -> list[dict]:
    if dataset:
        raw = COLUMNS.get(dataset, [])
    else:
        raw = [c for cols in COLUMNS.values() for c in cols]
    return [
        {
            "id":         c["id"],
            "name":       c["name"],
            "data_type":  c["dtype"],
            "pii_flag":   c["pii"],
            "deprecated": c.get("deprecated", False),
            "pk":         c.get("pk", False),
            "fk":         c.get("fk"),
        }
        for c in raw[:limit]
    ]


def get_jobs(search: str | None = None, limit: int = 100) -> list[dict]:
    result = [j for j in JOBS if not search or search.lower() in j["name"].lower()]
    return result[:limit]


def reads_for_job(job_id: str) -> list[dict]:
    ds_ids = {e["tgt"] for e in JOB_EDGES if e["src"] == job_id and e["rel"] == "READS_FROM"}
    return [
        {"id": d["id"], "name": d["name"], "qualified_name": d["qualified_name"],
         "format": d["format"], "status": d["status"]}
        for d in DATASETS if d["id"] in ds_ids
    ]


def writes_for_job(job_id: str) -> list[dict]:
    ds_ids = {e["tgt"] for e in JOB_EDGES if e["src"] == job_id and e["rel"] == "WRITES_TO"}
    return [
        {"id": d["id"], "name": d["name"], "qualified_name": d["qualified_name"],
         "format": d["format"], "status": d["status"]}
        for d in DATASETS if d["id"] in ds_ids
    ]


def datasets_with_columns() -> list[dict]:
    return [
        {
            "dataset_id":    d["id"],
            "dataset_name":  d["name"],
            "qualified_name": d["qualified_name"],
            "columns": [
                {"id": c["id"], "name": c["name"], "dtype": c["dtype"],
                 "pii": c["pii"], "deprecated": c.get("deprecated", False), "fk": c.get("fk")}
                for c in COLUMNS.get(d["name"], [])
            ],
        }
        for d in DATASETS
        if COLUMNS.get(d["name"])
    ]


def _build_col_lookup() -> dict:
    return {c["id"]: {**c, "dataset": ds} for ds, cols in COLUMNS.items() for c in cols}


def _connected_edges(focal_id: str) -> list:
    return [e for e in COLUMN_EDGES if e["src"] == focal_id or e["tgt"] == focal_id]


def _nodes_from_ids(focal_id: str, edges: list, col_lookup: dict) -> list:
    node_ids = {focal_id} | {e["src"] for e in edges} | {e["tgt"] for e in edges}
    return [
        {"id": nid, "name": c["name"], "dataset": c["dataset"],
         "dtype": c["dtype"], "pii": c["pii"]}
        for nid in node_ids
        for c in [col_lookup.get(nid)]
        if c
    ]


def column_lineage_graph(dataset_name: str, column_name: str) -> dict:
    focus = {"dataset": dataset_name, "column": column_name}
    focal_col = next(
        (c for c in COLUMNS.get(dataset_name, []) if c["name"] == column_name), None
    )
    if not focal_col:
        return {
            "nodes": [{"id": f"{dataset_name}.{column_name}", "name": column_name,
                       "dataset": dataset_name, "dtype": "UNKNOWN", "pii": False}],
            "edges": [], "focus": focus,
        }
    edges = _connected_edges(focal_col["id"])
    nodes = _nodes_from_ids(focal_col["id"], edges, _build_col_lookup())
    edge_list = [{"src": e["src"], "tgt": e["tgt"], "expression": e["expression"],
                  "confidence": e["confidence"]} for e in edges]
    return {"nodes": nodes, "edges": edge_list, "focus": focus}


def _job_nodes() -> list[dict]:
    return [{"id": j["id"], "name": j["name"], "type": "Job", "domain": j["domain"]} for j in JOBS]


def _used_dataset_ids() -> set:
    from_job_edges   = {e["tgt"] for e in JOB_EDGES if e["rel"] in {"READS_FROM", "WRITES_TO"}}
    from_joins_src   = {j["src"] for j in DATASET_JOINS}
    from_joins_tgt   = {j["tgt"] for j in DATASET_JOINS}
    return from_job_edges | from_joins_src | from_joins_tgt


def _dataset_nodes(used_ids: set) -> list[dict]:
    return [
        {"id": d["id"], "name": d["name"], "type": "Dataset", "domain": d["domain"]}
        for d in DATASETS if d["id"] in used_ids
    ]


def _job_edge_dicts() -> list[dict]:
    return [
        {"src": e["src"], "tgt": e["tgt"], "rel": e["rel"], "conf": e.get("conf", "verified")}
        for e in JOB_EDGES
    ]


def _join_edge_dicts() -> list[dict]:
    return [
        {"src": j["src"], "tgt": j["tgt"], "rel": j["rel"], "conf": "verified",
         "join_key": j["join_key"], "join_type": j["join_type"], "label": j["label"]}
        for j in DATASET_JOINS
    ]


def get_all_job_graph_mock() -> dict:
    """Full job + dataset graph including FK-relationship dataset edges (mock fallback)."""
    nodes = _job_nodes() + _dataset_nodes(_used_dataset_ids())
    edges = _job_edge_dicts() + _join_edge_dicts()
    return {"nodes": nodes, "edges": edges}
