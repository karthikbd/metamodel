"""
PoC mock data — returned as fallback when AuraDB returns empty results.
Provides a realistic financial-services lineage graph covering:
  6 Jobs · 6 Datasets · 26 Columns · 8 STM mappings · 5 Compliance findings
"""

# ── Nodes ─────────────────────────────────────────────────────────────────

JOBS = [
    {"id": "job-001", "name": "extract_customer_trades",  "path": "etl/extract_customer_trades.py",   "domain": "ETL",        "type": "etl",      "status": "active",  "risk_tags": ["PII", "regulatory_report"]},
    {"id": "job-002", "name": "load_risk_metrics",        "path": "analytics/load_risk_metrics.py",   "domain": "Analytics",  "type": "loader",   "status": "active",  "risk_tags": ["audit_required"]},
    {"id": "job-003", "name": "calc_pnl_summary",         "path": "finance/calc_pnl_summary.py",      "domain": "Finance",    "type": "transform","status": "active",  "risk_tags": ["audit_required", "regulatory_report"]},
    {"id": "job-004", "name": "fx_conversion_job",        "path": "finance/fx_conversion.py",         "domain": "Finance",    "type": "transform","status": "active",  "risk_tags": []},
    {"id": "job-005", "name": "compliance_report_gen",    "path": "compliance/report_generator.py",   "domain": "Compliance", "type": "report",   "status": "active",  "risk_tags": ["regulatory_report", "audit_required", "PII"]},
    {"id": "job-006", "name": "data_validation_job",      "path": "etl/data_validation.py",           "domain": "ETL",        "type": "validate", "status": "active",  "risk_tags": []},
]

DATASETS = [
    {"id": "ds-001", "name": "customer_trades",    "qualified_name": "pnc.raw.customer_trades",    "format": "CSV",     "status": "active",     "owner": "data_engineering"},
    {"id": "ds-002", "name": "risk_metrics",       "qualified_name": "pnc.analytics.risk_metrics", "format": "PARQUET", "status": "active",     "owner": "risk_team"},
    {"id": "ds-003", "name": "pnl_summary",        "qualified_name": "pnc.finance.pnl_summary",    "format": "CSV",     "status": "active",     "owner": "finance_team"},
    {"id": "ds-004", "name": "fx_rates",           "qualified_name": "pnc.ref.fx_rates",           "format": "JSON",    "status": "active",     "owner": "reference_data"},
    {"id": "ds-005", "name": "compliance_reports", "qualified_name": "pnc.reg.compliance_reports", "format": "PARQUET", "status": "active",     "owner": "compliance_team"},
    {"id": "ds-006", "name": "raw_orders",         "qualified_name": "pnc.raw.raw_orders",         "format": "CSV",     "status": "deprecated", "owner": "data_engineering"},
]

COLUMNS = {
    "customer_trades": [
        {"id": "col-001-01", "name": "trade_id",       "dtype": "STRING",  "pii": False},
        {"id": "col-001-02", "name": "customer_ssn",   "dtype": "STRING",  "pii": True},
        {"id": "col-001-03", "name": "amount",         "dtype": "DECIMAL", "pii": False},
        {"id": "col-001-04", "name": "trade_date",     "dtype": "DATE",    "pii": False},
        {"id": "col-001-05", "name": "currency",       "dtype": "STRING",  "pii": False},
        {"id": "col-001-06", "name": "old_amount",     "dtype": "DECIMAL", "pii": False},
        {"id": "col-001-07", "name": "counterparty_id","dtype": "STRING",  "pii": False},
    ],
    "risk_metrics": [
        {"id": "col-002-01", "name": "risk_score",       "dtype": "DECIMAL", "pii": False},
        {"id": "col-002-02", "name": "customer_id",      "dtype": "STRING",  "pii": False},
        {"id": "col-002-03", "name": "var_95",           "dtype": "DECIMAL", "pii": False},
        {"id": "col-002-04", "name": "credit_score_raw", "dtype": "DECIMAL", "pii": False},
    ],
    "pnl_summary": [
        {"id": "col-003-01", "name": "pnl_amount",     "dtype": "DECIMAL", "pii": False},
        {"id": "col-003-02", "name": "reporting_date", "dtype": "DATE",    "pii": False},
        {"id": "col-003-03", "name": "trader_id",      "dtype": "STRING",  "pii": False},
        {"id": "col-003-04", "name": "currency_pair",  "dtype": "STRING",  "pii": False},
    ],
    "fx_rates": [
        {"id": "col-004-01", "name": "currency_from", "dtype": "STRING",  "pii": False},
        {"id": "col-004-02", "name": "currency_to",   "dtype": "STRING",  "pii": False},
        {"id": "col-004-03", "name": "rate",          "dtype": "DECIMAL", "pii": False},
        {"id": "col-004-04", "name": "rate_date",     "dtype": "DATE",    "pii": False},
    ],
    "compliance_reports": [
        {"id": "col-005-01", "name": "report_id",    "dtype": "STRING",  "pii": False},
        {"id": "col-005-02", "name": "ssn_masked",   "dtype": "STRING",  "pii": True},
        {"id": "col-005-03", "name": "amount_usd",   "dtype": "DECIMAL", "pii": False},
        {"id": "col-005-04", "name": "trade_ref",    "dtype": "STRING",  "pii": False},
    ],
    "raw_orders": [
        {"id": "col-006-01", "name": "order_id",         "dtype": "STRING",  "pii": False},
        {"id": "col-006-02", "name": "legacy_customer_id","dtype": "STRING",  "pii": False},
        {"id": "col-006-03", "name": "spot_rate_old",    "dtype": "DECIMAL", "pii": False},
    ],
}

# ── Stats ─────────────────────────────────────────────────────────────────

STATS = {
    "jobs":           len(JOBS),
    "datasets":       len(DATASETS),
    "columns":        sum(len(v) for v in COLUMNS.values()),
    "scripts":        6,
    "datasources":    3,
    "business_rules": 4,
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

# ── Job → Dataset edges ───────────────────────────────────────────────────

JOB_EDGES = [
    # job-001: reads raw_orders, writes customer_trades
    {"src": "job-001", "tgt": "ds-006", "rel": "READS_FROM",  "conf": "verified"},
    {"src": "job-001", "tgt": "ds-001", "rel": "WRITES_TO",   "conf": "verified"},
    # job-002: reads customer_trades + fx_rates, writes risk_metrics
    {"src": "job-002", "tgt": "ds-001", "rel": "READS_FROM",  "conf": "verified"},
    {"src": "job-002", "tgt": "ds-004", "rel": "READS_FROM",  "conf": "verified"},
    {"src": "job-002", "tgt": "ds-002", "rel": "WRITES_TO",   "conf": "verified"},
    # job-003: reads customer_trades + fx_rates, writes pnl_summary
    {"src": "job-003", "tgt": "ds-001", "rel": "READS_FROM",  "conf": "verified"},
    {"src": "job-003", "tgt": "ds-004", "rel": "READS_FROM",  "conf": "verified"},
    {"src": "job-003", "tgt": "ds-003", "rel": "WRITES_TO",   "conf": "verified"},
    {"src": "job-003", "tgt": "job-001","rel": "DEPENDS_ON",  "conf": "verified"},
    # job-004: reads fx_rates
    {"src": "job-004", "tgt": "ds-004", "rel": "READS_FROM",  "conf": "verified"},
    {"src": "job-004", "tgt": "ds-001", "rel": "WRITES_TO",   "conf": "verified"},
    # job-005: reads customer_trades + pnl_summary, writes compliance_reports
    {"src": "job-005", "tgt": "ds-001", "rel": "READS_FROM",  "conf": "verified"},
    {"src": "job-005", "tgt": "ds-003", "rel": "READS_FROM",  "conf": "verified"},
    {"src": "job-005", "tgt": "ds-005", "rel": "WRITES_TO",   "conf": "verified"},
    {"src": "job-005", "tgt": "job-003","rel": "DEPENDS_ON",  "conf": "verified"},
    # job-006: reads customer_trades, writes risk_metrics
    {"src": "job-006", "tgt": "ds-001", "rel": "READS_FROM",  "conf": "verified"},
    {"src": "job-006", "tgt": "ds-002", "rel": "WRITES_TO",   "conf": "verified"},
]

# ── Column lineage (DERIVED_FROM: src derived FROM tgt = tgt is upstream) ──

COLUMN_EDGES = [
    # pnl_summary.pnl_amount <-- customer_trades.amount (via fx conversion)
    {"src": "col-003-01", "tgt": "col-001-03", "expression": "amount * fx_rate", "confidence": "verified"},
    # pnl_summary.currency_pair <-- fx_rates.currency_from + currency_to
    {"src": "col-003-04", "tgt": "col-004-01", "expression": "concat(currency_from, '_', currency_to)", "confidence": "verified"},
    # compliance_reports.amount_usd <-- pnl_summary.pnl_amount
    {"src": "col-005-03", "tgt": "col-003-01", "expression": "pnl_amount (USD normalised)", "confidence": "verified"},
    # compliance_reports.ssn_masked <-- customer_trades.customer_ssn
    {"src": "col-005-02", "tgt": "col-001-02", "expression": "mask(customer_ssn)", "confidence": "verified"},
    # risk_metrics.risk_score <-- customer_trades.amount
    {"src": "col-002-01", "tgt": "col-001-03", "expression": "risk_model(amount)", "confidence": "inferred"},
    # risk_metrics.customer_id <-- customer_trades.trade_id
    {"src": "col-002-02", "tgt": "col-001-01", "expression": "trade_id split customer", "confidence": "inferred"},
]

# ── Compliance ─────────────────────────────────────────────────────────────

PII_WITHOUT_AUDIT = [
    {
        "job_id":      "job-001",
        "job_name":    "extract_customer_trades",
        "path":        "etl/extract_customer_trades.py",
        "pii_columns": ["customer_trades.customer_ssn"],
    },
    {
        "job_id":      "job-005",
        "job_name":    "compliance_report_gen",
        "path":        "compliance/report_generator.py",
        "pii_columns": ["compliance_reports.ssn_masked", "customer_trades.customer_ssn"],
    },
]

REGULATORY_LINEAGE = [
    {"job_id": "job-001", "name": "extract_customer_trades",  "source_datasets": ["raw_orders"]},
    {"job_id": "job-003", "name": "calc_pnl_summary",         "source_datasets": ["customer_trades", "fx_rates"]},
    {"job_id": "job-005", "name": "compliance_report_gen",    "source_datasets": ["customer_trades", "pnl_summary"]},
]

DEPRECATED_COLS = [
    {"job_name": "extract_customer_trades",  "path": "etl/extract_customer_trades.py", "deprecated_column": "customer_trades.old_amount"},
    {"job_name": "load_risk_metrics",        "path": "analytics/load_risk_metrics.py", "deprecated_column": "risk_metrics.credit_score_raw"},
    {"job_name": "data_validation_job",      "path": "etl/data_validation.py",         "deprecated_column": "raw_orders.legacy_customer_id"},
    {"job_name": "fx_conversion_job",        "path": "finance/fx_conversion.py",        "deprecated_column": "raw_orders.spot_rate_old"},
]

# ── Impact ─────────────────────────────────────────────────────────────────

COLUMN_IMPACT = {
    ("customer_trades", "amount"): [
        {"job_id": "job-002", "name": "load_risk_metrics",     "path": "analytics/load_risk_metrics.py",  "relationship": "READS_FROM", "callers": [],             "risk_tags": ["audit_required"]},
        {"job_id": "job-003", "name": "calc_pnl_summary",      "path": "finance/calc_pnl_summary.py",     "relationship": "READS_FROM", "callers": ["job-005"],    "risk_tags": ["audit_required", "regulatory_report"]},
        {"job_id": "job-005", "name": "compliance_report_gen", "path": "compliance/report_generator.py",  "relationship": "READS_FROM", "callers": [],             "risk_tags": ["regulatory_report", "PII"]},
    ],
    ("customer_trades", "customer_ssn"): [
        {"job_id": "job-001", "name": "extract_customer_trades","path": "etl/extract_customer_trades.py", "relationship": "WRITES_TO",  "callers": [],             "risk_tags": ["PII", "regulatory_report"]},
        {"job_id": "job-005", "name": "compliance_report_gen", "path": "compliance/report_generator.py", "relationship": "READS_FROM", "callers": [],             "risk_tags": ["PII", "audit_required"]},
    ],
}

DATASET_IMPACT = {
    "customer_trades": {
        "dataset_id":   "ds-001",
        "dataset_name": "customer_trades",
        "read_by":    ["load_risk_metrics", "calc_pnl_summary", "compliance_report_gen", "data_validation_job"],
        "written_by": ["extract_customer_trades", "fx_conversion_job"],
    },
    "risk_metrics": {
        "dataset_id":   "ds-002",
        "dataset_name": "risk_metrics",
        "read_by":    [],
        "written_by": ["load_risk_metrics", "data_validation_job"],
    },
    "pnl_summary": {
        "dataset_id":   "ds-003",
        "dataset_name": "pnl_summary",
        "read_by":    ["compliance_report_gen"],
        "written_by": ["calc_pnl_summary"],
    },
}

# ── STM mappings ──────────────────────────────────────────────────────────

STM_MAPPINGS = [
    {"source_column_id": "col-001-01", "source_table": "customer_trades",  "source_column": "trade_id",       "source_dtype": "STRING",  "stm_id": "stm-001", "target_table": "dw_trades",      "target_column": "trade_key",     "target_system": "data_warehouse", "owner": "data_engineering", "transform_expr": "trade_id",                       "confidence": "verified", "used_by_jobs": ["extract_customer_trades"]},
    {"source_column_id": "col-001-02", "source_table": "customer_trades",  "source_column": "customer_ssn",   "source_dtype": "STRING",  "stm_id": "stm-002", "target_table": "dw_customers",   "target_column": "customer_id",   "target_system": "data_warehouse", "owner": "data_engineering", "transform_expr": "hash(customer_ssn)",              "confidence": "verified", "used_by_jobs": ["extract_customer_trades", "compliance_report_gen"]},
    {"source_column_id": "col-001-03", "source_table": "customer_trades",  "source_column": "amount",         "source_dtype": "DECIMAL", "stm_id": "stm-003", "target_table": "dw_trades",      "target_column": "trade_amount",  "target_system": "data_warehouse", "owner": "finance_team",     "transform_expr": "amount * fx_rate",               "confidence": "verified", "used_by_jobs": ["calc_pnl_summary"]},
    {"source_column_id": "col-001-04", "source_table": "customer_trades",  "source_column": "trade_date",     "source_dtype": "DATE",    "stm_id": "stm-004", "target_table": "dw_trades",      "target_column": "trade_dt",      "target_system": "data_warehouse", "owner": "data_engineering", "transform_expr": "to_date(trade_date)",            "confidence": "verified", "used_by_jobs": ["extract_customer_trades"]},
    {"source_column_id": "col-002-01", "source_table": "risk_metrics",     "source_column": "risk_score",     "source_dtype": "DECIMAL", "stm_id": "stm-005", "target_table": "dw_risk",        "target_column": "risk_score",    "target_system": "data_warehouse", "owner": "risk_team",        "transform_expr": "risk_score",                     "confidence": "verified", "used_by_jobs": ["load_risk_metrics"]},
    {"source_column_id": "col-003-01", "source_table": "pnl_summary",      "source_column": "pnl_amount",     "source_dtype": "DECIMAL", "stm_id": "stm-006", "target_table": "dw_pnl",         "target_column": "pnl_usd",       "target_system": "data_warehouse", "owner": "finance_team",     "transform_expr": "pnl_amount",                     "confidence": "verified", "used_by_jobs": ["calc_pnl_summary"]},
    {"source_column_id": "col-005-01", "source_table": "compliance_reports","source_column": "report_id",     "source_dtype": "STRING",  "stm_id": "stm-007", "target_table": "dw_compliance",  "target_column": "report_key",    "target_system": "reg_reporting",  "owner": "compliance_team",  "transform_expr": "report_id",                      "confidence": "verified", "used_by_jobs": ["compliance_report_gen"]},
    {"source_column_id": "col-005-03", "source_table": "compliance_reports","source_column": "amount_usd",    "source_dtype": "DECIMAL", "stm_id": "stm-008", "target_table": "dw_compliance",  "target_column": "reported_amount","target_system": "reg_reporting",  "owner": "compliance_team",  "transform_expr": "amount_usd",                     "confidence": "verified", "used_by_jobs": ["compliance_report_gen"]},
]

# ── Pipelines ─────────────────────────────────────────────────────────────

PIPELINES = [
    {"pipeline_id": "job-001", "name": "extract_customer_trades",  "description": "Extracts and normalises raw customer trade CSV files into the customer_trades dataset.",     "status": "active", "registered_at": 1742000000000, "script_id": "scr-001", "script_name": "extract_customer_trades.py", "script_path": "etl/extract_customer_trades.py"},
    {"pipeline_id": "job-002", "name": "load_risk_metrics",        "description": "Computes risk scores from customer trade data and FX rates; persists to risk_metrics.",      "status": "active", "registered_at": 1742000100000, "script_id": "scr-002", "script_name": "load_risk_metrics.py",       "script_path": "analytics/load_risk_metrics.py"},
    {"pipeline_id": "job-003", "name": "calc_pnl_summary",         "description": "Calculates daily PnL per trader using trade amounts and live FX conversion rates.",          "status": "active", "registered_at": 1742000200000, "script_id": "scr-003", "script_name": "calc_pnl_summary.py",        "script_path": "finance/calc_pnl_summary.py"},
    {"pipeline_id": "job-005", "name": "compliance_report_gen",    "description": "Generates regulatory compliance reports from PnL and masked customer identity data.",        "status": "active", "registered_at": 1742000300000, "script_id": "scr-005", "script_name": "report_generator.py",        "script_path": "compliance/report_generator.py"},
]

# ── Helpers ───────────────────────────────────────────────────────────────

def _job_by_id(job_id: str) -> dict:
    return next((j for j in JOBS if j["id"] == job_id), {})

def _ds_by_id(ds_id: str) -> dict:
    return next((d for d in DATASETS if d["id"] == ds_id), {})

def _edges_for_job(job_id: str, rel: str | None = None) -> list[dict]:
    return [e for e in JOB_EDGES if e["src"] == job_id and (rel is None or e["rel"] == rel)]


# ── Query-level helpers (used by graph.py route handlers & queries.py) ────

def get_datasets(name: str | None = None, limit: int = 100) -> list[dict]:
    """Return mock datasets filtered by name, up to limit."""
    result = [d for d in DATASETS if not name or d["name"] == name]
    return result[:limit]


def get_columns(dataset: str | None = None, limit: int = 100) -> list[dict]:
    """Return mock columns as flat list, optionally filtered by parent dataset."""
    if dataset:
        raw = COLUMNS.get(dataset, [])
    else:
        raw = [c for cols in COLUMNS.values() for c in cols]
    return [
        {"id": c["id"], "name": c["name"], "data_type": c["dtype"], "pii_flag": c["pii"]}
        for c in raw[:limit]
    ]


def get_jobs(search: str | None = None, limit: int = 100) -> list[dict]:
    """Return mock jobs filtered by name substring, up to limit."""
    result = [j for j in JOBS if not search or search.lower() in j["name"].lower()]
    return result[:limit]


def reads_for_job(job_id: str) -> list[dict]:
    """Return datasets read by job_id (mock READS_FROM edges → Dataset info)."""
    ds_ids = {e["tgt"] for e in JOB_EDGES if e["src"] == job_id and e["rel"] == "READS_FROM"}
    return [
        {"id": d["id"], "name": d["name"], "qualified_name": d["qualified_name"],
         "format": d["format"], "status": d["status"]}
        for d in DATASETS if d["id"] in ds_ids
    ]


def writes_for_job(job_id: str) -> list[dict]:
    """Return datasets written by job_id (mock WRITES_TO edges → Dataset info)."""
    ds_ids = {e["tgt"] for e in JOB_EDGES if e["src"] == job_id and e["rel"] == "WRITES_TO"}
    return [
        {"id": d["id"], "name": d["name"], "qualified_name": d["qualified_name"],
         "format": d["format"], "status": d["status"]}
        for d in DATASETS if d["id"] in ds_ids
    ]


def datasets_with_columns() -> list[dict]:
    """All datasets with their columns — for column lineage selector."""
    return [
        {
            "dataset_id":    d["id"],
            "dataset_name":  d["name"],
            "qualified_name": d["qualified_name"],
            "columns": [
                {"id": c["id"], "name": c["name"], "dtype": c["dtype"], "pii": c["pii"]}
                for c in COLUMNS.get(d["name"], [])
            ],
        }
        for d in DATASETS
        if COLUMNS.get(d["name"])
    ]


def _build_col_lookup() -> dict:
    """Build an id-keyed lookup of all columns with their parent dataset name."""
    return {c["id"]: {**c, "dataset": ds} for ds, cols in COLUMNS.items() for c in cols}


def _connected_edges(focal_id: str) -> list:
    """Return COLUMN_EDGES that touch the given focal column id."""
    return [e for e in COLUMN_EDGES if e["src"] == focal_id or e["tgt"] == focal_id]


def _nodes_from_ids(focal_id: str, edges: list, col_lookup: dict) -> list:
    """Build node list from focal + edge endpoints."""
    node_ids = {focal_id} | {e["src"] for e in edges} | {e["tgt"] for e in edges}
    return [
        {"id": nid, "name": c["name"], "dataset": c["dataset"],
         "dtype": c["dtype"], "pii": c["pii"]}
        for nid in node_ids
        for c in [col_lookup.get(nid)]
        if c
    ]


def column_lineage_graph(dataset_name: str, column_name: str) -> dict:
    """Build a mock column lineage graph for the given dataset/column."""
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
