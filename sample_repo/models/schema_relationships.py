"""
PNC Data Platform — Schema Relationship Map
===========================================

Central documentation of every foreign-key relationship, derived dependency,
and lookup join between tables in the PNC Meta Model sample repository.

This file is the single source of truth.  Every relationship is reflected in:
  - backend/graph/mock_data.py  (DATASET_JOINS list)
  - The Neo4j graph (REFERENCES / DERIVED_FROM / JOINS_WITH edges)
  - sample_repo SQL / Pandas merge calls in the ETL / Risk / Compliance scripts

──────────────────────────────────────────────────────────────────────────
LEGEND
  PK   = primary key
  FK → = foreign key pointing to parent table.column
  AGG  = derived by aggregation / calculation
  LKP  = lookup join (no RI enforcement, joined at runtime)
──────────────────────────────────────────────────────────────────────────
"""

# ── Primary Keys ──────────────────────────────────────────────────────────────

PRIMARY_KEYS = {
    "customer_master":     "customer_id      BIGINT",
    "accounts":            "account_id       BIGINT",
    "transactions":        "transaction_id   BIGINT",
    "market_data":         "market_id        BIGINT",
    "risk_scores":         "score_id         BIGINT",
    "var_results":         "var_id           BIGINT",
    "limit_breaches":      "breach_id        BIGINT",
    "capital_requirements":"req_id           BIGINT",
    "kyc_screening":       "kyc_id           BIGINT",
    "aml_alerts":          "alert_id         BIGINT",
    "ccar_output":         "report_id        STRING",
    "mis_report":          "(report_date, customer_segment)  COMPOSITE",
}

# ── Foreign Key Relationships ─────────────────────────────────────────────────
#
# Format: { child_table: [ (child_col, parent_table, parent_col, rel_type) ] }

FOREIGN_KEYS = {
    # accounts.customer_id → customer_master.customer_id
    # Enforced by: customer_ingest.py → write_accounts_table()
    # Used by:     credit_risk.py, risk_pipeline.py, mis_daily.py
    "accounts": [
        ("customer_id", "customer_master", "customer_id", "FK"),
    ],

    # transactions.account_id → accounts.account_id
    # Enforced by: transaction_feed.py → validate_account_references()
    # Used by:     credit_risk.py (join for utilisation), aml_screening.py, mis_daily.py
    "transactions": [
        ("account_id", "accounts", "account_id", "FK"),
    ],

    # risk_scores.customer_id → customer_master.customer_id
    # Enforced by: credit_risk.py → write_risk_scores()
    # Used by:     risk_pipeline.py (capital calc), ccar_report.py
    "risk_scores": [
        ("customer_id", "customer_master", "customer_id", "FK"),
    ],

    # limit_breaches.customer_id → customer_master.customer_id
    # limit_breaches.account_id  → accounts.account_id
    # Enforced by: limit_monitor.py → raise_breach()
    # Used by:     limit_monitor.py (alert deduplication read)
    "limit_breaches": [
        ("customer_id", "customer_master", "customer_id", "FK"),
        ("account_id",  "accounts",        "account_id",  "FK"),
    ],

    # kyc_screening.customer_id → customer_master.customer_id
    # Enforced by: kyc_validator.py → write_kyc_record()
    # Used by:     aml_screening.py (KYC tier read)
    "kyc_screening": [
        ("customer_id", "customer_master", "customer_id", "FK"),
    ],

    # aml_alerts.customer_id   → customer_master.customer_id
    # aml_alerts.transaction_id → transactions.transaction_id
    # Enforced by: aml_screening.py → create_alert()
    # Used by:     compliance_dw export
    "aml_alerts": [
        ("customer_id",   "customer_master", "customer_id",   "FK"),
        ("transaction_id","transactions",    "transaction_id","FK"),
    ],
}

# ── Derived (Aggregation) Relationships ───────────────────────────────────────
#
# capital_requirements is fully derived from risk_scores + var_results.
# No raw FK column — computed by risk_pipeline.py → compute_capital_requirements().

DERIVED_RELATIONSHIPS = {
    "capital_requirements": [
        {
            "source":     "risk_scores",
            "source_col": "customer_id",
            "agg":        "SUM(ead * risk_weight(pd_score, lgd_estimate))",
            "target_col": "rwa_credit",
            "script":     "sample_repo/etl/risk_pipeline.py",
        },
        {
            "source":     "var_results",
            "source_col": "as_of_date",
            "agg":        "var_99 * 12.5  (Basel III market-risk multiplier)",
            "target_col": "rwa_market",
            "script":     "sample_repo/etl/risk_pipeline.py",
        },
    ],
}

# ── Lookup Joins (no RI) ───────────────────────────────────────────────────────
#
# transactions JOIN market_data ON currency_pair / as_of_date
# Used in: transaction_feed.py → apply_fx_normalisation()
# Purpose: resolve spot_rate_mid to compute transactions.amount_usd

LOOKUP_JOINS = [
    {
        "left":      "transactions",
        "right":     "market_data",
        "join_keys": {"transactions.currency": "market_data.currency_pair",
                      "transactions.transaction_date": "market_data.as_of_date"},
        "purpose":   "FX normalisation — derive amount_usd = amount * spot_rate_mid",
        "script":    "sample_repo/etl/transaction_feed.py",
    },
    {
        "left":      "kyc_screening",
        "right":     "sanctions_list",
        "join_keys": {"kyc_screening.customer_id": "sanctions_list.entity_id (fuzzy)"},
        "purpose":   "OFAC SDN / HMT sanctions screening",
        "script":    "sample_repo/compliance/aml_screening.py",
    },
]

# ── End-to-End Lineage Summary ─────────────────────────────────────────────────
#
# Source → Core → Risk → Reporting chain:
#
#   crm_feed (ext CSV)
#       └─[customer_ingest]──▶ customer_master ──┬──▶ accounts
#                                                │          └─[transaction_feed]──▶ transactions
#                                                │                 ├─[credit_risk]──▶ risk_scores
#                                                │                 └─[aml_screening]──▶ aml_alerts
#                                                └──▶ kyc_screening
#   payment_feed (ext CSV)
#       └─[transaction_feed]──▶ transactions
#   market_data (ref table)
#       └─[transaction_feed]──▶ transactions.amount_usd (FX lookup)
#       └─[market_risk]──▶ var_results
#
#   risk_scores + var_results
#       └─[risk_pipeline]──▶ capital_requirements
#              └─[ccar_report]──▶ ccar_output  (Fed CCAR filing)
#              └─[basel3]──▶ ccar_output       (Basel III regulatory report)
#
#   risk_scores + var_results
#       └─[limit_monitor]──▶ limit_breaches
#
#   transactions + accounts + customer_master
#       └─[mis_daily]──▶ mis_report

LINEAGE_SUMMARY = {
    "source_feeds":  ["crm_feed", "payment_feed", "sanctions_list"],
    "core_tables":   ["customer_master", "accounts", "transactions", "market_data"],
    "risk_tables":   ["risk_scores", "var_results", "limit_breaches", "capital_requirements"],
    "compliance":    ["kyc_screening", "aml_alerts"],
    "reporting":     ["ccar_output", "mis_report"],
    "fk_edges":      sum(len(v) for v in FOREIGN_KEYS.values()),
    "derived_edges": sum(len(v) for v in DERIVED_RELATIONSHIPS.values()),
    "lookup_joins":  len(LOOKUP_JOINS),
}
