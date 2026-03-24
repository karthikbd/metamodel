"""
AML Screening — Anti-Money Laundering transaction and customer screening.
COMPLIANCE GAPS (deliberate, for PoC demonstration):
  - screen_customer_pii()   : @pii_handler but MISSING @audit_required
  - enrich_sanctions_data() : @pii_handler but MISSING @audit_required
  - get_customer_identity() : @pii_handler but MISSING @audit_required
These three functions will appear in the Compliance tab as PII violations.
"""
import logging
import pandas as pd
from datetime import date, datetime
from typing import List, Dict, Optional

from config.database import dw_engine, compliance_engine, get_raw_dw_connection
from utils.db_utils import execute_query
from utils.decorators import pii_handler, audit_required, retry

logger = logging.getLogger(__name__)

OFAC_SDN_THRESHOLD  = 0.85   # similarity score threshold for OFAC name match
FATF_HIGH_RISK_COUNTRIES = {"IR", "KP", "BY", "SY", "CU", "VE", "MM", "YE"}
CTR_THRESHOLD_USD    = 10_000.0
SAR_VELOCITY_THRESHOLD = 25    # n transactions in 24h


# ---------------------------------------------------------------------------
# Customer identity screening (PII-heavy, MISSING audit — deliberate)
# ---------------------------------------------------------------------------

@pii_handler
# COMPLIANCE VIOLATION: @audit_required MISSING
def screen_customer_pii(customer_id: int) -> Dict:
    """
    Pull full PII profile for AML identity screening.
    Reads: customer_master.ssn, date_of_birth, tax_id, email,
           customer_master.nationality, address_line1, address_city
    COMPLIANCE GAP: handles PII without audit trail.
    """
    rows = execute_query(f"""
        SELECT customer_id,
               first_name,
               last_name,
               ssn,
               date_of_birth,
               tax_id,
               email,
               phone_mobile,
               nationality,
               address_line1,
               address_city,
               address_state,
               address_zip,
               kyc_status
        FROM   dbo.customer_master
        WHERE  customer_id = {customer_id}
    """, engine=dw_engine)
    return rows.to_dict("records")[0] if not rows.empty else {}


@pii_handler
# COMPLIANCE VIOLATION: @audit_required MISSING
def enrich_sanctions_data(customer_profile: Dict) -> Dict:
    """
    Cross-reference customer PII against OFAC SDN and EU sanctions lists.
    Reads: customer_master.first_name, last_name, date_of_birth, nationality
    COMPLIANCE GAP: accesses PII for sanctions matching without audit.
    """
    full_name   = f"{customer_profile.get('first_name','')} {customer_profile.get('last_name','')}"
    nationality = customer_profile.get("nationality", "")

    # OFAC SDN similarity check (stub — would call sanctions API in production)
    sanctions_hit = execute_query(f"""
        SELECT sdn_name, match_score, list_type, reason_code
        FROM   compliance_db.dbo.sanctions_matches
        WHERE  customer_id  = {customer_profile.get('customer_id', 0)}
          AND  match_score >= {OFAC_SDN_THRESHOLD}
        ORDER  BY match_score DESC
    """, engine=compliance_engine)

    is_high_risk_country = nationality.upper() in FATF_HIGH_RISK_COUNTRIES
    customer_profile["sanctions_hits"]      = len(sanctions_hit)
    customer_profile["is_high_risk_country"] = is_high_risk_country
    customer_profile["pep_flag"]            = False   # placeholder
    logger.info(f"Sanctions check for customer {customer_profile.get('customer_id')}: "
                f"{len(sanctions_hit)} hits, high-risk-country={is_high_risk_country}")
    return customer_profile


@pii_handler
# COMPLIANCE VIOLATION: @audit_required MISSING
def get_customer_identity(customer_id: int) -> Dict:
    """
    Retrieve full identity details including legacy_customer_id (deprecated field).
    Reads: customer_master.ssn, customer_master.legacy_customer_id (DEPRECATED),
           customer_master.tax_id, customer_master.date_of_birth
    """
    rows = execute_query(f"""
        SELECT customer_id,
               first_name,
               last_name,
               ssn,
               date_of_birth,
               tax_id,
               legacy_customer_id,     -- DEPRECATED field
               nationality,
               kyc_status,
               kyc_verified_at
        FROM   dbo.customer_master
        WHERE  customer_id = {customer_id}
    """, engine=dw_engine)
    return rows.to_dict("records")[0] if not rows.empty else {}


# ---------------------------------------------------------------------------
# Transaction-level AML screening
# ---------------------------------------------------------------------------

def detect_ctr_obligations(business_date: date) -> pd.DataFrame:
    """
    Identify transactions requiring Currency Transaction Report (CTR) filing.
    Reads: transactions.amount_usd, account_id, transaction_type,
           transactions.channel, transaction_date
           accounts.customer_id, account_type
    Writes: ctr_filings.transaction_id, customer_id, amount_usd, filing_date
    """
    return execute_query(f"""
        SELECT t.transaction_id,
               t.account_id,
               a.customer_id,
               SUM(t.amount_usd)          AS daily_cash_total,
               COUNT(t.transaction_id)    AS txn_count,
               MIN(t.transaction_date)    AS first_txn_date
        FROM   dbo.transactions  t
        JOIN   dbo.accounts      a ON a.account_id = t.account_id
        WHERE  t.transaction_date = '{business_date}'
          AND  t.channel          IN ('BRANCH', 'ATM')
          AND  t.transaction_type IN ('DEBIT', 'CREDIT')
          AND  t.currency         = 'USD'
        GROUP BY t.transaction_id, t.account_id, a.customer_id
        HAVING SUM(t.amount_usd) > {CTR_THRESHOLD_USD}
    """, engine=dw_engine)


def detect_velocity_anomalies(business_date: date) -> pd.DataFrame:
    """
    Flag accounts with unusual transaction velocity (potential structuring).
    Reads: transactions.account_id, transaction_date, amount_usd, channel
    Writes: aml_alerts.account_id, alert_type, metric_value, alert_date
    """
    return execute_query(f"""
        SELECT t.account_id,
               a.customer_id,
               COUNT(*) AS txn_count_24h,
               SUM(t.amount_usd) AS total_amount_24h,
               MAX(t.amount_usd) AS max_single_txn
        FROM   dbo.transactions  t
        JOIN   dbo.accounts      a ON a.account_id = t.account_id
        WHERE  t.transaction_date = '{business_date}'
        GROUP BY t.account_id, a.customer_id
        HAVING COUNT(*) > {SAR_VELOCITY_THRESHOLD}
           OR  SUM(t.amount_usd) > 50000
    """, engine=dw_engine)


def screen_transactions_ofac(business_date: date) -> pd.DataFrame:
    """
    Cross-reference transaction counterparties against OFAC SDN list.
    Reads: transactions.merchant_id, merchant_name, amount_usd,
           customer_master.nationality (PII adjacent)
    Writes: aml_alerts.alert_type, transaction_id, alert_date, severity
    """
    return execute_query(f"""
        SELECT t.transaction_id,
               t.merchant_id,
               t.merchant_name,
               t.amount_usd,
               sm.sdn_name,
               sm.match_score,
               sm.list_type
        FROM   dbo.transactions      t
        JOIN   compliance_db.dbo.sanctions_matches sm
               ON sm.merchant_id   = t.merchant_id
              AND sm.match_score   >= {OFAC_SDN_THRESHOLD}
        WHERE  t.transaction_date   = '{business_date}'
    """, engine=dw_engine)


@pii_handler
@audit_required   # COMPLIANT — both decorators present
def file_sar_report(customer_id: int, reason: str, supporting_txn_ids: List[int],
                    business_date: date):
    """
    File a Suspicious Activity Report (SAR) with FinCEN.
    Reads: customer_master.ssn, date_of_birth, tax_id (PII for SAR narrative)
           transactions.amount_usd, merchant_name, transaction_date
    Writes: sar_filings.customer_id, filing_date, reason, status
    """
    profile = screen_customer_pii(customer_id)
    logger.info(f"Filing SAR for customer {customer_id}: {reason}")
    conn = get_raw_dw_connection()
    cursor = conn.cursor()
    cursor.execute(
        """
        INSERT INTO compliance_db.dbo.sar_filings
            (customer_id, filing_date, reason, transaction_ids, status, filed_by)
        VALUES (?, ?, ?, ?, 'PENDING_SUBMISSION', 'AML_SYSTEM')
        """,
        (customer_id, str(business_date), reason,
         ",".join(str(t) for t in supporting_txn_ids)),
    )
    conn.commit()
    conn.close()
    return {"status": "SAR_FILED", "customer_id": customer_id, "filing_date": str(business_date)}


def run_daily_aml_screening(business_date: date):
    """
    Orchestrate daily AML screening:
    detect_ctr_obligations → detect_velocity_anomalies → screen_transactions_ofac

    Calls: detect_ctr_obligations, detect_velocity_anomalies,
           screen_transactions_ofac, file_sar_report
    """
    logger.info(f"=== AML Screening START: {business_date} ===")
    ctr_df      = detect_ctr_obligations(business_date)
    velocity_df = detect_velocity_anomalies(business_date)
    ofac_hits   = screen_transactions_ofac(business_date)

    for _, row in velocity_df.iterrows():
        if float(row.get("total_amount_24h", 0)) > 100_000:
            file_sar_report(
                int(row["customer_id"]), "HIGH_VELOCITY",
                [], business_date,
            )

    logger.info(f"AML screening complete: CTR={len(ctr_df)}, "
                f"velocity={len(velocity_df)}, OFAC={len(ofac_hits)}")
    return {"ctr_count": len(ctr_df), "velocity_alerts": len(velocity_df),
            "ofac_hits": len(ofac_hits)}
