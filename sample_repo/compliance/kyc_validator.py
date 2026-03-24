"""
KYC Validator — Know Your Customer identity verification and onboarding checks.
All PII functions are COMPLIANT: both @pii_handler and @audit_required are present.
This is the positive example to contrast with aml_screening.py violations.
"""
import logging
import pandas as pd
from datetime import date, datetime
from typing import Dict, Optional

from config.database import dw_engine, compliance_engine, get_raw_dw_connection
from utils.db_utils import execute_query
from utils.decorators import pii_handler, audit_required, retry

logger = logging.getLogger(__name__)

KYC_EXPIRY_MONTHS = 24   # KYC must be refreshed every 24 months


@pii_handler
@audit_required   # COMPLIANT
def fetch_kyc_identity(customer_id: int) -> Dict:
    """
    Fetch PII identity fields required for KYC verification.
    Reads: customer_master.first_name, last_name, ssn, date_of_birth,
           tax_id, nationality, address_line1, address_city, kyc_status
    """
    rows = execute_query(f"""
        SELECT customer_id, first_name, last_name,
               ssn, date_of_birth, tax_id,
               nationality, address_line1, address_city,
               address_state, address_zip,
               kyc_status, kyc_verified_at
        FROM   dbo.customer_master
        WHERE  customer_id = {customer_id}
    """, engine=dw_engine)
    return rows.to_dict("records")[0] if not rows.empty else {}


@pii_handler
@audit_required   # COMPLIANT
def verify_government_id(customer_id: int, id_type: str,
                          id_number: str, issuing_country: str) -> Dict:
    """
    Verify government-issued ID against identity verification service.
    Reads: customer_master.date_of_birth, first_name, last_name, nationality
    Writes: kyc_documents.customer_id, id_type, id_number, verified_at, status
    """
    identity = fetch_kyc_identity(customer_id)
    if not identity:
        return {"status": "NOT_FOUND", "customer_id": customer_id}

    # Identity verification API call (stub)
    result = {
        "customer_id":     customer_id,
        "id_type":         id_type,
        "id_number":       id_number,
        "issuing_country": issuing_country,
        "verified":        True,
        "verified_at":     datetime.utcnow().isoformat(),
        "confidence":      0.98,
    }

    conn = get_raw_dw_connection()
    cursor = conn.cursor()
    cursor.execute(
        """
        INSERT INTO compliance_db.dbo.kyc_documents
            (customer_id, id_type, id_number, issuing_country,
             verified, verified_at, confidence)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (customer_id, id_type, id_number, issuing_country,
         result["verified"], result["verified_at"], result["confidence"]),
    )
    conn.commit()
    conn.close()
    logger.info(f"KYC ID verified for customer {customer_id}: {id_type}")
    return result


@pii_handler
@audit_required   # COMPLIANT
def run_adverse_media_check(customer_id: int) -> Dict:
    """
    Search adverse media (news, court records) for customer name + nationality.
    Reads: customer_master.first_name, last_name, date_of_birth, nationality
    Writes: kyc_adverse_media.customer_id, hit_count, sources, checked_at
    """
    identity = fetch_kyc_identity(customer_id)
    hits = execute_query(f"""
        SELECT source_name, article_url, risk_category, publication_date, relevance_score
        FROM   compliance_db.dbo.adverse_media_hits
        WHERE  customer_id      = {customer_id}
          AND  relevance_score  >= 0.70
        ORDER BY relevance_score DESC
        LIMIT 20
    """, engine=compliance_engine)

    result = {
        "customer_id": customer_id,
        "hit_count":   len(hits),
        "sources":     hits["source_name"].tolist() if not hits.empty else [],
        "checked_at":  datetime.utcnow().isoformat(),
    }
    conn = get_raw_dw_connection()
    cursor = conn.cursor()
    cursor.execute(
        "INSERT INTO compliance_db.dbo.kyc_adverse_media "
        "(customer_id, hit_count, checked_at) VALUES (?, ?, GETUTCDATE())",
        (customer_id, result["hit_count"]),
    )
    conn.commit()
    conn.close()
    return result


def assess_kyc_risk_rating(customer_id: int) -> str:
    """
    Determine KYC risk rating (LOW / MEDIUM / HIGH / PEP) based on screening results.
    Reads: kyc_documents.verified, kyc_adverse_media.hit_count,
           customer_master.nationality, compliance_db.sanctions_matches.match_score
    Writes: customer_master.kyc_status, customer_master.risk_rating
    """
    aml_data = execute_query(f"""
        SELECT sm.match_score,
               am.hit_count,
               c.nationality
        FROM   dbo.customer_master             c
        LEFT JOIN compliance_db.dbo.sanctions_matches  sm ON sm.customer_id = c.customer_id
        LEFT JOIN compliance_db.dbo.kyc_adverse_media  am ON am.customer_id = c.customer_id
        WHERE  c.customer_id = {customer_id}
    """, engine=dw_engine)

    if aml_data.empty:
        return "LOW"

    row = aml_data.iloc[0]
    if float(row.get("match_score", 0)) > 0.85:
        rating = "HIGH"
    elif int(row.get("hit_count", 0)) > 5:
        rating = "HIGH"
    elif row.get("nationality") in {"IR", "KP", "BY"}:
        rating = "HIGH"
    elif int(row.get("hit_count", 0)) > 0:
        rating = "MEDIUM"
    else:
        rating = "LOW"

    conn = get_raw_dw_connection()
    cursor = conn.cursor()
    cursor.execute(
        "UPDATE dbo.customer_master SET kyc_status='VERIFIED', risk_rating=? "
        "WHERE customer_id=?",
        (rating, customer_id),
    )
    conn.commit()
    conn.close()
    logger.info(f"KYC risk rating assigned for customer {customer_id}: {rating}")
    return rating


def get_kyc_due_for_refresh(as_of_date: date) -> pd.DataFrame:
    """
    List customers whose KYC is expiring within 30 days.
    Reads: customer_master.kyc_verified_at, customer_id, customer_segment
    """
    return execute_query(f"""
        SELECT customer_id,
               first_name,
               last_name,
               customer_segment,
               kyc_verified_at,
               DATEDIFF(day, kyc_verified_at, '{as_of_date}') AS days_since_kyc
        FROM   dbo.customer_master
        WHERE  kyc_status = 'VERIFIED'
          AND  DATEDIFF(month, kyc_verified_at, '{as_of_date}') >= {KYC_EXPIRY_MONTHS - 1}
          AND  is_active = 1
        ORDER BY kyc_verified_at ASC
    """, engine=dw_engine)


def run_kyc_refresh_batch(as_of_date: date):
    """
    Orchestrate KYC refresh for all customers expiring soon.
    Calls: get_kyc_due_for_refresh, fetch_kyc_identity,
           verify_government_id, run_adverse_media_check, assess_kyc_risk_rating
    """
    due_df = get_kyc_due_for_refresh(as_of_date)
    logger.info(f"KYC refresh batch: {len(due_df)} customers due for refresh")
    refreshed = 0
    for _, row in due_df.iterrows():
        cid = int(row["customer_id"])
        run_adverse_media_check(cid)
        assess_kyc_risk_rating(cid)
        refreshed += 1
    logger.info(f"KYC refresh complete: {refreshed} customers refreshed")
    return refreshed
