"""
Customer Ingestion ETL — loads customer PII data from upstream CRM feed.

COMPLIANCE NOTE: load_customer_raw() and enrich_customer_pii() are decorated
with @pii_handler but are MISSING @audit_required — this is a deliberate
compliance gap to demonstrate the Compliance tab in the Meta Model UI.
"""
import logging
import pandas as pd
from datetime import date, datetime
from typing import List, Dict

from config.database import dw_engine, get_raw_dw_connection
from models.customer import Customer
from utils.decorators import pii_handler, audit_required, retry
from utils.db_utils import bulk_insert, execute_query, log_etl_run

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Stage 1 — Raw load from CRM landing zone
# ---------------------------------------------------------------------------

@pii_handler
# INTENTIONALLY MISSING @audit_required — this will be flagged in Compliance tab
def load_customer_raw(source_file: str, batch_date: date) -> pd.DataFrame:
    """
    Load raw customer records from the CRM daily extract.
    Reads PII fields: ssn, date_of_birth, tax_id, email, phone_mobile.

    COMPLIANCE VIOLATION: @pii_handler present but @audit_required is absent.
    """
    logger.info(f"Loading raw customer extract from {source_file}")
    df = pd.read_csv(source_file, dtype=str)
    df["batch_date"] = batch_date
    df = df.rename(columns={
        "CUST_ID":  "customer_id",
        "SSN":      "ssn",
        "DOB":      "date_of_birth",
        "TAX_ID":   "tax_id",
        "EMAIL":    "email",
        "MOBILE":   "phone_mobile",
        "HOME_PH":  "phone_home",
        "SEGMENT":  "customer_segment",
    })
    logger.info(f"Loaded {len(df)} raw customer records")
    return df


@pii_handler
# INTENTIONALLY MISSING @audit_required
def enrich_customer_pii(df: pd.DataFrame) -> pd.DataFrame:
    """
    Enrich customer DataFrame with additional PII from identity service.
    Reads: customer_master.ssn, customer_master.date_of_birth,
           customer_master.address_line1, customer_master.email
    """
    existing = execute_query("""
        SELECT customer_id, ssn, date_of_birth, tax_id,
               address_line1, address_city, address_state, address_zip,
               email, phone_mobile, phone_home
        FROM dbo.customer_master
        WHERE is_active = 1
    """)
    enriched = df.merge(existing, on="customer_id", how="left", suffixes=("_new", "_old"))
    logger.info(f"Enriched {len(enriched)} customer records with PII from master")
    return enriched


# ---------------------------------------------------------------------------
# Stage 2 — Validate and clean
# ---------------------------------------------------------------------------

def validate_customer_records(df: pd.DataFrame) -> pd.DataFrame:
    """
    Validate SSN format, DOB range, mandatory fields.
    Reads: customer_master.ssn, customer_master.date_of_birth
    """
    invalid_mask = (
        df["ssn"].str.match(r"^\d{3}-\d{2}-\d{4}$").fillna(False) == False  # noqa: E712
    )
    invalid = df[invalid_mask]
    if not invalid.empty:
        logger.warning(f"{len(invalid)} records have invalid SSN format — rejected")
        conn = get_raw_dw_connection()
        cursor = conn.cursor()
        for _, row in invalid.iterrows():
            cursor.execute(
                "INSERT INTO etl_rejections (customer_id, rejection_reason, batch_date) "
                "VALUES (?, ?, ?)",
                (row["customer_id"], "INVALID_SSN", row.get("batch_date")),
            )
        conn.commit()
        conn.close()
    return df[~invalid_mask].copy()


def deduplicate_customers(df: pd.DataFrame) -> pd.DataFrame:
    """
    Remove duplicate customer_id rows, keeping the most recent.
    Reads: customer_master.customer_id, customer_master.updated_at
    """
    dupes_before = len(df)
    df = df.sort_values("updated_at", ascending=False).drop_duplicates("customer_id")
    logger.info(f"Deduplication: {dupes_before} → {len(df)} records")
    return df


# ---------------------------------------------------------------------------
# Stage 3 — Hash PII and write to master
# ---------------------------------------------------------------------------

@pii_handler
@audit_required   # ✅ Compliant — both decorators present
def hash_and_store_pii(df: pd.DataFrame, batch_date: date) -> int:
    """
    SHA-256 hash SSN and tax_id before writing to customer_master.
    Writes: customer_master.ssn, customer_master.tax_id,
            customer_master.date_of_birth, customer_master.email,
            customer_master.phone_mobile, customer_master.phone_home
    """
    import hashlib

    def sha256(val):
        if pd.isna(val):
            return None
        return hashlib.sha256(str(val).encode()).hexdigest()

    df["ssn"]    = df["ssn"].apply(sha256)
    df["tax_id"] = df["tax_id"].apply(sha256)

    conn = get_raw_dw_connection()
    cursor = conn.cursor()
    cursor.executemany(
        """
        MERGE dbo.customer_master AS target
        USING (VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)) AS source(
            customer_id, ssn, date_of_birth, tax_id,
            email, phone_mobile, phone_home, customer_segment, updated_at
        )
        ON target.customer_id = source.customer_id
        WHEN MATCHED THEN
            UPDATE SET ssn              = source.ssn,
                       date_of_birth    = source.date_of_birth,
                       tax_id           = source.tax_id,
                       email            = source.email,
                       phone_mobile     = source.phone_mobile,
                       phone_home       = source.phone_home,
                       customer_segment = source.customer_segment,
                       updated_at       = source.updated_at
        WHEN NOT MATCHED THEN
            INSERT (customer_id, ssn, date_of_birth, tax_id,
                    email, phone_mobile, phone_home, customer_segment, updated_at)
            VALUES (source.customer_id, source.ssn, source.date_of_birth, source.tax_id,
                    source.email, source.phone_mobile, source.phone_home,
                    source.customer_segment, source.updated_at);
        """,
        [(r["customer_id"], r.get("ssn"), r.get("date_of_birth"), r.get("tax_id"),
          r.get("email"), r.get("phone_mobile"), r.get("phone_home"),
          r.get("customer_segment"), datetime.utcnow()) for _, r in df.iterrows()],
    )
    conn.commit()
    conn.close()
    logger.info(f"Stored {len(df)} hashed PII records to customer_master")
    return len(df)


def update_kyc_status(customer_ids: List[int], status: str):
    """
    Bulk-update KYC status for a list of customers.
    Writes: customer_master.kyc_status, customer_master.kyc_verified_at
    """
    conn = get_raw_dw_connection()
    cursor = conn.cursor()
    cursor.executemany(
        """
        UPDATE dbo.customer_master
        SET kyc_status      = ?,
            kyc_verified_at = GETUTCDATE()
        WHERE customer_id   = ?
        """,
        [(status, cid) for cid in customer_ids],
    )
    conn.commit()
    conn.close()


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------

def run_customer_ingest(source_file: str, batch_date: date):
    """
    Orchestrates the full customer ingestion pipeline:
    load_customer_raw → enrich_customer_pii → validate_customer_records
    → deduplicate_customers → hash_and_store_pii
    """
    try:
        raw_df      = load_customer_raw(source_file, batch_date)
        enriched_df = enrich_customer_pii(raw_df)
        valid_df    = validate_customer_records(enriched_df)
        deduped_df  = deduplicate_customers(valid_df)
        rows_stored = hash_and_store_pii(deduped_df, batch_date)
        log_etl_run("customer_ingest", "SUCCESS", rows_stored)
        logger.info(f"Customer ingest complete: {rows_stored} rows written")
    except Exception as exc:
        log_etl_run("customer_ingest", "FAILED", 0, str(exc))
        raise
