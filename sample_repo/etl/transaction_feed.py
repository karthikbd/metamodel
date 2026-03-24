"""
Transaction Feed ETL — processes intraday and end-of-day transaction feeds.

DEPRECATED COLUMN USAGE: settle_legacy_amounts() reads transactions.old_amount
which is marked deprecated (use amount_usd instead). This will be flagged in
the Compliance → Deprecated Columns tab.
"""
import logging
import pandas as pd
from datetime import date, datetime
from typing import Optional

from config.database import dw_engine, get_raw_dw_connection
from models.customer import Transaction
from utils.decorators import pii_handler, audit_required, retry, deprecated_field
from utils.db_utils import bulk_insert, execute_query, log_etl_run

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Stage 1 — Ingest raw transaction feed
# ---------------------------------------------------------------------------

def load_raw_transactions(feed_file: str, business_date: date) -> pd.DataFrame:
    """
    Load raw transaction records from the payment processor feed (CSV).
    Reads: None (source file).
    Writes staging: transaction_staging.transaction_id, amount, currency, etc.
    """
    df = pd.read_csv(feed_file, dtype=str)
    df["business_date"] = business_date
    df = df.rename(columns={
        "TXN_ID":      "transaction_id",
        "ACCT_ID":     "account_id",
        "TXN_TYPE":    "transaction_type",
        "AMT":         "amount",
        "CCY":         "currency",
        "FX_RATE":     "fx_rate",
        "DESC":        "description",
        "MERCH_ID":    "merchant_id",
        "MERCH_NM":    "merchant_name",
        "CHANNEL":     "channel",
        "TXN_DT":      "transaction_date",
        "POST_DT":     "posted_date",
        "REF":         "reference_number",
    })
    logger.info(f"Loaded {len(df)} raw transactions for {business_date}")
    return df


def apply_fx_normalisation(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normalise all transaction amounts to USD using intraday FX rates.
    Reads:  market_data.spot_rate_mid, market_data.currency_pair
    Writes: transactions.amount_usd, transactions.fx_rate
    """
    fx_rates = execute_query("""
        SELECT currency_pair, spot_rate_mid
        FROM dbo.market_data
        WHERE as_of_date = CAST(GETDATE() AS DATE)
          AND instrument_type = 'FX'
    """)
    fx_map = dict(zip(fx_rates["currency_pair"], fx_rates["spot_rate_mid"]))

    def convert(row):
        pair = f"{row['currency']}/USD"
        rate = float(fx_map.get(pair, 1.0))
        return round(float(row["amount"]) * rate, 2)

    df["amount_usd"] = df.apply(convert, axis=1)
    df["fx_rate"]    = df["currency"].map(lambda c: fx_map.get(f"{c}/USD", 1.0))
    return df


@deprecated_field("old_amount", replacement="amount_usd")
def settle_legacy_amounts(business_date: date):
    """
    DEPRECATED USAGE: reads transactions.old_amount for legacy settlement system.
    This column is deprecated — downstream should use amount_usd.
    Flagged by the Compliance tab as a deprecated-column-in-use violation.
    """
    conn = get_raw_dw_connection()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT t.transaction_id,
               t.account_id,
               t.old_amount,          -- DEPRECATED column
               t.currency,
               a.routing_number
        FROM  dbo.transactions t
        JOIN  dbo.accounts     a ON a.account_id = t.account_id
        WHERE t.transaction_date = ?
          AND t.status = 'POSTED'
    """, (business_date,))
    rows = cursor.fetchall()
    logger.warning(f"Legacy settlement: processing {len(rows)} rows using deprecated old_amount")
    # Push to legacy settlement system
    for row in rows:
        cursor.execute(
            "INSERT INTO legacy_settlement_queue "
            "(transaction_id, amount_legacy, currency, routing_number, queue_date) "
            "VALUES (?, ?, ?, ?, ?)",
            (row[0], row[2], row[3], row[4], business_date),
        )
    conn.commit()
    conn.close()


# ---------------------------------------------------------------------------
# Stage 2 — AML pre-screening
# ---------------------------------------------------------------------------

def flag_suspicious_transactions(df: pd.DataFrame) -> pd.DataFrame:
    """
    Apply rule-based AML pre-screening and set is_suspicious flag.
    Reads:  transactions.amount, transactions.merchant_category, transactions.channel
    Writes: transactions.is_suspicious, transactions.aml_score
    """
    # Rule 1: large cash transactions
    df["is_suspicious"] = (
        (df["amount"].astype(float) > 10_000) &
        (df["channel"] == "BRANCH") &
        (df["transaction_type"] == "DEBIT")
    ).astype(int)

    # Rule 2: high-risk merchant categories
    high_risk_categories = {"GAMBLING", "CRYPTO_EXCHANGE", "MONEY_SERVICES", "PAWN_SHOP"}
    df["is_suspicious"] = df["is_suspicious"] | df["merchant_category"].isin(high_risk_categories)

    # Rule 3: velocity check — more than 20 txns from same account in 1 day
    velocity = execute_query("""
        SELECT account_id, COUNT(*) AS txn_count
        FROM dbo.transactions
        WHERE transaction_date = CAST(GETDATE() AS DATE)
        GROUP BY account_id
        HAVING COUNT(*) > 20
    """)
    high_vel_accounts = set(velocity["account_id"].tolist())
    df["is_suspicious"] = df["is_suspicious"] | df["account_id"].isin(high_vel_accounts)

    df["aml_score"] = df["is_suspicious"].astype(float) * 0.85
    logger.info(f"Flagged {df['is_suspicious'].sum()} suspicious transactions")
    return df


def calculate_transaction_metrics(df: pd.DataFrame) -> dict:
    """
    Compute summary metrics for the batch.
    Reads: transactions.amount_usd, transactions.transaction_type,
           transactions.is_suspicious, accounts.account_type
    """
    metrics = {
        "total_volume_usd": float(df["amount_usd"].sum()),
        "txn_count":        len(df),
        "suspicious_count": int(df["is_suspicious"].sum()),
        "debit_count":      int((df["transaction_type"] == "DEBIT").sum()),
        "credit_count":     int((df["transaction_type"] == "CREDIT").sum()),
        "avg_amount_usd":   float(df["amount_usd"].mean()),
    }
    logger.info(f"Batch metrics: {metrics}")
    return metrics


# ---------------------------------------------------------------------------
# Stage 3 — Write to transactions table
# ---------------------------------------------------------------------------

def write_transactions(df: pd.DataFrame, business_date: date) -> int:
    """
    Insert normalised transaction records to dbo.transactions.
    Writes: transactions.transaction_id, account_id, transaction_type,
            amount, currency, fx_rate, amount_usd, description,
            merchant_id, merchant_name, merchant_category, channel,
            transaction_date, posted_date, reference_number,
            is_suspicious, aml_score, status
    """
    cols = [
        "transaction_id", "account_id", "transaction_type",
        "amount", "currency", "fx_rate", "amount_usd",
        "description", "merchant_id", "merchant_name", "merchant_category",
        "channel", "transaction_date", "posted_date", "reference_number",
        "is_suspicious", "aml_score", "status",
    ]
    existing_cols = [c for c in cols if c in df.columns]
    bulk_insert(df[existing_cols], "transactions")
    logger.info(f"Wrote {len(df)} transactions for {business_date}")
    return len(df)


def archive_processed_feed(feed_file: str, business_date: date, row_count: int):
    """
    Record the processed feed in the ETL control table.
    Writes: etl_feed_control.feed_file, processed_at, row_count, status
    """
    conn = get_raw_dw_connection()
    cursor = conn.cursor()
    cursor.execute(
        "INSERT INTO etl_feed_control (feed_file, business_date, row_count, status, processed_at) "
        "VALUES (?, ?, ?, 'COMPLETE', GETUTCDATE())",
        (feed_file, business_date, row_count),
    )
    conn.commit()
    conn.close()


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------

def run_transaction_feed(feed_file: str, business_date: date, run_legacy: bool = True):
    """
    Orchestrates transaction feed processing:
    load_raw_transactions → apply_fx_normalisation → flag_suspicious_transactions
    → write_transactions → [settle_legacy_amounts]
    """
    try:
        raw_df      = load_raw_transactions(feed_file, business_date)
        norm_df     = apply_fx_normalisation(raw_df)
        flagged_df  = flag_suspicious_transactions(norm_df)
        metrics     = calculate_transaction_metrics(flagged_df)
        rows_stored = write_transactions(flagged_df, business_date)

        if run_legacy:
            settle_legacy_amounts(business_date)  # <- deprecated column usage

        archive_processed_feed(feed_file, business_date, rows_stored)
        log_etl_run("transaction_feed", "SUCCESS", rows_stored)
        return metrics
    except Exception as exc:
        log_etl_run("transaction_feed", "FAILED", 0, str(exc))
        raise
