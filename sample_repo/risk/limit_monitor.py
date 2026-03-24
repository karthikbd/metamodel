"""
Limit Monitoring — detects breaches of credit and market risk limits.
Escalates critical breaches to risk officers and writes to limit_breaches table.
"""
import logging
import pandas as pd
from datetime import date, datetime, timedelta
from typing import List, Dict

from config.database import dw_engine, get_raw_dw_connection
from utils.db_utils import execute_query
from utils.decorators import retry

logger = logging.getLogger(__name__)

# Centralised limit definitions
CREDIT_LIMITS = {
    "RETAIL":       {"pd_threshold": 0.15,  "el_threshold_usd": 5_000_000},
    "COMMERCIAL":   {"pd_threshold": 0.10,  "el_threshold_usd": 20_000_000},
    "INSTITUTIONAL":{"pd_threshold": 0.05,  "el_threshold_usd": 100_000_000},
    "DEFAULT":      {"pd_threshold": 0.20,  "el_threshold_usd": 1_000_000},
}
VAR_LIMITS_USD = {
    "FX_DESK":      10_000_000,
    "RATES_DESK":   25_000_000,
    "EQUITY_DESK":  15_000_000,
    "CREDIT_DESK":  30_000_000,
    "DEFAULT":      5_000_000,
}


def detect_credit_limit_breaches(scored_df: pd.DataFrame,
                                  as_of_date: date) -> List[Dict]:
    """
    Identify customers where PD or EL exceeds segment-level limits.
    Reads:  risk_scores.pd_score, expected_loss, ead, customer_segment
            customer_master.customer_id
    Writes: limit_breaches.customer_id, limit_type, breach_amount,
            limit_value, actual_value, breach_date, status, severity
    """
    breaches = []
    for _, row in scored_df.iterrows():
        seg = row.get("customer_segment", "DEFAULT")
        limits = CREDIT_LIMITS.get(seg, CREDIT_LIMITS["DEFAULT"])

        if float(row.get("pd_score", 0)) > limits["pd_threshold"]:
            breaches.append({
                "customer_id":   int(row["customer_id"]),
                "limit_type":    "CREDIT",
                "limit_name":    f"PD_THRESHOLD_{seg}",
                "breach_amount": float(row["pd_score"]) - limits["pd_threshold"],
                "limit_value":   limits["pd_threshold"],
                "actual_value":  float(row["pd_score"]),
                "breach_date":   str(as_of_date),
                "status":        "OPEN",
                "severity":      "CRITICAL" if float(row["pd_score"]) > 0.30 else "HIGH",
            })
        el = float(row.get("expected_loss", 0))
        if el > limits["el_threshold_usd"]:
            breaches.append({
                "customer_id":   int(row["customer_id"]),
                "limit_type":    "CREDIT",
                "limit_name":    f"EL_THRESHOLD_{seg}",
                "breach_amount": el - limits["el_threshold_usd"],
                "limit_value":   limits["el_threshold_usd"],
                "actual_value":  el,
                "breach_date":   str(as_of_date),
                "status":        "OPEN",
                "severity":      "HIGH",
            })
    logger.info(f"Detected {len(breaches)} credit limit breaches for {as_of_date}")
    return breaches


def detect_var_breaches(var_df: pd.DataFrame, as_of_date: date) -> List[Dict]:
    """
    Flag trading desks where VaR exceeds desk-level limits.
    Reads:  var_results.desk_id, desk_name, var_99
    Writes: limit_breaches.limit_type, limit_name, actual_value, breach_date
    """
    breaches = []
    for _, row in var_df.iterrows():
        desk_id = row.get("desk_id", "DEFAULT")
        limit   = VAR_LIMITS_USD.get(desk_id, VAR_LIMITS_USD["DEFAULT"])
        var_val = float(row.get("var_99", 0))
        if var_val > limit:
            breaches.append({
                "limit_type":    "VAR",
                "limit_name":    f"VAR_99_{desk_id}",
                "breach_amount": var_val - limit,
                "limit_value":   limit,
                "actual_value":  var_val,
                "breach_date":   str(as_of_date),
                "status":        "OPEN",
                "severity":      "CRITICAL" if var_val > limit * 1.5 else "HIGH",
            })
    logger.info(f"Detected {len(breaches)} VaR limit breaches for {as_of_date}")
    return breaches


def write_breaches(breaches: List[Dict]):
    """
    Persist breach records to limit_breaches table.
    Writes: limit_breaches.customer_id, account_id, limit_type, limit_name,
            breach_amount, limit_value, actual_value, breach_date, status, severity
    """
    if not breaches:
        return
    conn = get_raw_dw_connection()
    cursor = conn.cursor()
    cursor.executemany(
        """
        INSERT INTO dbo.limit_breaches
            (customer_id, limit_type, limit_name, breach_amount,
             limit_value, actual_value, breach_date, status, severity)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [(b.get("customer_id"), b["limit_type"], b["limit_name"],
          b["breach_amount"], b["limit_value"], b["actual_value"],
          b["breach_date"], b["status"], b["severity"])
         for b in breaches],
    )
    conn.commit()
    conn.close()


def escalate_critical_breaches(breaches: List[Dict]):
    """
    Send escalation notifications for CRITICAL severity breaches.
    Reads:  limit_breaches.severity, status, breach_amount
    Writes: escalation_log.breach_id, escalated_to, escalated_at
    """
    critical = [b for b in breaches if b.get("severity") == "CRITICAL"]
    if not critical:
        logger.info("No critical breaches requiring escalation")
        return

    write_breaches(critical)
    conn = get_raw_dw_connection()
    cursor = conn.cursor()
    for b in critical:
        logger.warning(
            f"CRITICAL BREACH: {b['limit_name']} — "
            f"actual={b['actual_value']:.2f} limit={b['limit_value']:.2f} "
            f"excess={b['breach_amount']:.2f}"
        )
        cursor.execute(
            "INSERT INTO dbo.escalation_log "
            "(limit_name, limit_type, actual_value, limit_value, escalated_at, escalated_to) "
            "VALUES (?, ?, ?, ?, GETUTCDATE(), 'RISK_OFFICER_TEAM')",
            (b["limit_name"], b["limit_type"], b["actual_value"], b["limit_value"]),
        )
    conn.commit()
    conn.close()
    logger.info(f"Escalated {len(critical)} critical breaches to Risk Officer Team")


def resolve_stale_breaches(older_than_days: int = 5):
    """
    Auto-resolve OPEN breaches that have not been updated beyond threshold.
    Reads:  limit_breaches.status, breach_date
    Writes: limit_breaches.status, resolved_date, resolution_notes
    """
    cutoff = (datetime.utcnow() - timedelta(days=older_than_days)).date()
    conn = get_raw_dw_connection()
    cursor = conn.cursor()
    cursor.execute(
        """
        UPDATE dbo.limit_breaches
        SET    status           = 'AUTO_RESOLVED',
               resolved_date   = CAST(GETUTCDATE() AS DATE),
               resolution_notes = 'Auto-resolved: no activity within SLA window'
        WHERE  status     = 'OPEN'
          AND  breach_date < ?
        """,
        (str(cutoff),),
    )
    rows = cursor.rowcount
    conn.commit()
    conn.close()
    logger.info(f"Auto-resolved {rows} stale limit breaches (older than {older_than_days} days)")


def get_open_breach_summary(as_of_date: date) -> pd.DataFrame:
    """
    Summarise open breaches by type and severity for the Risk Dashboard.
    Reads: limit_breaches.limit_type, severity, status, breach_date,
           breach_amount, customer_id
    """
    return execute_query(f"""
        SELECT limit_type,
               severity,
               COUNT(*)              AS breach_count,
               SUM(breach_amount)    AS total_excess,
               MAX(actual_value)     AS max_actual,
               MIN(breach_date)      AS oldest_breach
        FROM   dbo.limit_breaches
        WHERE  status IN ('OPEN', 'ACKNOWLEDGED')
          AND  breach_date <= '{as_of_date}'
        GROUP BY limit_type, severity
        ORDER BY severity, limit_type
    """, engine=dw_engine)
