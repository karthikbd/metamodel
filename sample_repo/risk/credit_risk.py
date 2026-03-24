"""
Credit Risk Engine — PD/LGD/EAD scoring using Basel FIRB approach.
Scorecard-based PD model + LGD haircut tables + EAD CCF factors.
"""
import logging
import numpy as np
import pandas as pd
from datetime import date
from typing import List

from config.database import dw_engine, risk_engine
from utils.db_utils import execute_query
from utils.decorators import retry

logger = logging.getLogger(__name__)

# Scorecard feature weights (simplified logistic regression coefficients)
SCORECARD_WEIGHTS = {
    "dpd_30":           -2.5,    # Days past due 30
    "dpd_60":           -4.1,    # Days past due 60
    "utilisation_ratio": -1.8,   # Credit utilisation
    "months_on_book":    0.04,   # Tenure
    "num_products":      0.12,   # Product breadth
    "income_stability":  0.95,   # Income volatility inverse
    "revolving_balance": -0.003, # Revolving balance
    "payment_ratio":     2.1,    # Min payment coverage
    "inquiry_count_6m": -0.55,   # Hard inquiries last 6m
}
INTERCEPT = -2.8


def load_customer_features(customer_ids: List[int], as_of_date: date) -> pd.DataFrame:
    """
    Load behavioural and bureau features for credit scoring.
    Reads: customer_master.customer_id, customer_segment, onboarding_date,
           accounts.current_balance, credit_limit, account_type,
           transactions.amount, transaction_date, transaction_type,
           risk_scores.pd_score (prior)
    """
    ids_str = ",".join(str(i) for i in customer_ids[:5000])  # batch limit
    return execute_query(f"""
        SELECT
            c.customer_id,
            c.customer_segment,
            DATEDIFF(month, c.onboarding_date, '{as_of_date}') AS months_on_book,
            SUM(CASE WHEN a.account_type IN ('CREDIT_CARD','LINE_OF_CREDIT')
                     THEN a.current_balance / NULLIF(a.credit_limit, 0) ELSE 0 END)
                     AS utilisation_ratio,
            COUNT(DISTINCT a.account_id)                        AS num_products,
            SUM(CASE WHEN t.transaction_date >= DATEADD(day,-30,'{as_of_date}')
                     AND t.transaction_type='DEBIT' AND t.status='RETURNED'
                     THEN 1 ELSE 0 END)                         AS dpd_30,
            SUM(CASE WHEN t.transaction_date >= DATEADD(day,-60,'{as_of_date}')
                     AND t.transaction_type='DEBIT' AND t.status='RETURNED'
                     THEN 1 ELSE 0 END)                         AS dpd_60,
            AVG(CAST(t.amount AS FLOAT))                        AS avg_txn_amount,
            MAX(a.credit_limit)                                 AS max_credit_limit,
            MAX(a.current_balance)                              AS max_balance,
            COALESCE(rs.pd_score, 0.05)                         AS prior_pd
        FROM      dbo.customer_master  c
        JOIN      dbo.accounts         a  ON a.customer_id   = c.customer_id
        LEFT JOIN dbo.transactions     t  ON t.account_id    = a.account_id
        LEFT JOIN dbo.risk_scores      rs ON rs.customer_id  = c.customer_id
                                         AND rs.as_of_date = DATEADD(day,-1,'{as_of_date}')
        WHERE c.customer_id IN ({ids_str})
          AND c.is_active = 1
        GROUP BY c.customer_id, c.customer_segment, c.onboarding_date, rs.pd_score
    """, engine=dw_engine)


def score_customers_pd(features_df: pd.DataFrame, as_of_date: date) -> pd.DataFrame:
    """
    Apply logistic regression scorecard to compute PD per customer.
    Reads:  (features_df) — output of load_customer_features
    Writes: risk_scores.pd_score, scorecard_band, model_version
    """
    df = features_df.fillna(0).copy()
    log_odds = INTERCEPT
    for feat, weight in SCORECARD_WEIGHTS.items():
        if feat in df.columns:
            log_odds = log_odds + weight * df[feat]

    df["pd_score"] = 1 / (1 + np.exp(-log_odds))
    df["pd_score"] = df["pd_score"].clip(0.0005, 0.9999)

    def band(pd):
        if pd < 0.005: return "AAA"
        if pd < 0.01:  return "AA"
        if pd < 0.02:  return "A"
        if pd < 0.05:  return "BBB"
        if pd < 0.10:  return "BB"
        if pd < 0.20:  return "B"
        if pd < 0.50:  return "CCC"
        return "D"

    df["scorecard_band"] = df["pd_score"].apply(band)
    df["model_version"]  = "CRM_LOGIT_v4.2"
    df["as_of_date"]     = as_of_date
    logger.info(f"Scored {len(df)} customers. Avg PD: {df['pd_score'].mean():.4f}")
    return df


def compute_lgd_estimates(scored_df: pd.DataFrame) -> pd.DataFrame:
    """
    LGD = collateral-adjusted haircut by product type and scorecard band.
    Reads:  accounts.account_type, accounts.credit_limit
    Writes: risk_scores.lgd_estimate
    """
    LGD_TABLE = {
        ("LOAN",            "AAA"): 0.20, ("LOAN",            "BBB"): 0.35,
        ("LOAN",            "CCC"): 0.60, ("LOAN",            "D"):   0.85,
        ("CREDIT_CARD",     "AAA"): 0.75, ("CREDIT_CARD",     "BBB"): 0.80,
        ("CREDIT_CARD",     "CCC"): 0.90, ("CREDIT_CARD",     "D"):   0.95,
        ("LINE_OF_CREDIT",  "AAA"): 0.50, ("LINE_OF_CREDIT",  "BBB"): 0.65,
    }
    default_lgd = 0.45

    def lookup_lgd(row):
        key = (row.get("account_type", "LOAN"), row.get("scorecard_band", "BBB"))
        return LGD_TABLE.get(key, default_lgd)

    scored_df["lgd_estimate"] = scored_df.apply(lookup_lgd, axis=1)
    return scored_df


def compute_ead(scored_df: pd.DataFrame) -> pd.DataFrame:
    """
    EAD = current balance + CCF * undrawn commitment. CCF varies by product.
    Reads:  accounts.current_balance, accounts.credit_limit
    Writes: risk_scores.ead
    """
    CCF = {"CREDIT_CARD": 0.75, "LINE_OF_CREDIT": 0.50, "LOAN": 1.00}
    scored_df["undrawn"]     = (scored_df["max_credit_limit"] - scored_df["max_balance"]).clip(0)
    scored_df["ccf"]         = scored_df.get("account_type", "LOAN").map(
        lambda t: CCF.get(t, 0.75)
    ) if "account_type" in scored_df.columns else 0.75
    scored_df["ead"]         = scored_df["max_balance"] + 0.75 * scored_df["undrawn"]
    scored_df["risk_weight"] = scored_df["pd_score"] * scored_df["lgd_estimate"] * 12.5
    scored_df["expected_loss"] = scored_df["pd_score"] * scored_df["lgd_estimate"] * scored_df["ead"]
    return scored_df


def aggregate_pd_by_segment(scored_df: pd.DataFrame) -> pd.DataFrame:
    """
    Aggregate PD statistics by customer segment for regulatory reporting.
    Reads:  risk_scores.pd_score, risk_scores.ead
    Writes: (in-memory — consumed by run_daily_risk_pipeline and ccar_report)
    """
    segment_stats = (
        scored_df.groupby("customer_segment")
        .agg(
            customer_count  = ("customer_id", "count"),
            avg_pd          = ("pd_score",    "mean"),
            median_pd       = ("pd_score",    "median"),
            total_ead       = ("ead",         "sum"),
            total_el        = ("expected_loss","sum"),
            pct_d_rated     = ("scorecard_band", lambda x: (x == "D").mean()),
        )
        .reset_index()
    )
    logger.info(f"Segment PD aggregation complete: {len(segment_stats)} segments")
    return segment_stats


def get_pd_time_series(customer_id: int, lookback_months: int = 12) -> pd.DataFrame:
    """
    Retrieve historical PD series for a customer (trend analysis / model validation).
    Reads: risk_scores.pd_score, risk_scores.as_of_date, risk_scores.scorecard_band
    """
    return execute_query("""
        SELECT as_of_date, pd_score, lgd_estimate, ead, scorecard_band, model_version
        FROM   dbo.risk_scores
        WHERE  customer_id = ?
          AND  as_of_date >= DATEADD(month, -?, CAST(GETDATE() AS DATE))
        ORDER BY as_of_date DESC
    """, params={"param_1": customer_id, "param_2": lookback_months}, engine=risk_engine)


def get_segment_migration_matrix(as_of_date: date) -> pd.DataFrame:
    """
    Compute month-on-month scorecard band migration matrix.
    Reads: risk_scores.scorecard_band, risk_scores.as_of_date, risk_scores.customer_id
    """
    return execute_query(f"""
        SELECT
            prev.scorecard_band AS from_band,
            curr.scorecard_band AS to_band,
            COUNT(*)            AS customer_count
        FROM      dbo.risk_scores curr
        JOIN      dbo.risk_scores prev
               ON prev.customer_id = curr.customer_id
              AND prev.as_of_date  = DATEADD(month, -1, curr.as_of_date)
        WHERE curr.as_of_date = '{as_of_date}'
        GROUP BY prev.scorecard_band, curr.scorecard_band
        ORDER BY from_band, to_band
    """, engine=risk_engine)
