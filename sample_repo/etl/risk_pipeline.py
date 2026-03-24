"""
Daily Risk Pipeline Orchestrator.
Runs credit scoring → market risk → capital calculation → limit monitoring.
Entry point for the nightly batch risk run.
"""
import logging
import pandas as pd
from datetime import date

from config.database import dw_engine, risk_engine
from utils.db_utils import execute_query, log_etl_run, upsert_risk_scores
from utils.decorators import retry

# Cross-module imports — demonstrates CALLS graph edges for lineage
from risk.credit_risk  import (
    load_customer_features,
    score_customers_pd,
    compute_lgd_estimates,
    compute_ead,
    aggregate_pd_by_segment,
)
from risk.market_risk  import (
    run_historical_var,
    run_stressed_var,
    compute_expected_shortfall,
    store_var_results,
)
from risk.limit_monitor import (
    detect_credit_limit_breaches,
    detect_var_breaches,
    escalate_critical_breaches,
    resolve_stale_breaches,
)

logger = logging.getLogger(__name__)


def load_active_portfolio(as_of_date: date) -> pd.DataFrame:
    """
    Load the active customer portfolio for risk scoring.
    Reads: customer_master.customer_id, customer_segment, risk_rating,
           accounts.account_id, account_type, current_balance, credit_limit,
           risk_scores.pd_score, risk_scores.as_of_date
    """
    return execute_query("""
        SELECT c.customer_id,
               c.customer_segment,
               c.risk_rating,
               c.onboarding_date,
               a.account_id,
               a.account_type,
               a.current_balance,
               a.credit_limit,
               COALESCE(rs.pd_score, 0.05)     AS prior_pd,
               COALESCE(rs.lgd_estimate, 0.45)  AS prior_lgd,
               COALESCE(rs.ead, a.credit_limit, 0) AS prior_ead
        FROM   dbo.customer_master   c
        JOIN   dbo.accounts          a  ON a.customer_id = c.customer_id
        LEFT JOIN dbo.risk_scores    rs ON rs.customer_id = c.customer_id
                                       AND rs.as_of_date = DATEADD(day, -1, ?)
        WHERE  c.is_active = 1
          AND  a.status = 'ACTIVE'
          AND  a.account_type IN ('LOAN', 'CREDIT_CARD', 'LINE_OF_CREDIT')
    """, engine=dw_engine)


def compute_expected_loss_by_segment(risk_df: pd.DataFrame) -> pd.DataFrame:
    """
    EL = PD x LGD x EAD, aggregated by customer_segment.
    Reads:  risk_scores.pd_score, risk_scores.lgd_estimate, risk_scores.ead
    Writes: capital_requirements.rwa_credit (via aggregation)
    """
    risk_df = risk_df.copy()
    risk_df["expected_loss"] = (
        risk_df["pd_score"] * risk_df["lgd_estimate"] * risk_df["ead"]
    )
    summary = (
        risk_df.groupby("customer_segment")
        .agg(
            total_ead       = ("ead", "sum"),
            avg_pd          = ("pd_score", "mean"),
            avg_lgd         = ("lgd_estimate", "mean"),
            total_el        = ("expected_loss", "sum"),
            customer_count  = ("customer_id", "count"),
        )
        .reset_index()
    )
    logger.info(f"Expected loss summary across {len(summary)} segments")
    return summary


def write_risk_scores(risk_df: pd.DataFrame, as_of_date: date) -> int:
    """
    Persist daily risk scores to risk_scores table.
    Writes: risk_scores.customer_id, pd_score, lgd_estimate, ead,
            risk_weight, expected_loss, scorecard_band, as_of_date, model_version
    """
    records = risk_df[[
        "customer_id", "pd_score", "lgd_estimate", "ead", "risk_weight", "expected_loss",
    ]].copy()
    records["as_of_date"] = str(as_of_date)
    upsert_risk_scores(records.to_dict("records"))
    logger.info(f"Upserted {len(records)} risk scores for {as_of_date}")
    return len(records)


def compute_rwa(risk_df: pd.DataFrame) -> pd.DataFrame:
    """
    Compute Risk-Weighted Assets per portfolio segment (Basel III FIRB).
    Reads:  risk_scores.pd_score, risk_scores.ead, risk_scores.risk_weight
    Writes: capital_requirements.rwa_credit
    """
    risk_df = risk_df.copy()
    risk_df["rwa"] = risk_df["ead"] * risk_df["risk_weight"]
    rwa_by_segment = (
        risk_df.groupby("customer_segment")
        .agg(rwa_credit=("rwa", "sum"), total_ead=("ead", "sum"))
        .reset_index()
    )
    logger.info(f"RWA computed for {len(rwa_by_segment)} segments")
    return rwa_by_segment


def store_capital_requirements(rwa_df: pd.DataFrame, var_df: pd.DataFrame,
                                as_of_date: date, stress_scenario: str = "BASELINE"):
    """
    Combine credit and market RWA into capital_requirements table.
    Writes: capital_requirements.as_of_date, portfolio_segment,
            rwa_credit, rwa_market, rwa_total,
            tier1_ratio, total_capital_ratio, stress_scenario
    """
    from config.database import get_raw_dw_connection
    conn = get_raw_dw_connection()
    cursor = conn.cursor()
    for _, row in rwa_df.iterrows():
        rwa_mkt = float(var_df["var_99"].mean()) * 12.5  # market RWA proxy
        rwa_total = float(row["rwa_credit"]) + rwa_mkt
        tier1 = rwa_total * 0.12       # assume 12% Tier 1 ratio
        cursor.execute(
            """
            INSERT INTO dbo.capital_requirements
                (as_of_date, portfolio_segment, rwa_credit, rwa_market, rwa_total,
                 tier1_capital, tier1_ratio, total_capital_ratio, stress_scenario)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (str(as_of_date), row["customer_segment"],
             float(row["rwa_credit"]), rwa_mkt, rwa_total,
             tier1, tier1 / rwa_total if rwa_total else 0,
             (tier1 + rwa_total * 0.02) / rwa_total if rwa_total else 0,
             stress_scenario),
        )
    conn.commit()
    conn.close()


def run_daily_risk_pipeline(as_of_date: date):
    """
    Main orchestrator for the nightly risk batch.
    Calls: load_active_portfolio, load_customer_features, score_customers_pd,
    compute_lgd_estimates, compute_ead, aggregate_pd_by_segment, write_risk_scores,
    compute_expected_loss_by_segment, compute_rwa, store_capital_requirements,
    run_historical_var, run_stressed_var, compute_expected_shortfall, store_var_results,
    detect_credit_limit_breaches, detect_var_breaches,
    escalate_critical_breaches, resolve_stale_breaches.
    """
    logger.info(f"=== Daily Risk Pipeline START {as_of_date} ===")
    try:
        portfolio_df    = load_active_portfolio(as_of_date)
        features_df     = load_customer_features(portfolio_df["customer_id"].tolist(), as_of_date)
        scored_df       = score_customers_pd(features_df, as_of_date)
        scored_df       = compute_lgd_estimates(scored_df)
        scored_df       = compute_ead(scored_df)
        segment_pd      = aggregate_pd_by_segment(scored_df)
        rows            = write_risk_scores(scored_df, as_of_date)
        el_summary      = compute_expected_loss_by_segment(scored_df)
        rwa_df          = compute_rwa(scored_df)

        var_df          = run_historical_var(as_of_date, lookback_days=250)
        stressed_df     = run_stressed_var(as_of_date, stress_window="2008-2009")
        es_df           = compute_expected_shortfall(var_df, confidence=0.975)
        store_var_results(var_df, stressed_df, as_of_date)
        store_capital_requirements(rwa_df, var_df, as_of_date)

        credit_breaches = detect_credit_limit_breaches(scored_df, as_of_date)
        var_breaches    = detect_var_breaches(var_df, as_of_date)
        escalate_critical_breaches(credit_breaches + var_breaches)
        resolve_stale_breaches(older_than_days=5)

        log_etl_run("daily_risk_pipeline", "SUCCESS", rows)
        logger.info(f"=== Daily Risk Pipeline COMPLETE {as_of_date} ===")
        return {"rows_scored": rows, "segments": len(segment_pd),
                "var_desks": len(var_df), "breaches": len(credit_breaches) + len(var_breaches)}
    except Exception as exc:
        log_etl_run("daily_risk_pipeline", "FAILED", 0, str(exc))
        raise
