"""
CCAR/DFAST Regulatory Reporting — Comprehensive Capital Analysis and Review.
All submission-generating functions carry @regulatory_report decorator,
which causes the AST Extractor to tag them with risk_tag='regulatory_report'.
These appear in the Compliance → Regulatory Lineage tab.
"""
import logging
import pandas as pd
from datetime import date
from typing import Dict, List

from config.database import dw_engine, reporting_engine, get_raw_dw_connection
from utils.db_utils import execute_query
from utils.decorators import regulatory_report, pii_handler, audit_required, retry
from risk.credit_risk import aggregate_pd_by_segment, get_segment_migration_matrix

logger = logging.getLogger(__name__)


def load_ccar_portfolio_data(stress_date: date, scenario: str) -> pd.DataFrame:
    """
    Load portfolio snapshot for a specific CCAR stress scenario.
    Reads: risk_scores.pd_score, lgd_estimate, ead, customer_segment, as_of_date,
           capital_requirements.rwa_credit, rwa_market,
           customer_master.customer_segment, accounts.account_type, credit_limit
    """
    return execute_query(f"""
        SELECT c.customer_segment,
               rs.pd_score,
               rs.lgd_estimate,
               rs.ead,
               rs.expected_loss,
               rs.scorecard_band,
               cr.rwa_credit,
               cr.rwa_market,
               cr.rwa_total,
               cr.tier1_capital,
               cr.tier1_ratio,
               cr.total_capital_ratio,
               cr.stress_scenario
        FROM   dbo.risk_scores           rs
        JOIN   dbo.customer_master       c  ON c.customer_id  = rs.customer_id
        JOIN   dbo.capital_requirements  cr ON cr.portfolio_segment = c.customer_segment
                                           AND cr.as_of_date  = rs.as_of_date
                                           AND cr.stress_scenario = '{scenario}'
        WHERE  rs.as_of_date = '{stress_date}'
    """, engine=dw_engine)


@regulatory_report(report_type="CCAR_BASELINE")
def build_ccar_baseline_submission(as_of_date: date) -> Dict:
    """
    Build the CCAR Baseline scenario capital projections.
    Reads: capital_requirements.tier1_ratio, total_capital_ratio, rwa_total,
           risk_scores.pd_score, expected_loss, customer_segment
    Writes: ccar_submissions.scenario, tier1_ratio, total_capital_ratio,
            rwa_total, submission_date, status
    """
    portfolio = load_ccar_portfolio_data(as_of_date, "BASELINE")
    segment_pd = aggregate_pd_by_segment(portfolio)

    summary = {
        "scenario":            "BASELINE",
        "as_of_date":          str(as_of_date),
        "total_rwa":           float(portfolio["rwa_total"].sum()),
        "avg_tier1_ratio":     float(portfolio["tier1_ratio"].mean()),
        "avg_total_cap_ratio": float(portfolio["total_capital_ratio"].mean()),
        "total_el":            float(portfolio["expected_loss"].sum()),
        "portfolio_pd":        float(portfolio["pd_score"].mean()),
        "segment_breakdown":   segment_pd.to_dict("records"),
    }

    conn = get_raw_dw_connection()
    cursor = conn.cursor()
    cursor.execute(
        """
        INSERT INTO dbo.ccar_submissions
            (scenario, as_of_date, total_rwa, avg_tier1_ratio,
             avg_total_cap_ratio, total_el, portfolio_pd, submission_status, submitted_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, 'DRAFT', GETUTCDATE())
        """,
        ("BASELINE", str(as_of_date), summary["total_rwa"], summary["avg_tier1_ratio"],
         summary["avg_total_cap_ratio"], summary["total_el"], summary["portfolio_pd"]),
    )
    conn.commit()
    conn.close()
    logger.info(f"CCAR Baseline submission built: RWA={summary['total_rwa']:,.0f}, "
                f"Tier1={summary['avg_tier1_ratio']:.2%}")
    return summary


@regulatory_report(report_type="CCAR_ADVERSE")
def build_ccar_adverse_submission(as_of_date: date) -> Dict:
    """
    Build CCAR Adverse stress scenario — applies Fed-defined macro shocks.
    Reads: capital_requirements.rwa_credit, rwa_market, tier1_capital,
           risk_scores.pd_score, var_results.var_99, stressed_var_99
    Writes: ccar_submissions.scenario='ADVERSE', projected_tier1_ratio
    """
    portfolio = load_ccar_portfolio_data(as_of_date, "ADVERSE")
    var_data = execute_query(f"""
        SELECT desk_id, var_99, stressed_var_99, expected_shortfall
        FROM   dbo.var_results
        WHERE  as_of_date = '{as_of_date}'
    """, engine=dw_engine)

    # Apply adverse macro shock multipliers (Fed-published)
    ADVERSE_PD_MULTIPLIER  = 3.5
    ADVERSE_LGD_MULTIPLIER = 1.8
    stressed_el = float(portfolio["expected_loss"].sum()) * ADVERSE_PD_MULTIPLIER * 0.6
    rwa_market_stressed = float(var_data["stressed_var_99"].sum()) * 12.5 if not var_data.empty else 0

    summary = {
        "scenario":             "ADVERSE",
        "as_of_date":           str(as_of_date),
        "stressed_el":          stressed_el,
        "rwa_market_stressed":  rwa_market_stressed,
        "projected_tier1_ratio": float(portfolio["tier1_ratio"].mean()) * (1 - 0.25),
        "capital_depletion_pct": 0.25,
    }
    conn = get_raw_dw_connection()
    cursor = conn.cursor()
    cursor.execute(
        "INSERT INTO dbo.ccar_submissions "
        "(scenario, as_of_date, total_rwa, avg_tier1_ratio, submission_status, submitted_at) "
        "VALUES (?, ?, ?, ?, 'DRAFT', GETUTCDATE())",
        ("ADVERSE", str(as_of_date),
         float(portfolio["rwa_total"].sum()) * 1.4,
         summary["projected_tier1_ratio"]),
    )
    conn.commit()
    conn.close()
    logger.info(f"CCAR Adverse submission built: StressedEL={stressed_el:,.0f}")
    return summary


@regulatory_report(report_type="CCAR_SEVERELY_ADVERSE")
def build_ccar_severely_adverse_submission(as_of_date: date) -> Dict:
    """
    Build CCAR Severely Adverse scenario — tail-risk capital floor test.
    Reads: capital_requirements.tier1_capital, rwa_total,
           var_results.stressed_var_99, expected_shortfall
           risk_scores.pd_score, lgd_estimate (stressed)
    Writes: ccar_submissions.scenario='SEVERELY_ADVERSE', stressed_tier1_ratio
    """
    portfolio = load_ccar_portfolio_data(as_of_date, "SEVERELY_ADVERSE")
    seg_migration = get_segment_migration_matrix(as_of_date)

    SEVERE_PD_MULTIPLIER  = 7.0
    stressed_el = float(portfolio["expected_loss"].sum()) * SEVERE_PD_MULTIPLIER * 0.5

    summary = {
        "scenario":             "SEVERELY_ADVERSE",
        "as_of_date":           str(as_of_date),
        "stressed_el":          stressed_el,
        "projected_tier1_ratio": max(0.04, float(portfolio["tier1_ratio"].mean()) * (1 - 0.50)),
        "passes_min_buffer":    float(portfolio["tier1_ratio"].mean()) * 0.50 >= 0.045,
        "migration_summary":    len(seg_migration),
    }
    logger.info(f"CCAR Severely Adverse built: StressedEL={stressed_el:,.0f}, "
                f"Tier1={summary['projected_tier1_ratio']:.2%}")
    return summary


@regulatory_report(report_type="DFAST_MIDCYCLE")
def build_dfast_midcycle_report(as_of_date: date) -> Dict:
    """
    Generate DFAST mid-cycle stress test disclosure report.
    Reads: ccar_submissions.scenario, avg_tier1_ratio, total_rwa,
           risk_scores.pd_score, expected_loss, customer_segment
    Writes: regulatory_submissions.report_type, submission_content, filed_at
    """
    baseline_data = execute_query(f"""
        SELECT scenario, avg_tier1_ratio, avg_total_cap_ratio, total_rwa,
               total_el, portfolio_pd, submitted_at
        FROM   dbo.ccar_submissions
        WHERE  as_of_date = '{as_of_date}'
          AND  submission_status IN ('DRAFT', 'SUBMITTED')
        ORDER BY submitted_at DESC
    """, engine=reporting_engine)

    summary = {
        "report_type":         "DFAST_MIDCYCLE",
        "as_of_date":          str(as_of_date),
        "scenarios_completed": len(baseline_data),
        "min_tier1_ratio":     float(baseline_data["avg_tier1_ratio"].min())
                               if not baseline_data.empty else 0,
        "max_portfolio_pd":    float(baseline_data["portfolio_pd"].max())
                               if not baseline_data.empty else 0,
    }

    conn = get_raw_dw_connection()
    cursor = conn.cursor()
    cursor.execute(
        "INSERT INTO dbo.regulatory_submissions "
        "(report_type, as_of_date, submission_status, filed_at) "
        "VALUES (?, ?, 'PENDING_REVIEW', GETUTCDATE())",
        ("DFAST_MIDCYCLE", str(as_of_date)),
    )
    conn.commit()
    conn.close()
    logger.info(f"DFAST Mid-cycle report generated for {as_of_date}")
    return summary


def get_ccar_submission_history(year: int) -> pd.DataFrame:
    """
    Retrieve all CCAR/DFAST submissions for a given year.
    Reads: ccar_submissions.scenario, as_of_date, avg_tier1_ratio,
           submission_status, submitted_at
    """
    return execute_query(f"""
        SELECT scenario,
               as_of_date,
               avg_tier1_ratio,
               avg_total_cap_ratio,
               total_rwa,
               total_el,
               submission_status,
               submitted_at
        FROM   dbo.ccar_submissions
        WHERE  YEAR(as_of_date) = {year}
        ORDER BY as_of_date DESC, scenario
    """, engine=reporting_engine)


def run_ccar_annual_cycle(base_date: date):
    """
    Orchestrate the full CCAR annual submission cycle.
    Calls: build_ccar_baseline_submission, build_ccar_adverse_submission,
           build_ccar_severely_adverse_submission, build_dfast_midcycle_report
    """
    logger.info(f"=== CCAR Annual Cycle START: {base_date} ===")
    baseline  = build_ccar_baseline_submission(base_date)
    adverse   = build_ccar_adverse_submission(base_date)
    severe    = build_ccar_severely_adverse_submission(base_date)
    dfast     = build_dfast_midcycle_report(base_date)
    logger.info(f"=== CCAR Annual Cycle COMPLETE ===")
    return {"baseline": baseline, "adverse": adverse,
            "severely_adverse": severe, "dfast": dfast}
