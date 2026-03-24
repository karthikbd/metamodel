"""
Basel III Capital Adequacy Reporting — CRR II / BCBS 239 compliance.
All regulatory submission functions carry @regulatory_report decorator.
"""
import logging
import pandas as pd
from datetime import date
from typing import Dict

from config.database import dw_engine, reporting_engine, get_raw_dw_connection
from utils.db_utils import execute_query
from utils.decorators import regulatory_report

logger = logging.getLogger(__name__)

# Basel III minimum ratios (CRR II)
MIN_CET1_RATIO        = 0.045
MIN_TIER1_RATIO       = 0.060
MIN_TOTAL_CAP_RATIO   = 0.080
CAPITAL_CONSERVATION_BUFFER = 0.025
LEVERAGE_RATIO_MIN    = 0.030
LCR_MIN               = 1.00
NSFR_MIN              = 1.00


def load_capital_components(as_of_date: date) -> pd.DataFrame:
    """
    Load regulatory capital components by portfolio segment.
    Reads: capital_requirements.tier1_capital, tier2_capital, rwa_credit,
           rwa_market, rwa_operational, rwa_total, tier1_ratio,
           total_capital_ratio, leverage_ratio, lcr, nsfr
    """
    return execute_query(f"""
        SELECT portfolio_segment,
               rwa_credit,
               rwa_market,
               rwa_operational,
               rwa_total,
               tier1_capital,
               tier2_capital,
               tier1_ratio,
               total_capital_ratio,
               leverage_ratio,
               lcr,
               nsfr,
               stress_scenario
        FROM   dbo.capital_requirements
        WHERE  as_of_date      = '{as_of_date}'
          AND  stress_scenario = 'BASELINE'
    """, engine=dw_engine)


def compute_cet1_ratio(as_of_date: date) -> Dict:
    """
    Compute CET1 (Common Equity Tier 1) ratio — most conservative capital metric.
    Reads: capital_requirements.tier1_capital, rwa_total
    Writes: capital_adequacy_report.cet1_ratio, passes_minimum
    """
    caps = load_capital_components(as_of_date)
    if caps.empty:
        return {"cet1_ratio": 0, "passes_minimum": False}

    total_tier1 = float(caps["tier1_capital"].sum())
    total_rwa   = float(caps["rwa_total"].sum())
    # CET1 ≈ Tier1 * 0.85 (AT1 instruments excluded)
    cet1        = (total_tier1 * 0.85) / total_rwa if total_rwa > 0 else 0

    return {
        "cet1_ratio":         round(cet1, 6),
        "tier1_ratio":        round(total_tier1 / total_rwa, 6) if total_rwa > 0 else 0,
        "total_capital_ratio": round(float((caps["tier1_capital"] + caps["tier2_capital"]).sum()) / total_rwa, 6),
        "total_rwa":          total_rwa,
        "passes_cet1_min":    cet1 >= MIN_CET1_RATIO,
        "passes_tier1_min":   (total_tier1 / total_rwa) >= MIN_TIER1_RATIO if total_rwa > 0 else False,
        "conservation_buffer_met": cet1 >= (MIN_CET1_RATIO + CAPITAL_CONSERVATION_BUFFER),
    }


def compute_leverage_ratio(as_of_date: date) -> Dict:
    """
    Compute Basel III leverage ratio — non-risk-sensitive capital backstop.
    Reads: capital_requirements.tier1_capital,
           trading_positions.market_value (total exposure proxy)
    Writes: capital_adequacy_report.leverage_ratio, passes_minimum
    """
    caps = load_capital_components(as_of_date)
    total_exposures = execute_query(f"""
        SELECT SUM(ABS(market_value)) AS total_exposure
        FROM   dbo.trading_positions
        WHERE  as_of_date = '{as_of_date}'
          AND  status     = 'ACTIVE'
    """, engine=dw_engine)

    tier1 = float(caps["tier1_capital"].sum()) if not caps.empty else 0
    exposures = float(total_exposures.iloc[0]["total_exposure"]) if not total_exposures.empty else 1
    leverage_ratio = tier1 / exposures if exposures > 0 else 0

    return {
        "tier1_capital":   tier1,
        "total_exposures": exposures,
        "leverage_ratio":  round(leverage_ratio, 6),
        "passes_minimum":  leverage_ratio >= LEVERAGE_RATIO_MIN,
    }


def compute_liquidity_ratios(as_of_date: date) -> Dict:
    """
    Compute LCR (Liquidity Coverage Ratio) and NSFR (Net Stable Funding Ratio).
    Reads: capital_requirements.lcr, nsfr
            liquidity_pool.hqla_tier1, hqla_tier2, net_cash_outflow_30d,
            available_stable_funding, required_stable_funding
    """
    caps = load_capital_components(as_of_date)
    avg_lcr  = float(caps["lcr"].mean())  if not caps.empty else 0
    avg_nsfr = float(caps["nsfr"].mean()) if not caps.empty else 0
    return {
        "avg_lcr":       round(avg_lcr, 4),
        "avg_nsfr":      round(avg_nsfr, 4),
        "passes_lcr":    avg_lcr  >= LCR_MIN,
        "passes_nsfr":   avg_nsfr >= NSFR_MIN,
    }


@regulatory_report(report_type="BASEL3_PILLAR1")
def generate_pillar1_report(as_of_date: date) -> Dict:
    """
    Generate Basel III Pillar 1 capital adequacy report.
    Reads: capital_requirements.rwa_credit, rwa_market, rwa_operational, rwa_total,
           tier1_capital, tier2_capital, tier1_ratio, total_capital_ratio
    Writes: regulatory_submissions.report_type='PILLAR1', content_json, filed_at
    """
    cet1      = compute_cet1_ratio(as_of_date)
    leverage  = compute_leverage_ratio(as_of_date)
    liquidity = compute_liquidity_ratios(as_of_date)
    caps      = load_capital_components(as_of_date)

    report = {
        "report_type":    "BASEL3_PILLAR1",
        "as_of_date":     str(as_of_date),
        "cet1":           cet1,
        "leverage":       leverage,
        "liquidity":      liquidity,
        "rwa_breakdown": {
            "credit":      float(caps["rwa_credit"].sum())      if not caps.empty else 0,
            "market":      float(caps["rwa_market"].sum())      if not caps.empty else 0,
            "operational": float(caps["rwa_operational"].sum()) if not caps.empty else 0,
            "total":       float(caps["rwa_total"].sum())       if not caps.empty else 0,
        },
        "all_minimums_met": all([
            cet1.get("passes_cet1_min",  False),
            cet1.get("passes_tier1_min", False),
            leverage.get("passes_minimum", False),
            liquidity.get("passes_lcr",  False),
            liquidity.get("passes_nsfr", False),
        ]),
    }
    conn = get_raw_dw_connection()
    cursor = conn.cursor()
    cursor.execute(
        "INSERT INTO dbo.regulatory_submissions "
        "(report_type, as_of_date, submission_status, filed_at) "
        "VALUES ('BASEL3_PILLAR1', ?, 'FILED', GETUTCDATE())",
        (str(as_of_date),),
    )
    conn.commit()
    conn.close()
    logger.info(f"Basel III Pillar 1 report filed: CET1={cet1.get('cet1_ratio',0):.2%}, "
                f"Leverage={leverage.get('leverage_ratio',0):.2%}")
    return report


@regulatory_report(report_type="BASEL3_PILLAR3")
def generate_pillar3_disclosure(as_of_date: date) -> Dict:
    """
    Generate Basel III Pillar 3 public disclosure (BCBS 239 data lineage required).
    Reads: capital_requirements.*, var_results.var_99, stressed_var_99,
           risk_scores.pd_score, customer_segment
    Writes: regulatory_submissions.report_type='PILLAR3'
    """
    pillar1 = generate_pillar1_report(as_of_date)
    var_data = execute_query(f"""
        SELECT desk_id, var_99, stressed_var_99, expected_shortfall,
               backtesting_exceptions
        FROM   dbo.var_results
        WHERE  as_of_date = '{as_of_date}'
    """, engine=dw_engine)

    disclosure = {
        "report_type":           "BASEL3_PILLAR3",
        "as_of_date":            str(as_of_date),
        "capital_summary":       pillar1,
        "market_risk_desks":     len(var_data),
        "total_var_99":          float(var_data["var_99"].sum()) if not var_data.empty else 0,
        "max_backtesting_exc":   int(var_data["backtesting_exceptions"].max()) if not var_data.empty else 0,
        "disclosure_complete":   True,
    }
    logger.info(f"Basel III Pillar 3 disclosure complete for {as_of_date}")
    return disclosure


@regulatory_report(report_type="COREP")
def generate_corep_return(as_of_date: date) -> Dict:
    """
    Generate EBA COREP (Common Reporting) return — EU regulatory submission.
    Reads: capital_requirements.rwa_total, tier1_ratio, total_capital_ratio,
           leverage_ratio, lcr, nsfr
    Writes: regulatory_submissions.report_type='COREP'
    """
    cet1     = compute_cet1_ratio(as_of_date)
    liquidity = compute_liquidity_ratios(as_of_date)
    corep = {
        "report_type":            "COREP",
        "as_of_date":             str(as_of_date),
        "C_01_00_cet1_ratio":     cet1.get("cet1_ratio", 0),
        "C_02_00_tier1_ratio":    cet1.get("tier1_ratio", 0),
        "C_03_00_total_cap_ratio": cet1.get("total_capital_ratio", 0),
        "C_40_00_lcr":            liquidity.get("avg_lcr", 0),
        "C_60_00_nsfr":           liquidity.get("avg_nsfr", 0),
        "submission_format":      "XBRL",
        "filing_deadline":        "T+15 business days",
    }
    logger.info(f"COREP return generated for {as_of_date}")
    return corep
