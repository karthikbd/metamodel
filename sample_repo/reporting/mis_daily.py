"""
MIS Daily Reporting — Management Information System dashboard feeds.
Aggregates risk, capital, liquidity, and business metrics for executive view.
"""
import logging
import pandas as pd
from datetime import date

from config.database import dw_engine, reporting_engine, get_raw_dw_connection
from utils.db_utils import execute_query, truncate_and_reload
from utils.decorators import regulatory_report

logger = logging.getLogger(__name__)


def build_daily_portfolio_snapshot(as_of_date: date) -> pd.DataFrame:
    """
    Build cross-portfolio performance and risk snapshot.
    Reads: accounts.account_type, current_balance, credit_limit,
           risk_scores.pd_score, expected_loss, scorecard_band,
           transactions.amount_usd, transaction_type, transaction_date
    Writes: mis_daily_snapshot.portfolio_segment, balance_total,
            el_total, avg_pd, debit_volume, credit_volume
    """
    return execute_query(f"""
        SELECT c.customer_segment,
               a.account_type,
               COUNT(DISTINCT c.customer_id)     AS customer_count,
               SUM(a.current_balance)            AS balance_total,
               SUM(a.credit_limit)               AS limit_total,
               AVG(rs.pd_score)                  AS avg_pd,
               SUM(rs.expected_loss)             AS el_total,
               SUM(CASE WHEN t.transaction_type='DEBIT'  THEN t.amount_usd ELSE 0 END) AS debit_vol,
               SUM(CASE WHEN t.transaction_type='CREDIT' THEN t.amount_usd ELSE 0 END) AS credit_vol,
               COUNT(DISTINCT CASE WHEN t.is_suspicious=1 THEN t.transaction_id END)    AS suspicious_count
        FROM   dbo.customer_master  c
        JOIN   dbo.accounts         a  ON a.customer_id   = c.customer_id
        LEFT JOIN dbo.risk_scores   rs ON rs.customer_id  = c.customer_id
                                      AND rs.as_of_date   = '{as_of_date}'
        LEFT JOIN dbo.transactions  t  ON t.account_id    = a.account_id
                                      AND t.transaction_date = '{as_of_date}'
        WHERE  c.is_active = 1
        GROUP BY c.customer_segment, a.account_type
        ORDER BY c.customer_segment
    """, engine=dw_engine)


def build_risk_heat_map(as_of_date: date) -> pd.DataFrame:
    """
    Build risk heat-map: PD x LGD matrix count by segment and band.
    Reads: risk_scores.pd_score, lgd_estimate, scorecard_band, customer_segment
    Writes: mis_risk_heatmap.segment, band, customer_count, total_el
    """
    return execute_query(f"""
        SELECT c.customer_segment,
               rs.scorecard_band,
               COUNT(*)               AS customer_count,
               SUM(rs.expected_loss)  AS total_el,
               AVG(rs.pd_score)       AS avg_pd,
               AVG(rs.lgd_estimate)   AS avg_lgd,
               SUM(rs.ead)            AS total_ead
        FROM   dbo.risk_scores   rs
        JOIN   dbo.customer_master  c ON c.customer_id = rs.customer_id
        WHERE  rs.as_of_date = '{as_of_date}'
        GROUP BY c.customer_segment, rs.scorecard_band
        ORDER BY c.customer_segment, rs.scorecard_band
    """, engine=dw_engine)


def build_liquidity_dashboard(as_of_date: date) -> pd.DataFrame:
    """
    Pull LCR/NSFR and intraday liquidity metrics for treasury dashboard.
    Reads: capital_requirements.lcr, nsfr, tier1_capital,
           var_results.var_99, expected_shortfall
    """
    return execute_query(f"""
        SELECT cr.portfolio_segment,
               cr.lcr,
               cr.nsfr,
               cr.tier1_capital,
               cr.leverage_ratio,
               vr.var_99,
               vr.expected_shortfall
        FROM   dbo.capital_requirements  cr
        LEFT JOIN dbo.var_results         vr ON vr.as_of_date = cr.as_of_date
        WHERE  cr.as_of_date      = '{as_of_date}'
          AND  cr.stress_scenario = 'BASELINE'
    """, engine=dw_engine)


def build_breach_summary_dashboard(as_of_date: date) -> pd.DataFrame:
    """
    Summarise open limit breaches for senior management dashboard.
    Reads: limit_breaches.limit_type, severity, status, breach_amount,
           breach_date, customer_id
    """
    return execute_query(f"""
        SELECT lb.limit_type,
               lb.severity,
               lb.status,
               COUNT(*)               AS breach_count,
               SUM(lb.breach_amount)  AS total_excess,
               MAX(lb.actual_value)   AS peak_breach,
               MIN(lb.breach_date)    AS oldest_open_breach
        FROM   dbo.limit_breaches lb
        WHERE  lb.breach_date <= '{as_of_date}'
          AND  lb.status IN ('OPEN', 'ACKNOWLEDGED')
        GROUP BY lb.limit_type, lb.severity, lb.status
        ORDER BY lb.severity, lb.limit_type
    """, engine=dw_engine)


@regulatory_report(report_type="MIS_DAILY")
def publish_daily_mis_pack(as_of_date: date) -> dict:
    """
    Assemble and publish the daily MIS pack to the reporting database.
    Reads: (all above queries) portfolio data, risk heatmap, liquidity,
           breach summary, capital_requirements, var_results
    Writes: mis_daily_snapshot.*, mis_risk_heatmap.*, mis_liquidity.*,
            mis_breach_summary.*, mis_publication_log.*
    """
    portfolio = build_daily_portfolio_snapshot(as_of_date)
    heatmap   = build_risk_heat_map(as_of_date)
    liquidity = build_liquidity_dashboard(as_of_date)
    breaches  = build_breach_summary_dashboard(as_of_date)

    truncate_and_reload(portfolio, "mis_daily_snapshot",   engine=reporting_engine)
    truncate_and_reload(heatmap,   "mis_risk_heatmap",     engine=reporting_engine)
    truncate_and_reload(liquidity, "mis_liquidity",        engine=reporting_engine)
    truncate_and_reload(breaches,  "mis_breach_summary",   engine=reporting_engine)

    conn = get_raw_dw_connection()
    cursor = conn.cursor()
    cursor.execute(
        "INSERT INTO dbo.mis_publication_log "
        "(as_of_date, published_at, portfolio_rows, heatmap_rows, breach_rows) "
        "VALUES (?, GETUTCDATE(), ?, ?, ?)",
        (str(as_of_date), len(portfolio), len(heatmap), len(breaches)),
    )
    conn.commit()
    conn.close()

    logger.info(f"MIS Daily Pack published for {as_of_date}: "
                f"portfolio={len(portfolio)}, heatmap={len(heatmap)}, "
                f"liquidity={len(liquidity)}, breaches={len(breaches)}")
    return {
        "as_of_date":      str(as_of_date),
        "portfolio_rows":  len(portfolio),
        "heatmap_rows":    len(heatmap),
        "liquidity_rows":  len(liquidity),
        "breach_rows":     len(breaches),
        "status":          "PUBLISHED",
    }
