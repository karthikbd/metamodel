"""
Market Risk Engine — Historical and Stressed VaR, Expected Shortfall (ES/CVaR).
Implements FRTB IMA-style calculations for trading book positions.
Also reads market_data.spot_rate_old (deprecated) in one legacy function.
"""
import logging
import numpy as np
import pandas as pd
from datetime import date
from typing import Tuple

from config.database import dw_engine, risk_engine, get_raw_dw_connection
from utils.db_utils import execute_query
from utils.decorators import retry, deprecated_field

logger = logging.getLogger(__name__)


def load_trading_positions(as_of_date: date) -> pd.DataFrame:
    """
    Load current trading book positions across all desks.
    Reads: trading_positions.desk_id, desk_name, instrument_id,
           position_size, market_value, delta, gamma, vega
    """
    return execute_query(f"""
        SELECT tp.position_id,
               tp.desk_id,
               tp.desk_name,
               tp.instrument_id,
               tp.instrument_type,
               tp.position_size,
               tp.market_value,
               tp.delta,
               tp.gamma,
               tp.vega,
               tp.currency,
               md.spot_rate_mid,
               md.volatility_1d,
               md.volatility_10d,
               md.credit_spread_5y
        FROM   dbo.trading_positions  tp
        JOIN   dbo.market_data        md ON md.instrument_id = tp.instrument_id
                                        AND md.as_of_date    = '{as_of_date}'
        WHERE  tp.as_of_date  = '{as_of_date}'
          AND  tp.status      = 'ACTIVE'
    """, engine=dw_engine)


def load_market_data_history(instrument_ids: list, lookback_days: int = 250) -> pd.DataFrame:
    """
    Load historical price series for VaR scenario generation.
    Reads: market_data.as_of_date, instrument_id, spot_rate_mid,
           volatility_1d, yield_curve_2y, yield_curve_5y, yield_curve_10y
    """
    ids_str = ",".join(f"'{i}'" for i in instrument_ids)
    return execute_query(f"""
        SELECT as_of_date,
               instrument_id,
               spot_rate_mid,
               volatility_1d,
               volatility_10d,
               yield_curve_2y,
               yield_curve_5y,
               yield_curve_10y,
               credit_spread_5y
        FROM   dbo.market_data
        WHERE  instrument_id IN ({ids_str})
          AND  as_of_date >= DATEADD(day, -{lookback_days}, CAST(GETDATE() AS DATE))
        ORDER BY as_of_date ASC
    """, engine=dw_engine)


@deprecated_field("spot_rate_old", replacement="spot_rate_mid")
def load_legacy_fx_rates(as_of_date: date) -> pd.DataFrame:
    """
    DEPRECATED USAGE: reads market_data.spot_rate_old for legacy FX reconciliation.
    Compliance tab will flag this as deprecated-column-in-use.
    Reads: market_data.spot_rate_old  <-- DEPRECATED
    """
    return execute_query(f"""
        SELECT instrument_id,
               currency_pair,
               spot_rate_old,         -- DEPRECATED column
               spot_rate_mid,
               as_of_date
        FROM   dbo.market_data
        WHERE  as_of_date    = '{as_of_date}'
          AND  instrument_type = 'FX'
        ORDER BY currency_pair
    """, engine=dw_engine)


def compute_pnl_scenarios(positions_df: pd.DataFrame,
                           history_df: pd.DataFrame) -> pd.DataFrame:
    """
    Generate 250 daily P&L scenarios using delta-normal approximation.
    Reads:  trading_positions.delta, gamma, market_value
            market_data.spot_rate_mid, volatility_1d
    """
    dates = history_df["as_of_date"].unique()
    scenarios = []
    for i in range(1, len(dates)):
        prev_date = dates[i - 1]
        curr_date = dates[i]
        prev_prices = history_df[history_df["as_of_date"] == prev_date].set_index("instrument_id")["spot_rate_mid"]
        curr_prices = history_df[history_df["as_of_date"] == curr_date].set_index("instrument_id")["spot_rate_mid"]
        returns = (curr_prices - prev_prices) / prev_prices.replace(0, np.nan)

        desk_pnl = {}
        for _, pos in positions_df.iterrows():
            r = returns.get(pos["instrument_id"], 0.0)
            if pd.isna(r):
                r = 0.0
            pnl = pos["delta"] * pos["market_value"] * r
            desk = pos["desk_id"]
            desk_pnl[desk] = desk_pnl.get(desk, 0.0) + pnl

        scenarios.append({"scenario_date": curr_date, **desk_pnl})

    return pd.DataFrame(scenarios).fillna(0)


def run_historical_var(as_of_date: date, lookback_days: int = 250,
                       confidence: float = 0.99) -> pd.DataFrame:
    """
    Compute 1-day historical VaR at 99% confidence per trading desk.
    Reads:  trading_positions.desk_id, delta, gamma, market_value
            market_data.spot_rate_mid, volatility_1d
    Writes: var_results.var_99, var_95, desk_id, as_of_date
    """
    positions_df = load_trading_positions(as_of_date)
    if positions_df.empty:
        logger.warning("No positions found for VaR calculation")
        return pd.DataFrame()

    instrument_ids = positions_df["instrument_id"].unique().tolist()
    history_df     = load_market_data_history(instrument_ids, lookback_days)
    scenarios_df   = compute_pnl_scenarios(positions_df, history_df)

    desks = [c for c in scenarios_df.columns if c != "scenario_date"]
    var_rows = []
    for desk in desks:
        pnl = scenarios_df[desk].sort_values()
        var_99 = abs(np.percentile(pnl, 1))
        var_95 = abs(np.percentile(pnl, 5))
        var_rows.append({
            "desk_id":    desk,
            "desk_name":  positions_df[positions_df["desk_id"] == desk]["desk_name"].iloc[0]
                          if not positions_df[positions_df["desk_id"] == desk].empty else desk,
            "var_99":     round(var_99, 2),
            "var_95":     round(var_95, 2),
            "as_of_date": str(as_of_date),
            "model_version": "HIS_VAR_v3.1",
        })
    result = pd.DataFrame(var_rows)
    logger.info(f"Historical VaR computed for {len(result)} desks on {as_of_date}")
    return result


def run_stressed_var(as_of_date: date, stress_window: str = "2008-2009") -> pd.DataFrame:
    """
    Compute Stressed VaR using a 12-month stressed period (Basel 2.5).
    Reads:  market_data.spot_rate_mid, volatility_1d for stress window
            trading_positions.delta, market_value, desk_id
    Writes: var_results.stressed_var_99
    """
    stress_start, stress_end = {
        "2008-2009": ("2008-09-01", "2009-03-31"),
        "2020-covid": ("2020-02-01", "2020-04-30"),
        "2022-rates": ("2022-01-01", "2022-12-31"),
    }.get(stress_window, ("2008-09-01", "2009-03-31"))

    positions_df = load_trading_positions(as_of_date)
    if positions_df.empty:
        return pd.DataFrame()

    instrument_ids = positions_df["instrument_id"].unique().tolist()
    ids_str = ",".join(f"'{i}'" for i in instrument_ids)
    stress_history = execute_query(f"""
        SELECT as_of_date, instrument_id, spot_rate_mid, volatility_1d
        FROM   dbo.market_data
        WHERE  instrument_id IN ({ids_str})
          AND  as_of_date BETWEEN '{stress_start}' AND '{stress_end}'
        ORDER BY as_of_date
    """, engine=dw_engine)

    if stress_history.empty:
        logger.warning(f"No stress history for window {stress_window}")
        return pd.DataFrame()

    scenarios_df = compute_pnl_scenarios(positions_df, stress_history)
    desks        = [c for c in scenarios_df.columns if c != "scenario_date"]
    stressed_rows = []
    for desk in desks:
        pnl = scenarios_df[desk].sort_values()
        stressed_rows.append({
            "desk_id":        desk,
            "stressed_var_99": round(abs(np.percentile(pnl, 1)), 2),
            "stress_window":  stress_window,
            "as_of_date":     str(as_of_date),
        })
    return pd.DataFrame(stressed_rows)


def compute_expected_shortfall(var_df: pd.DataFrame,
                                confidence: float = 0.975) -> pd.DataFrame:
    """
    ES (CVaR) at 97.5% — average of losses beyond VaR threshold (FRTB IMA).
    Reads:  var_results.var_99 (proxy for tail distribution)
    Writes: var_results.expected_shortfall
    """
    if var_df.empty:
        return pd.DataFrame()
    es_df = var_df.copy()
    es_df["expected_shortfall"] = es_df["var_99"] * 1.28  # approximate ES/VaR multiplier
    logger.info(f"Expected Shortfall computed for {len(es_df)} desks")
    return es_df


def store_var_results(var_df: pd.DataFrame, stressed_df: pd.DataFrame,
                       as_of_date: date):
    """
    Write VaR, Stressed VaR, and ES results to var_results table.
    Writes: var_results.desk_id, desk_name, var_99, var_95,
            stressed_var_99, expected_shortfall, as_of_date, model_version
    """
    if var_df.empty:
        logger.warning("No VaR results to store")
        return

    merged = var_df.merge(
        stressed_df[["desk_id", "stressed_var_99"]] if not stressed_df.empty else pd.DataFrame(),
        on="desk_id", how="left",
    )
    merged["expected_shortfall"] = merged["var_99"] * 1.28
    conn = get_raw_dw_connection()
    cursor = conn.cursor()
    cursor.executemany(
        """
        INSERT INTO dbo.var_results
            (as_of_date, desk_id, desk_name,
             var_99, var_95, stressed_var_99, expected_shortfall, model_version)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [(str(as_of_date), r["desk_id"], r.get("desk_name", ""),
          r["var_99"], r["var_95"],
          r.get("stressed_var_99", r["var_99"] * 2.0),
          r.get("expected_shortfall", r["var_99"] * 1.28),
          r.get("model_version", "HIS_VAR_v3.1"))
         for _, r in merged.iterrows()],
    )
    conn.commit()
    conn.close()
    logger.info(f"Stored {len(merged)} VaR result rows for {as_of_date}")
