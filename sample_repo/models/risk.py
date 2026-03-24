"""
SQLAlchemy ORM models for risk domain: scores, limits, capital, market data.
"""
from datetime import date, datetime
from sqlalchemy import (
    Column, String, Integer, BigInteger, Date, DateTime,
    Numeric, Boolean, Text, ForeignKey, Index
)
from sqlalchemy.orm import relationship
from config.database import Base


class RiskScore(Base):
    """
    Daily credit risk scores per customer: PD, LGD, EAD, RiskWeight.
    Feeds Basel III capital calculations.
    """
    __tablename__ = "risk_scores"

    score_id         = Column(BigInteger, primary_key=True)
    customer_id      = Column(BigInteger, ForeignKey("customer_master.customer_id"))
    as_of_date       = Column(Date, nullable=False)
    pd_score         = Column(Numeric(8, 6))   # Probability of Default
    lgd_estimate     = Column(Numeric(8, 6))   # Loss Given Default
    ead              = Column(Numeric(18, 2))  # Exposure at Default
    risk_weight      = Column(Numeric(8, 6))
    expected_loss    = Column(Numeric(18, 2))
    unexpected_loss  = Column(Numeric(18, 2))
    model_version    = Column(String(20))
    scorecard_band   = Column(String(10))      # AAA | AA | A | BBB | BB | B | CCC | D
    override_flag    = Column(Boolean, default=False)
    override_reason  = Column(Text)
    created_at       = Column(DateTime, default=datetime.utcnow)

    customer = relationship("Customer", back_populates="risk_scores")

    __table_args__ = (
        Index("ix_risk_scores_customer_date", "customer_id", "as_of_date"),
        Index("ix_risk_scores_date",          "as_of_date"),
    )


class LimitBreach(Base):
    """
    Tracks risk limit breaches — feeding CCAR/DFAST stress scenarios.
    """
    __tablename__ = "limit_breaches"

    breach_id        = Column(BigInteger, primary_key=True)
    customer_id      = Column(BigInteger, ForeignKey("customer_master.customer_id"))
    account_id       = Column(BigInteger)
    limit_type       = Column(String(50))    # CREDIT | CONCENTRATION | VAR | CORRELATED
    limit_name       = Column(String(200))
    breach_amount    = Column(Numeric(18, 2))
    limit_value      = Column(Numeric(18, 2))
    actual_value     = Column(Numeric(18, 2))
    breach_date      = Column(Date, nullable=False)
    resolved_date    = Column(Date)
    status           = Column(String(20), default="OPEN")    # OPEN | ACKNOWLEDGED | RESOLVED
    severity         = Column(String(10))                    # CRITICAL | HIGH | MEDIUM | LOW
    assigned_to      = Column(String(100))
    resolution_notes = Column(Text)


class CapitalRequirement(Base):
    """
    Basel III / CRR II capital requirements per portfolio segment.
    """
    __tablename__ = "capital_requirements"

    req_id               = Column(BigInteger, primary_key=True)
    as_of_date           = Column(Date, nullable=False)
    portfolio_segment    = Column(String(100))
    rwa_credit           = Column(Numeric(18, 2))   # Risk-Weighted Assets — Credit
    rwa_market           = Column(Numeric(18, 2))   # Risk-Weighted Assets — Market
    rwa_operational      = Column(Numeric(18, 2))   # Risk-Weighted Assets — Operational
    rwa_total            = Column(Numeric(18, 2))
    tier1_capital        = Column(Numeric(18, 2))
    tier2_capital        = Column(Numeric(18, 2))
    tier1_ratio          = Column(Numeric(8, 6))
    total_capital_ratio  = Column(Numeric(8, 6))
    leverage_ratio       = Column(Numeric(8, 6))
    lcr                  = Column(Numeric(8, 6))    # Liquidity Coverage Ratio
    nsfr                 = Column(Numeric(8, 6))    # Net Stable Funding Ratio
    stress_scenario      = Column(String(50))       # BASELINE | ADVERSE | SEVERELY_ADVERSE
    model_run_id         = Column(String(50))


class MarketData(Base):
    """
    Daily market data: rates, spreads, FX, equity indices.
    spot_rate_old — DEPRECATED: use spot_rate_mid
    """
    __tablename__ = "market_data"

    market_id       = Column(BigInteger, primary_key=True)
    as_of_date      = Column(Date, nullable=False)
    instrument_id   = Column(String(50), nullable=False)
    instrument_type = Column(String(30))   # FX | RATE | EQUITY | CREDIT_SPREAD
    currency_pair   = Column(String(10))
    spot_rate_mid   = Column(Numeric(18, 8))
    spot_rate_bid   = Column(Numeric(18, 8))
    spot_rate_ask   = Column(Numeric(18, 8))
    spot_rate_old   = Column(Numeric(18, 8))          # DEPRECATED: use spot_rate_mid
    volatility_1d   = Column(Numeric(12, 8))
    volatility_10d  = Column(Numeric(12, 8))
    volatility_1y   = Column(Numeric(12, 8))
    yield_curve_2y  = Column(Numeric(10, 6))
    yield_curve_5y  = Column(Numeric(10, 6))
    yield_curve_10y = Column(Numeric(10, 6))
    credit_spread_5y = Column(Numeric(10, 6))
    source          = Column(String(50))

    __table_args__ = (
        Index("ix_mkt_date_instrument", "as_of_date", "instrument_id"),
    )


class VaRResult(Base):
    """
    Daily Value-at-Risk results per trading desk.
    """
    __tablename__ = "var_results"

    var_id            = Column(BigInteger, primary_key=True)
    as_of_date        = Column(Date, nullable=False)
    desk_id           = Column(String(50))
    desk_name         = Column(String(200))
    var_99            = Column(Numeric(18, 2))   # 99% confidence 1-day VaR
    var_95            = Column(Numeric(18, 2))   # 95% confidence 1-day VaR
    stressed_var_99   = Column(Numeric(18, 2))   # Stressed VaR (12-month window)
    expected_shortfall = Column(Numeric(18, 2))  # CVaR / ES
    incremental_var   = Column(Numeric(18, 2))
    component_var     = Column(Numeric(18, 2))
    backtesting_exceptions = Column(Integer, default=0)
    model_version     = Column(String(20))
    scenario_set      = Column(String(50))
