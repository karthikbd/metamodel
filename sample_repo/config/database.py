"""
Database connection management for PNC Meta Model sample system.
Provides SQLAlchemy engines and raw pyodbc connections for each data domain.
"""
import os
import pyodbc
import sqlalchemy
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, declarative_base

# ---------------------------------------------------------------------------
# Connection strings (read from environment)
# ---------------------------------------------------------------------------
DW_CONN = os.getenv(
    "PNC_DW_CONN",
    "mssql+pyodbc://dw_user:password@pnc-dw-prod.database.windows.net:1433/PNC_DataWarehouse"
    "?driver=ODBC+Driver+18+for+SQL+Server",
)
RISK_DB_CONN = os.getenv(
    "PNC_RISK_DB_CONN",
    "mssql+pyodbc://risk_user:password@pnc-risk-prod.database.windows.net:1433/PNC_RiskDB"
    "?driver=ODBC+Driver+18+for+SQL+Server",
)
COMPLIANCE_DB_CONN = os.getenv(
    "PNC_COMPLIANCE_CONN",
    "postgresql+psycopg2://compliance_svc:password@pnc-compliance.postgres.database.azure.com"
    "/compliance_db",
)
REPORTING_DB_CONN = os.getenv(
    "PNC_REPORTING_CONN",
    "mssql+pyodbc://report_user:password@pnc-reporting.database.windows.net:1433/PNC_Reporting"
    "?driver=ODBC+Driver+18+for+SQL+Server",
)

# ---------------------------------------------------------------------------
# SQLAlchemy engines
# ---------------------------------------------------------------------------
dw_engine         = create_engine(DW_CONN,         pool_size=10, max_overflow=20, echo=False)
risk_engine       = create_engine(RISK_DB_CONN,     pool_size=5,  max_overflow=10, echo=False)
compliance_engine = create_engine(COMPLIANCE_DB_CONN, pool_size=5, max_overflow=5, echo=False)
reporting_engine  = create_engine(REPORTING_DB_CONN,  pool_size=5, max_overflow=5, echo=False)

# ---------------------------------------------------------------------------
# Session factories
# ---------------------------------------------------------------------------
DWSession         = sessionmaker(bind=dw_engine)
RiskSession       = sessionmaker(bind=risk_engine)
ComplianceSession = sessionmaker(bind=compliance_engine)
ReportingSession  = sessionmaker(bind=reporting_engine)

Base = declarative_base()


def get_dw_session():
    """Yield a DataWarehouse session with automatic rollback on error."""
    session = DWSession()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


def get_risk_session():
    session = RiskSession()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


def get_raw_dw_connection() -> pyodbc.Connection:
    """Return a raw pyodbc connection to the DataWarehouse (for bulk operations)."""
    conn_str = (
        "DRIVER={ODBC Driver 18 for SQL Server};"
        "SERVER=pnc-dw-prod.database.windows.net;"
        "DATABASE=PNC_DataWarehouse;"
        "UID=dw_user;PWD=password;"
    )
    return pyodbc.connect(conn_str, timeout=30)
