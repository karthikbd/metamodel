"""
Common database utility functions shared across ETL, risk, and reporting.
"""
import logging
import pandas as pd
from typing import Optional, List, Dict, Any
from config.database import dw_engine, risk_engine, get_raw_dw_connection
from utils.decorators import retry

logger = logging.getLogger(__name__)


@retry(max_attempts=3, delay=2.0)
def execute_query(sql: str, engine=None, params: dict = None) -> pd.DataFrame:
    """Execute a SELECT query and return results as a DataFrame."""
    eng = engine or dw_engine
    return pd.read_sql(sql, eng, params=params)


@retry(max_attempts=3, delay=2.0)
def bulk_insert(df: pd.DataFrame, table: str, schema: str = "dbo", engine=None):
    """Bulk insert a DataFrame into a SQL table using fast_executemany."""
    eng = engine or dw_engine
    df.to_sql(table, eng, schema=schema, if_exists="append", index=False, method="multi")
    logger.info(f"Bulk inserted {len(df)} rows into {schema}.{table}")


def upsert_risk_scores(records: List[Dict[str, Any]]):
    """Upsert risk score rows using MERGE statement."""
    conn = get_raw_dw_connection()
    cursor = conn.cursor()
    cursor.executemany(
        """
        MERGE risk_scores AS target
        USING (VALUES (?, ?, ?, ?, ?, ?)) AS source(
            customer_id, pd_score, lgd_estimate, ead, risk_weight, as_of_date
        )
        ON target.customer_id = source.customer_id
           AND target.as_of_date = source.as_of_date
        WHEN MATCHED THEN
            UPDATE SET pd_score      = source.pd_score,
                       lgd_estimate  = source.lgd_estimate,
                       ead           = source.ead,
                       risk_weight   = source.risk_weight
        WHEN NOT MATCHED THEN
            INSERT (customer_id, pd_score, lgd_estimate, ead, risk_weight, as_of_date)
            VALUES (source.customer_id, source.pd_score, source.lgd_estimate,
                    source.ead, source.risk_weight, source.as_of_date);
        """,
        [(r["customer_id"], r["pd_score"], r["lgd_estimate"],
          r["ead"], r["risk_weight"], r["as_of_date"]) for r in records],
    )
    conn.commit()
    conn.close()


def truncate_and_reload(table: str, df: pd.DataFrame, schema: str = "dbo", engine=None):
    """Truncate a staging table and reload from DataFrame."""
    eng = engine or dw_engine
    with eng.begin() as conn:
        conn.execute(f"TRUNCATE TABLE [{schema}].[{table}]")
    bulk_insert(df, table, schema, eng)
    logger.info(f"Truncated and reloaded {schema}.{table} with {len(df)} rows")


def get_date_range_data(table: str, start_date: str, end_date: str,
                        date_col: str = "business_date") -> pd.DataFrame:
    """Fetch rows for a business date range from any table."""
    sql = f"""
        SELECT *
        FROM dbo.{table}
        WHERE {date_col} BETWEEN '{start_date}' AND '{end_date}'
        ORDER BY {date_col}
    """
    return execute_query(sql)


def log_etl_run(pipeline_name: str, status: str, rows_processed: int,
                error_msg: str = None):
    """Record ETL run metadata to the pipeline_audit_log table."""
    conn = get_raw_dw_connection()
    cursor = conn.cursor()
    cursor.execute(
        """
        INSERT INTO pipeline_audit_log
            (pipeline_name, run_status, rows_processed, error_message, run_timestamp)
        VALUES (?, ?, ?, ?, GETUTCDATE())
        """,
        (pipeline_name, status, rows_processed, error_msg),
    )
    conn.commit()
    conn.close()


# ── Dynamic SQL helpers ───────────────────────────────────────────────────────
# NOTE: These functions build SQL dynamically and are flagged as DYNAMIC_SQL
# by the schema extractor because the query string contains {placeholder} tokens
# or %s-style substitution that prevent fully static analysis.


def fetch_partition_data(table_name: str, partition_col: str, partition_key: str) -> pd.DataFrame:
    """Load a specific partition of any staging table by a named column."""
    sql = "SELECT * FROM dbo.{table} WHERE {col} = %s ORDER BY business_date".format(
        table=table_name, col=partition_col
    )
    return execute_query(sql, params={"p": partition_key})


def build_summary_query(table_name: str, group_col: str, agg_col: str) -> pd.DataFrame:
    """Build an aggregation query dynamically — table and columns supplied at runtime."""
    sql = (
        "SELECT {group_col}, COUNT(*) AS row_count, SUM({agg_col}) AS total "
        "FROM dbo.{table} "
        "WHERE status = 'active' "
        "GROUP BY {group_col}"
    ).format(table=table_name, group_col=group_col, agg_col=agg_col)
    return execute_query(sql)


def fetch_column_sample(table_name: str, columns: list, limit: int = 1000) -> pd.DataFrame:
    """Fetch a random sample of specified columns from any source table."""
    col_list = ", ".join(columns)
    sql = "SELECT TOP {limit} {cols} FROM dbo.{table} ORDER BY NEWID()".format(
        limit=limit, cols=col_list, table=table_name
    )
    return pd.read_sql(sql, dw_engine)


def upsert_dynamic_target(records: list, target_table: str, key_col: str = "id"):
    """Upsert records into a dynamically specified target table."""
    conn = get_raw_dw_connection()
    cursor = conn.cursor()
    upsert_sql = (
        "MERGE dbo.{table} AS tgt "
        "USING (VALUES (%s, %s)) AS src({key}, payload) "
        "ON tgt.{key} = src.{key} "
        "WHEN MATCHED THEN UPDATE SET tgt.payload = src.payload "
        "WHEN NOT MATCHED THEN INSERT ({key}, payload) VALUES (src.{key}, src.payload)"
    ).format(table=target_table, key=key_col)
    cursor.executemany(upsert_sql, [(r[key_col], str(r)) for r in records])
    conn.commit()
    conn.close()
